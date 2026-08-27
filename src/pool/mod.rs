//! Pool work-stealing của một lane: N thread worker, mỗi thread một ring, một hàng đợi chung.
//!
//! Đọc [`docs_internal/lanes.md`](../../docs_internal/lanes.md) để biết vì sao engine chỉ có một
//! pool cho toàn bộ việc CPU-bound thay vì mỗi hệ thống một pool. Ở đây chỉ nói cách pool chạy.
//!
//! # Một job đi đường nào
//!
//! ```text
//!   spawn từ trong một job         spawn từ ngoài pool
//!            │                              │
//!            ▼                              ▼
//!      ô LIFO của worker              lane queue (dùng chung)
//!            │  (đầy thì đẩy xuống)         │
//!            ▼                              │
//!      ring local của worker  ◀─────────────┘  nạp cả cụm khi worker ngó tới
//!            │  (đầy thì spill nửa cũ xuống lane queue)
//!            ▼
//!      kẻ trộm bốc lô từ đầu ring
//! ```
//!
//! Ô LIFO giữ đúng một job: job vừa spawn ra thường là job mà cache của chính thread này còn nóng
//! nhất, nên chạy nó ngay là rẻ nhất. Ring local giữ phần còn lại và cho người khác trộm. Lane
//! queue hứng mọi thứ tràn ra, và là chỗ duy nhất thread ngoài pool đẩy job vào được.
//!
//! # Vòng tìm việc của một worker
//!
//! ```text
//!  1. ô LIFO         job vừa spawn, nóng nhất
//!  2. ring local     việc của chính mình
//!  3. lane queue     việc từ ngoài, và việc bị xả ra
//!  4. trộm           đắt: phải CAS vào ring người khác
//!  5. lane queue     ngó lần cuối trước khi ngủ
//!  6. ngủ
//! ```
//!
//! Cứ [`LANE_QUEUE_TICK`] vòng thì bước 3 được kéo lên trước bước 1. Không có luật đó thì một
//! worker mà job của nó cứ đẻ job con sẽ tự nuôi mình mãi mãi, và job của thread ngoài nằm trong
//! lane queue có thể chờ rất lâu dù pool nhìn từ ngoài vẫn "đang chạy".

use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::apis::priority::Priority;
use crate::bump::Bump;
use crate::custom_type::Job;
use crate::lane_queue::LaneQueue;
use crate::per_worker::PerWorker;
use crate::ring_buffer_fifo::RingBufferFifo;
use crate::sync::cell::UnsafeCell;
use crate::sync::thread::{self, JoinHandle, ThreadId};
use crate::sync::{Arc, AtomicBool, Mutex, Ordering};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::{available_cores, ignore_poison};
use std::time::Duration;

pub mod counters;
pub mod sleep;

use counters::{Counters, PoolCounters};
use sleep::{Sleep, Wake};

/// Cứ bấy nhiêu vòng thì worker ngó lane queue trước cả ring của mình.
///
/// Số nguyên tố, và không phải để cho đẹp: một hằng chia hết cho số worker, hoặc chia hết cho nhịp
/// đẻ job của một thuật toán chia đôi, sẽ khiến nhiều worker cùng ngó lane queue đúng một lúc rồi
/// cùng giành một cái khoá. Số nguyên tố đủ lớn thì các worker rải đều ra.
pub const LANE_QUEUE_TICK: u32 = 61;

/// Một giấc ngủ ngắn của thread đang chờ ở [`ThreadPool::run_until`] mà pool thì hết việc.
///
/// Ngắn có chủ ý: nó là hạn chót cho trường hợp xấu nhất, tức là khi thứ đang được chờ hoàn thành
/// mà không gọi ai dậy. Đường bình thường thì [`Latch`](crate::latch::Latch) gọi dậy ngay.
const IDLE_NAP: Duration = Duration::from_micros(50);

/// "Thread này chưa từng thuộc pool nào". Pool thật đánh số từ 1.
const NO_POOL: u64 = 0;

fn next_pool_id() -> u64
{
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(NO_POOL + 1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Chỗ đứng của thread hiện tại: nó là người thứ mấy của pool nào, và nó có đang ở trong vòng chạy
/// job hay không.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Context
{
    pool:    u64,
    index:   usize,
    /// Đang ở trong vòng chạy job của pool đó.
    ///
    /// Phân biệt này quyết định job mới đi đâu. Ở trong vòng thì ô LIFO là chỗ tốt nhất, vì chính
    /// thread này sẽ quay lại lấy nó ngay sau job hiện tại. Ở ngoài vòng (host vừa gọi `spawn` rồi
    /// đi làm việc khác) mà nhét vào ô LIFO thì job nằm im: không ai trộm được ô LIFO, và chủ của
    /// nó thì đang không nhìn tới.
    in_loop: bool,
}

impl Context
{
    const NONE: Self = Self {
        pool:    NO_POOL,
        index:   usize::MAX,
        in_loop: false,
    };
}

thread_local! {
    static CONTEXT: Cell<Context> = const { Cell::new(Context::NONE) };

    /// Id của chính thread này, tính một lần.
    ///
    /// `thread::current()` phải clone một `Arc`, quá đắt cho một thứ nằm trên đường push. Cái id
    /// thì chỉ là mấy byte, và nó không đổi suốt đời thread.
    static SELF_ID: ThreadId = thread::current().id();
}

/// Cách dựng một pool. Xem [`ThreadPool::new`].
#[derive(Debug, Clone)]
pub struct Config
{
    /// Số thread worker sẽ spawn.
    ///
    /// Đây **không** phải tổng số thread chạy job: thread gọi vào pool cũng chạy job cùng chúng,
    /// nên tổng là `threads + 1`. Mặc định là `cores - 1` đúng vì lý do đó.
    ///
    /// `0` là hợp lệ và có nghĩa mọi job chạy ngay trên thread gọi. Xem [`ThreadPool::new`].
    pub threads:       usize,
    /// Số ô của **mỗi** ring local. Được làm tròn lên luỹ thừa hai.
    ///
    /// Ring đầy không phải lỗi: chủ xả nửa cũ xuống lane queue rồi push tiếp. Nên con số này đổi
    /// chác giữa bộ nhớ và số lần phải xả.
    pub ring_capacity: usize,
    /// Số byte arena nháp cho **mỗi** người tham gia. Xem [`ThreadPool::scratch`].
    pub scratch_bytes: usize,
    /// Bao nhiêu vòng quay và nhường CPU trước khi một worker chịu đi ngủ.
    ///
    /// Đánh thức một thread đang park tốn một cặp syscall cộng một lần chuyển ngữ cảnh. Pool nào
    /// ngủ quá nhanh thì trả cái giá đó ở mỗi đợt việc mới. Ngược lại, giữ nó cao thì một máy đang
    /// rảnh vẫn có core quay tại chỗ.
    pub spin_rounds:   u32,
    /// Tiền tố tên thread, để nhìn ra chúng trong debugger hoặc profiler.
    pub thread_name:   String,
    /// Standing mà pool xin OS cho thread của mình. Xem [`Priority`].
    pub priority:      Priority,
}

impl Default for Config
{
    /// Một worker cho mỗi core, trừ một core để lại cho thread gọi.
    ///
    /// `available_parallelism` đếm core **logic**, nên trên x86 có siêu phân luồng thì đây là đếm
    /// dư. Với việc nặng bộ nhớ, thread thứ hai trên cùng một core gần như không mua được gì. Chỗ
    /// này đáng chỉnh lại theo từng nền tảng khi đã có số đo, chứ đoán ở đây thì tệ hơn.
    fn default() -> Self
    {
        Self {
            threads:       available_cores().saturating_sub(1),
            ring_capacity: 256,
            scratch_bytes: 1 << 20,
            spin_rounds:   40,
            thread_name:   "xynok-worker".to_string(),
            priority:      Priority::Frame,
        }
    }
}

impl Config
{
    /// [`Self::default`], với `XYNOK_LANE_THREADS` đè lên số thread.
    ///
    /// Biến này là **tổng** số thread chạy job, tính cả thread gọi, nên `XYNOK_LANE_THREADS=1`
    /// nghĩa là không spawn worker nào và mọi job chạy inline. Đó là thứ đầu tiên nên thử khi một
    /// con bug xuất hiện trong game: nó trả lời câu "cái này có phải do đa luồng không" trong một
    /// lần chạy, không phải sửa dòng code nào.
    pub fn from_env() -> Self
    {
        let mut config = Self::default();

        if let Ok(raw) = std::env::var("XYNOK_LANE_THREADS")
            && let Ok(total) = raw.trim().parse::<usize>()
        {
            config.threads = total.saturating_sub(1);
        }

        config
    }

    /// Pool chạy inline: không thread nào được spawn, job chạy ngay trên thread gọi.
    pub fn inline() -> Self
    {
        Self { threads: 0, ..Self::default() }
    }
}

/// Hai chỗ chứa job của một người tham gia pool.
struct Local
{
    /// Job vừa được spawn ra, giữ nguyên ở đây thay vì đẩy xuống ring.
    ///
    /// Chỉ chủ của ô này chạm vào: thread đang mang đúng `index` đó, và chỉ khi nó đang ở trong
    /// vòng chạy job. Không ai trộm được ô LIFO, nên nó phải được vét sạch trước khi chủ rời vòng,
    /// nếu không job nằm đó không ai chạy.
    lifo: UnsafeCell<Option<Job>>,
    /// Việc của người này, ai cũng trộm được.
    ring: RingBufferFifo<Job>,
}

/// An toàn vì bất biến ở [`Local::lifo`]: đúng một thread chạm vào ô đó, và nó chạm khi đang giữ
/// `index` tương ứng. `ring` thì tự nó đã `Sync`.
unsafe impl Sync for Local {}

impl Local
{
    /// Lấy job trong ô LIFO ra, nếu có.
    ///
    /// # Safety
    ///
    /// Chỉ được gọi từ thread đang mang `index` của chính `Local` này.
    #[inline]
    unsafe fn take_lifo(&self) -> Option<Job>
    {
        self.lifo.with_mut(|slot| unsafe { (*slot).take() })
    }

    /// Đặt job vào ô LIFO, trả lại job cũ đang nằm đó để người gọi đẩy xuống ring.
    ///
    /// # Safety
    ///
    /// Như [`Self::take_lifo`].
    #[inline]
    unsafe fn swap_lifo(&self, job: Job) -> Option<Job>
    {
        self.lifo.with_mut(|slot| unsafe { (*slot).replace(job) })
    }
}

/// Mọi thứ worker cần, và không có gì giữ cho pool sống.
pub(crate) struct Shared
{
    /// Chỗ job từ ngoài pool rơi vào, và chỗ hứng phần bị xả ra khỏi ring.
    lane_queue:  LaneQueue<Job>,
    /// Một ô cho mỗi người tham gia: `workers` worker, cộng host ở chỉ số cuối.
    locals:      Box<[CachePadded<Local>]>,
    sleep:       Sleep,
    /// Phân biệt pool này với mọi pool khác trong tiến trình. Xem [`worker_index_in`].
    id:          u64,
    /// Dựng một lần, trên đường đi xuống. Worker vét nốt hàng đợi rồi thoát.
    shutdown:    AtomicBool,
    workers:     usize,
    /// Thread đã dựng pool, và là người ngoài duy nhất có một chỗ trong `locals`.
    host:        thread::Thread,
    /// Id của [`Self::host`], để so mà không phải clone một `Arc`.
    host_id:     ThreadId,
    spin_rounds: u32,
    priority:    Priority,
    /// Một arena nháp cho mỗi người tham gia. Xem [`ThreadPool::scratch`].
    scratch:     PerWorker<Bump>,
    counters:    PoolCounters,
}

/// Nửa của pool mà destructor của nó dừng các thread.
///
/// Tồn tại chỉ để `Drop` nổ đúng lúc. Worker giữ `Arc<Shared>`, nên nếu join handle nằm trong
/// `Shared` thì worker tự giữ sống cái cờ shutdown của chính nó và pool không bao giờ dừng được.
struct Owner
{
    shared:  Arc<Shared>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

/// Tay cầm tới một nhóm worker và hàng đợi chúng ăn.
///
/// Clone rẻ và trỏ tới **cùng** một pool. Thread bị dừng và join khi tay cầm cuối cùng bị thả, hoặc
/// sớm hơn nếu ai đó gọi [`Self::shutdown`].
#[derive(Clone)]
pub struct ThreadPool
{
    pub(crate) shared: Arc<Shared>,
    /// Giữ join handle. Cố ý **không** để worker giữ, xem [`Owner`].
    _owner:            Arc<Owner>,
}

impl ThreadPool
{
    /// Spawn worker và trả về tay cầm tới chúng.
    ///
    /// Khởi động là việc tường minh, có chủ ý. Dựng pool lười lúc dùng lần đầu nghĩa là số thread,
    /// tên thread và cỡ ring bị quyết bởi call site nào chạy trước, mà đó đúng là thứ engine muốn
    /// nắm từ `main`.
    ///
    /// # `threads: 0`
    ///
    /// Không thread nào được spawn và mọi job chạy ngay trên thread gọi [`Self::spawn`]. Đây không
    /// phải chế độ thoái hoá cho vui: nó là cách trả lời "bug này có phải do đa luồng không" mà
    /// không phải sửa một dòng code nào.
    ///
    /// # Panics
    ///
    /// Nếu không spawn nổi một thread worker.
    pub fn new(config: Config) -> Self
    {
        let workers = config.threads;
        let id = next_pool_id();
        // Host là một người tham gia, không phải khán giả, nên nó có ô riêng ngay sau worker cuối.
        let participants = workers + 1;
        let ring_capacity = config.ring_capacity.max(2).min(u32::MAX as usize) as u32;

        let shared = Arc::new(Shared {
            lane_queue:  LaneQueue::new(),
            locals:      (0..participants)
                .map(|_| {
                    CachePadded::new(Local {
                        lifo: UnsafeCell::new(None),
                        ring: RingBufferFifo::new(ring_capacity),
                    })
                })
                .collect(),
            sleep:       Sleep::new(workers),
            id:          id,
            shutdown:    AtomicBool::new(false),
            workers:     workers,
            host:        thread::current(),
            host_id:     thread::current().id(),
            spin_rounds: config.spin_rounds.max(1),
            priority:    config.priority,
            scratch:     PerWorker::with_len(participants, |_| Bump::with_capacity(config.scratch_bytes)),
            counters:    PoolCounters::new(participants),
        });

        CONTEXT.set(Context {
            pool:    id,
            index:   workers,
            in_loop: false,
        });

        let mut handles = Vec::with_capacity(workers);
        for index in 0..workers
        {
            let shared = Arc::clone(&shared);
            let name = format!("{}-{index}", config.thread_name);
            let handle = thread::spawn_named(name, move || worker_loop(shared, index)).expect("không spawn được worker cho pool");
            handles.push(handle);
        }

        Self {
            shared: Arc::clone(&shared),
            _owner: Arc::new(Owner {
                shared:  shared,
                handles: Mutex::new(handles),
            }),
        }
    }

    /// Số thread worker đã spawn. Số người tham gia nhiều hơn thế một, xem [`Config::threads`].
    #[inline]
    pub fn worker_threads(&self) -> usize
    {
        self.shared.workers
    }

    /// Số chỉ số worker khác nhau, và cũng là độ dài một mảng "mỗi worker một ô".
    #[inline]
    pub fn worker_count(&self) -> usize
    {
        self.shared.workers + 1
    }

    /// Chỉ số của thread hiện tại trong pool này, trong `0..worker_count()`.
    ///
    /// # Panics
    ///
    /// Nếu gọi từ thread không phải worker của pool này và cũng không phải host của nó. Đưa cho
    /// một thread lạ chỉ số của người khác thì hai thread cùng ghi vào một ô, mà đó đúng là thứ
    /// chỉ số này sinh ra để loại bỏ.
    #[inline]
    pub fn worker_index(&self) -> usize
    {
        worker_index_in(&self.shared)
    }

    /// Mượn arena nháp của thread hiện tại.
    ///
    /// Đây là chỗ để chứa những buffer sống đúng một frame: danh sách entity nhìn thấy được, đám
    /// draw call chờ sắp xếp, kết quả tạm của một pass. Cấp phát ở đây là một phép cộng, không khoá,
    /// không tranh chấp, vì không thread nào khác với tới arena của thread này.
    ///
    /// Mọi thứ trong arena biến mất ở [`Self::end_frame`], nên đừng giữ gì qua ranh giới frame.
    ///
    /// ```
    /// use xynok_concurrency::pool::{Config, ThreadPool};
    ///
    /// let pool = ThreadPool::new(Config {
    ///     threads: 2,
    ///     ..Default::default()
    /// });
    ///
    /// pool.scratch(|arena| {
    ///     let visible = arena.alloc_slice(128, 0u32).expect("arena hết chỗ");
    ///     visible[0] = 7;
    ///     assert_eq!(visible.len(), 128);
    /// });
    /// ```
    ///
    /// # Panics
    ///
    /// Nếu thread hiện tại không thuộc pool này, hoặc nếu nó đã đang ở trong `scratch` rồi. Cái sau
    /// với tới được vì thread đang chờ ở điểm join sẽ chạy job khác: một job chạy bên dưới không
    /// được mượn lại chính cái arena đang bị mượn. Xem [`PerWorker::with`].
    pub fn scratch<R>(&self, f: impl FnOnce(&Bump) -> R) -> R
    {
        self.shared.scratch.with(self.worker_index(), |arena| f(arena))
    }

    /// Dọn sạch mọi arena nháp, một nhát cho cả pool.
    ///
    /// Gọi ở ranh giới frame, và chỉ khi pool đang rảnh: mọi job đã xong, không ai còn cầm tham
    /// chiếu nào do [`Self::scratch`] phát ra.
    ///
    /// # Panics
    ///
    /// Nếu còn arena nào đang được mượn. Đó là dấu hiệu ranh giới frame được tuyên bố trong lúc job
    /// vẫn đang chạy, và reset lúc ấy sẽ kéo đổ vùng nhớ mà job đang dùng.
    pub fn end_frame(&self)
    {
        // Safety: mọi ô đang mượn đều bị `for_each_unchecked` phát hiện và biến thành panic ngay
        // bên dưới, nên phần còn lại là các arena không ai chạm vào.
        unsafe {
            self.shared.scratch.for_each_unchecked(|arena| arena.reset());
        }

        if let Some(sink) = crate::profile::sink()
        {
            sink.frame_end();
        }
    }

    /// Số job đang nằm chờ trong lane queue. Ảnh chụp, đừng rẽ nhánh theo nó.
    #[inline]
    pub fn queued(&self) -> usize
    {
        self.shared.lane_queue.len()
    }

    /// Bộ đếm của cả pool. Xem [`Counters`] để biết từng con số nói lên điều gì.
    pub fn counters(&self) -> Counters
    {
        let mut total = self.shared.counters.total();
        total.queued = self.shared.lane_queue.len();
        total
    }

    /// Bộ đếm của riêng một người tham gia.
    ///
    /// Chênh lệch giữa các worker mới là thứ đáng nhìn: một worker chạy gấp mấy lần những worker
    /// khác nghĩa là việc đang dồn về một chỗ, và lúc đó con số tổng của cả pool trông vẫn rất đẹp.
    ///
    /// # Panics
    ///
    /// Nếu `index` không phải một chỗ trong pool này.
    pub fn counters_of(&self, index: usize) -> Counters
    {
        assert!(
            index < self.worker_count(),
            "chỉ số {index} nằm ngoài pool có {} người tham gia",
            self.worker_count()
        );
        self.shared.counters.snapshot_of(index)
    }

    /// Giao một job cho pool.
    ///
    /// Job đi đâu là tuỳ chỗ đứng của thread gọi, xem tài liệu của module. Với `threads: 0` thì job
    /// chạy ngay tại đây, trước khi hàm này trả về.
    #[inline]
    pub fn spawn<F>(&self, f: F)
    where F: FnOnce() + Send + 'static
    {
        self.inject(Job::new(f));
    }

    /// Như [`Self::spawn`] nhưng nhận sẵn một [`Job`].
    pub fn inject(&self, job: Job)
    {
        self.shared.inject(job);
    }

    /// Gọi một worker đang ngủ dậy, nếu có ai ngủ và không ai đang đi lùng việc.
    ///
    /// Đường bình thường thì [`Self::spawn`] đã tự làm việc này. Cái này dành cho lúc thread hiện
    /// tại sắp làm một việc dài và muốn pool bù lại chỗ nó để trống, xem
    /// [`block_in_place`](crate::lanes::block_in_place).
    #[inline]
    pub fn wake_one(&self)
    {
        self.shared.sleep.notify();
    }

    /// Phần dùng chung của pool, tức là mọi thứ trừ đám join handle.
    ///
    /// Thứ mà nó **không** giữ mới là điểm quan trọng: nó không giữ `Owner`, nên cầm một
    /// `Arc<Shared>` thì không giữ cho pool sống. Đó là cách [`Scope`](crate::scope::Scope) và
    /// [`JobGraph`](crate::job_graph) tham chiếu về pool mà không rơi vào cảnh một worker thả tay
    /// cầm cuối cùng rồi đi join chính nó.
    #[inline]
    pub(crate) fn shared(&self) -> &Arc<Shared>
    {
        &self.shared
    }

    /// Chạy job của pool cho tới khi `done` trả `true`.
    ///
    /// Đây là **work-while-waiting**: thread gọi không nằm chờ suông mà lấy job về chạy như một
    /// worker. Không có nó thì mọi điểm join lồng nhau hoặc là deadlock (thread đang giữ chỗ trong
    /// pool lại đi ngủ chờ chính pool), hoặc là bỏ phí một core suốt thời gian chờ.
    ///
    /// `done` được gọi rất nhiều lần, nên nó phải rẻ: một `load` atomic là vừa.
    ///
    /// Thread không thuộc pool cũng gọi được, nó chỉ chờ chứ không chạy giúp: nó không sở hữu ring
    /// nào nên không có chỗ nạp job vào.
    pub fn run_until(&self, done: impl Fn() -> bool)
    {
        self.shared.run_until(done);
    }

    /// Dừng worker và chờ chúng xong.
    ///
    /// Có thứ tự, vì bỏ bước nào cũng là treo:
    ///
    /// 1. Dựng cờ shutdown, để không worker nào bắt đầu vòng mới.
    /// 2. Gọi mọi worker đang ngủ dậy. Thread đang park không tự biết là đã tới lúc chết.
    /// 3. Để worker chạy nốt phần còn xếp hàng, thay vì vứt đi. Vứt đi thì mọi `scope` còn đang đếm
    ///    những job đó sẽ đếm mãi không về 0, tức là một tiến trình không bao giờ thoát.
    /// 4. Join từng thread.
    ///
    /// Gọi nhiều lần cũng không sao, và nó tự chạy khi tay cầm cuối cùng bị thả.
    ///
    /// # Gọi từ trong một job thì sao
    ///
    /// Bước 4 join mọi worker, mà worker đang chạy chính cái job gọi `shutdown` thì hoá ra nó đợi
    /// chính mình, và `join` chính mình là hành vi không xác định chứ không phải một lần treo tử tế.
    /// Chuyện này với tới được mà không ai cố ý: chỉ cần một job giữ một `ThreadPool` clone rồi
    /// tình cờ là kẻ thả cái tay cầm cuối cùng.
    ///
    /// Nên trường hợp đó được xử lý thay vì cấm suông: nếu thread đang gọi là worker của chính pool
    /// này thì bước 4 **bỏ join**, chỉ thả các handle ra và để thread tự kết thúc khi thấy cờ
    /// shutdown. Đổi lại, `shutdown` trả về trước khi worker thật sự dừng hẳn.
    ///
    /// Đường tử tế vẫn là tắt pool từ thread đã dựng nó, ở đó bạn được đảm bảo là mọi worker đã
    /// dừng khi hàm này trả về.
    pub fn shutdown(&self)
    {
        self._owner.stop();
    }
}

impl std::fmt::Debug for ThreadPool
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("ThreadPool")
            .field("workers", &self.shared.workers)
            .field("queued", &self.shared.lane_queue.len())
            .field("sleep", &self.shared.sleep)
            .finish()
    }
}

impl Owner
{
    fn stop(&self)
    {
        if self.shared.shutdown.swap(true, Ordering::SeqCst)
        {
            return;
        }

        self.shared.sleep.notify_all();

        let mut handles = ignore_poison(self.handles.lock());
        match self.shared.on_own_worker()
        {
            // Đang đứng trên chính một worker của pool này: join sẽ là join chính mình. Thả handle
            // ra, thread thấy cờ shutdown thì tự thoát. Xem [`ThreadPool::shutdown`].
            true => handles.clear(),
            false =>
            {
                for handle in handles.drain(..)
                {
                    // Worker nào panic thì panic hook đã in ra rồi, và ở đây không có ai để trao
                    // payload.
                    let _ = handle.join();
                }
            }
        }
        drop(handles);

        // Worker vét được phần lớn trước khi thoát, nhưng thứ được đẩy vào sau lần ngó cuối của
        // worker cuối cùng thì vẫn nằm đó, không còn thread nào với tới. Chạy nốt ở đây thay vì thả
        // trôi: một `scope` đang đếm chúng sẽ không bao giờ về 0 nếu chúng biến mất.
        self.shared.drain_all();
    }
}

impl Drop for Owner
{
    fn drop(&mut self)
    {
        self.stop();
    }
}

impl Shared
{
    /// Số thread worker đã spawn.
    #[inline]
    pub(crate) fn worker_threads(&self) -> usize
    {
        self.workers
    }

    /// Số chỗ đứng khác nhau, tức là số worker cộng host.
    #[inline]
    pub(crate) fn worker_count(&self) -> usize
    {
        self.workers + 1
    }

    /// Thread hiện tại có phải một worker của chính pool này không.
    ///
    /// Chỉ worker, không tính host: host thì join được cả pool mà không join chính nó.
    #[inline]
    pub(crate) fn on_own_worker(&self) -> bool
    {
        let context = CONTEXT.get();
        context.pool == self.id && context.index < self.workers
    }

    /// Chỉ số của thread hiện tại nếu nó là người tham gia pool này.
    ///
    /// Cố ý **không** phải [`worker_index_in`] đầy đủ: hàm đó có bước so handle thread, mà so handle
    /// thì phải clone một `Arc`, quá đắt cho một thứ nằm trên đường push. Host bị pool dựng sau ghi
    /// đè chỗ đứng thì ở đây đọc ra "người lạ", và nó chỉ mất cái ring của mình chứ không sai.
    #[inline]
    fn context(&self) -> Option<Context>
    {
        let context = CONTEXT.get();
        if context.pool == self.id
        {
            return Some(context);
        }

        // Không khớp, mà vẫn có thể là host của chính pool này: ô `CONTEXT` chỉ giữ được **một** chỗ
        // đứng, nên một thread dựng hai pool sẽ bị pool sau ghi đè chỗ của pool trước. Bỏ qua
        // trường hợp đó thì host mất cái ring của mình và mọi job nó spawn phải đi vòng qua lane
        // queue, tức là qua một cái khoá, cho từng job một. Bộ đếm `lane_pops` trong một ví dụ thật
        // là chỗ chuyện này lộ ra.
        let is_host = SELF_ID.with(|id| *id == self.host_id);
        is_host.then_some(Context {
            pool:    self.id,
            index:   self.workers,
            in_loop: false,
        })
    }

    #[inline]
    fn participants(&self) -> usize
    {
        self.locals.len()
    }

    /// Giao một job. Xem [`ThreadPool::inject`].
    pub(crate) fn inject(&self, job: Job)
    {
        // Pool không có worker nào: không có ai để giao, nên chạy luôn. Điều này giữ cho
        // `threads: 0` không phải là một nhánh riêng ở mọi call site phía trên.
        if self.workers == 0 && !matches!(self.context(), Some(context) if context.in_loop)
        {
            run_job(job);
            return;
        }

        match self.context()
        {
            Some(context) if context.in_loop =>
            {
                // Safety: `in_loop` chỉ được bật bởi chính thread này, trong vòng chạy job của pool
                // này, với đúng `context.index` đó.
                let displaced = unsafe { self.locals[context.index].swap_lifo(job) };
                if let Some(older) = displaced
                {
                    self.push_local(context.index, older);
                }
            }
            Some(context) => self.push_local(context.index, job),
            None => self.lane_queue.push(job),
        }

        self.sleep.notify();
    }

    /// Đẩy vào ring của người tham gia `index`, xả nửa cũ xuống lane queue nếu ring đã đầy.
    ///
    /// Chỉ gọi từ chính chủ của `index`.
    fn push_local(&self, index: usize, job: Job)
    {
        // Safety: chỉ chủ của ring mới tạo `Producer`, và chủ ở đây là thread đang gọi.
        let mut producer = unsafe { self.locals[index].ring.producer() };

        let Err(job) = producer.push(job)
        else
        {
            return;
        };

        // Ring đầy. Xả nửa cũ xuống lane queue rồi push lại: biên cứng của ring thành ngưỡng xả,
        // và phần bị xả là phần nguội nhất, ai chạy cũng như nhau.
        let mut spilled = Vec::new();
        producer.spill_half(&mut spilled);
        self.lane_queue.push_batch(spilled);
        self.counters.of(index).spill();

        if let Err(job) = producer.push(job)
        {
            // Không xả được ô nào (kẻ trộm đang giữ cả vùng): job vẫn phải có chỗ, và lane queue thì
            // không bao giờ từ chối.
            self.lane_queue.push(job);
        }
    }

    /// Bước 1 tới 3: ô LIFO, ring của mình, rồi lane queue.
    ///
    /// `tick` là số vòng worker đã chạy. Cứ [`LANE_QUEUE_TICK`] vòng thì lane queue được ngó trước
    /// cả ô LIFO, xem tài liệu module.
    fn next_local_job(&self, index: usize, tick: u32) -> Option<Job>
    {
        // Safety: chỉ chủ tạo `Producer`.
        let mut producer = unsafe { self.locals[index].ring.producer() };

        if tick.is_multiple_of(LANE_QUEUE_TICK)
            && let Some(job) = self.lane_queue.steal_batch_and_pop(&mut producer, self.participants())
        {
            self.counters.of(index).lane_pop();
            return Some(job);
        }

        // Safety: chỉ chủ chạm ô LIFO.
        if let Some(job) = unsafe { self.locals[index].take_lifo() }
        {
            return Some(job);
        }

        if let Some(job) = producer.pop()
        {
            return Some(job);
        }

        let job = self.lane_queue.steal_batch_and_pop(&mut producer, self.participants());
        if job.is_some()
        {
            self.counters.of(index).lane_pop();
        }
        job
    }

    /// Bước 4 và 5: trộm của người khác, rồi ngó lane queue lần cuối.
    ///
    /// Bắt đầu từ `index + 1` chứ không từ 0, để những kẻ đói không xếp hàng cùng nhắm vào người
    /// thứ nhất.
    fn search_job(&self, index: usize) -> Option<Job>
    {
        let participants = self.participants();
        // Safety: chỉ chủ tạo `Producer`.
        let mut producer = unsafe { self.locals[index].ring.producer() };

        let counters = self.counters.of(index);

        for offset in 1..participants
        {
            let victim = (index + offset) % participants;
            let consumer = self.locals[victim].ring.consumer();

            if consumer.steal_into(&mut producer) > 0
                && let Some(job) = producer.pop()
            {
                counters.steal_hit();
                return Some(job);
            }
            counters.steal_miss();
        }

        let job = self.lane_queue.steal_batch_and_pop(&mut producer, participants);
        if job.is_some()
        {
            counters.lane_pop();
        }
        job
    }

    /// Còn việc ở đâu đó trong lane không. Dùng cho lần ngó lại trước khi ngủ, nên nó chỉ được
    /// nhìn, không được lấy.
    fn has_work(&self) -> bool
    {
        if !self.lane_queue.is_empty()
        {
            return true;
        }
        self.locals.iter().any(|local| !local.ring.is_empty())
    }

    /// Lấy một job bất kỳ và chạy, cho thread đang chờ chứ không phải worker. Trả về có tìm thấy
    /// gì không.
    fn run_one(&self, index: usize, tick: u32) -> bool
    {
        let job = self.next_local_job(index, tick).or_else(|| self.search_job(index));

        match job
        {
            Some(job) =>
            {
                run_in_loop(self, index, job);
                true
            }
            None => false,
        }
    }

    /// Xem [`ThreadPool::run_until`].
    pub(crate) fn run_until(&self, done: impl Fn() -> bool)
    {
        let Some(context) = self.context()
        else
        {
            // Người ngoài: không có ring để nạp job vào, nên chỉ còn nước chờ. Quay tại chỗ một
            // lúc rồi ngủ có hạn giờ, chứ quay mãi thì đốt một core cho không.
            let mut backoff = Backoff::new();
            while !done()
            {
                match backoff.is_completed()
                {
                    true => thread::park_timeout(IDLE_NAP),
                    false => backoff.snooze(),
                }
            }
            return;
        };

        let mut tick = 0u32;
        let mut backoff = Backoff::new();

        while !done()
        {
            tick = tick.wrapping_add(1);

            if self.run_one(context.index, tick)
            {
                backoff.reset();
                continue;
            }

            // Không có việc mà điều kiện cũng chưa xong: thứ mình chờ đang chạy trên thread khác.
            if !backoff.is_completed()
            {
                backoff.snooze();
                continue;
            }

            // Quay mãi thì đốt một core cho không. Ngủ có hạn giờ là đường ở giữa: [`Latch`] gọi
            // dậy ngay khi vé cuối cùng được thả, còn điều kiện nào không biết cách gọi ai thì hạn
            // giờ tự lo. Nó ngắn, vì lỡ một nhịp ở đây là lỡ luôn cả phần việc còn lại của frame.
            thread::park_timeout(IDLE_NAP);
        }

        // Ô LIFO không ai trộm được, nên nó không được phép còn gì khi mình rời vòng.
        self.flush_lifo(context.index);
    }

    /// Đẩy job còn kẹt trong ô LIFO xuống ring, để người khác với tới được.
    fn flush_lifo(&self, index: usize)
    {
        // Safety: chỉ chủ chạm ô LIFO.
        if let Some(job) = unsafe { self.locals[index].take_lifo() }
        {
            self.push_local(index, job);
            self.sleep.notify();
        }
    }

    /// Chạy sạch mọi thứ còn xếp hàng, kể cả job sinh ra trong lúc đang vét.
    ///
    /// Dành cho cuối [`Owner::stop`], khi worker đã đi hết: thứ chúng để lại không còn thread nào
    /// chạy, mà thả trôi thì một scope đang đếm chúng sẽ treo.
    ///
    /// # Vì sao chỗ này chỉ trộm chứ không dùng ring của ai
    ///
    /// `shutdown` chạy trên thread nào là chuyện của người gọi, và `Drop` của tay cầm cuối cùng thì
    /// còn tuỳ tiện hơn nữa: nó chạy trên bất cứ thread nào tình cờ thả cái tay cầm ấy. Nếu ở đây
    /// dựng một `Producer` cho ô của host, mà host lúc đó lại đang ở trong `run_until` với
    /// `Producer` của chính nó, thì có hai `Producer` trên cùng một ring, mà đó là đúng cái mà
    /// `Producer` không cho phép.
    ///
    /// Đường trộm thì an toàn với mọi thread: `Consumer` là `Copy` và `Sync`, sinh ra để nhiều
    /// thread cùng dùng.
    fn drain_all(&self)
    {
        // Ô LIFO chỉ chủ mới được chạm. Nếu thread đang vét chính là chủ của một ô thì vét nốt ô
        // đó, còn không thì để yên: chủ của nó đã tự vét trước khi thoát.
        if let Some(context) = self.context()
        {
            self.flush_lifo(context.index);
        }

        let mut leftovers = Vec::new();

        loop
        {
            self.lane_queue.drain_into(&mut leftovers);

            for local in &self.locals
            {
                let consumer = local.ring.consumer();
                while consumer.steal_batch(&mut leftovers, usize::MAX) > 0
                {}
            }

            if leftovers.is_empty()
            {
                return;
            }

            // Job chạy ở đây có thể đẻ ra job mới, và chúng rơi vào lane queue hoặc vào ring của
            // thread này, nên vòng ngoài phải quay lại vét tiếp.
            for job in leftovers.drain(..)
            {
                run_job(job);
            }
        }
    }
}

/// Chạy một job, giữ panic lại thay vì để nó giết thread worker.
///
/// Payload bị thả ở đây có chủ đích. Job nào có đường báo lỗi về (mọi job đi qua `scope`) thì đã tự
/// bắt panic và sẽ dựng lại nó ở điểm join. Job được giao thẳng qua [`ThreadPool::spawn`] thì không
/// có điểm join nào, tức là không có ai để trao payload, và panic hook thì đã in message với
/// backtrace từ trước khi tới đây.
fn run_job(job: Job)
{
    let _ = catch_unwind(AssertUnwindSafe(move || job.run_once()));
}

/// Chạy một job với `in_loop` bật, để job con của nó biết đường vào ô LIFO.
fn run_in_loop(shared: &Shared, index: usize, job: Job)
{
    let previous = CONTEXT.get();
    CONTEXT.set(Context {
        pool:    shared.id,
        index:   index,
        in_loop: true,
    });

    shared.counters.of(index).job_run();
    crate::profile::job(index, || run_job(job));

    CONTEXT.set(previous);
}

fn worker_loop(shared: Arc<Shared>, index: usize)
{
    CONTEXT.set(Context {
        pool:    shared.id,
        index:   index,
        in_loop: false,
    });

    // Từ chính worker chứ không phải từ thread đã spawn nó: API của mọi nền tảng ở đây đều đặt
    // priority cho thread **đang gọi**, không nền nào nhận một thread khác làm đối tượng.
    shared.priority.apply_to_current_thread();

    let mut tick = 0u32;
    let mut is_searching = false;
    let mut backoff = Backoff::new();

    loop
    {
        tick = tick.wrapping_add(1);

        // Đọc bộ đếm sự kiện **trước** khi đi tìm. Job nào xuất hiện sau lời gọi này đều làm nó
        // đổi, nên lát nữa nếu tìm không ra gì mà bộ đếm vẫn thế thì chắc chắn không có job nào lọt
        // qua dưới mũi mình. Xem `sleep::Sleep`.
        let seen = shared.sleep.events();

        if let Some(job) = shared.next_local_job(index, tick)
        {
            if is_searching
            {
                is_searching = false;
                // Người lùng cuối cùng vừa vớ được việc thì chỗ nó lấy có thể còn nữa, mà từ giờ
                // không còn ai đi tìm. Gọi thêm một người dậy.
                if shared.sleep.end_searching()
                {
                    shared.sleep.notify();
                }
            }
            backoff.reset();
            run_in_loop(&shared, index, job);
            continue;
        }

        // Kiểm sau khi ngó hàng đợi, không phải trước, để shutdown vét nốt phần còn lại thay vì bỏ
        // chúng lại.
        if shared.shutdown.load(Ordering::Acquire)
        {
            break;
        }

        if !is_searching
        {
            is_searching = shared.sleep.try_start_searching();
        }

        if is_searching && let Some(job) = shared.search_job(index)
        {
            is_searching = false;
            if shared.sleep.end_searching()
            {
                shared.sleep.notify();
            }
            backoff.reset();
            run_in_loop(&shared, index, job);
            continue;
        }

        if backoff.rounds() < shared.spin_rounds
        {
            backoff.snooze();
            continue;
        }

        if shared.shutdown.load(Ordering::Acquire)
        {
            break;
        }

        shared.counters.of(index).park();
        if let Some(sink) = crate::profile::sink()
        {
            sink.worker_park(index);
        }

        let wake = shared.sleep.park(index, is_searching, seen, || shared.has_work());

        if let Some(sink) = crate::profile::sink()
        {
            sink.worker_unpark(index);
        }

        match wake
        {
            // Người đánh thức đã tính mình vào `searching` rồi, nên đừng xin thêm một lần nữa.
            Wake::Notified => is_searching = true,
            Wake::Cancelled => is_searching = false,
        }
        backoff.reset();
    }

    // Không ai trộm được ô LIFO, nên thread này không được mang nó xuống mồ.
    shared.flush_lifo(index);
    CONTEXT.set(Context::NONE);
}

/// Chỉ số của thread hiện tại trong `0..pool.worker_count()`.
///
/// Worker nhận `0..worker_threads()`, host nhận ô ngay sau chúng, vì host cũng chạy job. Dùng nó để
/// đánh chỉ số cho dữ liệu "mỗi worker một ô": một command buffer, một bộ đếm profiler, một arena.
/// Nhờ nó mà không bao giờ có hai thread ghi vào cùng một cache line.
///
/// # Vì sao phải so id chứ không chỉ so khoảng
///
/// Chỉ kiểm "chỉ số có nằm trong `0..=workers` không" thì chấp nhận luôn cái chỉ số mà một pool
/// **khác** đã phát cho thread này. Dựng hai pool trên cùng một thread là đủ để dính: lần đăng ký
/// sau ghi đè lần trước, và host đọc lại ra một con số vốn cũng là chỉ số hợp lệ của pool thứ nhất.
/// Hai thread cùng qua được bài kiểm tra, cùng được trao một ô, và thế là hai thread ghi vào một
/// chỗ. So id biến chuyện đó thành một lần trượt, và trượt thì rơi xuống nhánh kiểm host bên dưới.
pub(crate) fn worker_index_in(shared: &Shared) -> usize
{
    let context = CONTEXT.get();

    if context.pool == shared.id
    {
        return context.index;
    }

    assert_eq!(
        thread::current().id(),
        shared.host.id(),
        "worker_index() gọi từ một thread không phải worker của pool này, cũng không phải thread đã dựng nó"
    );
    shared.workers
}

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
