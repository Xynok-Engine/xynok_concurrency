//! Tay cầm công khai tới pool.

use crate::bump::Bump;
use crate::custom_type::Job;
use crate::lane_queue::LaneQueue;
use crate::per_worker::PerWorker;
use crate::ring_buffer_fifo::RingBufferFifo;
use crate::scope::Scope;
use crate::scope::params::ParamsParReduce;
use crate::scope::scope_in::scope_in;
use crate::sync::cell::UnsafeCell;
use crate::sync::{Arc, AtomicBool, Mutex, thread};
use crate::utils::cache_padded::CachePadded;

use super::config::Config;
use super::context::{CONTEXT, Context, next_pool_id, worker_index_in};
use super::counters::{Counters, PoolCounters};
use super::local::Local;
use super::owner::Owner;
use super::shared::Shared;
use super::sleep::Sleep;
use super::worker::worker_loop;

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

        if let Some(sink) = crate::profile::current_sink()
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

    /// Mở một scope, chạy `f`, rồi chờ mọi thứ `f` spawn ra.
    ///
    /// Chờ cả khi `f` panic: bỏ mặc job chạy tiếp trong lúc khung stack chúng đang mượn bị tháo dỡ
    /// thì đó đúng là cái use-after-free mà scope sinh ra để chặn.
    ///
    /// ```
    /// use xynok_concurrency::pool::{Config, ThreadPool};
    ///
    /// let pool = ThreadPool::new(Config {
    ///     threads: 3,
    ///     ..Default::default()
    /// });
    /// let mut totals = [0usize; 4];
    ///
    /// pool.scope(|s| {
    ///     for (i, slot) in totals.iter_mut().enumerate()
    ///     {
    ///         s.spawn(move || *slot = i * i);
    ///     }
    /// });
    ///
    /// assert_eq!(totals, [0, 1, 4, 9]);
    /// ```
    ///
    /// # Panics
    ///
    /// Nếu `f` panic, hoặc bất cứ job nào bên trong nó panic. Panic của job được bắt ngay tại chỗ,
    /// giữ lại, và ném lại ở đây sau khi mọi job khác đã xong: một panic thoát thẳng ra khỏi worker
    /// sẽ làm bộ đếm job thiếu một, và điểm chờ này treo mãi mãi. Nhiều job cùng panic thì cái được
    /// ghi nhận đầu tiên thắng, phần còn lại bị thả.
    pub fn scope<'scope, R>(&self, f: impl FnOnce(&Scope<'scope>) -> R) -> R
    {
        scope_in(self.shared(), f)
    }

    /// Chạy hai việc song song và trả về cả hai kết quả.
    ///
    /// `b` chạy ngay trên thread gọi, `a` được giao cho pool. Nếu không ai bốc `a` thì chính thread
    /// này sẽ chạy nó trong lúc chờ, nên `join` không bao giờ tệ hơn chạy tuần tự quá một chút chi
    /// phí ghi sổ.
    ///
    /// ```
    /// use xynok_concurrency::pool::{Config, ThreadPool};
    ///
    /// let pool = ThreadPool::new(Config {
    ///     threads: 2,
    ///     ..Default::default()
    /// });
    /// let (left, right) = pool.join(|| (1..=50u64).sum::<u64>(), || (51..=100u64).sum::<u64>());
    /// assert_eq!(left + right, 5050);
    /// ```
    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB,
        RA: Send,
    {
        let mut left = None;

        let right = self.scope(|s| {
            let slot = &mut left;
            s.spawn(move || *slot = Some(a()));
            b()
        });

        // Scope đã trả về, nên job của `a` chắc chắn đã chạy xong và đã ghi vào ô.
        (left.expect("job của `a` xong mà không để lại kết quả"), right)
    }

    /// Chia `0..n` thành từng lô rồi chạy `f` trên mọi chỉ số. Xem [`Scope::parallel_for`].
    pub fn parallel_for<F>(&self, n: usize, batch: usize, f: F)
    where F: Fn(usize) + Sync
    {
        self.scope(|s| s.parallel_for(n, batch, f));
    }

    /// Gộp `0..n` thành một giá trị, song song, kết quả không phụ thuộc lúc nào thread nào xong.
    /// Xem [`Scope::par_reduce`].
    pub fn par_reduce<T, I, F, J>(&self, params: ParamsParReduce<I, F, J>) -> T
    where
        T: Send,
        I: Fn() -> T + Sync,
        F: Fn(T, usize) -> T + Sync,
        J: Fn(T, T) -> T,
    {
        self.scope(|s| s.par_reduce(params))
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
