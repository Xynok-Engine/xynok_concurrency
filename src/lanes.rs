//! Các lane của engine: ai chạy ở đâu, và job đi từ lane này sang lane kia bằng đường nào.
//!
//! Sai lầm dễ mắc nhất khi nghe "engine có lane physics, lane render, lane audio, lane IO" là cho
//! mỗi lane một pool riêng. Sáu lane nhân tám thread là 48 thread trên 8 core, mà work-stealing chỉ
//! có nghĩa khi số worker xấp xỉ số core. Vượt qua đó thì thứ mua được là các worker giành CPU của
//! nhau và cache bị đá qua đá lại.
//!
//! Nên lane ở đây chia theo **tính chất chạy**, không theo tên miền:
//!
//! | lane | chạy cái gì | thread | vì sao tách |
//! |---|---|---|---|
//! | [`LaneId::Compute`] | frame logic, ECS, physics, culling, animation, ghi command buffer | một pool work-stealing, `N = cores - 1`, thread gọi tham gia thành người thứ N | CPU-bound, không bao giờ block, muốn cache nóng và muốn ăn hết core |
//! | [`LaneId::Blocking`] | đọc file, decode texture, compile shader, giải nén | pool riêng 2 tới 4 thread, priority thấp | thời gian nằm trong syscall, và một thread block không được phép chiếm core của lane compute |
//! | [`LaneId::Main`] | present, gọi API cửa sổ, vài lời gọi driver | không thread nào cả: một hàng đợi mà chính main thread vét | ràng buộc cứng, phải đúng thread đó |
//!
//! Physics, rendering và compute **không** phải ba lane riêng. Chúng là ba nhóm job trong lane
//! compute, phân biệt nhau bằng vị trí trong đồ thị phụ thuộc chứ không bằng thread riêng. Cho
//! physics một pool riêng nghĩa là trong lúc physics chạy thì các core dành cho render ngồi không,
//! và ngược lại.
//!
//! Thread audio thì không nằm trong bảng này, và đó là chuyện có chủ ý: nó không bao giờ là worker
//! của pool nào. Nó nhận lệnh qua [`ring_buffer_spsc`](crate::ring_buffer_spsc) và chỉ đọc.

use crate::apis::priority::Priority;
use crate::channel::{Receiver, oneshot};
use crate::custom_type::Job;
use crate::lane_queue::LaneQueue;
use crate::pool::{Config, ThreadPool};
use crate::sync::thread::{self, ThreadId};
use crate::utils::available_cores;

/// Lane nào chạy job này.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum LaneId
{
    /// Việc CPU-bound của frame. Mặc định, và là chỗ phần lớn job sống.
    #[default]
    Compute,
    /// Việc mà phần lớn thời gian nằm trong syscall.
    Blocking,
    /// Việc buộc phải chạy trên main thread.
    Main,
}

/// Cách dựng cả bộ lane.
#[derive(Debug, Clone)]
pub struct LanesConfig
{
    pub compute:  Config,
    pub blocking: Config,
}

impl Default for LanesConfig
{
    /// Lane compute ăn gần hết core, lane blocking chỉ vài thread và priority thấp.
    ///
    /// Lane blocking cố ý **không** co giãn theo số core: nó không tồn tại để chạy nhanh mà để giữ
    /// cho thread đang nằm trong syscall khỏi chiếm core của frame. Hai tới bốn thread là đủ cho
    /// vài file lớn đọc song song, và nhiều hơn thế chỉ tổ làm ổ đĩa phải nhảy đầu đọc.
    fn default() -> Self
    {
        Self {
            compute:  Config {
                thread_name: "xynok-compute".to_string(),
                priority: Priority::Frame,
                ..Config::default()
            },
            blocking: Config {
                threads:       available_cores().clamp(2, 4),
                ring_capacity: 64,
                scratch_bytes: 0,
                thread_name:   "xynok-blocking".to_string(),
                priority:      Priority::Io,
                // Thread ở đây phần lớn thời gian nằm trong syscall, nên quay tại chỗ chờ việc là
                // đốt core của lane compute. Ngủ sớm.
                spin_rounds:   1,
            },
        }
    }
}

impl LanesConfig
{
    /// [`Self::default`], với `XYNOK_LANE_THREADS` và `XYNOK_BLOCKING_THREADS` đè lên số thread.
    ///
    /// Đọc từ env để tune được mà không phải build lại: đổi số thread rồi chạy lại game là một vòng
    /// lặp vài giây, còn build lại engine thì không.
    pub fn from_env() -> Self
    {
        let mut config = Self::default();
        config.compute.threads = Config::from_env().threads;

        if let Ok(raw) = std::env::var("XYNOK_BLOCKING_THREADS")
            && let Ok(threads) = raw.trim().parse::<usize>()
        {
            config.blocking.threads = threads;
        }

        config
    }
}

/// Hàng đợi của những job buộc phải chạy trên main thread.
///
/// Không có thread nào ở đây cả, và đó chính là điểm: main thread là thread của người dùng, engine
/// chỉ mượn nó ở những chỗ nó tự gọi [`Lanes::run_pending_on_main`] trong vòng lặp frame.
struct MainQueue
{
    jobs:   LaneQueue<Job>,
    /// Thread được coi là main, chụp lúc dựng [`Lanes`].
    thread: ThreadId,
}

/// Cả bộ lane, dựng một lần lúc khởi động.
pub struct Lanes
{
    compute:  ThreadPool,
    blocking: ThreadPool,
    main:     MainQueue,
}

impl Lanes
{
    /// Dựng cả hai pool. Thread gọi hàm này được coi là main thread.
    ///
    /// Lane blocking dựng trước, lane compute dựng sau, và thứ tự đó có lý do: mỗi thread chỉ nhớ
    /// được chỗ đứng ở **một** pool, nên pool dựng sau là pool mà thread này có ring riêng. Main
    /// thread thì làm việc với lane compute suốt cả frame, còn lane blocking thì nó chỉ gửi việc
    /// sang chứ không tham gia chạy.
    pub fn new(config: LanesConfig) -> Self
    {
        let blocking = ThreadPool::new(config.blocking);

        Self {
            compute:  ThreadPool::new(config.compute),
            blocking: blocking,
            main:     MainQueue {
                jobs:   LaneQueue::new(),
                thread: thread::current().id(),
            },
        }
    }

    /// [`Self::new`] với [`LanesConfig::from_env`].
    pub fn from_env() -> Self
    {
        Self::new(LanesConfig::from_env())
    }

    /// Pool của lane compute. Đây là thứ mà ECS scheduler và frame graph gọi tới.
    #[inline]
    pub fn compute(&self) -> &ThreadPool
    {
        &self.compute
    }

    /// Pool của lane blocking.
    ///
    /// Đừng gửi việc CPU-bound sang đây: thread ở lane này chạy với priority thấp, nên một job tính
    /// toán nặng đặt nhầm chỗ sẽ chạy chậm hơn hẳn mà không có lý do nào nhìn thấy được từ code.
    #[inline]
    pub fn blocking(&self) -> &ThreadPool
    {
        &self.blocking
    }

    /// Giao một job cho lane được chỉ định.
    pub fn spawn<F>(&self, lane: LaneId, f: F)
    where F: FnOnce() + Send + 'static
    {
        match lane
        {
            LaneId::Compute => self.compute.spawn(f),
            LaneId::Blocking => self.blocking.spawn(f),
            LaneId::Main => self.spawn_on_main(f),
        }
    }

    /// Xếp một job để main thread chạy ở lần [`Self::run_pending_on_main`] tiếp theo.
    ///
    /// Gọi được từ bất cứ thread nào, kể cả từ trong một job. Nó không chạy job ngay tại chỗ dù bạn
    /// đang đứng trên chính main thread: chạy ngay sẽ làm thứ tự phụ thuộc vào việc ai gọi từ đâu,
    /// mà một lời gọi driver chạy giữa chừng frame thay vì ở đúng điểm đồng bộ là loại bug rất khó
    /// lần.
    pub fn spawn_on_main<F>(&self, f: F)
    where F: FnOnce() + Send + 'static
    {
        self.main.jobs.push(Job::new(f));
    }

    /// Số job đang chờ main thread.
    #[inline]
    pub fn pending_on_main(&self) -> usize
    {
        self.main.jobs.len()
    }

    /// Chạy hết những gì đang xếp hàng cho main thread, và trả về đã chạy bao nhiêu cái.
    ///
    /// Gọi nó ở một chỗ cố định trong vòng lặp frame, thường là ngay trước `present`. Chỉ chạy
    /// những job đã có mặt lúc bắt đầu vét: job mà chúng đẻ ra sẽ đợi frame sau, để một job đẻ job
    /// không giữ main thread lại vô hạn.
    ///
    /// # Panics
    ///
    /// Nếu gọi từ thread khác với thread đã dựng [`Lanes`]. Chạy job "chỉ main mới được chạy" trên
    /// một thread khác là phá đúng cái lời hứa mà hàng đợi này tồn tại để giữ.
    pub fn run_pending_on_main(&self) -> usize
    {
        assert_eq!(
            thread::current().id(),
            self.main.thread,
            "run_pending_on_main phải chạy trên chính thread đã dựng Lanes"
        );

        let mut batch = Vec::new();
        self.main.jobs.drain_into(&mut batch);

        let count = batch.len();
        for job in batch
        {
            job.run_once();
        }
        count
    }

    /// Gửi một việc chặn sang lane blocking và trả về đầu nhận kết quả.
    ///
    /// Đây là hình dạng mà mọi thứ IO nên đi qua: người gọi cầm một [`Receiver`] chứ không phải một
    /// giá trị, nên họ vốn đã không giả định "gọi xong là có kết quả". Ngày lane blocking đổi thành
    /// một reactor async, chỗ này không phải sửa dòng nào.
    ///
    /// ```
    /// use xynok_concurrency::lanes::{Lanes, LanesConfig};
    ///
    /// let lanes = Lanes::new(LanesConfig::default());
    /// let reading = lanes.run_blocking(|| "nội dung file".to_string());
    ///
    /// // Trong lúc chờ, thread này vẫn chạy job của lane compute.
    /// assert_eq!(
    ///     reading.recv_in(lanes.compute()).as_deref(),
    ///     Some("nội dung file")
    /// );
    /// ```
    pub fn run_blocking<F, R>(&self, f: F) -> Receiver<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot();
        self.blocking.spawn(move || tx.send(f()));
        rx
    }

    /// Dọn ranh giới frame: reset mọi arena nháp của lane compute.
    ///
    /// Gọi khi lane compute đã rảnh, tức là mọi job của frame đã xong.
    pub fn end_frame(&self)
    {
        self.compute.end_frame();
    }

    /// Tắt cả hai pool và chạy nốt phần còn xếp hàng cho main thread.
    ///
    /// Gọi từ main thread, và đừng gọi từ trong một job: tắt pool nghĩa là join mọi worker, mà
    /// worker đang chạy chính cái job gọi hàm này thì hoá ra nó đang đợi chính mình.
    pub fn shutdown(&self)
    {
        self.compute.shutdown();
        self.blocking.shutdown();

        if thread::current().id() == self.main.thread
        {
            self.run_pending_on_main();
        }
    }
}

impl std::fmt::Debug for Lanes
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("Lanes")
            .field("compute", &self.compute)
            .field("blocking", &self.blocking)
            .field("pending_on_main", &self.pending_on_main())
            .finish()
    }
}

/// Cửa thoát cho một job lane compute lỡ phải chờ một thứ ngoài tầm với: một fence GPU, một cái
/// khoá của thư viện bên thứ ba, một lời gọi driver đồng bộ.
///
/// Nó gọi thêm một worker dậy trước khi chạy `f`, để chỗ trống mà thread này để lại có người bù
/// vào, rồi chạy `f` ngay tại đây.
///
/// # Giới hạn, nói thẳng ra
///
/// Đây **không** phải `block_in_place` của tokio: thread này vẫn giữ chỗ của nó trong pool, không
/// có worker thay thế nào được spawn ra. Nếu mọi worker cùng gọi hàm này một lúc thì pool đứng
/// im cho tới khi có ai đó xong. Nó đủ cho trường hợp thật hay gặp, tức là một job lẻ phải chờ một
/// thứ ngắn, và không đủ để thay cho việc gửi hẳn công việc sang [`LaneId::Blocking`]. Chờ lâu thì
/// dùng [`Lanes::run_blocking`].
pub fn block_in_place<F, R>(pool: &ThreadPool, f: F) -> R
where F: FnOnce() -> R
{
    pool.wake_one();
    f()
}

#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn small_lanes() -> Lanes
    {
        Lanes::new(LanesConfig {
            compute:  Config {
                threads: 2,
                ..Config::default()
            },
            blocking: Config {
                threads: 2,
                ..LanesConfig::default().blocking
            },
        })
    }

    #[test]
    fn job_di_dung_lane_duoc_chi_dinh()
    {
        let lanes = small_lanes();
        let compute_done = Arc::new(AtomicUsize::new(0));
        let blocking_done = Arc::new(AtomicUsize::new(0));

        for _ in 0..100
        {
            let done = Arc::clone(&compute_done);
            lanes.spawn(LaneId::Compute, move || {
                done.fetch_add(1, Ordering::Relaxed);
            });

            let done = Arc::clone(&blocking_done);
            lanes.spawn(LaneId::Blocking, move || {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }

        lanes.compute().run_until(|| compute_done.load(Ordering::Acquire) == 100);
        lanes.blocking().run_until(|| blocking_done.load(Ordering::Acquire) == 100);

        assert_eq!(compute_done.load(Ordering::Acquire), 100);
        assert_eq!(blocking_done.load(Ordering::Acquire), 100);
    }

    #[test]
    fn job_cua_main_chi_chay_khi_main_vet()
    {
        let lanes = small_lanes();
        let ran = Arc::new(AtomicUsize::new(0));
        let main_thread = std::thread::current().id();
        let wrong_thread = Arc::new(AtomicUsize::new(0));

        for _ in 0..8
        {
            let ran = Arc::clone(&ran);
            let wrong = Arc::clone(&wrong_thread);
            lanes.spawn_on_main(move || {
                if std::thread::current().id() != main_thread
                {
                    wrong.fetch_add(1, Ordering::Relaxed);
                }
                ran.fetch_add(1, Ordering::Relaxed);
            });
        }

        // Đợi một lúc: không ai được phép chạy chúng thay main thread.
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(ran.load(Ordering::Acquire), 0, "job của main bị chạy trước khi main vét");
        assert_eq!(lanes.pending_on_main(), 8);

        assert_eq!(lanes.run_pending_on_main(), 8);
        assert_eq!(ran.load(Ordering::Acquire), 8);
        assert_eq!(wrong_thread.load(Ordering::Acquire), 0);
        assert_eq!(lanes.pending_on_main(), 0);
    }

    #[test]
    fn job_tu_lane_khac_van_xep_duoc_cho_main()
    {
        let lanes = small_lanes();
        let ran = Arc::new(AtomicUsize::new(0));
        let queued = Arc::new(AtomicUsize::new(0));

        // Một job của lane compute xếp việc cho main thread, đúng hình dạng của "present phải chạy
        // trên main".
        for _ in 0..16
        {
            let ran = Arc::clone(&ran);
            let queued = Arc::clone(&queued);
            let lanes_ref = &lanes;
            lanes.compute().scope(|s| {
                s.spawn(move || {
                    lanes_ref.spawn_on_main(move || {
                        ran.fetch_add(1, Ordering::Relaxed);
                    });
                    queued.fetch_add(1, Ordering::Relaxed);
                });
            });
        }

        assert_eq!(queued.load(Ordering::Acquire), 16);
        assert_eq!(lanes.run_pending_on_main(), 16);
        assert_eq!(ran.load(Ordering::Acquire), 16);
    }

    #[test]
    fn vet_main_khong_chay_job_do_chinh_dot_nay_de_ra()
    {
        let ran = Arc::new(AtomicUsize::new(0));

        // Job này, khi chạy, lại xếp thêm một job cho main. Cái mới phải đợi lần vét sau.
        //
        // `Arc` chứ không phải tham chiếu: job phải `'static`, mà đây đúng là hình dạng thật của
        // engine, nơi `Lanes` sống trong một `Arc` dùng chung.
        let lanes = Arc::new(small_lanes());
        let ran_outer = Arc::clone(&ran);
        let lanes_inner = Arc::clone(&lanes);
        lanes.spawn_on_main(move || {
            ran_outer.fetch_add(1, Ordering::Relaxed);
            lanes_inner.spawn_on_main(|| {});
        });

        assert_eq!(lanes.run_pending_on_main(), 1);
        assert_eq!(ran.load(Ordering::Acquire), 1);
        assert_eq!(lanes.pending_on_main(), 1, "job do đợt này đẻ ra phải đợi lần vét sau");

        assert_eq!(lanes.run_pending_on_main(), 1);
        assert_eq!(lanes.pending_on_main(), 0);
    }

    #[test]
    #[should_panic(expected = "phải chạy trên chính thread đã dựng Lanes")]
    fn vet_main_tu_thread_khac_thi_panic()
    {
        let lanes = Arc::new(small_lanes());
        let foreign = Arc::clone(&lanes);

        let outcome = std::thread::spawn(move || foreign.run_pending_on_main()).join();
        match outcome
        {
            Ok(_) => panic!("thread lạ vét được hàng đợi của main"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    #[test]
    fn viec_chan_tra_ket_qua_ve_qua_kenh()
    {
        let lanes = small_lanes();

        let reading = lanes.run_blocking(|| {
            std::thread::sleep(std::time::Duration::from_millis(10));
            "nội dung file".to_string()
        });

        // Trong lúc chờ, lane compute vẫn chạy việc của nó.
        let done = Arc::new(AtomicUsize::new(0));
        for _ in 0..100
        {
            let done = Arc::clone(&done);
            lanes.compute().spawn(move || {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }

        assert_eq!(reading.recv_in(lanes.compute()).as_deref(), Some("nội dung file"));
        lanes.compute().run_until(|| done.load(Ordering::Acquire) == 100);
    }

    #[test]
    fn block_in_place_van_tra_ve_ket_qua()
    {
        let lanes = small_lanes();
        let value = block_in_place(lanes.compute(), || {
            std::thread::sleep(std::time::Duration::from_millis(5));
            7u32
        });
        assert_eq!(value, 7);
    }

    #[test]
    fn shutdown_chay_not_viec_cua_main()
    {
        let ran = Arc::new(AtomicUsize::new(0));
        {
            let lanes = small_lanes();
            for _ in 0..4
            {
                let ran = Arc::clone(&ran);
                lanes.spawn_on_main(move || {
                    ran.fetch_add(1, Ordering::Relaxed);
                });
            }
            lanes.shutdown();
        }
        assert_eq!(ran.load(Ordering::Acquire), 4);
    }
}
