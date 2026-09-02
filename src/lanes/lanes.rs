use std::future::Future;

use crate::channel::{Receiver, oneshot};
use crate::custom_type::Job;
use crate::lane_queue::LaneQueue;
use crate::lanes::lane_id::LaneId;
use crate::lanes::lanes_config::LanesConfig;
use crate::lanes::main_queue::MainQueue;
use crate::pool::ThreadPool;
use crate::sync::thread;
use crate::task;

/// Cả bộ lane, dựng một lần lúc khởi động.
pub struct Lanes
{
    compute:    ThreadPool,
    async_lane: ThreadPool,
    main:       MainQueue,
}

impl Lanes
{
    /// Dựng cả hai pool. Thread gọi hàm này được coi là main thread.
    ///
    /// Lane async dựng trước, lane compute dựng sau, và thứ tự đó có lý do: mỗi thread chỉ nhớ được
    /// chỗ đứng ở **một** pool, nên pool dựng sau là pool mà thread này có ring riêng. Main thread
    /// thì làm việc với lane compute suốt cả frame, còn lane async thì nó chỉ gửi việc sang chứ
    /// không tham gia chạy.
    pub fn new(config: LanesConfig) -> Self
    {
        let async_lane = ThreadPool::new(config.async_lane);

        Self {
            compute:    ThreadPool::new(config.compute),
            async_lane: async_lane,
            main:       MainQueue {
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

    /// Pool của lane async.
    ///
    /// Đừng gửi việc CPU-bound sang đây: thread ở lane này chạy với priority thấp, nên một job tính
    /// toán nặng đặt nhầm chỗ sẽ chạy chậm hơn hẳn mà không có lý do nào nhìn thấy được từ code.
    #[inline]
    pub fn async_lane(&self) -> &ThreadPool
    {
        &self.async_lane
    }

    /// Giao một job cho lane được chỉ định.
    pub fn spawn<F>(&self, lane: LaneId, f: F)
    where F: FnOnce() + Send + 'static
    {
        match lane
        {
            LaneId::Compute => self.compute.spawn(f),
            LaneId::Async => self.async_lane.spawn(f),
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

    /// Giao một future cho lane async và trả về đầu nhận kết quả.
    ///
    /// Đây là cửa chính của lane async. Task chạy độc lập với vòng lặp frame: nó bắt đầu ngay, và
    /// mỗi lần nó `.await` một thứ chưa xong thì nó trả thread lại cho lane chứ không giữ.
    ///
    /// Kết quả trả về là một [`Receiver`], nên chờ nó kiểu nào là tuỳ chỗ bạn đang đứng: `.await`
    /// từ một task khác, [`recv_in`](Receiver::recv_in) từ một thread của lane compute, hoặc
    /// [`try_recv`](Receiver::try_recv) mỗi frame một lần cho tới khi có. Task panic giữa chừng thì
    /// người chờ nhận `None`, không phải một lần treo.
    ///
    /// ```
    /// use xynok_concurrency::lanes::{Lanes, LanesConfig};
    ///
    /// let lanes = Lanes::new(LanesConfig::default());
    ///
    /// let loading = lanes.spawn_async(async { "nội dung file".to_string() });
    ///
    /// // Trong lúc chờ, thread này vẫn chạy job của lane compute.
    /// assert_eq!(
    ///     loading.recv_in(lanes.compute()).as_deref(),
    ///     Some("nội dung file")
    /// );
    /// ```
    pub fn spawn_async<F>(&self, future: F) -> Receiver<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        task::spawn(&self.async_lane, future)
    }

    /// Gửi một việc **chặn thật** sang lane async và trả về đầu nhận kết quả.
    ///
    /// Đây là đáy của mọi chuỗi async: một `File::read`, một lệnh decode của thư viện bên thứ ba,
    /// bất cứ thứ gì chỉ có API đồng bộ. Nó chiếm một thread của lane trong suốt thời gian chạy, và
    /// đó chính là việc lane này sinh ra để hứng.
    ///
    /// Kết quả là một [`Receiver`], nên trong một task nó là `.await`:
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use xynok_concurrency::lanes::{Lanes, LanesConfig};
    ///
    /// let lanes = Arc::new(Lanes::new(LanesConfig::default()));
    ///
    /// let inner = Arc::clone(&lanes);
    /// let loaded = lanes.spawn_async(async move {
    ///     let text = inner.run_blocking(|| "nội dung file".to_string()).await?;
    ///     Some(text.len())
    /// });
    ///
    /// assert_eq!(loaded.recv_in(lanes.compute()), Some(Some(15)));
    /// ```
    ///
    /// # Đừng gửi việc CPU-bound qua đây
    ///
    /// Lane chỉ có hai tới bốn thread, nên một closure tính toán nặng vừa chạy chậm (priority thấp)
    /// vừa chặn mất một phần lane, và mọi task đang chờ được poll phải xếp hàng sau nó. Việc
    /// CPU-bound thuộc về lane compute.
    pub fn run_blocking<F, R>(&self, f: F) -> Receiver<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot();
        self.async_lane.spawn(move || tx.send(f()));
        rx
    }

    /// Chạy một future ngay trên thread gọi, và chạy job của lane compute trong lúc chờ.
    ///
    /// Dành cho main thread ở một điểm đồng bộ, ví dụ lúc khởi động khi cần đủ asset rồi mới vào
    /// vòng lặp frame. Trong frame thì thường không phải cái bạn muốn: chờ ở đây là chờ thật, còn
    /// đường bình thường là [`Self::spawn_async`] rồi ngó kết quả ở frame sau.
    ///
    /// ```
    /// use xynok_concurrency::lanes::{Lanes, LanesConfig};
    ///
    /// let lanes = Lanes::new(LanesConfig::default());
    /// let loaded = lanes.block_on(async { 6 * 7 });
    /// assert_eq!(loaded, 42);
    /// ```
    pub fn block_on<F: Future>(&self, future: F) -> F::Output
    {
        task::block_on_in(&self.compute, future)
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
        self.async_lane.shutdown();

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
            .field("async_lane", &self.async_lane)
            .field("pending_on_main", &self.pending_on_main())
            .finish()
    }
}
