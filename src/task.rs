//! Executor của lane async: một future thành một task, task tự xếp mình vào pool mỗi lần được gọi dậy.
//!
//! Lane async không phải một pool thread ngồi chặn trong `read()` nữa. Nó chạy **task**, và một task
//! là một future: poll ra `Pending` thì nó trả thread lại cho lane ngay tại đó, còn cái waker mà nó
//! để lại là đường để bất cứ ai (một job lane khác, một kênh vừa có kết quả, sau này là một reactor)
//! xếp nó lại vào hàng.
//!
//! # Một task đi đường nào
//!
//! ```text
//!   spawn(future)
//!        │
//!        ▼
//!   [SCHEDULED] ──▶ worker lấy job ra ──▶ [RUNNING] ──poll──┬── Ready  ──▶ [DONE]
//!        ▲                                     │            │
//!        │                                     │            └── Pending ──▶ [IDLE]
//!        │                                     │                              │
//!        └──────── waker gọi dậy ◀─────────────┴── waker gọi dậy giữa lúc poll ┘
//!                                                        [NOTIFIED]
//! ```
//!
//! Cái ô trạng thái ấy tồn tại vì hai chuyện có thật: waker được gọi nhiều lần liền nhau (task chỉ
//! được nằm trong hàng đợi **một** bản, không thì hai worker cùng poll một future), và waker được
//! gọi đúng lúc future đang bị poll (lần gọi ấy không được rơi mất, nếu không task ngủ luôn). Trạng
//! thái `NOTIFIED` là chỗ ghi lại lần gọi dậy đó cho tới khi poll xong.
//!
//! # Nó vẫn không phải một reactor, và nói thẳng ra ở đây
//!
//! Chưa có epoll hay kqueue nào bên dưới. Một future gọi `File::read` ngay trong `poll` thì vẫn giữ
//! nguyên một thread của lane cho tới lúc syscall xong, đúng như trước. Cái đổi là **hình dạng**:
//! việc chặn giờ nằm gọn trong [`Lanes::run_blocking`](crate::lanes::Lanes::run_blocking) và trả về
//! một [`Receiver`] mà bạn `.await` được, nên một task đang đợi IO không ngồi trên thread nào cả.
//! Ngày cắm reactor vào, chỗ phải sửa là `run_blocking`, không phải các call site.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
// `std::task::Wake` chỉ nhận `std::sync::Arc`, nên chỗ này không đi qua `crate::sync`. Không mất gì:
// lane async dựng trên một `ThreadPool` thật, mà pool thì loom không mô hình hoá được (nó dùng
// `thread_local` để biết mình là ai), nên các đường ở đây vốn không nằm trong mô hình loom nào.
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use crate::channel::{Receiver, oneshot};
use crate::custom_type::Job;
use crate::pool::{Shared, ThreadPool};
// Tay cầm về pool thì phải là cùng loại `Arc` mà chính pool dùng, vì dưới `--cfg loom` cả crate đổi
// sang `loom::sync::Arc`. Chỉ riêng `Arc<Task>` là bắt buộc phải của std, do `std::task::Wake` chỉ
// nhận đúng loại đó.
use crate::sync::Arc as PoolArc;
use crate::sync::thread::{self, Thread};
use crate::utils::ignore_poison;

/// Không nằm trong hàng đợi, không ai đang poll. Chờ một lần gọi dậy.
const IDLE: u8 = 0;
/// Đang nằm trong hàng đợi của lane dưới dạng một [`Job`].
const SCHEDULED: u8 = 1;
/// Một worker đang poll future.
const RUNNING: u8 = 2;
/// Có người gọi dậy trong lúc worker đang poll: poll xong phải xếp lại vào hàng.
const NOTIFIED: u8 = 3;
/// Future đã xong, hoặc đã panic. Mọi lần gọi dậy sau đó là vô hại.
const DONE: u8 = 4;

/// Một future cộng chỗ đứng của nó trong lane.
struct Task
{
    /// `None` sau khi future xong: thả sớm để mọi thứ nó giữ (kể cả đầu gửi của kênh kết quả) đi
    /// ngay, chứ không nằm chờ tới lúc cái `Arc` cuối cùng của task biến mất.
    ///
    /// Ô trạng thái đã đảm bảo mỗi lúc chỉ một thread poll, nên khoá này không bao giờ có ai giành.
    /// Nó ở đây để cái đảm bảo ấy được kiểu dữ liệu nói ra, thay vì chỉ nằm trong đầu người đọc.
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    state:  AtomicU8,
    /// Lane mà task này chạy trên đó.
    ///
    /// `Arc<Shared>` chứ không phải `ThreadPool`, và đó là chuyện sống chết: `ThreadPool` giữ đám
    /// join handle, nên một task tình cờ là kẻ thả tay cầm cuối cùng sẽ đi join chính cái worker
    /// đang chạy nó. Xem mục 5 của `docs_internal/lanes.md`.
    lane:   PoolArc<Shared>,
}

impl Task
{
    /// Dựng task và xếp nó vào lane ngay.
    fn spawn<F>(lane: &PoolArc<Shared>, future: F)
    where F: Future<Output = ()> + Send + 'static
    {
        let task = Arc::new(Self {
            future: Mutex::new(Some(Box::pin(future))),
            state:  AtomicU8::new(SCHEDULED),
            lane:   PoolArc::clone(lane),
        });
        Self::enqueue(task);
    }

    /// Đẩy task vào lane dưới dạng một job.
    ///
    /// Chỉ gọi khi vừa giành được quyền đó qua ô trạng thái, tức là vừa đặt nó thành `SCHEDULED`.
    fn enqueue(task: Arc<Self>)
    {
        let lane = PoolArc::clone(&task.lane);
        // Một `Arc` là 8 byte, nên job này nằm gọn trong `InlineFn` và không tốn lần cấp phát nào.
        lane.inject(Job::new(move || Self::run(task)));
    }

    /// Poll một nhát. Đây là thân của cái job mà [`Self::enqueue`] đẩy vào lane.
    fn run(task: Arc<Self>)
    {
        if task.state.compare_exchange(SCHEDULED, RUNNING, Ordering::AcqRel, Ordering::Acquire).is_err()
        {
            // `DONE` là lối duy nhất tới đây: task đã xong ở một đường khác. Không có gì để làm.
            return;
        }

        let waker = Waker::from(Arc::clone(&task));
        let mut context = Context::from_waker(&waker);

        let finished = {
            let mut slot = ignore_poison(task.future.lock());
            match slot.as_mut()
            {
                None => true,
                Some(future) =>
                {
                    // Panic của một task không được giết worker. Nó cũng không có ai để trao lại:
                    // panic hook đã in message với backtrace, còn người đang đợi kết quả thì nhận
                    // `None` khi đầu gửi bị thả ngay dưới đây.
                    match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)))
                    {
                        Ok(Poll::Pending) => false,
                        Ok(Poll::Ready(())) | Err(_) =>
                        {
                            *slot = None;
                            true
                        }
                    }
                }
            }
        };

        if finished
        {
            task.state.store(DONE, Ordering::Release);
            return;
        }

        // Còn `Pending`. Nếu không ai gọi dậy trong lúc poll thì task nằm im chờ waker; nếu có, lần
        // gọi ấy đang nằm trong `NOTIFIED` và phải được trả lại thành một lượt poll nữa.
        if task.state.compare_exchange(RUNNING, IDLE, Ordering::AcqRel, Ordering::Acquire).is_err()
        {
            task.state.store(SCHEDULED, Ordering::Release);
            Self::enqueue(task);
        }
    }
}

impl Wake for Task
{
    fn wake(self: Arc<Self>)
    {
        Wake::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>)
    {
        loop
        {
            match self.state.load(Ordering::Acquire)
            {
                IDLE =>
                {
                    if self.state.compare_exchange(IDLE, SCHEDULED, Ordering::AcqRel, Ordering::Acquire).is_ok()
                    {
                        Self::enqueue(Arc::clone(self));
                        return;
                    }
                }
                RUNNING =>
                {
                    if self.state.compare_exchange(RUNNING, NOTIFIED, Ordering::AcqRel, Ordering::Acquire).is_ok()
                    {
                        return;
                    }
                }
                // `SCHEDULED` đã có một bản trong hàng đợi, `NOTIFIED` đã hẹn sẵn một lượt poll nữa,
                // `DONE` thì không còn gì để poll. Cả ba đều là gọi dậy thừa, và gọi dậy thừa là
                // chuyện bình thường của waker.
                _ => return,
            }
        }
    }
}

/// Giao một future cho pool và trả về đầu nhận kết quả.
///
/// Trả về [`Receiver`] chứ không phải một handle riêng, vì `Receiver` đã là cả hai thứ cần thiết:
/// `.await` được từ trong một task khác, và [`recv_in`](Receiver::recv_in) được từ một thread đang
/// chạy job của lane compute. Future panic giữa chừng thì người nhận thấy `None`, đúng như một job
/// panic.
///
/// ```
/// use xynok_concurrency::pool::{Config, ThreadPool};
/// use xynok_concurrency::task;
///
/// let pool = ThreadPool::new(Config {
///     threads: 2,
///     ..Default::default()
/// });
///
/// let answer = task::spawn(&pool, async { 6 * 7 });
/// assert_eq!(task::block_on(answer), Some(42));
/// ```
pub fn spawn<F>(pool: &ThreadPool, future: F) -> Receiver<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = oneshot();
    Task::spawn(pool.shared(), async move { tx.send(future.await) });
    rx
}

/// Người chờ của [`block_on`]: một thread ngủ, và một cờ để phân biệt "đã có ai gọi mình" với một
/// lần `unpark` lạc.
struct ParkWaker
{
    thread: Thread,
    woken:  AtomicBool,
}

impl Wake for ParkWaker
{
    fn wake(self: Arc<Self>)
    {
        Wake::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>)
    {
        // Cờ dựng **trước** `unpark`: người chờ có thể chưa park, và lúc đó cái nó đọc được phải là
        // cờ chứ không phải cái token `unpark` để lại.
        self.woken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

impl ParkWaker
{
    fn new() -> Arc<Self>
    {
        Arc::new(Self {
            thread: thread::current(),
            woken:  AtomicBool::new(false),
        })
    }

    /// Đã có ai gọi dậy chưa, và xoá dấu đi để lượt sau đếm lại từ đầu.
    #[inline]
    fn take(&self) -> bool
    {
        self.woken.swap(false, Ordering::Acquire)
    }
}

/// Chạy một future tới khi xong, bằng cách ngủ giữa các lần poll.
///
/// Dành cho thread không thuộc pool nào, ví dụ main thread lúc khởi động. Đang đứng trong một lane
/// thì dùng [`block_on_in`]: nó chạy job giúp lane trong lúc chờ thay vì nằm không.
///
/// # Đừng gọi từ trong một task
///
/// Chặn một thread của lane async để đợi một future khác của chính lane đó là cách deadlock nhanh
/// nhất khi lane chỉ có hai thread. Trong một task thì `.await` là đường duy nhất.
pub fn block_on<F: Future>(future: F) -> F::Output
{
    let mut future = std::pin::pin!(future);
    let signal = ParkWaker::new();
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);

    loop
    {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context)
        {
            return output;
        }

        // `take` sau khi poll chứ không phải trước: waker có thể đã được gọi ngay giữa lúc poll, và
        // lần gọi ấy phải được đếm, không thì thread này ngủ chờ một tiếng gọi đã đi qua rồi.
        while !signal.take()
        {
            thread::park();
        }
    }
}

/// Như [`block_on`], nhưng chờ bằng cách chạy job của `pool`.
///
/// Đây là cách chờ đúng khi thread gọi là một người tham gia pool, y như
/// [`Receiver::recv_in`]: nằm không là bỏ phí một chỗ trong lane, và với join lồng nhau thì có thể
/// chính thread này mới là người phải chạy cái job mà future đang đợi.
pub fn block_on_in<F: Future>(pool: &ThreadPool, future: F) -> F::Output
{
    let mut future = std::pin::pin!(future);
    let signal = ParkWaker::new();
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);

    loop
    {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context)
        {
            return output;
        }

        pool.run_until(|| signal.woken.load(Ordering::Acquire));
        signal.take();
    }
}

/// Nhường lượt: task xếp lại vào cuối hàng của lane rồi mới chạy tiếp.
///
/// Dùng khi một task làm một mạch việc dài mà vẫn muốn task khác chen vào được. Nó không phải một
/// điểm chờ: waker được gọi ngay tại chỗ, nên task này không bao giờ ngủ.
pub async fn yield_now()
{
    struct YieldOnce
    {
        yielded: bool,
    }

    impl Future for YieldOnce
    {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()>
        {
            if self.yielded
            {
                return Poll::Ready(());
            }

            self.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }

    YieldOnce { yielded: false }.await
}

#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};
    use std::time::Duration;

    use super::*;
    use crate::pool::Config;

    fn pool_with(threads: usize) -> ThreadPool
    {
        ThreadPool::new(Config {
            threads: threads,
            spin_rounds: 1,
            ..Config::default()
        })
    }

    #[test]
    fn future_khong_cho_gi_thi_chay_mot_nhat()
    {
        let pool = pool_with(2);
        let answer = spawn(&pool, async { 6 * 7 });
        assert_eq!(block_on(answer), Some(42));
    }

    #[test]
    fn await_ket_qua_cua_mot_task_khac()
    {
        let pool = pool_with(2);

        let inner = spawn(&pool, async { "nội dung file".to_string() });
        let outer = spawn(&pool, async move {
            let text = inner.await.expect("task trong không trả kết quả");
            text.len()
        });

        assert_eq!(block_on(outer), Some(15));
    }

    #[test]
    fn task_ngu_roi_duoc_thread_ngoai_goi_day()
    {
        let pool = pool_with(2);
        let (tx, rx) = oneshot::<u32>();

        let task = spawn(&pool, async move { rx.await.map(|value| value * 2) });

        // Task đã `Pending` và không giữ thread nào của lane: lane phải rảnh để chạy việc khác.
        let ran = StdArc::new(AtomicUsize::new(0));
        for _ in 0..100
        {
            let ran = StdArc::clone(&ran);
            pool.spawn(move || {
                ran.fetch_add(1, StdOrdering::Relaxed);
            });
        }
        pool.run_until(|| ran.load(StdOrdering::Acquire) == 100);

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            tx.send(21);
        });

        assert_eq!(block_on(task), Some(Some(42)));
    }

    #[test]
    fn goi_day_nhieu_lan_khong_lam_task_chay_hai_lan()
    {
        let pool = pool_with(2);
        let polls = StdArc::new(AtomicUsize::new(0));
        let (tx, rx) = oneshot::<()>();

        let counted = StdArc::clone(&polls);
        let task = spawn(&pool, async move {
            counted.fetch_add(1, StdOrdering::Relaxed);
            let _ = rx.await;
            counted.fetch_add(1, StdOrdering::Relaxed);
        });

        // Đầu gửi bị thả: người chờ được gọi dậy đúng một lần dù có bao nhiêu waker đi nữa.
        drop(tx);
        assert_eq!(block_on(task), Some(()));
        assert_eq!(polls.load(StdOrdering::Acquire), 2);
    }

    #[test]
    fn yield_now_tra_thread_lai_cho_lane()
    {
        let pool = pool_with(2);
        let order = StdArc::new(AtomicUsize::new(0));

        let ticket = StdArc::clone(&order);
        let long = spawn(&pool, async move {
            let mut seen = Vec::new();
            for _ in 0..4
            {
                seen.push(ticket.fetch_add(1, StdOrdering::Relaxed));
                yield_now().await;
            }
            seen
        });

        let seen = block_on(long).expect("task nhường lượt không trả kết quả");
        assert_eq!(seen.len(), 4);
    }

    #[test]
    fn task_panic_thi_nguoi_cho_nhan_none()
    {
        let pool = pool_with(2);

        let task = spawn(&pool, async {
            panic!("task này chết giữa chừng");
        });

        assert_eq!(block_on(task), None::<()>);

        // Lane vẫn sống sau cú panic đó.
        let after = spawn(&pool, async { 7u32 });
        assert_eq!(block_on(after), Some(7));
    }

    #[test]
    fn block_on_in_chay_job_trong_luc_cho()
    {
        let pool = pool_with(2);
        let ran = StdArc::new(AtomicUsize::new(0));
        let (tx, rx) = oneshot::<u32>();

        for _ in 0..200
        {
            let ran = StdArc::clone(&ran);
            pool.spawn(move || {
                ran.fetch_add(1, StdOrdering::Relaxed);
            });
        }

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            tx.send(9);
        });

        assert_eq!(block_on_in(&pool, rx), Some(9));
        pool.run_until(|| ran.load(StdOrdering::Acquire) == 200);
        assert_eq!(ran.load(StdOrdering::Acquire), 200);
    }

    /// Trần cứng cho mọi lần chờ trong nhóm test này. Một waker rơi mất thì task ngủ vĩnh viễn, và
    /// nếu không có hạn giờ thì lần chạy test treo luôn chứ không báo hỏng.
    const DEADLINE: Duration = Duration::from_secs(60);

    /// Miri diễn giải từng lệnh một, nên số vòng chạy vài giây trên máy thật sẽ chạy hàng giờ ở đó.
    #[cfg(miri)]
    const SCALE: usize = 50;
    #[cfg(not(miri))]
    const SCALE: usize = 1;

    const fn scaled(n: usize) -> usize
    {
        if n / SCALE == 0 { 1 } else { n / SCALE }
    }

    /// Chờ kết quả nhưng có hạn giờ, xem [`DEADLINE`].
    fn recv_before_deadline<T>(mut rx: Receiver<T>, what: &str) -> Option<T>
    {
        let deadline = std::time::Instant::now() + DEADLINE;
        loop
        {
            match rx.try_recv()
            {
                Ok(value) => return Some(value),
                Err(back) =>
                {
                    if back.is_cancelled()
                    {
                        return None;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "{what}: quá {DEADLINE:?} mà task vẫn chưa xong, có waker bị rơi mất"
                    );
                    rx = back;
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Đúng cái mà ô trạng thái của task sinh ra để chịu: rất nhiều task cùng ngủ, cùng được gọi
    /// dậy từ mấy thread khác nhau, rồi mỗi task còn tự xếp lại vào hàng thêm mấy lượt nữa.
    ///
    /// Số task và số lượt nhường đều là hằng, không phải một vòng gom lớn dần: một test đo tính
    /// đúng đắn không có lý do gì để ăn thêm bộ nhớ theo thời gian chạy.
    #[test]
    fn stress_nhieu_task_cung_ngu_roi_cung_bi_goi_day()
    {
        const TASKS: usize = 200;
        const YIELDS: usize = 16;
        const SENDERS: usize = 4;

        let tasks = scaled(TASKS);
        let pool = pool_with(2);

        let mut senders = Vec::with_capacity(tasks);
        let mut results = Vec::with_capacity(tasks);

        for i in 0..tasks
        {
            let (tx, rx) = oneshot::<usize>();
            senders.push(tx);
            results.push(spawn(&pool, async move {
                let seed = rx.await?;
                // Mỗi lượt nhường là một lần task tự xếp lại vào lane, tức là một lần đi qua đúng
                // cái đường mà waker gọi giữa lúc poll cũng đi.
                for _ in 0..YIELDS
                {
                    yield_now().await;
                }
                Some(seed + i)
            }));
        }

        // Gửi từ nhiều thread một lúc: waker của task bị gọi từ ngoài lane, xen vào giữa lúc lane
        // đang poll những task khác.
        let mut chunks: Vec<Vec<(usize, crate::channel::Sender<usize>)>> = (0..SENDERS).map(|_| Vec::new()).collect();
        for (i, tx) in senders.into_iter().enumerate()
        {
            chunks[i % SENDERS].push((i, tx));
        }

        std::thread::scope(|s| {
            for chunk in chunks
            {
                s.spawn(move || {
                    for (i, tx) in chunk
                    {
                        if i % 7 == 0
                        {
                            std::thread::yield_now();
                        }
                        tx.send(i * 10);
                    }
                });
            }
        });

        for (i, rx) in results.into_iter().enumerate()
        {
            assert_eq!(recv_before_deadline(rx, "stress task"), Some(Some(i * 10 + i)), "task {i} trả sai kết quả");
        }
    }

    #[test]
    fn lane_khong_co_worker_van_chay_duoc_task()
    {
        // `threads: 0` chạy mọi job ngay trên thread gọi, và một task cũng chỉ là một job.
        let pool = pool_with(0);
        let answer = spawn(&pool, async { 1u32 + 1 });
        assert_eq!(block_on(answer), Some(2));
    }
}
