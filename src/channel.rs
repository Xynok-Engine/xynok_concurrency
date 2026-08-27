//! Kênh một lần: một job gửi đúng một giá trị về cho một người đang chờ.
//!
//! Đây là cách một job trả kết quả **ra khỏi** pool, và cố ý là một kênh chứ không phải giá trị trả
//! về của một lời gọi chặn. Lý do nằm ở [`docs_internal/lanes.md`](../docs_internal/lanes.md) mục
//! 8.2: người gọi vốn đã không giả định "gọi xong là có kết quả" thì đổi nền phía dưới không đụng
//! gì tới họ.
//!
//! Chỗ đó giờ đã được thu về: [`Receiver`] là một [`Future`], nên cùng một cái
//! kênh phục vụ cả ba kiểu chờ. Thread ngoài pool thì [`recv`](Receiver::recv) và ngủ, thread đang
//! đứng trong một lane thì [`recv_in`](Receiver::recv_in) và chạy job giúp lane, còn một task của
//! lane async thì `.await` và không giữ thread nào cả.

use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::pool::ThreadPool;
use crate::sync::cell::UnsafeCell;
use crate::sync::thread::{self, Thread};
use crate::sync::{Arc, AtomicBool, Mutex, Ordering};
use crate::utils::ignore_poison;

/// Phần chung giữa hai đầu.
struct Inner<T>
{
    /// Giá trị đã nằm trong ô chưa. `Release` khi ghi, `Acquire` khi đọc: thấy `true` là thấy luôn
    /// nội dung.
    ready:   AtomicBool,
    /// Đầu gửi đã biến mất mà chưa gửi gì.
    dropped: AtomicBool,
    value:   UnsafeCell<MaybeUninit<T>>,
    /// Người đang chờ, do chính họ ghi vào trước khi ngủ hoặc trước khi trả `Pending`.
    ///
    /// Không chụp sẵn lúc tạo kênh: người tạo kênh và người chờ kết quả không nhất thiết là một, ví
    /// dụ một job dựng kênh rồi đưa đầu nhận cho chỗ khác.
    waiter:  Mutex<Option<Waiter>>,
}

/// Ai đang chờ ở đầu nhận, và gọi họ dậy bằng cách nào.
enum Waiter
{
    /// Một thread đã ghi tên rồi `park`.
    Thread(Thread),
    /// Một task đã trả `Pending`. Waker của nó lo việc xếp task lại vào lane, nên ở đây không cần
    /// biết task ấy sống ở lane nào.
    Task(std::task::Waker),
}

impl Waiter
{
    fn wake(self)
    {
        match self
        {
            Self::Thread(thread) => thread.unpark(),
            Self::Task(waker) => waker.wake(),
        }
    }
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/// Đầu gửi. Gửi được đúng một lần, vì [`Self::send`] nuốt luôn chính nó.
pub struct Sender<T>
{
    inner: Arc<Inner<T>>,
}

/// Đầu nhận. Nhận được đúng một lần.
pub struct Receiver<T>
{
    inner: Arc<Inner<T>>,
}

/// Dựng một kênh một lần.
///
/// ```
/// use xynok_concurrency::channel::oneshot;
/// use xynok_concurrency::pool::{Config, ThreadPool};
///
/// let pool = ThreadPool::new(Config {
///     threads: 2,
///     ..Default::default()
/// });
/// let (tx, rx) = oneshot();
///
/// pool.spawn(move || tx.send(6 * 7));
///
/// assert_eq!(rx.recv_in(&pool), Some(42));
/// ```
pub fn oneshot<T>() -> (Sender<T>, Receiver<T>)
{
    let inner = Arc::new(Inner {
        ready:   AtomicBool::new(false),
        dropped: AtomicBool::new(false),
        value:   UnsafeCell::new(MaybeUninit::uninit()),
        waiter:  Mutex::new(None),
    });

    (Sender { inner: Arc::clone(&inner) }, Receiver { inner: inner })
}

impl<T> Sender<T>
{
    /// Gửi giá trị và đánh thức người đang chờ, nếu có ai chờ.
    ///
    /// Không bao giờ panic, và không bao giờ chặn. Đầu nhận có thể đã biến mất, và khi đó giá trị
    /// nằm lại trong kênh cho tới lúc kênh bị thả: người gửi không có việc gì phải bận tâm chuyện
    /// đó, nhất là khi người gửi là một job đang chạy trong pool.
    pub fn send(self, value: T)
    {
        // Safety: đây là đầu gửi duy nhất và nó chỉ gửi được một lần, nên không ai khác ghi vào ô
        // này; đầu nhận chỉ đọc sau khi thấy `ready`.
        self.inner.value.with_mut(|slot| unsafe { (*slot).write(value) });
        self.inner.ready.store(true, Ordering::Release);

        if let Some(waiter) = ignore_poison(self.inner.waiter.lock()).take()
        {
            waiter.wake();
        }
    }
}

impl<T> Drop for Sender<T>
{
    fn drop(&mut self)
    {
        if self.inner.ready.load(Ordering::Acquire)
        {
            return;
        }

        // Đầu gửi đi mà không gửi gì: người chờ phải biết, không thì họ chờ tới hết đời tiến trình.
        // Chuyện này xảy ra thật khi một job panic giữa chừng.
        self.inner.dropped.store(true, Ordering::Release);

        if let Some(waiter) = ignore_poison(self.inner.waiter.lock()).take()
        {
            waiter.wake();
        }
    }
}

impl<T> Receiver<T>
{
    /// Có kết quả chưa. Không chờ.
    #[inline]
    pub fn is_ready(&self) -> bool
    {
        self.inner.ready.load(Ordering::Acquire)
    }

    /// Đầu gửi đã biến mất mà chưa gửi gì chưa.
    #[inline]
    pub fn is_cancelled(&self) -> bool
    {
        self.inner.dropped.load(Ordering::Acquire) && !self.is_ready()
    }

    /// Lấy kết quả nếu đã có, còn không thì trả về chính đầu nhận để dùng tiếp.
    pub fn try_recv(self) -> Result<T, Self>
    {
        match self.is_ready()
        {
            // Safety: `ready` là `true` nên ô đã được ghi, và đây là đầu nhận duy nhất nên không ai
            // lấy nó trước.
            true => Ok(unsafe { self.take() }),
            false => Err(self),
        }
    }

    /// Chờ kết quả bằng cách chạy job giúp pool.
    ///
    /// Đây là cách chờ đúng khi bạn đang ở trong pool: thread này vẫn là một người tham gia, nên
    /// nằm không là bỏ phí một core, và với join lồng nhau thì còn có thể là chính nó phải chạy cái
    /// job đang được chờ.
    ///
    /// Trả `None` nếu đầu gửi biến mất mà không gửi gì.
    pub fn recv_in(self, pool: &ThreadPool) -> Option<T>
    {
        pool.run_until(|| self.is_ready() || self.is_cancelled());
        self.finish()
    }

    /// Chờ bằng cách ngủ. Dành cho thread không thuộc pool nào.
    ///
    /// Trả `None` nếu đầu gửi biến mất mà không gửi gì.
    pub fn recv(self) -> Option<T>
    {
        loop
        {
            if self.is_ready() || self.is_cancelled()
            {
                return self.finish();
            }

            // Ghi tên rồi kiểm lại: nếu người gửi xong ngay trước lúc mình ghi tên, lần kiểm này
            // thấy; nếu xong sau, họ đọc được tên mình dưới cùng cái khoá và gọi mình dậy. Không có
            // khe nào ở giữa.
            {
                let mut waiter = ignore_poison(self.inner.waiter.lock());
                if self.is_ready() || self.is_cancelled()
                {
                    continue;
                }
                *waiter = Some(Waiter::Thread(thread::current()));
            }

            if self.is_ready() || self.is_cancelled()
            {
                return self.finish();
            }
            thread::park();
        }
    }

    #[inline]
    fn finish(self) -> Option<T>
    {
        match self.is_ready()
        {
            // Safety: như ở `try_recv`.
            true => Some(unsafe { self.take() }),
            false => None,
        }
    }

    /// # Safety
    ///
    /// `ready` phải là `true`, và giá trị chưa bị ai lấy.
    #[inline]
    unsafe fn take(self) -> T
    {
        unsafe { self.take_ref() }
    }

    /// Như [`Self::take`] nhưng không nuốt đầu nhận, vì `poll` chỉ mượn được `&mut Self`.
    ///
    /// Lấy xong thì `ready` về `false`, nên lần lấy thứ hai không tồn tại: nó rơi vào nhánh "chưa
    /// có gì" chứ không đọc lại một ô đã bị move đi.
    ///
    /// # Safety
    ///
    /// `ready` phải là `true`, và giá trị chưa bị ai lấy.
    #[inline]
    unsafe fn take_ref(&self) -> T
    {
        let value = self.inner.value.with_mut(|slot| unsafe { (*slot).assume_init_read() });
        // Đánh dấu ô đã rỗng, để `Drop` của kênh không thả thêm một lần nữa.
        self.inner.ready.store(false, Ordering::Release);
        value
    }
}

/// Chờ kiểu thứ ba: một task async `.await` đầu nhận, và trong lúc chờ nó không giữ thread nào.
///
/// Kết quả là `Option<T>` chứ không phải `T`, giống hệt [`Receiver::recv`]: `None` nghĩa là đầu gửi
/// biến mất mà chưa gửi gì, chuyện xảy ra thật mỗi khi một job hoặc một task panic giữa chừng.
///
/// Poll tiếp sau khi đã `Ready` thì nhận `None`, vì giá trị đã bị lấy đi rồi. Đó là hợp đồng bình
/// thường của `Future`, chỉ là ở đây nó không panic mà trả về một câu trả lời vô hại.
impl<T> Future for Receiver<T>
{
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output>
    {
        if self.is_ready()
        {
            // Safety: `ready` là `true` nên ô đã được ghi, và đây là đầu nhận duy nhất.
            return Poll::Ready(Some(unsafe { self.take_ref() }));
        }
        if self.is_cancelled()
        {
            return Poll::Ready(None);
        }

        // Ghi waker rồi kiểm lại, đúng cái vũ điệu của `recv`: nếu người gửi xong ngay trước lúc
        // mình ghi thì lần kiểm này thấy, còn nếu xong sau thì họ đọc được waker dưới cùng cái khoá
        // và gọi mình dậy. Không có khe nào ở giữa.
        {
            let mut waiter = ignore_poison(self.inner.waiter.lock());
            if !self.is_ready() && !self.is_cancelled()
            {
                *waiter = Some(Waiter::Task(context.waker().clone()));
            }
        }

        if self.is_ready()
        {
            // Safety: như ở nhánh đầu.
            return Poll::Ready(Some(unsafe { self.take_ref() }));
        }
        if self.is_cancelled()
        {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

impl<T> Drop for Inner<T>
{
    fn drop(&mut self)
    {
        // `load` chứ không phải `get_mut`: chỗ này phải dựng được cả dưới `--cfg loom`, mà atomic
        // của loom không có `get_mut`. Ở đây đang giữ `&mut self` nên không ai đua với nó cả.
        if self.ready.load(Ordering::Relaxed)
        {
            // Có giá trị mà không ai lấy: thả nó ở đây, không thì nó rò.
            self.value.with_mut(|slot| unsafe { (*slot).assume_init_drop() });
        }
    }
}

impl<T> std::fmt::Debug for Sender<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("channel::Sender").finish_non_exhaustive()
    }
}

impl<T> std::fmt::Debug for Receiver<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("channel::Receiver").field("ready", &self.is_ready()).finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};

    use super::*;
    use crate::pool::Config;

    fn pool_with(threads: usize) -> ThreadPool
    {
        ThreadPool::new(Config {
            threads: threads,
            ..Config::default()
        })
    }

    #[test]
    fn gia_tri_di_tu_job_ve_nguoi_cho()
    {
        let pool = pool_with(2);
        let (tx, rx) = oneshot();

        pool.spawn(move || tx.send(42u32));
        assert_eq!(rx.recv_in(&pool), Some(42));
    }

    #[test]
    fn thread_ngoai_pool_ngu_cho_ket_qua()
    {
        let pool = pool_with(2);
        let (tx, rx) = oneshot();

        pool.spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            tx.send("xong".to_string());
        });

        assert_eq!(rx.recv().as_deref(), Some("xong"));
    }

    #[test]
    fn ket_qua_toi_truoc_khi_ai_cho_thi_khong_mat()
    {
        let pool = pool_with(1);
        let (tx, rx) = oneshot();

        tx.send(7u8);
        std::thread::sleep(std::time::Duration::from_millis(5));

        assert!(rx.is_ready());
        assert_eq!(rx.recv_in(&pool), Some(7));
    }

    #[test]
    fn try_recv_tra_lai_dau_nhan_khi_chua_co_gi()
    {
        let (tx, rx) = oneshot::<u8>();

        let rx = rx.try_recv().expect_err("chưa gửi mà đã có kết quả");
        tx.send(3);
        assert_eq!(rx.try_recv().ok(), Some(3));
    }

    #[test]
    fn dau_gui_bien_mat_thi_nguoi_cho_khong_treo()
    {
        let pool = pool_with(2);
        let (tx, rx) = oneshot::<u32>();

        pool.spawn(move || {
            // Thả đầu gửi mà không gửi gì, đúng như một job panic giữa chừng.
            drop(tx);
        });

        assert_eq!(rx.recv_in(&pool), None);
    }

    #[test]
    fn job_panic_thi_nguoi_cho_nhan_duoc_none()
    {
        let pool = pool_with(2);
        let (tx, rx) = oneshot::<u32>();

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        pool.spawn(move || {
            let _tx = tx;
            panic!("job chết trước khi gửi");
        });

        let got = rx.recv_in(&pool);
        std::panic::set_hook(previous);
        assert_eq!(got, None);
    }

    #[test]
    fn khong_ai_lay_thi_gia_tri_van_duoc_tha()
    {
        let tracker = StdArc::new(AtomicUsize::new(0));

        struct Tracked(StdArc<AtomicUsize>);
        impl Drop for Tracked
        {
            fn drop(&mut self)
            {
                self.0.fetch_add(1, StdOrdering::Release);
            }
        }

        {
            let (tx, rx) = oneshot();
            tx.send(Tracked(StdArc::clone(&tracker)));
            drop(rx);
        }

        assert_eq!(tracker.load(StdOrdering::Acquire), 1, "giá trị không ai lấy mà cũng không được thả");
    }

    #[test]
    fn await_kenh_da_co_ket_qua_thi_khong_can_ai_goi_day()
    {
        let (tx, rx) = oneshot();
        tx.send(42u32);

        // Kết quả tới trước cả lần poll đầu tiên: `poll` phải trả `Ready` ngay, không ghi waker nào,
        // vì sẽ không còn ai gọi nó nữa.
        assert_eq!(crate::task::block_on(rx), Some(42));
    }

    #[test]
    fn await_dau_gui_bien_mat_thi_nhan_none()
    {
        let pool = pool_with(2);
        let (tx, rx) = oneshot::<u32>();

        pool.spawn(move || drop(tx));
        assert_eq!(crate::task::block_on(rx), None);
    }

    #[test]
    fn nhieu_kenh_chay_song_song_khong_lan_ket_qua()
    {
        const JOBS: usize = 500;

        let pool = pool_with(4);
        let mut receivers = Vec::with_capacity(JOBS);

        for job in 0..JOBS
        {
            let (tx, rx) = oneshot();
            pool.spawn(move || tx.send(job * 3));
            receivers.push(rx);
        }

        for (job, rx) in receivers.into_iter().enumerate()
        {
            assert_eq!(rx.recv_in(&pool), Some(job * 3), "kênh của job {job} nhận nhầm kết quả");
        }
    }
}
