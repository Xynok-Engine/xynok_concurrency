use crate::latch::latch_ticket::LatchTicket;
use crate::pool::ThreadPool;
use crate::sync::thread::{self, Thread};
use crate::sync::{AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;

/// Một bộ đếm ngược, gọi dậy đúng một người khi về 0.
pub struct Latch
{
    /// Job còn chưa báo về. 0 nghĩa là xong hết.
    pub(super) remaining: CachePadded<AtomicUsize>,
    /// Thread cần gọi dậy, chụp lại lúc dựng latch.
    ///
    /// Người chờ thường không ngủ mà đi chạy job giúp pool ([`Self::wait_in`]), nên handle này chỉ
    /// dùng tới ở đường [`Self::wait`]: thread không thuộc pool nào thì không có việc để chạy giúp,
    /// ngủ hẳn còn hơn quay tại chỗ đốt một core.
    waiter:               Thread,
}

unsafe impl Send for Latch {}
unsafe impl Sync for Latch {}

impl Latch
{
    /// Latch chờ `count` lần báo về. `0` là hợp lệ và làm mọi lần chờ trả về ngay.
    ///
    /// Phải dựng trên chính thread sẽ chờ, vì đó là thread duy nhất mà vé biết cách gọi dậy.
    pub fn new(count: usize) -> Self
    {
        Self {
            remaining: CachePadded::new(AtomicUsize::new(count)),
            waiter:    thread::current(),
        }
    }

    /// Ghi thêm một job vào sổ và trả về vé của nó.
    ///
    /// Cộng **trước** khi job có cơ hội chạy. Cộng sau thì có một khoảnh khắc bộ đếm bằng 0 trong
    /// khi job vẫn đang bay, và người chờ đọc đúng lúc đó sẽ bỏ đi trong lúc job còn cầm tham chiếu
    /// tới stack của nó.
    #[inline]
    pub fn ticket(&self) -> LatchTicket
    {
        self.remaining.fetch_add(1, Ordering::Relaxed);
        LatchTicket {
            latch:  self as *const Latch,
            waiter: self.waiter.clone(),
        }
    }

    /// Số job còn chưa báo về. Ảnh chụp, dùng cho counter và log.
    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.remaining.load(Ordering::Acquire)
    }

    /// Đã xong hết chưa. Đây là thứ để đưa cho [`ThreadPool::run_until`].
    #[inline]
    pub fn is_done(&self) -> bool
    {
        self.remaining() == 0
    }

    /// Chờ bằng cách chạy job giúp pool, thay vì nằm không.
    ///
    /// Đây là đường mà mọi điểm join bên trong pool đi. Thread đang chờ vẫn là một người tham gia
    /// pool, nên nếu nó ngủ thì pool mất một core đúng lúc đang cần nhất, và với join lồng nhau thì
    /// còn tệ hơn: thread ngủ có thể chính là thread lẽ ra phải chạy cái job mà nó đang chờ.
    #[inline]
    pub fn wait_in(&self, pool: &ThreadPool)
    {
        pool.run_until(|| self.is_done());
    }

    /// Chờ bằng cách ngủ. Dành cho thread không thuộc pool nào.
    ///
    /// # Panics
    ///
    /// Nếu gọi từ thread khác với thread đã dựng latch. Đó là thread duy nhất mà vé biết cách gọi
    /// dậy, nên chờ ở chỗ khác là park một thread không ai đánh thức. Kiểm cả trong bản release:
    /// đổi một tiến trình đứng im lấy một stack trace là món hời.
    pub fn wait(&self)
    {
        assert_eq!(
            thread::current().id(),
            self.waiter.id(),
            "Latch::wait phải chạy trên chính thread đã dựng latch, đó là thread duy nhất mà vé biết cách gọi dậy"
        );

        // `park` được phép trả về vu vơ, và một `unpark` tới sớm cũng làm nó trả về ngay. Cả hai
        // đều nói rằng chỉ có bộ đếm mới đáng tin, nên đọc lại nó mỗi vòng.
        while !self.is_done()
        {
            thread::park();
        }
    }
}

impl std::fmt::Debug for Latch
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("Latch").field("remaining", &self.remaining()).finish()
    }
}
