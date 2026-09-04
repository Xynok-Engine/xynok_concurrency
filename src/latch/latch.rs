use crate::latch::latch_ticket::LatchTicket;
use crate::pool::ThreadPool;
use crate::sync::thread::{self, Thread};
use crate::sync::{AtomicBool, AtomicUsize, Ordering};
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
    /// "Nhóm việc này thôi, đừng làm nữa." Job chưa chạy đọc thấy cờ này thì bỏ qua phần thân.
    ///
    /// Cờ chỉ nói về *phần thân* của job, nó không đụng gì tới bộ đếm. Xem [`Self::cancel`].
    cancelled:            CachePadded<AtomicBool>,
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
            cancelled: CachePadded::new(AtomicBool::new(false)),
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

    /// Báo cho cả nhóm rằng phần việc này không cần làm nữa.
    ///
    /// Job nào chưa chạy mà đọc thấy cờ (qua [`LatchTicket::is_cancelled`]) thì bỏ qua phần thân
    /// của mình. Job đang chạy dở không bị cắt ngang, ở đây không có ai đi giết thread cả, muốn
    /// dừng sớm thì chính thân job phải tự ngó cờ ở những chỗ ngắt được.
    ///
    /// Trả `true` nếu chính lần gọi này là lần bật cờ. Dùng cho kiểu "người đầu tiên thắng": ai
    /// nhận `true` thì được quyền ghi lý do huỷ vào chỗ dùng chung, khỏi phải thêm một cái khoá
    /// nữa chỉ để tranh xem lỗi nào được giữ lại.
    ///
    /// # Huỷ không rút ngắn lệnh chờ
    ///
    /// [`Self::wait`] và [`Self::wait_in`] vẫn nằm đó tới khi bộ đếm về 0, huỷ hay không cũng vậy.
    /// Đây không phải chỗ để tối ưu thêm: bảng đếm nằm trên ngăn xếp của người chờ, vé thì cầm con
    /// trỏ thô tới nó, nên người chờ mà bỏ đi sớm là để lại một đống vé trỏ vào khung ngăn xếp đã
    /// bị thu hồi. Cái huỷ mua được là các job còn xếp hàng trả vé gần như tức thì thay vì chạy
    /// hết phần việc của chúng.
    #[inline]
    pub fn cancel(&self) -> bool
    {
        // `AcqRel` chứ không phải `Relaxed`: người bật cờ thường vừa ghi xong lý do huỷ ở đâu đó,
        // và job đọc thấy cờ phải nhìn thấy luôn cả những gì ghi trước đó.
        !self.cancelled.swap(true, Ordering::AcqRel)
    }

    /// Nhóm việc này đã bị huỷ chưa.
    #[inline]
    pub fn is_cancelled(&self) -> bool
    {
        self.cancelled.load(Ordering::Acquire)
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
