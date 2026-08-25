//! Đầu đọc của [`RingBuffer`]. Sao chép thoải mái, nhiều thread cùng bốc là chuyện bình thường.

use super::RingBuffer;
use crate::sync::Ordering;
use crate::utils::{pack, unpack};

/// Quyền của kẻ trộm. `Copy` vì nhiều worker cùng bốc từ một ring buffer là đường chạy bình thường —
/// `head` đã lo hết phần giành giật.
pub struct Consumer<'a, T>
{
    ring: &'a RingBuffer<T>,
}

// `derive` sẽ đòi `T: Clone`, mà `Consumer` chỉ giữ một tham chiếu nên không cần gì ở `T` cả.
impl<T> Clone for Consumer<'_, T>
{
    #[inline]
    fn clone(&self) -> Self
    {
        *self
    }
}
impl<T> Copy for Consumer<'_, T> {}

impl<'a, T> Consumer<'a, T>
{
    #[inline]
    pub(super) fn new(ring: &'a RingBuffer<T>) -> Self
    {
        Self { ring }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.ring.capacity()
    }

    /// Số job còn bốc được — xem [`RingBuffer::available`]. Đây là con số để chọn "bốc ring buffer nào".
    #[inline]
    pub fn available(&self) -> usize
    {
        self.ring.available()
    }

    /// Không còn gì để bốc. Cặp với [`available`](Self::available) — kẻ trộm không quan tâm tới
    /// vùng đang có người bê, nó chỉ hỏi "còn gì cho tôi không".
    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.ring.is_empty()
    }

    /// Nhịp 1: **dán biển**. Đẩy `real` tiến lên tối đa `max` ô, giữ `steal` tại chỗ.
    ///
    /// Khoảng hở `[steal, real)` sinh ra chính là tấm biển "đang thi công": chủ ring buffer không được ghi
    /// đè lên đó, và kẻ trộm khác nhìn thấy `steal != real` thì bỏ đi.
    ///
    /// Trả về `(start, n)`, hoặc `None` khi ring buffer rỗng **hoặc** đã có kẻ trộm khác đang bê. Không
    /// chờ, không thử lại: đứng đợi ở một ring buffer đang bị bốc thì thà đi tìm ring buffer khác.
    fn claim(&self, max: u32) -> Option<(u32, u32)>
    {
        let mut head = self.ring.head.load(Ordering::Acquire);

        loop
        {
            let (steal, real) = unpack(head);
            if steal != real
            {
                return None; // đã có đứa khác đang bê
            }

            // Đọc `tail` **sau** `head`. Cả hai chỉ tiến, nên `tail` đọc sau luôn ≥ `real` đọc
            // trước và hiệu không bao giờ âm. `Acquire` ghép với `store(Release)` của
            // `Producer::push`: thấy chỉ số mới là chắc chắn thấy cả job đằng sau nó.
            let tail = self.ring.tail.load(Ordering::Acquire);
            let avail = tail.wrapping_sub(real);
            if avail == 0
            {
                return None;
            }

            let n = avail.min(max);
            // `steal` đứng yên tại `real` cũ — đó là điều làm nên tấm biển.
            match self
                .ring
                .head
                .compare_exchange_weak(head, pack(real, real.wrapping_add(n)), Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some((real, n)),
                Err(actual) => head = actual,
            }
        }
    }

    /// Nhịp 2: **gỡ biển**. Chỉ được gọi sau khi đã chép xong toàn bộ vùng đã nhận.
    ///
    /// Gỡ sớm là chủ ring buffer ghi đè lên job đang bê — lỗi mà stress test gần như không bao giờ bắt
    /// được, phải để loom bắt.
    ///
    /// `steal` được kéo thẳng lên bằng `real` **hiện tại**, không phải bằng mốc cuối vùng ta đã
    /// nhận. Vì trong lúc ta chép, [`Producer::pop`](super::Producer::pop) có thể đã đẩy `real` đi
    /// tiếp và tự bê những ô đó đi rồi. Đặt `steal` bằng mốc cũ thì `steal != real` vĩnh viễn: mọi
    /// kẻ trộm sau đó đều tưởng có người đang bê và bỏ đi, còn `push` thì tưởng ring buffer đầy hơn thực
    /// tế — ring buffer kẹt cứng mà không ai báo lỗi.
    ///
    /// Kéo lên `real` hiện tại là đúng, vì `[mốc cũ, real)` đã bị chính chủ ring buffer lấy đi, và chủ ring buffer
    /// chỉ có một thread: nó không thể vừa ở trong `pop` vừa quay ra `push` đè lên.
    fn release(&self)
    {
        let mut head = self.ring.head.load(Ordering::Relaxed);

        loop
        {
            let (_, real) = unpack(head);
            // `Release` chốt mọi lần đọc ô ở trên lại trước thao tác này: chủ ring buffer chỉ thấy `steal`
            // tiến lên sau khi từng byte đã được chép xong.
            match self
                .ring
                .head
                .compare_exchange_weak(head, pack(real, real), Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    /// Bốc đúng một job.
    ///
    /// Trả `None` khi ring buffer rỗng hoặc đang có kẻ trộm khác. Hai lần CAS cho một job là đắt — nếu
    /// chỗ gọi lấy được nhiều hơn một lúc thì dùng [`steal_batch`](Self::steal_batch) hay
    /// [`steal_half`](Self::steal_half), chúng trả cùng chừng đó cho cả lô.
    pub fn steal(&self) -> Option<T>
    {
        let (start, _) = self.claim(1)?;
        // SAFETY: `claim` đã nhận trọn ô `start`; không đường nào khác đọc lại nó.
        let val = self.ring.slot(start).with(|p| unsafe { (*p).assume_init_read() });
        self.release();
        Some(val)
    }

    /// Bốc tới `max` job vào `dst`. Trả về số job lấy được.
    ///
    /// Hai nhịp, và nhịp 2 **bắt buộc** phải sau nhịp chép — xem [`release`](Self::release).
    pub fn steal_batch(&self, dst: &mut Vec<T>, max: usize) -> usize
    {
        if max == 0
        {
            return 0;
        }
        let max = max.min(u32::MAX as usize) as u32;

        let Some((start, n)) = self.claim(max)
        else
        {
            return 0;
        };

        // SAFETY: `claim` đã nhận trọn `[start, start + n)` bằng một lần CAS thành công, và vùng đó
        // chưa được đọc ra lần nào.
        unsafe { self.ring.drain_claimed(start, n, dst) };
        self.release();
        n as usize
    }

    /// Bốc **nửa số job đang có**, làm tròn lên. Trả về số job lấy được.
    ///
    /// Nửa chứ không phải tất cả: bốc sạch thì chủ ring buffer lập tức đói trở lại và một kẻ trộm khác
    /// vừa tới cũng chẳng còn gì — công việc chỉ dồn từ chỗ này sang chỗ kia. Đây là con số mặc
    /// định cho một vòng đi trộm.
    pub fn steal_half(&self, dst: &mut Vec<T>) -> usize
    {
        let avail = self.available();
        match avail
        {
            0 => 0,
            _ => self.steal_batch(dst, avail - avail / 2),
        }
    }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/thief.rs"]
mod test;
