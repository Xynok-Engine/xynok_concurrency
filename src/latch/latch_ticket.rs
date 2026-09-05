use crate::latch::latch::Latch;
use crate::sync::thread::Thread;
use crate::sync::Ordering;

/// Vé báo "một job đã xong", mang được vào trong một job.
///
/// Trừ bộ đếm khi bị thả, kể cả khi job panic, nên không có đường nào để một job biến mất mà không
/// báo về. Đó cũng là lý do nó dùng `Drop` chứ không phải một hàm `done()` phải nhớ gọi.
pub struct LatchTicket
{
    /// Con trỏ tới latch trên stack của người chờ. Xem phần đầu file về vì sao nó an toàn.
    pub(super) latch:  *const Latch,
    /// Bản sao riêng của handle thread cần gọi dậy.
    ///
    /// Cố ý sao ở đây chứ không đọc từ latch lúc trừ về 0: ngay khi bộ đếm chạm 0, người chờ có
    /// quyền trả về và cái latch trên stack của nó biến mất. Đọc `latch.waiter` sau thời điểm đó là
    /// đọc một khung stack đã bị thu hồi.
    pub(super) waiter: Thread,
}

unsafe impl Send for LatchTicket {}

impl LatchTicket
{
    /// Nhóm việc mà vé này thuộc về đã bị huỷ chưa.
    ///
    /// Đây là đường để một job ngó cờ huỷ, vì vé là thứ duy nhất nó mang theo được. Đọc thấy `true`
    /// thì bỏ qua phần thân và trả vé luôn, đừng quên là vé vẫn phải được thả: người chờ đếm đủ mới
    /// đi, huỷ cũng không thay đổi điều đó.
    #[inline]
    pub fn is_cancelled(&self) -> bool
    {
        // Safety: latch còn sống chừng nào còn vé, xem phần đầu file.
        unsafe { &*self.latch }.is_cancelled()
    }
}

impl Drop for LatchTicket
{
    #[inline]
    fn drop(&mut self)
    {
        // Safety: latch còn sống chừng nào còn vé, xem phần đầu file.
        let latch = unsafe { &*self.latch };
        let previous = latch.remaining.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "latch was decremented more times than tickets were issued");

        if previous == 1
        {
            self.waiter.unpark();
        }
    }
}
