use crate::latch::latch::Latch;
use crate::sync::Ordering;
use crate::sync::thread::Thread;

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

impl Drop for LatchTicket
{
    #[inline]
    fn drop(&mut self)
    {
        // Safety: latch còn sống chừng nào còn vé, xem phần đầu file.
        let latch = unsafe { &*self.latch };
        let previous = latch.remaining.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "latch bị trừ nhiều hơn số vé đã phát");

        if previous == 1
        {
            // Từ đây trở đi cái latch có thể đã biến mất: người chờ được phép trả về ngay khi nó
            // thấy số 0 vừa được ghi. `self.waiter` là bản sao của chính vé này nên vẫn sống.
            // Không được chạm vào `self.latch` sau dòng trên nữa.
            self.waiter.unpark();
        }
    }
}
