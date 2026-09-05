use std::mem::MaybeUninit;

use crate::channel::waiter::Waiter;
use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicBool, Mutex, Ordering};

/// Phần chung giữa hai đầu.
pub struct Inner<T>
{
    /// Giá trị đã nằm trong ô chưa. `Release` khi ghi, `Acquire` khi đọc: thấy `true` là thấy luôn
    /// nội dung.
    pub(crate) ready:   AtomicBool,
    /// Đầu gửi đã biến mất mà chưa gửi gì.
    pub(crate) dropped: AtomicBool,
    pub(crate) value:   UnsafeCell<MaybeUninit<T>>,
    /// Người đang chờ, do chính họ ghi vào trước khi ngủ hoặc trước khi trả `Pending`.
    ///
    /// Không chụp sẵn lúc tạo kênh: người tạo kênh và người chờ kết quả không nhất thiết là một, ví
    /// dụ một job dựng kênh rồi đưa đầu nhận cho chỗ khác.
    pub(crate) waiter:  Mutex<Option<Waiter>>,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

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
