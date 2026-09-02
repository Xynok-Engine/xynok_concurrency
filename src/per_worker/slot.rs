use crate::sync::AtomicBool;
use crate::sync::cell::UnsafeCell;

/// Ô của một thread, cộng cái cờ bắt lỗi mượn hai lần.
pub struct Slot<T>
{
    /// Chủ của ô này có đang ở trong [`PerWorker::with`] hay không.
    ///
    /// Chỉ đúng một thread ghi nó, nên hai lần chạm ở đây không cần gộp thành một thao tác atomic.
    /// Nó là [`AtomicBool`] là vì **người đọc**: [`PerWorker::for_each_unchecked`] soi cờ của mọi ô
    /// từ thread host, mà đọc một `Cell` do thread khác ghi là data race bất kể giá trị đọc ra là
    /// gì.
    pub(crate) borrowed: AtomicBool,
    pub(crate) value:    UnsafeCell<T>,
}

impl<T> Slot<T>
{
    #[inline]
    pub fn new(value: T) -> Self
    {
        Self {
            borrowed: AtomicBool::new(false),
            value:    UnsafeCell::new(value),
        }
    }
}
