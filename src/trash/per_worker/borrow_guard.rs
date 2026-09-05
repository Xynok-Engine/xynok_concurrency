use crate::sync::{AtomicBool, Ordering};

/// Xoá cờ mượn của một ô, kể cả khi closure nó canh đang unwind.
pub struct BorrowGuard<'a>(pub(crate) &'a AtomicBool);

impl Drop for BorrowGuard<'_>
{
    #[inline]
    fn drop(&mut self)
    {
        self.0.store(false, Ordering::Relaxed);
    }
}
