use crate::sync::Ordering;
use crate::utils::spinlock::spin_lock::SpinLock;

/// Tấm vé chứng minh đang giữ [`SpinLock`], và cũng là đường vào giá trị bên trong.
///
/// Vé rơi khỏi tầm nhìn là khoá tự nhả, nên không có đường nào quên trả khoá. Đừng giữ vé qua một
/// đoạn code có thể chờ đợi: mọi thread khác sẽ quay tại chỗ suốt thời gian đó.
pub struct SpinGuard<'a, T>
{
    pub(super) lock: &'a SpinLock<T>,
}

unsafe impl<T: Sync> Sync for SpinGuard<'_, T> {}

impl<T> Drop for SpinGuard<'_, T>
{
    fn drop(&mut self)
    {
        self.lock.locked.store(false, Ordering::Release);
    }
}

impl<T> std::ops::Deref for SpinGuard<'_, T>
{
    type Target = T;

    fn deref(&self) -> &Self::Target
    {
        self.lock.val.with(|p| unsafe { &*p })
    }
}

impl<T> std::ops::DerefMut for SpinGuard<'_, T>
{
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        self.lock.val.with_mut(|p| unsafe { &mut *p })
    }
}
