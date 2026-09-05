use crate::sync::Ordering;
use crate::utils::spinlock::spin_lock::SpinLock;

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
