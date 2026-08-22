use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicBool, Ordering};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;

pub struct SpinLock<T>
{
    val:    UnsafeCell<T>,
    locked: CachePadded<AtomicBool>,
}
pub struct SpinGuard<'a, T>
{
    lock: &'a SpinLock<T>,
}
unsafe impl<T: Send> Send for SpinLock<T> {}
unsafe impl<T: Send> Sync for SpinLock<T> {}
//impl<T> !Send for SpinGuard<'_, T> {}
unsafe impl<T: Sync> Sync for SpinGuard<'_, T> {}

impl<T> SpinLock<T>
{
    #[cfg(not(loom))]
    pub const fn new(val: T) -> Self
    {
        Self {
            val:    UnsafeCell::new(val),
            locked: CachePadded::new(AtomicBool::new(false)),
        }
    }
    #[cfg(loom)]
    pub fn new(val: T) -> Self
    {
        Self {
            val:    UnsafeCell::new(val),
            locked: CachePadded::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    pub fn get(&self) -> SpinGuard<'_, T>
    {
        if self.try_lock_weak()
        {
            return SpinGuard { lock: self };
        }
        self.get_contended()
    }

    #[inline]
    pub fn try_get(&self) -> Option<SpinGuard<'_, T>>
    {
        if self.try_lock()
        {
            return Some(SpinGuard { lock: self });
        }
        None
    }

    #[inline]
    pub fn get_mut(&mut self) -> &mut T
    {
        self.val.with_mut(|p| unsafe { &mut *p })
    }

    #[inline]
    pub fn take(self) -> T
    {
        let this = std::mem::ManuallyDrop::new(self);
        this.val.with_mut(|p| unsafe { p.read() })
    }
}

impl<T> SpinLock<T>
{
    #[cold]
    fn get_contended(&self) -> SpinGuard<'_, T>
    {
        let mut backoff = Backoff::new();
        loop
        {
            if !self.is_locked() && self.try_lock_weak()
            {
                return SpinGuard { lock: self };
            }
            backoff.snooze();
        }
    }
    #[inline]
    fn try_lock(&self) -> bool
    {
        self.locked.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }
    #[inline]
    fn try_lock_weak(&self) -> bool
    {
        self.locked.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }
    #[inline]
    fn is_locked(&self) -> bool
    {
        self.locked.load(Ordering::Relaxed)
    }
}

impl<T: Default> Default for SpinLock<T>
{
    fn default() -> Self
    {
        Self::new(T::default())
    }
}
impl<T> From<T> for SpinLock<T>
{
    fn from(value: T) -> Self
    {
        Self::new(value)
    }
}
impl<T> Drop for SpinGuard<'_, T>
{
    fn drop(&mut self)
    {
        self.lock.locked.store(false, Ordering::Release);
    }
}
impl<T> std::ops::DerefMut for SpinGuard<'_, T>
{
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        self.lock.val.with_mut(|p| unsafe { &mut *p })
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

#[cfg(test)]
mod test
{

    #![cfg(loom)]
    use super::SpinLock;
    use loom::sync::Arc;

    #[test]
    fn mutual_exclusion()
    {
        loom::model(|| {
            // ← loom chạy closure này hàng nghìn lần
            let lock = Arc::new(SpinLock::new(0usize));
            let l2 = Arc::clone(&lock);
            let t = loom::thread::spawn(move || {
                *l2.get() += 1;
            });
            *lock.get() += 1;
            t.join().unwrap();
            assert_eq!(*lock.get(), 2);
        });
    }
}
