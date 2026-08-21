use crossbeam::utils::CachePadded;

use crate::consts;
use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicBool, Ordering};

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
        let mut counter = 0u32;
        loop
        {
            if self.try_lock_weak()
            {
                return SpinGuard { lock: self };
            }
            while self.is_locked()
            {
                if counter < consts::SPIN_LIMIT
                {
                    counter += 1;
                    crate::sync::spin_loop();
                }
                else
                {
                    crate::sync::thread::yield_now();
                }
            }
        }
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
