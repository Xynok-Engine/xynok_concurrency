use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicBool, Ordering};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::spinlock::spin_guard::SpinGuard;

/// Khoá xoay bọc quanh một giá trị, dành cho đoạn găng chỉ dài vài lệnh.
///
/// Thread nào không giành được khoá sẽ chờ tại chỗ chứ không nhờ hệ điều hành cho ngủ, nên không
/// mất phí chuyển ngữ cảnh. Đổi lại, giữ khoá lâu là đốt CPU của tất cả những người đang chờ.
pub struct SpinLock<T>
{
    pub(super) val:    UnsafeCell<T>,
    pub(super) locked: CachePadded<AtomicBool>,
}

unsafe impl<T: Send> Send for SpinLock<T> {}
unsafe impl<T: Send> Sync for SpinLock<T> {}

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

    /// Chờ tới khi mượn được giá trị bên trong.
    #[inline]
    pub fn get(&self) -> SpinGuard<'_, T>
    {
        if self.try_lock_weak()
        {
            return SpinGuard { lock: self };
        }
        self.get_contended()
    }

    /// Thử mượn đúng một lần, đang có người giữ thì trả về `None` chứ không chờ.
    #[inline]
    pub fn try_get(&self) -> Option<SpinGuard<'_, T>>
    {
        if self.try_lock()
        {
            return Some(SpinGuard { lock: self });
        }
        None
    }

    /// Mượn thẳng giá trị khi đã cầm tham chiếu độc quyền, khỏi cần đụng tới khoá.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T
    {
        self.val.with_mut(|p| unsafe { &mut *p })
    }

    /// Tháo vỏ khoá ra, lấy lại giá trị bên trong.
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

#[cfg(all(test, loom))]
mod test
{
    use loom::sync::Arc;

    use super::SpinLock;

    #[test]
    fn t0_hai_thread_khong_bao_gio_cung_vao_mot_luc()
    {
        loom::model(|| {
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
