use crate::utils::ignore_poison;
use std::fmt;
use std::sync::{Condvar, Mutex, MutexGuard, TryLockError, WaitTimeoutResult};
use std::time::Duration;

pub struct MutexCondition<T>
{
    val:    Mutex<T>,
    signal: Condvar,
}

impl<T> MutexCondition<T>
{
    #[inline]
    pub const fn new(val: T) -> Self
    {
        Self {
            val:    Mutex::new(val),
            signal: Condvar::new(),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T
    {
        ignore_poison(self.val.into_inner())
    }

    #[track_caller]
    #[inline]
    pub fn get(&self) -> MutexGuard<'_, T>
    {
        ignore_poison(self.val.lock())
    }

    #[inline]
    pub fn try_get(&self) -> Option<MutexGuard<'_, T>>
    {
        match self.val.try_lock()
        {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(err)) => Some(err.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    #[inline]
    pub fn get_mut(&mut self) -> &mut T
    {
        ignore_poison(self.val.get_mut())
    }

    #[track_caller]
    #[inline]
    pub fn wait<'a>(&'a self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T>
    {
        ignore_poison(self.signal.wait(guard))
    }

    #[track_caller]
    #[inline]
    pub fn wait_while<'a, F>(&'a self, guard: MutexGuard<'a, T>, condition: F) -> MutexGuard<'a, T>
    where F: FnMut(&mut T) -> bool
    {
        ignore_poison(self.signal.wait_while(guard, condition))
    }

    #[track_caller]
    #[inline]
    pub fn wait_timeout<'a>(&'a self, guard: MutexGuard<'a, T>, dur: Duration) -> (MutexGuard<'a, T>, WaitTimeoutResult)
    {
        ignore_poison(self.signal.wait_timeout(guard, dur))
    }

    #[track_caller]
    #[inline]
    pub fn wait_timeout_while<'a, F>(&'a self, guard: MutexGuard<'a, T>, dur: Duration, condition: F) -> (MutexGuard<'a, T>, WaitTimeoutResult)
    where F: FnMut(&mut T) -> bool
    {
        ignore_poison(self.signal.wait_timeout_while(guard, dur, condition))
    }

    #[inline]
    pub fn notify_one(&self)
    {
        self.signal.notify_one();
    }

    #[inline]
    pub fn notify_all(&self)
    {
        self.signal.notify_all();
    }
}

impl<T: Default> Default for MutexCondition<T>
{
    #[inline]
    fn default() -> Self
    {
        Self::new(T::default())
    }
}

impl<T> From<T> for MutexCondition<T>
{
    #[inline]
    fn from(val: T) -> Self
    {
        Self::new(val)
    }
}

impl<T: fmt::Debug> fmt::Debug for MutexCondition<T>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        let mut out = f.debug_struct("MutexChannel");
        match self.try_get()
        {
            Some(guard) => out.field("val", &*guard),
            None => out.field("val", &format_args!("<locked>")),
        };
        out.finish()
    }
}
