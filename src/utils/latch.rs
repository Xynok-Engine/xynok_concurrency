use crate::sync::{AtomicBool, AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;
pub struct Latch
{
    remaining: CachePadded<AtomicUsize>,
    canceled:  CachePadded<AtomicBool>,
}
unsafe impl Send for Latch {}

pub struct LatchTicket<'a>
{
    latch: &'a Latch,
}
impl Default for Latch
{
    fn default() -> Self
    {
        Self::new()
    }
}
impl Latch
{
    pub fn new() -> Self
    {
        Self {
            remaining: CachePadded::new(AtomicUsize::new(1)),
            canceled:  CachePadded::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    pub fn ticket(&self) -> LatchTicket<'_>
    {
        self.remaining.fetch_add(1, Ordering::Relaxed);
        LatchTicket { latch: self }
    }

    /// returns true if `canceled` is currently false, and then CAS it to true
    #[cold]
    pub fn try_cancel(&self) -> bool
    {
        self.canceled.compare_exchange_weak(false, true, Ordering::Release, Ordering::Acquire).is_ok()
    }
    #[inline]
    pub fn cancel(&self)
    {
        if self.is_canceled()
        {
            return;
        }
        self.cas_cancel(false, true);
    }
    #[inline]
    pub fn is_canceled(&self) -> bool
    {
        self.canceled.load(Ordering::Acquire)
    }
    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.remaining.load(Ordering::Acquire)
    }
    #[inline]
    pub fn is_completed(&self) -> bool
    {
        self.remaining.load(Ordering::Acquire) <= 1
    }
}

impl Latch
{
    #[cold]
    fn cas_cancel(&self, mut current: bool, next: bool)
    {
        loop
        {
            match self.canceled.compare_exchange_weak(current, next, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => break,

                Err(r) =>
                {
                    if r == next
                    {
                        return;
                    }
                    current = r;
                }
            }
        }
    }
}
impl<'a> LatchTicket<'a>
{
    #[inline]
    pub fn is_canceled(&self) -> bool
    {
        self.latch.is_canceled()
    }
}
impl<'a> Drop for LatchTicket<'a>
{
    fn drop(&mut self)
    {
        let previous = self.latch.remaining.fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0, "latch was decremented more times than tickets were issued");
    }
}
