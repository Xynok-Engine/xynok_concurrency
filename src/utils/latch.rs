use crate::sync::thread::Thread;
use crate::sync::{AtomicBool, AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;

/// ## The Concept
/// Imagine you are a manager. You assign three tasks to three people, then go take a nap. The only rule is:
/// There is a number 3 written on a whiteboard. Whoever finishes their task subtracts 1 from that number. The person who subtracts to reach 0 has the duty of waking the boss up.
/// That is it. That is the entire Latch. The counter is the whiteboard, and "waking the boss up" is the `unpark` operation.
pub struct Latch
{
    remaining: CachePadded<AtomicUsize>,
    locked:    CachePadded<AtomicBool>,
    waiter:    Thread,
}
unsafe impl Send for Latch {}
unsafe impl Sync for Latch {}

pub struct LatchSignal<'a>
{
    latch: &'a Latch,
}
impl Latch
{
    pub fn new(worker_amount: usize) -> Self
    {
        Self {
            remaining: CachePadded::new(AtomicUsize::new(worker_amount)),
            locked:    CachePadded::new(AtomicBool::new(false)),
            waiter:    crate::sync::thread::current(),
        }
    }
    pub fn ticket(&self) -> LatchSignal<'_>
    {
        LatchSignal { latch: self }
    }
}
impl Drop for LatchSignal<'_>
{
    fn drop(&mut self)
    {
        self.latch.remaining.fetch_sub(1, Ordering::Release);
    }
}
