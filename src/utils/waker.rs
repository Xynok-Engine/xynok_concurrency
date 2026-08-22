use crate::sync::thread::{self, Thread};
use crate::sync::{AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;

/// ## The Concept
/// Imagine you are a manager. You assign three tasks to three people, then go take a nap. The only rule is:
/// There is a number 3 written on a whiteboard. Whoever finishes their task subtracts 1 from that number. The person who subtracts to reach 0 has the duty of waking the boss up.
/// That is it. That is the entire Latch. The counter is the whiteboard, and "waking the boss up" is the `unpark` operation.
pub struct Waker
{
    remaining: CachePadded<AtomicUsize>,
    waiter:    Thread,
}
unsafe impl Send for Waker {}
unsafe impl Sync for Waker {}

pub struct LatchSignal<'a>
{
    waker:  &'a Waker,
    waiter: Thread,
}
impl Waker
{
    #[inline]
    pub fn new(worker_amount: usize) -> Self
    {
        Self {
            remaining: CachePadded::new(AtomicUsize::new(worker_amount)),
            waiter:    crate::sync::thread::current(),
        }
    }
    #[inline]
    pub fn ticket(&self) -> LatchSignal<'_>
    {
        LatchSignal {
            waker:  self,
            waiter: self.waiter.clone(),
        }
    }
    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.remaining.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn wait(&self)
    {
        debug_assert!(
            self.waiter.id() == thread::current().id(),
            "this must be called from the same thread that created the `Waker`"
        );
        while self.remaining() > 0
        {
            thread::park();
        }
    }
}

impl Drop for LatchSignal<'_>
{
    fn drop(&mut self)
    {
        if self.waker.remaining.fetch_sub(1, Ordering::Relaxed) == 1
        {
            self.waiter.unpark();
        }
    }
}
#[cfg(test)]
mod test
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    #[test]
    fn fetch_test()
    {
        let total_thread = 4usize;
        let handles: Vec<_> = (0..total_thread)
            .map(|_| {
                thread::spawn(|| {
                    COUNTER.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        for h in handles
        {
            h.join().unwrap();
        }
        assert_eq!(COUNTER.load(Ordering::Relaxed), total_thread);
    }
}
