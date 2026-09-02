use crate::sync::thread::{self, Thread};
use crate::sync::{AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::waker::waker_signal::WakerSignal;

/// ## The Concept
/// Imagine you are a manager. You assign three tasks to three people, then go take a nap. The only rule is:
/// There is a number 3 written on a whiteboard. Whoever finishes their task subtracts 1 from that number. The person who subtracts to reach 0 has the duty of waking the boss up.
/// That is it. That is the entire Latch. The counter is the whiteboard, and "waking the boss up" is the `unpark` operation.
pub struct Waker
{
    pub(super) remaining: CachePadded<AtomicUsize>,
    waiter:               Thread,
}

unsafe impl Send for Waker {}
unsafe impl Sync for Waker {}

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

    /// Phát ra một tấm vé cho một việc. Vé rơi khỏi tầm nhìn là con số giảm một.
    #[inline]
    pub fn ticket(&self) -> WakerSignal<'_>
    {
        WakerSignal {
            waker:   self,
            sleeper: self.waiter.clone(),
        }
    }

    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.remaining.load(Ordering::Acquire)
    }

    /// Ngủ cho tới khi mọi vé đã được bỏ đi.
    ///
    /// Gọi lại lần nữa sau khi đã mở thì trả về ngay, không ngủ thêm.
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
