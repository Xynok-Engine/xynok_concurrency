use crate::sync::thread::Thread;
use crate::sync::Ordering;
use crate::utils::waker::waker::Waker;
pub struct WakerSignal<'a>
{
    pub(super) waker:   &'a Waker,
    pub(super) sleeper: Thread,
}

impl Drop for WakerSignal<'_>
{
    fn drop(&mut self)
    {
        if self.waker.remaining.fetch_sub(1, Ordering::Release) == 1
        {
            self.sleeper.unpark();
        }
    }
}
