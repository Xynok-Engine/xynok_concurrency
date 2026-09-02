use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;

use crate::sync::thread::{self, Thread};

/// Người chờ của [`block_on`]: một thread ngủ, và một cờ để phân biệt "đã có ai gọi mình" với một
/// lần `unpark` lạc.
pub struct ParkWaker
{
    thread:           Thread,
    pub(crate) woken: AtomicBool,
}

impl Wake for ParkWaker
{
    fn wake(self: Arc<Self>)
    {
        Wake::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>)
    {
        // Cờ dựng **trước** `unpark`: người chờ có thể chưa park, và lúc đó cái nó đọc được phải là
        // cờ chứ không phải cái token `unpark` để lại.
        self.woken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

impl ParkWaker
{
    pub(crate) fn new() -> Arc<Self>
    {
        Arc::new(Self {
            thread: thread::current(),
            woken:  AtomicBool::new(false),
        })
    }

    /// Đã có ai gọi dậy chưa, và xoá dấu đi để lượt sau đếm lại từ đầu.
    #[inline]
    pub(crate) fn take(&self) -> bool
    {
        self.woken.swap(false, Ordering::Acquire)
    }
}
