use xynok_std::unsafe_ptr::HeapMut;

use crate::apis::priority::Priority;
use crate::thread_pool::ThreadPoolInner;
use crate::thread_pool::worker::Worker;

#[derive(Clone, Copy)]
pub struct ParamsWorker
{
    pub priority: Priority,
    pub worker:   HeapMut<Worker>,
    pub root:     HeapMut<ThreadPoolInner>,
}
