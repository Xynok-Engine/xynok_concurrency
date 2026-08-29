use std::thread::JoinHandle;

use xynok_std::unsafe_ptr::HeapPtr;

use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;
use crate::utils::inline_fn::InlineFn;
use crate::worker_pool::thread_meta::ThreadData;
pub struct Worker
{
    pub meta_data: ThreadData,
    pub tasks:     HeapPtr<SpmcRingBufferFifo<InlineFn>>,
}

impl Worker
{
    #[track_caller]
    pub fn new(handle: JoinHandle<()>, task_capacity: usize) -> Self
    {
        let tasks = HeapPtr::new(SpmcRingBufferFifo::<InlineFn>::new(task_capacity));
        Self {
            meta_data: ThreadData::new(handle),
            tasks,
        }
    }
}
