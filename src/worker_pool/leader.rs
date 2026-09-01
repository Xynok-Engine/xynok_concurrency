use std::thread::JoinHandle;

use xynok_std::unsafe_ptr::HeapPtr;

use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;
use crate::utils::inline_fn::InlineFn;
use crate::worker_pool::thread_meta::ThreadData;
pub struct Leader
{
    pub meta_data: ThreadData,
    pub tasks:     HeapPtr<SpmcRingBufferFifo<InlineFn>>,
}

impl Leader
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

    pub fn push(&mut self, task: InlineFn) -> Result<(), InlineFn>
    {
        self.tasks.push(task)
    }
}
