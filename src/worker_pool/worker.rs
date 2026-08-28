use std::thread::JoinHandle;

use xynok_std::unsafe_ptr::HeapPtr;

use crate::collection::ring_buffer::fifo::RingBuffer;
use crate::utils::inline_fn::InlineFn;
use crate::worker_pool::thread_meta::ThreadData;
pub struct Worker
{
    pub meta_data: ThreadData,
    pub tasks:     HeapPtr<RingBuffer<InlineFn>>,
}

impl Worker
{
    #[track_caller]
    pub fn new(handle: JoinHandle<()>, task_capacity: usize) -> Self
    {
        let tasks = HeapPtr::new(RingBuffer::<InlineFn>::new(task_capacity));
        Self {
            meta_data: ThreadData::new(handle),
            tasks,
        }
    }
}
