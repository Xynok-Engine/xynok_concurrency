use crate::collection::ring_buffer::fifo::RingBuffer;
use crate::utils::inline_fn::InlineFn;
use crate::worker_pool::thread_meta::ThreadData;
use crate::worker_pool::worker::Worker;
pub struct Leader
{
    pub thread_data: ThreadData,
    pub tasks:       RingBuffer<InlineFn>,
    pub workers:     Vec<Worker>,
}

impl Leader
{
    pub fn push(&mut self, task: InlineFn)
    {
        match self.tasks.push(task)
        {
            Ok(_) => todo!(),
            Err(_) => todo!(),
        }
    }

    pub fn update() {}
}
