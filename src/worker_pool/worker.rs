use crate::collection::ring_buffer::spmc_lifo_produce_fifo_consume::SpmcRingBufferLifoProduceFifoConsume;
use crate::custom_type::Job;
use crate::sync::thread::{self, JoinHandle};
use crate::worker_pool::thread_meta::WorkerHandle;
use xynok_std::unsafe_ptr::HeapPtr;

pub struct Worker
{
    pub meta_data: WorkerHandle,
    pub tasks:     HeapPtr<SpmcRingBufferLifoProduceFifoConsume<Job>>,
}

impl Worker
{
    #[track_caller]
    pub fn new(handle: JoinHandle<()>, task_capacity: usize) -> Self
    {
        let tasks = HeapPtr::new(SpmcRingBufferLifoProduceFifoConsume::<Job>::new(task_capacity));
        Self {
            meta_data: WorkerHandle::new(handle),
            tasks,
        }
    }

    pub fn update()
    {
        println!("{}: looping !", thread::current().name().unwrap_or("<no_name>"));
        thread::park_timeout(std::time::Duration::from_millis(300));
    }
}
