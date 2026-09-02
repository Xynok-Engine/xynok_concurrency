use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;
use crate::sync::thread::{self, JoinHandle};
use crate::utils::inline_fn::InlineFn;
use crate::worker_pool::thread_meta::WorkerData;
use xynok_std::unsafe_ptr::HeapPtr;

pub struct Worker
{
    pub meta_data: WorkerData,
    pub tasks:     HeapPtr<SpmcRingBufferFifo<InlineFn>>,
}

impl Worker
{
    #[track_caller]
    pub fn new(handle: JoinHandle<()>, task_capacity: usize) -> Self
    {
        let tasks = HeapPtr::new(SpmcRingBufferFifo::<InlineFn>::new(task_capacity));
        Self {
            meta_data: WorkerData::new(handle),
            tasks,
        }
    }

    pub fn update()
    {
        println!("{}: looping !", thread::current().name().unwrap_or("<no_name>"));
        thread::park_timeout(std::time::Duration::from_millis(300));
    }
}
