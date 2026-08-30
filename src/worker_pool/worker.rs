use crate::collection::ring_buffer::spmc::SpmcRingBuffer;
use crate::sync::thread::{self, JoinHandle};
use crate::utils::inline_fn::InlineFn;
use crate::worker_pool::thread_meta::ThreadData;
use xynok_std::unsafe_ptr::HeapPtr;

pub struct Worker
{
    pub meta_data: ThreadData,
    pub tasks:     HeapPtr<SpmcRingBuffer<InlineFn>>,
}

impl Worker
{
    #[track_caller]
    pub fn new(handle: JoinHandle<()>, task_capacity: usize) -> Self
    {
        let tasks = HeapPtr::new(SpmcRingBuffer::<InlineFn>::new(task_capacity));
        Self {
            meta_data: ThreadData::new(handle),
            tasks,
        }
    }

    pub fn update()
    {
        println!("{}: looping !", thread::current().name().unwrap_or("<no_name>"));
        thread::park_timeout(std::time::Duration::from_millis(300));
    }
}
