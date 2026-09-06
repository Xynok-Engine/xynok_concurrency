use crate::custom_type::Job;
use crate::sync::thread::{self, park_timeout};
use crate::sync::{AtomicBool, Ordering};
use crate::thread_pool::consts::WORKER_SLEEP_DURATION;
use crate::thread_pool::local::THREAD_LOCAL_CTX;
use crate::thread_pool::worker::WorkerSpec;
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;

pub struct ThreadPoolInner
{
    pub id:             u64,
    pub tasks:          QueueBatching<Job>,
    pub workers:        FixedBuffer<WorkerSpec>,
    pub host_index:     usize,
    pub init_completed: CachePadded<AtomicBool>,
    pub is_running:     CachePadded<AtomicBool>,
    pub sleepings:      QueueBatching<usize>,
}

impl ThreadPoolInner
{
    /// teturns only the worker belonging to the current pool
    pub fn current_worker(&self) -> Option<&WorkerSpec>
    {
        let local_ctx = THREAD_LOCAL_CTX.get();
        if local_ctx.pool == self.id
        {
            return Some(unsafe { self.workers.get_at(local_ctx.index) });
        }
        None
    }

    pub fn run_until(&self, done: impl Fn() -> bool)
    {
        let Some(handle) = self.current_worker()
        else
        {
            let mut backoff = Backoff::new();
            while !done()
            {
                match backoff.is_completed()
                {
                    true => park_timeout(WORKER_SLEEP_DURATION),
                    false => backoff.snooze(),
                }
            }
            return;
        };

        let mut tick = 0u64;
        let mut backoff = Backoff::new();
        while !done()
        {
            tick = tick.wrapping_add(1);

            if handle.worker.pop_and_run_a_task()
            {
                backoff.reset();
                continue;
            }

            match backoff.is_completed()
            {
                true =>
                {
                    handle.worker.sleep();
                    backoff.reset();
                }
                false => backoff.snooze(),
            }
        }
    }

    pub fn push(&self, task: Job)
    {
        self.tasks.push(task);
        self.wake_one();
    }

    /// wakes up, joins, and drops all current workers
    pub fn shutdown(inner: &ThreadPoolInner, handles: &mut Vec<thread::JoinHandle<()>>, worker_count: usize)
    {
        inner.is_running.store(false, Ordering::Release);

        for handle in handles.iter()
        {
            handle.thread().unpark();
        }

        for handle in handles.drain(..)
        {
            let _ = handle.join();
        }

        for i in 0..worker_count
        {
            unsafe { inner.workers.drop_at(i) };
        }
    }
}

impl ThreadPoolInner
{
    #[inline]
    fn wake_one(&self)
    {
        if let Some(worker_idx) = self.sleepings.pop()
        {
            let worker = unsafe { self.workers.get_at(worker_idx) };
            worker.thread.unpark();
        }
    }
}
