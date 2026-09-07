#![allow(unused)]
use crate::custom_type::Job;
use crate::sync::{thread, AtomicBool, Ordering};
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
    pub sleepers:       QueueBatching<usize>,
    pub total_worker:   usize,
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
    #[inline]
    pub fn total_worker(&self) -> usize
    {
        self.total_worker
    }
    pub fn run_until(&self, done: impl Fn() -> bool)
    {
        let handle = match self.current_worker()
        {
            Some(r) => r,
            None => panic!("`{:?}` is not belong to this thread pool !?", thread::current().name()),
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

            backoff.snooze();
        }
    }
    #[inline]
    pub fn push_no_wake(&self, task: Job)
    {
        self.tasks.push(task);
    }
    pub fn push_and_wake_one(&self, task: Job)
    {
        self.tasks.push(task);
        self.wake_one();
    }

    /// wakes up, joins, and drops all current workers
    pub fn shutdown(&self, handles: &mut Vec<thread::JoinHandle<()>>, worker_count: usize)
    {
        self.is_running.store(false, Ordering::Release);

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
            unsafe { self.workers.drop_at(i) };
        }
    }
    #[inline]
    pub fn wake_one(&self)
    {
        // Always acquire the registration lock, even when the cached length is zero.
        // A worker may still be publishing its registration while holding this lock.
        let worker_idx = self.sleepers.get().pop();
        if let Some(worker_idx) = worker_idx
        {
            let worker = unsafe { self.workers.get_at(worker_idx) };
            worker.thread.unpark();
        }
    }

    pub fn sleep_if_idle(&self, worker_idx: usize)
    {
        let mut sleepers = self.sleepers.get();
        // Publishers enqueue before taking this same lock in wake_one(). If they
        // already notified, we see their work; otherwise they see our registration.
        if !self.is_running.load(Ordering::Acquire) || !self.tasks.is_empty()
        {
            return;
        }
        for i in 0..self.workers.len()
        {
            let worker = unsafe { self.workers.get_at(i) };
            if !worker.worker.tasks.is_empty()
            {
                return;
            }
        }
        sleepers.push(worker_idx);
        drop(sleepers);

        // An unpark between registration and park leaves a token for this call.
        thread::park();

        // park can also return spuriously or consume a token from another caller.
        // Remove any registration wake_one() did not consume before registering again.
        self.sleepers.get().retain(|idx| *idx != worker_idx);
    }
    #[inline]
    pub fn wake_all(&self)
    {
        let mut guard = self.sleepers.get();
        while let Some(worker_idx) = guard.pop()
        {
            let worker = unsafe { self.workers.get_at(worker_idx) };
            worker.thread.unpark();
        }
    }
}
