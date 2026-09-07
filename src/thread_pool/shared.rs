#![allow(unused)]
use crate::custom_type::Job;
use crate::sync::{AtomicBool, Ordering, thread};
use crate::thread_pool::local::THREAD_LOCAL_CTX;
use crate::thread_pool::worker::{Worker, WorkerSpec};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;
use crate::utils::spinlock::SpinLock;

pub struct ThreadPoolInner
{
    pub id:             u64,
    pub tasks:          QueueBatching<Job>,
    pub workers:        FixedBuffer<WorkerSpec>,
    pub host_index:     usize,
    pub host_owner:     SpinLock<()>,
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
    pub fn run_until(&self, worker: &Worker, done: impl Fn() -> bool)
    {
        let mut tick = 0usize;
        let mut backoff = Backoff::new();
        while !done()
        {
            tick = tick.wrapping_add(1);

            if worker.pop_and_run_a_task() || worker.steal_and_run_a_task(tick)
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

    /// Lock inboxes in index order, assign the entire batch, and only then release any inbox.
    /// Consumers never hold an inbox lock while running a job or acquiring another inbox.
    /// This also lets nested batches publish concurrently without reversing lock order.
    pub fn publish_batch<I: IntoIterator<Item = Job>>(&self, jobs: I, caller: usize)
    {
        debug_assert!(caller < self.workers.len());
        let participants = self.total_worker + usize::from(caller == self.host_index);
        let assign = |push: &mut dyn FnMut(usize, Job)| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                for (index, job) in jobs.into_iter().enumerate()
                {
                    let turn = index % participants;
                    let target = if turn == 0
                    {
                        caller
                    }
                    else if turn <= caller
                    {
                        turn - 1
                    }
                    else
                    {
                        turn
                    };
                    push(target, job);
                }
            }))
        };
        let result = self.with_batch_inboxes(0, &mut |_, _| unreachable!(), &mut Some(assign));
        if let Err(payload) = result
        {
            std::panic::resume_unwind(payload);
        }

        // Use the same registration lock as sleep_if_idle: a worker either observes its inbox
        // before parking, or its registration is consumed here and unpark leaves a wake token.
        self.sleepers.get().retain(|index| {
            let worker = unsafe { self.workers.get_at(*index) };
            if worker.worker.inbox.is_empty()
            {
                true
            }
            else
            {
                worker.thread.unpark();
                false
            }
        });
    }

    // Each stack frame owns one inbox guard. The callback chain routes jobs into those
    // guards without allocating a guard vector. All locks are acquired in index order.
    fn with_batch_inboxes<F>(&self, index: usize, push_previous: &mut dyn FnMut(usize, Job), assign: &mut Option<F>) -> std::thread::Result<()>
    where F: FnOnce(&mut dyn FnMut(usize, Job)) -> std::thread::Result<()>
    {
        if index == self.workers.len()
        {
            return assign.take().unwrap()(push_previous);
        }
        let mut inbox = unsafe { self.workers.get_at(index) }.worker.inbox.get();
        let original_len = inbox.len();
        let result = self.with_batch_inboxes(
            index + 1,
            &mut |target, job| {
                if target == index
                {
                    inbox.push(job);
                }
                else
                {
                    push_previous(target, job);
                }
            },
            assign,
        );
        if result.is_err()
        {
            // Only remove this batch: older queued jobs must survive iterator failure.
            inbox.truncate(original_len);
        }
        result
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
            if !worker.worker.tasks.is_empty() || !worker.worker.inbox.is_empty()
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
