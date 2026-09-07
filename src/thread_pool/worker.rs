use xynok_std::unsafe_ptr::{HeapMut, HeapPtr};

use crate::custom_type::Job;
use crate::sync::Ordering;
use crate::sync::thread::Thread;
use crate::thread_pool::local::{Context, THREAD_LOCAL_CTX};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::shared::ThreadPoolInner;
use crate::thread_pool::worker_queue::WorkerQueue;
use crate::utils::backoff::Backoff;
use crate::utils::queue_batching::QueueBatching;
use crate::utils::steal::Steal;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub struct Worker
{
    pub tasks: WorkerQueue<Job>,
    /// Batch publishers and thieves use this synchronized queue. Only this worker may
    /// push to its single-producer local deque above.
    pub inbox: QueueBatching<Job>,
    idx:       usize,
    root:      HeapMut<ThreadPoolInner>,
}

pub struct WorkerSpec
{
    pub thread: Thread,
    pub worker: HeapPtr<Worker>,
}

impl Worker
{
    pub fn new(task_size: usize, root: HeapMut<ThreadPoolInner>, idx: usize) -> Self
    {
        Self {
            tasks: WorkerQueue::new(task_size),
            inbox: QueueBatching::with_capacity(task_size),
            root:  root,
            idx:   idx,
        }
    }

    pub fn index(&self) -> usize
    {
        self.idx
    }

    #[inline]
    pub fn pop_and_run_a_task(&self) -> bool
    {
        if let Some(task) = self.tasks.pop().or_else(|| self.inbox.pop())
        {
            run_task(task);
            return true;
        }
        false
    }

    /// Called only by this deque's owner, after its local and assigned work have run out.
    /// Check every victim before backing off so an empty queue does not delay discovery
    /// of work on the next worker. Rotate the starting point to avoid a fixed hot victim.
    pub fn steal_and_run_a_task(&self, tick: usize) -> bool
    {
        if self.tasks.push_batch_by_taking_from_queue(self.tasks.capacity(), &self.root.tasks) > 0 && self.pop_and_run_a_task()
        {
            return true;
        }

        let count = self.root.workers.len();
        let start = tick.wrapping_add(self.idx) % count;
        for offset in 0..count
        {
            let index = (start + offset) % count;
            if index == self.idx
            {
                continue;
            }
            let victim = &unsafe { self.root.workers.get_at(index) }.worker;
            // Do not block behind a batch publisher. Drop the guard before invoking user code,
            // which can itself publish a nested batch.
            let assigned = if !victim.inbox.is_empty()
            {
                victim.inbox.try_get().and_then(|mut inbox| inbox.pop())
            }
            else
            {
                None
            };
            if let Some(task) = assigned
            {
                run_task(task);
                return true;
            }

            let batch = victim.tasks.len().div_ceil(2).max(1);
            if let Steal::Success(_) = victim.tasks.try_steal_batch_to(batch, &self.tasks)
            {
                // Another thief may take the transferred jobs before we pop. In that case
                // keep searching; no job was lost and no foreign producer writes our deque.
                if self.pop_and_run_a_task()
                {
                    return true;
                }
            }
        }
        false
    }

    pub fn update(params: ParamsWorker)
    {
        params.priority.apply_to_current_thread();

        let mut backoff = Backoff::new();
        // warmup
        loop
        {
            if !params.root.is_running.load(Ordering::Acquire)
            {
                return;
            }
            if params.root.init_completed.load(Ordering::Acquire)
            {
                break;
            }
            backoff.snooze();
        }
        backoff.reset();

        // update local data
        THREAD_LOCAL_CTX.set(Context {
            pool:  params.root.id,
            index: params.worker.idx,
        });

        let mut tick = 0usize;

        while params.root.is_running.load(Ordering::Acquire)
        {
            if params.worker.pop_and_run_a_task() || params.worker.steal_and_run_a_task(tick)
            {
                backoff.reset();
            }
            else if backoff.is_completed()
            {
                backoff.reset();
                params.worker.sleep();
            }
            else
            {
                backoff.snooze();
            }
            tick = tick.wrapping_add(1);
        }
    }
}

impl Worker
{
    #[inline]
    fn sleep(&self)
    {
        self.root.sleep_if_idle(self.idx);
    }
}

/// Runs one task and keeps a panic from escaping the worker loop.
///
/// A task handed to `push` has no scope to carry a panic back to, so letting it unwind would take
/// the whole worker thread down with it, and every task already sitting in that worker's local
/// deque would go with it. On a single core machine that is the only worker there is, so the pool
/// would be dead for good. `scope` jobs catch their own panic and hand it back to the waiting
/// thread, so this only ever fires for `push`.
///
/// The payload is dropped on purpose: the panic hook has already printed the message and the
/// backtrace by the time unwinding starts, and there is nobody left to hand it to.
#[inline]
fn run_task(task: Job)
{
    let _ = catch_unwind(AssertUnwindSafe(move || task.run_once()));
}
