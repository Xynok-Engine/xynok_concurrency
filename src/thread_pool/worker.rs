use xynok_std::unsafe_ptr::{HeapMut, HeapPtr};

use crate::custom_type::Job;
use crate::sync::thread::{park, Thread};
use crate::sync::Ordering;
use crate::thread_pool::local::{Context, THREAD_LOCAL_CTX};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::shared::ThreadPoolInner;
use crate::thread_pool::worker::WorkerState::Stealing;
use crate::thread_pool::worker_queue::WorkerQueue;
use crate::utils::backoff::Backoff;
use crate::utils::random::Random;
use crate::utils::steal::Steal;
use std::panic::{catch_unwind, AssertUnwindSafe};

pub struct Worker
{
    pub tasks: WorkerQueue<Job>,
    idx:       usize,
    root:      HeapMut<ThreadPoolInner>,
}

pub struct WorkerSpec
{
    pub thread: Thread,
    pub worker: HeapPtr<Worker>,
}

enum WorkerState
{
    Idle,
    Stealing(Option<usize>),
}
struct UpdateData
{
    total_worker: usize,
    tick:         u64,
}
impl UpdateData
{
    pub fn new(params: &ParamsWorker) -> Self
    {
        Self {
            tick:         0,
            total_worker: params.root.workers.len(),
        }
    }
}
impl Worker
{
    pub fn new(task_size: usize, root: HeapMut<ThreadPoolInner>, idx: usize) -> Self
    {
        Self {
            tasks: WorkerQueue::new(task_size),
            root:  root,
            idx:   idx,
        }
    }

    #[inline]
    pub fn pop_and_run_a_task(&self) -> bool
    {
        if let Some(task) = self.tasks.pop()
        {
            run_task(task);
            return true;
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

        let mut update_data = UpdateData::new(&params);
        let mut state = WorkerState::Idle;

        // real update lofic
        while params.root.is_running.load(Ordering::Acquire)
        {
            let next_state = match state
            {
                WorkerState::Idle => drain_local_task(params),
                WorkerState::Stealing(r) => steal(params, &mut update_data, r),
            };

            // `Idle` means we just picked up a task, while `Stealing` means we came back empty-handed.
            // The more times we come back empty-handed, the longer we wait before trying again. Once we hit the limit, we put the thread to sleep.
            match next_state
            {
                WorkerState::Idle => backoff.reset(),

                // If we steal too many times, we should sleep
                WorkerState::Stealing(_) => match backoff.is_completed()
                {
                    true =>
                    {
                        params.worker.sleep();
                        backoff.reset();
                    }
                    false => backoff.snooze(),
                },
            }

            state = next_state;
            update_data.tick = update_data.tick.wrapping_add(1);
        }
    }

    pub fn sleep(&self)
    {
        self.root.sleepings.push(self.idx);
        if self.root.tasks.is_empty() && self.root.is_running.load(Ordering::Acquire)
        {
            // hmm, we should park instead of park, more detail at issue #6
            park();
        }
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

fn drain_local_task(params: ParamsWorker) -> WorkerState
{
    while let Some(task) = params.worker.tasks.pop()
    //if let Some(task) = params.worker.tasks.pop()
    {
        run_task(task);
    }

    WorkerState::Stealing(None)
}
fn steal(params: ParamsWorker, update_data: &mut UpdateData, last_stealing: Option<usize>) -> WorkerState
{
    let worker = params.worker;

    match last_stealing
    {
        Some(steal_idx) => steal_from(params, update_data, steal_idx),
        None =>
        {
            // when stealing from the root, the worker attempts to steal all available tasks to fill its own queue
            if worker.tasks.push_batch_by_taking_from_queue(worker.tasks.capacity(), &params.root.tasks) > 0
            {
                return WorkerState::Idle;
            }

            let mut rnd = Random::new(update_data.tick ^ worker.idx as u64);
            let steal_idx = rnd.below(update_data.total_worker as u64) as usize;
            steal_from(params, update_data, steal_idx)
        }
    }
}

/// attempt to steal a batch of tasks from the `steal_idx` worker's deque, and decide on the next move based on the result
fn steal_from(params: ParamsWorker, update_data: &mut UpdateData, steal_idx: usize) -> WorkerState
{
    let worker = params.worker;

    // if the source and destination are the same ring, the cursor state will become corrupted.
    // Revert to polling the shared queue and pick up a different task.
    if steal_idx == worker.idx
    {
        return Stealing(None);
    }

    // when stealing from another worker, we only attempt to take half of the queue
    let batch = (worker.tasks.capacity() / 2).max(1);

    let other = unsafe { params.root.workers.get_at(steal_idx) };
    match other.worker.tasks.try_steal_batch_to(batch, &worker.tasks)
    {
        Steal::Empty => WorkerState::Stealing(Some(next_victim_idx(steal_idx, update_data.total_worker))),
        Steal::Busy => WorkerState::Stealing(Some(steal_idx)),
        Steal::Success(_) => WorkerState::Idle,
    }
}
#[inline]
fn next_victim_idx(now: usize, max: usize) -> usize
{
    (now + 1) % max
}
