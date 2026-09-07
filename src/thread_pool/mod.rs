use crate::custom_type::Job;
use crate::sync::{AtomicBool, Ordering, thread};
use crate::thread_pool::consts::MAX_WORKER_TASK_CAPACITY;
use crate::thread_pool::local::{Context, THREAD_LOCAL_CTX, next_pool_id};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::scope::Scope;
use crate::thread_pool::shared::ThreadPoolInner;
use crate::thread_pool::worker::{Worker, WorkerSpec};
use crate::utils::available_cores;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;
use crate::utils::spinlock::SpinLock;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use xynok_std::unsafe_ptr::HeapPtr;

pub(crate) mod worker;
pub(crate) mod params;
pub(crate) mod local;
pub(crate) mod shared;
pub(crate) mod worker_queue;
pub(crate) mod consts;

pub mod scope;
pub mod cfg;

use cfg::CfgThreadPool;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;

#[cfg(test)]
#[path = "tests/wakeup.rs"]
mod wakeup_test;

pub struct ThreadPool
{
    cfg:     CfgThreadPool,
    handles: Vec<thread::JoinHandle<()>>,
    inner:   HeapPtr<ThreadPoolInner>,
}
impl ThreadPool
{
    /// Currently, when initializing a ThreadPool, the number of workers we push is the number of threads created, not the total number of threads running in the program.
    /// This is because we do not include the main thread, which is the thread that performs the initial setup.
    /// In reality, the main thread still participates in sharing and processing tasks, but it is not categorized as a worker.
    /// It still utilizes worker mechanisms, yet it is not a newly created thread. Therefore, when we call
    /// methods like ThreadPool.TotalWorker, it returns the number of additional threads created rather than including the main thread.
    /// Essentially, in every current scenario, we always have at least two threads running.
    /// Even in the worst-case scenario, or with the lowest number of workers, the program is actually running two threads:
    /// the main thread and one additional worker thread, even if you pass 0 or 1 as the input.
    #[track_caller]
    pub fn new(cfg: CfgThreadPool) -> Self
    {
        let total_worker = cfg.worker_count.min(available_cores()).max(1);

        let per_worker_task_capacity = cfg.per_worker_task_capacity.max(1).next_power_of_two();
        assert!(
            per_worker_task_capacity < MAX_WORKER_TASK_CAPACITY,
            "`per_worker_task_capacity` rounded up to `{}`, which exceeds the `u32` limit of `{}`",
            per_worker_task_capacity,
            MAX_WORKER_TASK_CAPACITY
        );

        let host_index = total_worker;

        let inner = HeapPtr::new(ThreadPoolInner {
            id:             next_pool_id(),
            tasks:          QueueBatching::with_capacity(cfg.task_capacity),
            workers:        FixedBuffer::<WorkerSpec>::new(total_worker + 1),
            host_index:     host_index,
            host_owner:     SpinLock::new(()),
            init_completed: CachePadded::new(AtomicBool::new(false)),
            is_running:     CachePadded::new(AtomicBool::new(true)),
            sleepers:       QueueBatching::with_capacity(total_worker),
            total_worker:   total_worker,
        });

        let mut handles = Vec::with_capacity(total_worker);
        for i in 0..total_worker
        {
            let name = format!("{}.workers[{}]", cfg.name, i);
            let worker = HeapPtr::new(Worker::new(per_worker_task_capacity, inner.as_ref_mut(), i));

            let params = ParamsWorker {
                priority: cfg.priority,
                worker:   worker.as_ref_mut(),
                root:     inner.as_ref_mut(),
            };
            match thread::spawn_named(name, move || {
                Worker::update(params);
            })
            {
                Ok(handle) =>
                {
                    let worker_handle = WorkerSpec {
                        thread: handle.thread().clone(),
                        worker: worker,
                    };
                    handles.push(handle);
                    unsafe {
                        inner.workers.write(i, worker_handle);
                    }
                }
                Err(e) =>
                {
                    ThreadPoolInner::shutdown(&inner, &mut handles, i);
                    panic!("Failed to create worker for `{}`: {}", cfg.name, e);
                }
            }
        }

        // host
        unsafe {
            inner.workers.write(
                host_index,
                WorkerSpec {
                    thread: thread::current(),
                    worker: HeapPtr::new(Worker::new(per_worker_task_capacity, inner.as_ref_mut(), host_index)),
                },
            );
        }

        inner.init_completed.store(true, Ordering::Release);

        Self {
            cfg:     cfg,
            handles: handles,
            inner:   inner,
        }
    }

    pub fn push(&self, task: Job)
    {
        self.inner.push_and_wake_one(task);
    }

    /// Runs a complete group of jobs, borrowing data until every job has finished.
    ///
    /// The iterator is exhausted before any of its jobs can start. Jobs are distributed
    /// round-robin across the caller and the background workers, starting with the caller.
    /// A call from a worker uses the background workers only; an inactive host is not assigned
    /// work. Once the whole group has been assigned, workers drain their own work and then steal.
    /// Assignment balances job counts, not execution times, and does not guarantee thread affinity.
    /// If another external caller owns the host queue, jobs are assigned to background workers
    /// and this caller helps by stealing through a private queue.
    ///
    /// Unlike `scope`, iterator construction must not wait for one of these jobs to run.
    /// If the iterator panics, its queued jobs are dropped without running. Job panics are
    /// rethrown after the group drains, and the pool remains usable. Jobs go directly into
    /// preallocated inboxes; publication needs no temporary heap storage. Inboxes grow only
    /// when their configured capacity is exceeded.
    ///
    /// ```
    /// use xynok_concurrency::thread_pool::ThreadPool;
    /// use xynok_concurrency::thread_pool::cfg::CfgThreadPool;
    /// let pool = ThreadPool::new(CfgThreadPool::new("batch", 4));
    /// let mut values = [1, 2, 3, 4];
    /// pool.run_batch(values.iter_mut().map(|value| move || *value *= 2));
    /// assert_eq!(values, [2, 4, 6, 8]);
    /// ```
    pub fn run_batch<'scope, I, F>(&self, jobs: I)
    where
        I: IntoIterator<Item = F>,
        F: FnOnce() + Send + 'scope,
    {
        self.scope(|scope| {
            let jobs = jobs.into_iter().map(|job| scope.new_job(job));
            // An external helper without the host slot assigns work to background workers.
            let caller = self.inner.current_worker().map_or(0, |worker| worker.worker.index());
            self.inner.publish_batch(jobs, caller);
        });
    }

    pub fn scope<'scope, R>(&self, f: impl FnOnce(&Scope<'scope>) -> R) -> R
    {
        let scope = Scope::new(self.inner.as_ref_mut());

        let previous_ctx = THREAD_LOCAL_CTX.get();
        let is_outsider = previous_ctx.pool != self.inner.id;
        // Only one external caller can own the single-producer host deque. Do not wait for
        // ownership: a nested call through another pool may already hold it on this thread.
        let host_owner = if is_outsider { self.inner.host_owner.try_get() } else { None };
        let helper = if is_outsider && host_owner.is_none()
        {
            // An unregistered caller publishes through the shared queue and helps through a
            // private one-slot deque. It cannot leave inaccessible jobs behind after a steal.
            Some(Worker::new(1, self.inner.as_ref_mut(), usize::MAX))
        }
        else
        {
            None
        };
        if host_owner.is_some()
        {
            THREAD_LOCAL_CTX.set(Context {
                pool:  self.inner.id,
                index: self.inner.host_index,
            });
        }

        let outcome = catch_unwind(AssertUnwindSafe(|| f(&scope)));

        if outcome.is_err()
        {
            scope.cancel();
        }

        // we need to wait even if `f` panics. Letting a job continue running while its borrowed stack frame
        // is being torn down is exactly the kind of use-after-free that scopes are designed to prevent
        let worker = match &helper
        {
            Some(worker) => worker,
            None => &self.inner.current_worker().expect("scope must have a worker or an external helper").worker,
        };
        self.inner.run_until(worker, || scope.is_completed());

        if host_owner.is_some()
        {
            THREAD_LOCAL_CTX.set(previous_ctx);
        }

        // At this point, every ticket has been released, so no one else can write to the panic cell.
        let job_panic = scope.take_panic();

        match (outcome, job_panic)
        {
            (Err(payload), _) => resume_unwind(payload),
            (Ok(_), Some(payload)) => resume_unwind(payload),
            (Ok(value), None) => value,
        }
    }
}

impl fmt::Debug for ThreadPool
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        f.debug_struct("ThreadPool")
            .field("name", &self.cfg.name)
            .field("id", &self.inner.id)
            .field("workers", &self.handles.len())
            .field("priority", &self.cfg.priority)
            .field("per_worker_task_capacity", &self.cfg.per_worker_task_capacity)
            .field("task_capacity", &self.cfg.task_capacity)
            .field("pending_tasks", &self.inner.tasks.len())
            .field("running", &self.inner.is_running.load(Ordering::Relaxed))
            .finish()
    }
}

impl fmt::Display for ThreadPool
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        write!(
            f,
            "{}: {} workers, {:?} priority, {} pending",
            self.cfg.name,
            self.handles.len(),
            self.cfg.priority,
            self.inner.tasks.len()
        )?;

        if !self.inner.is_running.load(Ordering::Relaxed)
        {
            f.write_str(" (stopped)")?;
        }

        Ok(())
    }
}

impl Drop for ThreadPool
{
    fn drop(&mut self)
    {
        let worker_count = self.inner.workers.len();
        ThreadPoolInner::shutdown(&self.inner, &mut self.handles, worker_count);
    }
}
