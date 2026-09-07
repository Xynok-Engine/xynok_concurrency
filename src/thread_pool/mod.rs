use crate::custom_type::Job;
use crate::sync::{thread, AtomicBool, Ordering};
use crate::thread_pool::consts::MAX_WORKER_TASK_CAPACITY;
use crate::thread_pool::local::{next_pool_id, Context, THREAD_LOCAL_CTX};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::scope::Scope;
use crate::thread_pool::shared::ThreadPoolInner;
use crate::thread_pool::worker::{Worker, WorkerSpec};
use crate::utils::available_cores;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;
use std::fmt;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
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

    pub fn scope<'scope, R>(&self, f: impl FnOnce(&Scope<'scope>) -> R) -> R
    {
        let scope = Scope::new(self.inner.as_ref_mut());

        // when the scope is draining, the caller might come from a different pool. We don't really care about that,
        // but if it happens, we need to override the current thread context to match the caller's data. After
        // all tasks are drained, we can set it back.
        // we need to override the caller data here, as the scope will push the task into the caller's queue.
        let previous_ctx = THREAD_LOCAL_CTX.get();
        let is_outsider = previous_ctx.pool != self.inner.id;
        if is_outsider
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
        self.inner.run_until(|| scope.is_completed());

        if is_outsider
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
