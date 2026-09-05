use std::marker::PhantomData;

use crate::apis::priority::Priority;
use crate::collection::ring_buffer::consts::MAX_CAPACITY;
use crate::custom_type::Job;
use crate::sync::cell::UnsafeCell;
use crate::sync::thread::{park_timeout, ThreadId};
use crate::sync::{thread, AtomicBool, AtomicUsize, Ordering};
use crate::thread_pool::local::{next_pool_id, THREAD_LOCAL_CTX, THREAD_LOCAL_SELF_ID};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::scope::Scope;
use crate::thread_pool::shared::ThreadPoolInner;
use crate::thread_pool::worker::{sleep, Worker, WorkerHandle, SLEEP_SLICE};
use crate::utils::available_cores;
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::latch::Latch;
use crate::utils::queue_batching::QueueBatching;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use xynok_std::unsafe_ptr::HeapPtr;

pub(crate) mod worker;
pub(crate) mod params;
pub(crate) mod local;
pub(crate) mod shared;

pub mod scope;
pub mod cfg;

use cfg::CfgThreadPool;

//#[cfg(all(test, not(loom)))]
//#[path = "tests/unit.rs"]
//mod unit_test;

pub struct ThreadPool
{
    cfg:     CfgThreadPool,
    handles: Vec<thread::JoinHandle<()>>,
    inner:   HeapPtr<ThreadPoolInner>,
}
impl ThreadPool
{
    #[track_caller]
    pub fn new(cfg: CfgThreadPool) -> Self
    {
        let total_worker = cfg.worker_count.min(available_cores()).max(1);

        let per_worker_task_capacity = cfg.per_worker_task_capacity.max(1).next_power_of_two();
        assert!(
            per_worker_task_capacity < MAX_CAPACITY,
            "`per_worker_task_capacity` rounded up to `{}`, which exceeds the `u32` limit of `{}`",
            per_worker_task_capacity,
            MAX_CAPACITY
        );

        let host_index = total_worker;
        let inner = HeapPtr::new(ThreadPoolInner {
            id:             next_pool_id(),
            tasks:          QueueBatching::with_capacity(cfg.task_capacity),
            workers:        FixedBuffer::<WorkerHandle>::new(total_worker + 1),
            host_index:     host_index,
            host_id:        THREAD_LOCAL_SELF_ID.with(|id| *id),
            init_completed: CachePadded::new(AtomicBool::new(false)),
            is_running:     CachePadded::new(AtomicBool::new(true)),
            sleeping:       CachePadded::new(AtomicUsize::new(0)),
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
                    let worker_handle = WorkerHandle {
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
                    // Các worker đã spawn đang đứng chờ ở cổng, và cổng thì không bao giờ mở nữa.
                    // Phải gọi chúng về và đợi đủ trước khi panic, vì lúc unwind `inner` bị thả mà
                    // chúng vẫn đang đọc.
                    shutdown(&inner, &mut handles, i);
                    panic!("Failed to create worker for `{}`: {}", cfg.name, e);
                }
            }
        }

        // host
        unsafe {
            inner.workers.write(
                host_index,
                WorkerHandle {
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
        self.inner.push(task);
    }
    pub fn spawn_local(&self, task: Job) -> Result<(), Job>
    {
        let Some(handle) = self.inner.local_worker()
        else
        {
            return Err(task);
        };

        handle.worker.tasks.push(task)?;

        self.inner.wake_one();
        Ok(())
    }

    pub fn scope<'scope, R>(&self, f: impl FnOnce(&Scope<'scope>) -> R) -> R
    {
        let scope = Scope {
            root:   self.inner.as_ref_mut(),
            latch:  Latch::new(),
            panic:  UnsafeCell::new(None),
            marker: PhantomData,
        };

        let outcome = catch_unwind(AssertUnwindSafe(|| f(&scope)));

        // Thân scope chết giữa chừng thì mấy job còn xếp hàng không còn ý nghĩa gì nữa. Vẫn phải chờ
        // chúng trả vé, nhưng không việc gì phải chạy hết phần thân của chúng.
        if outcome.is_err()
        {
            scope.latch.cancel();
        }

        // Chờ cả khi `f` đã panic: bỏ mặc job chạy tiếp trong lúc khung stack chúng mượn đang bị tháo
        // dỡ thì đó đúng là cái use-after-free mà scope sinh ra để chặn.
        scope.root.run_until(|| scope.latch.is_completed());

        // Tới đây mọi vé đã thả, nên không còn ai ghi vào ô panic nữa.
        let job_panic = scope.panic.with_mut(|slot| unsafe { (*slot).take() });

        match (outcome, job_panic)
        {
            (Err(payload), _) => resume_unwind(payload),
            (Ok(_), Some(payload)) => resume_unwind(payload),
            (Ok(value), None) => value,
        }
    }
}

impl Drop for ThreadPool
{
    fn drop(&mut self)
    {
        // Worker giữ con trỏ thô tới `inner`, nên phải chắc chắn không còn ai chạy trước khi
        // `inner` được thả ở ngay sau đây. Việc còn tồn trong hàng đợi bị bỏ, ai cần chạy cho hết
        // thì phải đồng bộ trước khi thả pool.
        let worker_count = self.inner.workers.len();
        shutdown(&self.inner, &mut self.handles, worker_count);
    }
}

/// Tắt cờ, đợi mọi worker dừng hẳn, rồi mới thu hồi `worker_count` ô đầu của `workers`.
///
/// Thứ tự ở đây là phần quan trọng nhất: chừng nào còn một worker sống thì nó vẫn có quyền ngó
/// sang deque của bất kỳ ai, nên không được thả ô nào trước khi join xong tất cả.
///
/// `FixedBuffer` không tự drop phần tử, nên đây là nơi duy nhất lấy chúng ra, và chỉ được gọi đúng
/// một lần cho mỗi pool.
fn shutdown(inner: &ThreadPoolInner, handles: &mut Vec<thread::JoinHandle<()>>, worker_count: usize)
{
    inner.is_running.store(false, Ordering::Release);

    // Ai đang ngủ thì gõ cửa, không thì phải đợi hết `SLEEP_SLICE` mới thấy cờ đã tắt. Gõ cả
    // những người đang thức cũng không hại gì, permit thừa sẽ bị bỏ qua ở vòng sau.
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
