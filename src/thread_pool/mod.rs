use std::marker::PhantomData;

use crate::apis::priority::Priority;
use crate::collection::ring_buffer::consts::MAX_CAPACITY;
use crate::custom_type::Job;
use crate::latch::Latch;
use crate::sync::cell::UnsafeCell;
use crate::sync::thread::{park_timeout, ThreadId};
use crate::sync::{thread, AtomicBool, AtomicUsize, Ordering};
use crate::thread_pool::local::{next_pool_id, THREAD_LOCAL_CTX, THREAD_LOCAL_SELF_ID};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::scope::Scope;
use crate::thread_pool::worker::{sleep, Worker, WorkerHandle, SLEEP_SLICE};
use crate::utils::available_cores;
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use xynok_std::unsafe_ptr::HeapPtr;
pub(crate) mod worker;
pub(crate) mod params;
pub(crate) mod local;
pub mod scope;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
pub struct CfgThreadPool
{
    name:                     String,
    priority:                 Priority,
    per_worker_task_capacity: usize,
    task_capacity:            usize,
    worker_count:             usize,
}
impl CfgThreadPool
{
    /// Cấu hình mặc định cho `worker_count` worker, đặt tên là `name`.
    ///
    /// Tên chỉ để đặt cho thread, nó hiện ra trong debugger và trong profiler. Mấy con số còn lại
    /// đủ dùng cho phần lớn trường hợp, chỉnh sau bằng [`Self::with_capacity`] hay
    /// [`Self::with_priority`] nếu cần.
    pub fn new(name: impl Into<String>, worker_count: usize) -> Self
    {
        Self {
            name:                     name.into(),
            priority:                 Priority::default(),
            per_worker_task_capacity: 256,
            task_capacity:            1024,
            worker_count:             worker_count,
        }
    }

    /// Sức chứa deque riêng của mỗi worker, và sức chứa ban đầu của hàng đợi chung.
    ///
    /// Deque riêng được làm tròn lên power of two, vì nó đánh chỉ số bằng mask.
    pub fn with_capacity(mut self, per_worker: usize, shared: usize) -> Self
    {
        self.per_worker_task_capacity = per_worker;
        self.task_capacity = shared;
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self
    {
        self.priority = priority;
        self
    }
}

pub struct ThreadPool
{
    cfg:     CfgThreadPool,
    /// Handle nằm ở đây chứ không nằm trong `inner`, để worker không với tới được. Nhờ vậy lúc
    /// dừng pool có thể join cho bằng hết trước khi động vào `workers`.
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

        // Host không ghi chỗ đứng vào thread local. Nó đã được nhận ra qua `host_id`, mà mỗi pool
        // giữ một `host_id` riêng, nên một thread dựng nhiều pool vẫn đứng đúng chỗ ở từng pool.
        // Ghi vào đó thì pool dựng sau lại đè lên chỗ đứng của pool trước.
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
            latch:  Latch::new(0),
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
        scope.root.run_until(|| scope.latch.is_done());

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
pub(crate) struct ThreadPoolInner
{
    /// Số hiệu riêng của pool này, để [`Context`] của thread nói được nó là worker của *pool nào*.
    ///
    /// [`Context`]: crate::thread_pool::local::Context
    pub id:             u64,
    pub tasks:          QueueBatching<Job>,
    /// Một ô cho mỗi người tham gia: `worker_count` worker thật, cộng host ở ô cuối.
    pub workers:        FixedBuffer<WorkerHandle>,
    /// Chỗ đứng của host, tức là ô cuối của [`Self::workers`].
    pub host_index:     usize,
    /// Thread đã dựng pool. Nó là người ngoài duy nhất có một ô trong `workers`.
    ///
    /// Giữ `ThreadId` chứ không giữ `Thread`, để so mà không phải clone một `Arc`. Handle để gõ cửa
    /// thì đã nằm trong `WorkerHandle` của ô host rồi.
    pub host_id:        ThreadId,
    /// Cổng khởi tạo. Worker sinh ra trước khi `new` kịp ghi xong `workers`, nên nó phải đợi ở đây
    /// rồi mới được nhìn vào buffer đó.
    pub init_completed: CachePadded<AtomicBool>,
    /// Tắt cờ này là mọi worker thoát vòng lặp. Dùng cho cả lúc `Drop` lẫn lúc dựng pool hỏng
    /// giữa chừng.
    pub is_running:     CachePadded<AtomicBool>,
    /// Số worker đang ngủ. Chỉ để người đẩy việc biết có cần gọi ai dậy không, khỏi phải quét cả
    /// dãy worker trên đường nóng.
    pub sleeping:       CachePadded<AtomicUsize>,
}

impl ThreadPoolInner
{
    fn local_worker(&self) -> Option<&WorkerHandle>
    {
        let local_ctx = THREAD_LOCAL_CTX.get();
        if local_ctx.pool == self.id
        {
            return Some(unsafe { self.workers.get_at(local_ctx.index) });
        }
        let is_host = THREAD_LOCAL_SELF_ID.with(|id| *id == self.host_id);

        is_host.then(|| unsafe { self.workers.get_at(self.host_index) })
    }

    /// Đợi tới khi `done` đúng, và đợi bằng cách chạy việc giúp pool chứ không nằm không.
    ///
    /// Đây là đường mà mọi điểm chờ bên trong pool đi. Worker đang chờ vẫn là một người tham gia
    /// pool, nên nếu nó ngủ thì pool mất một core đúng lúc đang cần nhất, và với các điểm chờ lồng
    /// nhau thì còn tệ hơn: người ngủ có thể chính là người lẽ ra phải chạy cái job mà nó đang đợi.
    ///
    /// Thread ngoài pool thì không có deque để nạp việc vào, nên chỉ còn nước chờ. Quay tại chỗ
    /// một lúc rồi ngủ có hạn giờ, chứ quay mãi là đốt một core cho không.
    fn run_until(&self, done: impl Fn() -> bool)
    {
        let Some(handle) = self.local_worker()
        else
        {
            let mut backoff = Backoff::new();
            while !done()
            {
                match backoff.is_completed()
                {
                    true => park_timeout(SLEEP_SLICE),
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

            if handle.worker.run_one(tick)
            {
                backoff.reset();
                continue;
            }

            // Không tìm được việc nào. Giãn nhịp rồi ngủ, y như vòng lặp chính, vì lúc này chờ
            // hay làm việc thì cũng chỉ có bấy nhiêu việc trong pool.
            match backoff.is_completed()
            {
                true =>
                {
                    sleep(&handle.worker, self);
                    backoff.reset();
                }
                false => backoff.snooze(),
            }
        }
    }

    /// Giao một việc cho cả pool. Việc rơi vào hàng đợi chung, ai rảnh trước thì nhặt.
    pub fn push(&self, task: Job)
    {
        self.tasks.push(task);

        // Vừa có việc mới. Người đang ngủ không tự biết được, không gõ cửa thì họ phải nằm hết
        // `SLEEP_SLICE` mới dậy.
        self.wake_one();
    }

    /// Gõ cửa một worker đang ngủ. Không ai ngủ thì chỉ tốn đúng một lần đọc.
    ///
    /// Gọi nhầm người đang thức cũng không sao: `park_timeout` chỉ nhận thêm một permit, và vòng
    /// lặp của worker luôn kiểm lại xem có việc thật hay không.
    fn wake_one(&self)
    {
        if self.sleeping.load(Ordering::SeqCst) == 0
        {
            return;
        }

        for i in 0..self.workers.len()
        {
            let handle = self.workers.at(i);
            let handle = handle.with_mut(|p| unsafe { p.as_mut_unchecked().assume_init_ref() });
            if handle.worker.is_sleeping()
            {
                handle.thread.unpark();
                return;
            }
        }
    }
}
