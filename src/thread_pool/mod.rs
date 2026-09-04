use xynok_std::unsafe_ptr::HeapPtr;

use crate::apis::priority::Priority;
use crate::collection::ring_buffer::consts::MAX_CAPACITY;
use crate::custom_type::Job;
use crate::sync::{AtomicBool, AtomicUsize, Ordering, thread};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::worker::{Worker, WorkerHandle};
use crate::utils::available_cores;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;
pub(crate) mod worker;
pub(crate) mod params;

pub struct CfgThreadPool
{
    name:                     String,
    priority:                 Priority,
    per_worker_task_capacity: usize,
    task_capacity:            usize,
    worker_count:             usize,
}
pub struct ThreadPool
{
    cfg:     CfgThreadPool,
    /// Handle nằm ở đây chứ không nằm trong `inner`, để worker không với tới được. Nhờ vậy lúc
    /// dừng pool có thể join cho bằng hết trước khi động vào `workers`.
    handles: Vec<thread::JoinHandle<()>>,
    inner:   HeapPtr<ThreadPoolInner>,
}

pub(crate) struct ThreadPoolInner
{
    pub tasks:          QueueBatching<Job>,
    pub workers:        FixedBuffer<WorkerHandle>,
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
    /// Gõ cửa một worker đang ngủ. Không ai ngủ thì chỉ tốn đúng một lần đọc.
    ///
    /// Gọi nhầm người đang thức cũng không sao: `park_timeout` chỉ nhận thêm một permit, và vòng
    /// lặp của worker luôn kiểm lại xem có việc thật hay không.
    pub fn wake_one(&self)
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
impl ThreadPool
{
    #[track_caller]
    pub fn new(cfg: CfgThreadPool) -> Self
    {
        let total_worker = cfg.worker_count.min(available_cores()).max(1);

        // Deque của worker đánh chỉ số bằng mask nên bắt buộc là power of two. Làm tròn lên hộ
        // người dùng, vì nếu để lọt thì bản release không assert, chỉ lặng lẽ tính sai mask.
        let per_worker_task_capacity = cfg.per_worker_task_capacity.max(1).next_power_of_two();
        assert!(
            per_worker_task_capacity < MAX_CAPACITY,
            "`per_worker_task_capacity` làm tròn lên thành `{}`, vượt trần `{}` của chỉ số u32",
            per_worker_task_capacity,
            MAX_CAPACITY
        );

        let inner = HeapPtr::new(ThreadPoolInner {
            tasks:          QueueBatching::with_capacity(cfg.task_capacity),
            workers:        FixedBuffer::<WorkerHandle>::new(total_worker),
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
        inner.init_completed.store(true, Ordering::Release);

        Self {
            cfg:     cfg,
            handles: handles,
            inner:   inner,
        }
    }
    pub fn push(&mut self, task: Job)
    {
        self.inner.tasks.push(task);
        self.inner.wake_one();
    }

    pub fn scope(&mut self) {}
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
        drop(unsafe { inner.workers.take_at(i) });
    }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
