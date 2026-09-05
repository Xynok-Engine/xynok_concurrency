use std::thread::park_timeout;

use crate::custom_type::Job;
use crate::sync::thread::{self, Thread, ThreadId};
use crate::sync::{AtomicBool, AtomicUsize, Ordering};
use crate::thread_pool::local::{THREAD_LOCAL_CTX, THREAD_LOCAL_SELF_ID};
use crate::thread_pool::worker::{sleep, WorkerHandle, SLEEP_SLICE};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;

pub struct ThreadPoolInner
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
    pub fn local_worker(&self) -> Option<&WorkerHandle>
    {
        let local_ctx = THREAD_LOCAL_CTX.get();
        if local_ctx.pool == self.id
        {
            return Some(unsafe { self.workers.get_at(local_ctx.index) });
        }
        let is_host = THREAD_LOCAL_SELF_ID.with(|id| *id == self.host_id);

        is_host.then(|| unsafe { self.workers.get_at(self.host_index) })
    }

    pub fn run_until(&self, done: impl Fn() -> bool)
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
