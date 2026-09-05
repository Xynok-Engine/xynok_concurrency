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
    pub id:             u64,
    pub tasks:          QueueBatching<Job>,
    pub workers:        FixedBuffer<WorkerHandle>,
    pub host_index:     usize,
    pub host_id:        ThreadId,
    pub init_completed: CachePadded<AtomicBool>,
    pub is_running:     CachePadded<AtomicBool>,
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

    pub fn push(&self, task: Job)
    {
        self.tasks.push(task);
    }
}
