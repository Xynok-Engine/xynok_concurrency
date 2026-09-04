use std::time::Duration;

use xynok_std::unsafe_ptr::{HeapMut, HeapPtr};

use crate::collection::ring_buffer::spmc_lifo_produce_fifo_consume::SpmcRingBufferLifoProduceFifoConsume;
use crate::custom_type::Job;
use crate::ring_buffer_fifo::Steal;
use crate::sync::thread::{park_timeout, Thread};
use crate::sync::{AtomicBool, Ordering};
use crate::thread_pool::local::{Context, THREAD_LOCAL_CTX};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::worker::WorkerState::Stealing;
use crate::thread_pool::ThreadPoolInner;
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::random::Random;

const STEAL_AMOUNT: usize = 64;

/// Trần cho một giấc ngủ. Worker được gọi dậy ngay khi có việc mới, nên hạn giờ này chỉ là lưới an
/// toàn cho những đường không ai báo được, ví dụ việc còn nằm trong deque riêng của worker khác.
const SLEEP_SLICE: Duration = Duration::from_millis(100);

pub struct Worker
{
    pub tasks:   SpmcRingBufferLifoProduceFifoConsume<Job>,
    idx:         usize,
    root:        HeapMut<ThreadPoolInner>,
    state:       WorkerState,
    /// Bật lên trong lúc đang ngủ, để người muốn gọi dậy biết nên gõ cửa ai.
    is_sleeping: CachePadded<AtomicBool>,
}

/// Phần của một worker mà người khác nhìn thấy: deque để trộm việc, và thread để gọi dậy.
pub struct WorkerHandle
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
    visited:      usize,
    total_worker: usize,
    tick:         u64,
}
impl UpdateData
{
    pub fn new(params: &ParamsWorker) -> Self
    {
        Self {
            tick:         0,
            visited:      0,
            total_worker: params.root.workers.len(),
        }
    }
}
impl Worker
{
    pub fn new(task_size: usize, root: HeapMut<ThreadPoolInner>, idx: usize) -> Self
    {
        Self {
            tasks:       SpmcRingBufferLifoProduceFifoConsume::new(task_size),
            root:        root,
            state:       WorkerState::Idle,
            idx:         idx,
            is_sleeping: CachePadded::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    pub fn is_sleeping(&self) -> bool
    {
        self.is_sleeping.load(Ordering::SeqCst)
    }

    pub fn update(mut params: ParamsWorker)
    {
        params.priority.apply_to_current_thread();

        let mut backoff = Backoff::new();
        // `new` còn đang ghi `workers`, chưa được nhìn vào đó. Cổng này cũng là đường ra cho
        // trường hợp dựng pool hỏng: lúc đó `is_running` tắt và cổng không bao giờ mở.
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

        // Ghi chỗ đứng sau khi qua cổng, không phải trước. Job chỉ chạy được từ đây trở đi, nên
        // trước cổng chưa ai cần tới nó, mà đường thoát sớm ở trên thì lại không phải dọn.
        THREAD_LOCAL_CTX.set(Context {
            pool:  params.root.id,
            index: params.worker.idx,
        });

        let mut update_data = UpdateData::new(&params);
        while params.root.is_running.load(Ordering::Acquire)
        {
            let next_state = match params.worker.state
            {
                WorkerState::Idle => drain_local_task(params),
                WorkerState::Stealing(r) => steal(params, &mut update_data, r),
            };

            // `Idle` nghĩa là vừa vơ được việc, `Stealing` là đi một vòng tay không. Tay không
            // càng nhiều lần liên tiếp thì giãn nhịp càng rộng, hết cỡ thì ngủ hẳn thay vì quay
            // vòng đốt core.
            match next_state
            {
                WorkerState::Idle => backoff.reset(),

                // If we steal too many times, we should sleep.
                WorkerState::Stealing(_) => match backoff.is_completed()
                {
                    true =>
                    {
                        sleep(params);
                        backoff.reset();
                    }
                    false => backoff.snooze(),
                },
            }

            params.worker.state = next_state;
            update_data.tick = update_data.tick.wrapping_add(1);
        }
    }
}

/// Nằm chờ tới khi có người gọi dậy, hoặc tới khi hết [`SLEEP_SLICE`].
///
/// Ghi danh trước rồi mới kiểm tra hàng đợi, không được làm ngược lại: nếu kiểm trước thì có thể
/// có người đẩy việc vào ngay giữa hai bước, họ thấy chưa ai ngủ nên không gọi ai, còn mình thì
/// vừa kịp nằm xuống.
fn sleep(params: ParamsWorker)
{
    let root = params.root;
    let worker = params.worker;

    worker.is_sleeping.store(true, Ordering::SeqCst);
    root.sleeping.fetch_add(1, Ordering::SeqCst);

    if root.tasks.is_empty() && root.is_running.load(Ordering::SeqCst)
    {
        park_timeout(SLEEP_SLICE);
    }

    root.sleeping.fetch_sub(1, Ordering::SeqCst);
    worker.is_sleeping.store(false, Ordering::SeqCst);
}

fn drain_local_task(params: ParamsWorker) -> WorkerState
{
    while let Some(task) = params.worker.tasks.pop_lifo()
    {
        task.run_once();
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
            if worker.tasks.push_batch_by_taking_from_queue(STEAL_AMOUNT, &params.root.tasks) > 0
            {
                // Vừa vơ được một lô, hàng đợi chung có thể còn nữa. Gọi thêm một người dậy để
                // không phải mình mình gánh.
                params.root.wake_one();
                return WorkerState::Idle;
            }

            let mut rnd = Random::new(update_data.tick ^ worker.idx as u64);
            let steal_idx = rnd.below(update_data.total_worker as u64) as usize;
            steal_from(params, update_data, steal_idx)
        }
    }
}

/// Thử vét một lô từ deque của worker `steal_idx`, và chọn nước đi tiếp theo dựa trên kết quả.
fn steal_from(params: ParamsWorker, update_data: &mut UpdateData, steal_idx: usize) -> WorkerState
{
    let worker = params.worker;

    // If the source and destination are the same ring, the cursor state will become corrupted.
    // Revert to polling the shared queue and pick up a different task.
    if steal_idx == worker.idx
    {
        return Stealing(None);
    }

    let other = params.root.workers.at(steal_idx);
    let other = other.with_mut(|p| unsafe { p.as_mut_unchecked().assume_init_ref() });
    match other.worker.tasks.try_steal_batch_to(STEAL_AMOUNT, &worker.tasks)
    {
        Steal::Empty => WorkerState::Stealing(Some(next_victim_idx(steal_idx, update_data.total_worker))),
        Steal::Busy => WorkerState::Stealing(Some(steal_idx)),
        Steal::Success(_) =>
        {
            params.root.wake_one();
            WorkerState::Idle
        }
    }
}
#[inline]
fn next_victim_idx(now: usize, max: usize) -> usize
{
    (now + 1) % max
}
