use std::time::Duration;

use xynok_std::unsafe_ptr::{HeapMut, HeapPtr};

use crate::collection::ring_buffer::spmc_lifo_produce_fifo_consume::SpmcRingBufferLifoProduceFifoConsume;
use crate::custom_type::Job;
use crate::sync::thread::{park_timeout, Thread};
use crate::sync::{AtomicBool, Ordering};
use crate::thread_pool::local::{Context, THREAD_LOCAL_CTX};
use crate::thread_pool::params::ParamsWorker;
use crate::thread_pool::shared::ThreadPoolInner;
use crate::thread_pool::worker::WorkerState::Stealing;
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::random::Random;
use crate::utils::steal::Steal;

const STEAL_AMOUNT: usize = 64;

/// Trần cho một giấc ngủ. Worker được gọi dậy ngay khi có việc mới, nên hạn giờ này chỉ là lưới an
/// toàn cho những đường không ai báo được, ví dụ việc còn nằm trong deque riêng của worker khác.
pub(crate) const SLEEP_SLICE: Duration = Duration::from_millis(100);

pub struct Worker
{
    pub tasks:   SpmcRingBufferLifoProduceFifoConsume<Job>,
    idx:         usize,
    root:        HeapMut<ThreadPoolInner>,
    /// Bật lên trong lúc đang ngủ, để người muốn gọi dậy biết nên gõ cửa ai.
    is_sleeping: CachePadded<AtomicBool>,
}

/// Phần của một worker mà người khác nhìn thấy: deque để trộm việc, và thread để gọi dậy.
pub struct WorkerHandle
{
    pub thread: Thread,
    pub worker: HeapPtr<Worker>,
}
/// Chỗ đứng của vòng lặp chính, và nó là **biến cục bộ** của [`Worker::update`] chứ không phải một
/// trường của [`Worker`].
///
/// Để trong `Worker` thì mỗi lần ghi trạng thái là một lần mượn `&mut Worker`, tức là mượn cả cái
/// struct, trong khi người khác đang đọc `is_sleeping` của chính nó để tìm người mà gõ cửa. Miri
/// gọi đó là data race và nó nói đúng: hai bên chạm vào hai trường khác nhau, nhưng cái mượn thì
/// phủ lên toàn bộ.
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
            idx:         idx,
            is_sleeping: CachePadded::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    pub fn is_sleeping(&self) -> bool
    {
        self.is_sleeping.load(Ordering::SeqCst)
    }

    /// Chạy đúng một việc nếu tìm được, trả về `true` khi có chạy.
    ///
    /// Đây là nhịp việc của một điểm chờ: worker đang kẹt giữa chừng một job, đợi nhóm job con của
    /// mình xong, thì không được nằm không. Ngủ ở đây là pool mất một core đúng lúc đang cần nhất,
    /// và với các điểm chờ lồng nhau thì người ngủ có thể chính là người lẽ ra phải chạy cái job
    /// mà nó đang đợi.
    ///
    /// Khác [`Worker::update`] ở chỗ nó chạy đúng một việc rồi trả quyền lại cho người gọi, để
    /// người kia còn kiểm xem đã tới lúc đi chưa. Cũng vì thế mà nó không đụng tới `state`:
    /// vòng lặp chính đang đứng ở đâu thì cứ để nguyên đó, chờ xong là quay về chỗ cũ.
    pub fn run_one(&self, tick: u64) -> bool
    {
        // Deque riêng trước. Đây thường là chính mấy job con vừa spawn ra, dữ liệu còn nóng.
        if let Some(task) = self.tasks.pop_lifo()
        {
            task.run_once();
            return true;
        }

        if self.tasks.push_batch_by_taking_from_queue(STEAL_AMOUNT, &self.root.tasks) > 0
        {
            self.root.wake_one();
            return self.run_one_from_local();
        }

        let mut rnd = Random::new(tick ^ self.idx as u64);
        let steal_idx = rnd.below(self.root.workers.len() as u64) as usize;

        // Trộm của chính mình thì hai đầu con trỏ cùng một ring, trạng thái con trỏ hỏng ngay.
        if steal_idx == self.idx
        {
            return false;
        }

        let other = self.root.workers.at(steal_idx);
        let other = other.with_mut(|p| unsafe { p.as_mut_unchecked().assume_init_ref() });
        match other.worker.tasks.try_steal_batch_to(STEAL_AMOUNT, &self.tasks)
        {
            Steal::Success(_) =>
            {
                self.root.wake_one();
                self.run_one_from_local()
            }
            // Trượt một nạn nhân thì thôi, người gọi sẽ quay lại ngay sau khi kiểm điều kiện dừng.
            Steal::Empty | Steal::Busy => false,
        }
    }

    /// Vừa nạp được một lô thì lấy ngay một cái ra chạy.
    ///
    /// Vẫn có thể về tay không: lô vừa nạp có thể bị người khác trộm mất sạch ngay trong lúc này.
    #[inline]
    fn run_one_from_local(&self) -> bool
    {
        match self.tasks.pop_lifo()
        {
            Some(task) =>
            {
                task.run_once();
                true
            }
            None => false,
        }
    }

    pub fn update(params: ParamsWorker)
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
        let mut state = WorkerState::Idle;
        while params.root.is_running.load(Ordering::Acquire)
        {
            let next_state = match state
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
                        sleep(&params.worker, &params.root);
                        backoff.reset();
                    }
                    false => backoff.snooze(),
                },
            }

            state = next_state;
            update_data.tick = update_data.tick.wrapping_add(1);
        }
    }
}

/// Nằm chờ tới khi có người gọi dậy, hoặc tới khi hết [`SLEEP_SLICE`].
///
/// Ghi danh trước rồi mới kiểm tra hàng đợi, không được làm ngược lại: nếu kiểm trước thì có thể
/// có người đẩy việc vào ngay giữa hai bước, họ thấy chưa ai ngủ nên không gọi ai, còn mình thì
/// vừa kịp nằm xuống.
pub(crate) fn sleep(worker: &Worker, root: &ThreadPoolInner)
{
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
