use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
// `std::task::Wake` chỉ nhận `std::sync::Arc`, nên chỗ này không đi qua `crate::sync`. Không mất gì:
// lane async dựng trên một `ThreadPool` thật, mà pool thì loom không mô hình hoá được (nó dùng
// `thread_local` để biết mình là ai), nên các đường ở đây vốn không nằm trong mô hình loom nào.
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use crate::custom_type::Job;
use crate::pool::Shared;
use crate::task::consts::{DONE, IDLE, NOTIFIED, RUNNING, SCHEDULED};
// Tay cầm về pool thì phải là cùng loại `Arc` mà chính pool dùng, vì dưới `--cfg loom` cả crate đổi
// sang `loom::sync::Arc`. Chỉ riêng `Arc<Task>` là bắt buộc phải của std, do `std::task::Wake` chỉ
// nhận đúng loại đó.
use crate::sync::Arc as PoolArc;
use crate::utils::poison::ignore_poison;

/// Một future cộng chỗ đứng của nó trong lane.
pub struct Task
{
    /// `None` sau khi future xong: thả sớm để mọi thứ nó giữ (kể cả đầu gửi của kênh kết quả) đi
    /// ngay, chứ không nằm chờ tới lúc cái `Arc` cuối cùng của task biến mất.
    ///
    /// Ô trạng thái đã đảm bảo mỗi lúc chỉ một thread poll, nên khoá này không bao giờ có ai giành.
    /// Nó ở đây để cái đảm bảo ấy được kiểu dữ liệu nói ra, thay vì chỉ nằm trong đầu người đọc.
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    state:  AtomicU8,
    /// Lane mà task này chạy trên đó.
    ///
    /// `Arc<Shared>` chứ không phải `ThreadPool`, và đó là chuyện sống chết: `ThreadPool` giữ đám
    /// join handle, nên một task tình cờ là kẻ thả tay cầm cuối cùng sẽ đi join chính cái worker
    /// đang chạy nó. Xem mục 5 của `docs_internal/lanes.md`.
    lane:   PoolArc<Shared>,
}

impl Task
{
    /// Dựng task và xếp nó vào lane ngay.
    pub(crate) fn spawn<F>(lane: &PoolArc<Shared>, future: F)
    where F: Future<Output = ()> + Send + 'static
    {
        let task = Arc::new(Self {
            future: Mutex::new(Some(Box::pin(future))),
            state:  AtomicU8::new(SCHEDULED),
            lane:   PoolArc::clone(lane),
        });
        Self::enqueue(task);
    }

    /// Đẩy task vào lane dưới dạng một job.
    ///
    /// Chỉ gọi khi vừa giành được quyền đó qua ô trạng thái, tức là vừa đặt nó thành `SCHEDULED`.
    fn enqueue(task: Arc<Self>)
    {
        let lane = PoolArc::clone(&task.lane);
        // Một `Arc` là 8 byte, nên job này nằm gọn trong `InlineFn` và không tốn lần cấp phát nào.
        lane.inject(Job::new(move || Self::run(task)));
    }

    /// Poll một nhát. Đây là thân của cái job mà [`Self::enqueue`] đẩy vào lane.
    fn run(task: Arc<Self>)
    {
        if task.state.compare_exchange(SCHEDULED, RUNNING, Ordering::AcqRel, Ordering::Acquire).is_err()
        {
            // `DONE` là lối duy nhất tới đây: task đã xong ở một đường khác. Không có gì để làm.
            return;
        }

        let waker = Waker::from(Arc::clone(&task));
        let mut context = Context::from_waker(&waker);

        let finished = {
            let mut slot = ignore_poison(task.future.lock());
            match slot.as_mut()
            {
                None => true,
                Some(future) =>
                {
                    // Panic của một task không được giết worker. Nó cũng không có ai để trao lại:
                    // panic hook đã in message với backtrace, còn người đang đợi kết quả thì nhận
                    // `None` khi đầu gửi bị thả ngay dưới đây.
                    match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)))
                    {
                        Ok(Poll::Pending) => false,
                        Ok(Poll::Ready(())) | Err(_) =>
                        {
                            *slot = None;
                            true
                        }
                    }
                }
            }
        };

        if finished
        {
            task.state.store(DONE, Ordering::Release);
            return;
        }

        // Còn `Pending`. Nếu không ai gọi dậy trong lúc poll thì task nằm im chờ waker; nếu có, lần
        // gọi ấy đang nằm trong `NOTIFIED` và phải được trả lại thành một lượt poll nữa.
        if task.state.compare_exchange(RUNNING, IDLE, Ordering::AcqRel, Ordering::Acquire).is_err()
        {
            task.state.store(SCHEDULED, Ordering::Release);
            Self::enqueue(task);
        }
    }
}

impl Wake for Task
{
    fn wake(self: Arc<Self>)
    {
        Wake::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>)
    {
        loop
        {
            match self.state.load(Ordering::Acquire)
            {
                IDLE =>
                {
                    if self.state.compare_exchange(IDLE, SCHEDULED, Ordering::AcqRel, Ordering::Acquire).is_ok()
                    {
                        Self::enqueue(Arc::clone(self));
                        return;
                    }
                }
                RUNNING =>
                {
                    if self.state.compare_exchange(RUNNING, NOTIFIED, Ordering::AcqRel, Ordering::Acquire).is_ok()
                    {
                        return;
                    }
                }
                // `SCHEDULED` đã có một bản trong hàng đợi, `NOTIFIED` đã hẹn sẵn một lượt poll nữa,
                // `DONE` thì không còn gì để poll. Cả ba đều là gọi dậy thừa, và gọi dậy thừa là
                // chuyện bình thường của waker.
                _ => return,
            }
        }
    }
}
