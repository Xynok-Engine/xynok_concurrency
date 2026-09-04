use crate::custom_type::Job;
use crate::sync::thread::ThreadId;
use crate::utils::queue_batching::QueueBatching;

/// Hàng đợi của những job buộc phải chạy trên main thread.
///
/// Không có thread nào ở đây cả, và đó chính là điểm: main thread là thread của người dùng, engine
/// chỉ mượn nó ở những chỗ nó tự gọi [`Lanes::run_pending_on_main`] trong vòng lặp frame.
pub struct MainQueue
{
    pub(crate) jobs:   QueueBatching<Job>,
    /// Thread được coi là main, chụp lúc dựng [`Lanes`].
    pub(crate) thread: ThreadId,
}
