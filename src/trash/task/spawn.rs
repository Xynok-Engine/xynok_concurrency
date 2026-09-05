use std::future::Future;

use crate::channel::{Receiver, oneshot};
use crate::pool::ThreadPool;
use crate::task::task::Task;

/// Giao một future cho pool và trả về đầu nhận kết quả.
///
/// Trả về [`Receiver`] chứ không phải một handle riêng, vì `Receiver` đã là cả hai thứ cần thiết:
/// `.await` được từ trong một task khác, và [`recv_in`](Receiver::recv_in) được từ một thread đang
/// chạy job của lane compute. Future panic giữa chừng thì người nhận thấy `None`, đúng như một job
/// panic.
///
/// ```
/// use xynok_concurrency::pool::{Config, ThreadPool};
/// use xynok_concurrency::task;
///
/// let pool = ThreadPool::new(Config {
///     threads: 2,
///     ..Default::default()
/// });
///
/// let answer = task::spawn(&pool, async { 6 * 7 });
/// assert_eq!(task::block_on(answer), Some(42));
/// ```
pub fn spawn<F>(pool: &ThreadPool, future: F) -> Receiver<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = oneshot();
    Task::spawn(pool.shared(), async move { tx.send(future.await) });
    rx
}
