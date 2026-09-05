use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

use crate::pool::ThreadPool;
use crate::sync::thread;
use crate::task::park_waker::ParkWaker;

/// Chạy một future tới khi xong, bằng cách ngủ giữa các lần poll.
///
/// Dành cho thread không thuộc pool nào, ví dụ main thread lúc khởi động. Đang đứng trong một lane
/// thì dùng [`block_on_in`]: nó chạy job giúp lane trong lúc chờ thay vì nằm không.
///
/// # Đừng gọi từ trong một task
///
/// Chặn một thread của lane async để đợi một future khác của chính lane đó là cách deadlock nhanh
/// nhất khi lane chỉ có hai thread. Trong một task thì `.await` là đường duy nhất.
pub fn block_on<F: Future>(future: F) -> F::Output
{
    let mut future = std::pin::pin!(future);
    let signal = ParkWaker::new();
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);

    loop
    {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context)
        {
            return output;
        }

        // `take` sau khi poll chứ không phải trước: waker có thể đã được gọi ngay giữa lúc poll, và
        // lần gọi ấy phải được đếm, không thì thread này ngủ chờ một tiếng gọi đã đi qua rồi.
        while !signal.take()
        {
            thread::park();
        }
    }
}

/// Như [`block_on`], nhưng chờ bằng cách chạy job của `pool`.
///
/// Đây là cách chờ đúng khi thread gọi là một người tham gia pool, y như
/// [`Receiver::recv_in`]: nằm không là bỏ phí một chỗ trong lane, và với join lồng nhau thì có thể
/// chính thread này mới là người phải chạy cái job mà future đang đợi.
pub fn block_on_in<F: Future>(pool: &ThreadPool, future: F) -> F::Output
{
    let mut future = std::pin::pin!(future);
    let signal = ParkWaker::new();
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);

    loop
    {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context)
        {
            return output;
        }

        pool.run_until(|| signal.woken.load(Ordering::Acquire));
        signal.take();
    }
}
