use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Nhường lượt: task xếp lại vào cuối hàng của lane rồi mới chạy tiếp.
///
/// Dùng khi một task làm một mạch việc dài mà vẫn muốn task khác chen vào được. Nó không phải một
/// điểm chờ: waker được gọi ngay tại chỗ, nên task này không bao giờ ngủ.
pub async fn yield_now()
{
    struct YieldOnce
    {
        yielded: bool,
    }

    impl Future for YieldOnce
    {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()>
        {
            if self.yielded
            {
                return Poll::Ready(());
            }

            self.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }

    YieldOnce { yielded: false }.await
}
