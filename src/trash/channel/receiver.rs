use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::channel::inner::Inner;
use crate::channel::waiter::Waiter;
use crate::pool::ThreadPool;
use crate::sync::{Arc, Ordering, thread};
use crate::utils::ignore_poison;

/// Đầu nhận. Nhận được đúng một lần.
pub struct Receiver<T>
{
    pub(crate) inner: Arc<Inner<T>>,
}

impl<T> Receiver<T>
{
    /// Có kết quả chưa. Không chờ.
    #[inline]
    pub fn is_ready(&self) -> bool
    {
        self.inner.ready.load(Ordering::Acquire)
    }

    /// Đầu gửi đã biến mất mà chưa gửi gì chưa.
    #[inline]
    pub fn is_cancelled(&self) -> bool
    {
        self.inner.dropped.load(Ordering::Acquire) && !self.is_ready()
    }

    /// Lấy kết quả nếu đã có, còn không thì trả về chính đầu nhận để dùng tiếp.
    pub fn try_recv(self) -> Result<T, Self>
    {
        match self.is_ready()
        {
            // Safety: `ready` là `true` nên ô đã được ghi, và đây là đầu nhận duy nhất nên không ai
            // lấy nó trước.
            true => Ok(unsafe { self.take() }),
            false => Err(self),
        }
    }

    /// Chờ kết quả bằng cách chạy job giúp pool.
    ///
    /// Đây là cách chờ đúng khi bạn đang ở trong pool: thread này vẫn là một người tham gia, nên
    /// nằm không là bỏ phí một core, và với join lồng nhau thì còn có thể là chính nó phải chạy cái
    /// job đang được chờ.
    ///
    /// Trả `None` nếu đầu gửi biến mất mà không gửi gì.
    pub fn recv_in(self, pool: &ThreadPool) -> Option<T>
    {
        pool.run_until(|| self.is_ready() || self.is_cancelled());
        self.finish()
    }

    /// Chờ bằng cách ngủ. Dành cho thread không thuộc pool nào.
    ///
    /// Trả `None` nếu đầu gửi biến mất mà không gửi gì.
    pub fn recv(self) -> Option<T>
    {
        loop
        {
            if self.is_ready() || self.is_cancelled()
            {
                return self.finish();
            }

            // Ghi tên rồi kiểm lại: nếu người gửi xong ngay trước lúc mình ghi tên, lần kiểm này
            // thấy; nếu xong sau, họ đọc được tên mình dưới cùng cái khoá và gọi mình dậy. Không có
            // khe nào ở giữa.
            {
                let mut waiter = ignore_poison(self.inner.waiter.lock());
                if self.is_ready() || self.is_cancelled()
                {
                    continue;
                }
                *waiter = Some(Waiter::Thread(thread::current()));
            }

            if self.is_ready() || self.is_cancelled()
            {
                return self.finish();
            }
            thread::park();
        }
    }

    #[inline]
    fn finish(self) -> Option<T>
    {
        match self.is_ready()
        {
            // Safety: như ở `try_recv`.
            true => Some(unsafe { self.take() }),
            false => None,
        }
    }

    /// # Safety
    ///
    /// `ready` phải là `true`, và giá trị chưa bị ai lấy.
    #[inline]
    unsafe fn take(self) -> T
    {
        unsafe { self.take_ref() }
    }

    /// Như [`Self::take`] nhưng không nuốt đầu nhận, vì `poll` chỉ mượn được `&mut Self`.
    ///
    /// Lấy xong thì `ready` về `false`, nên lần lấy thứ hai không tồn tại: nó rơi vào nhánh "chưa
    /// có gì" chứ không đọc lại một ô đã bị move đi.
    ///
    /// # Safety
    ///
    /// `ready` phải là `true`, và giá trị chưa bị ai lấy.
    #[inline]
    unsafe fn take_ref(&self) -> T
    {
        let value = self.inner.value.with_mut(|slot| unsafe { (*slot).assume_init_read() });
        // Đánh dấu ô đã rỗng, để `Drop` của kênh không thả thêm một lần nữa.
        self.inner.ready.store(false, Ordering::Release);
        value
    }
}

// Chờ kiểu thứ ba: một task async `.await` đầu nhận, và trong lúc chờ nó không giữ thread nào.
//
// Kết quả là `Option<T>` chứ không phải `T`, giống hệt `Receiver::recv`: `None` nghĩa là đầu gửi
// biến mất mà chưa gửi gì, chuyện xảy ra thật mỗi khi một job hoặc một task panic giữa chừng.
//
// Poll tiếp sau khi đã `Ready` thì nhận `None`, vì giá trị đã bị lấy đi rồi. Đó là hợp đồng bình
// thường của `Future`, chỉ là ở đây nó không panic mà trả về một câu trả lời vô hại.
impl<T> Future for Receiver<T>
{
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output>
    {
        if self.is_ready()
        {
            // Safety: `ready` là `true` nên ô đã được ghi, và đây là đầu nhận duy nhất.
            return Poll::Ready(Some(unsafe { self.take_ref() }));
        }
        if self.is_cancelled()
        {
            return Poll::Ready(None);
        }

        // Ghi waker rồi kiểm lại, đúng cái vũ điệu của `recv`: nếu người gửi xong ngay trước lúc
        // mình ghi thì lần kiểm này thấy, còn nếu xong sau thì họ đọc được waker dưới cùng cái khoá
        // và gọi mình dậy. Không có khe nào ở giữa.
        {
            let mut waiter = ignore_poison(self.inner.waiter.lock());
            if !self.is_ready() && !self.is_cancelled()
            {
                *waiter = Some(Waiter::Task(context.waker().clone()));
            }
        }

        if self.is_ready()
        {
            // Safety: như ở nhánh đầu.
            return Poll::Ready(Some(unsafe { self.take_ref() }));
        }
        if self.is_cancelled()
        {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

impl<T> std::fmt::Debug for Receiver<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("channel::Receiver").field("ready", &self.is_ready()).finish_non_exhaustive()
    }
}
