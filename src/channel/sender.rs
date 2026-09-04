use crate::channel::inner::Inner;
use crate::sync::{Arc, Ordering};
use crate::utils::ignore_poison;

/// Đầu gửi. Gửi được đúng một lần, vì [`Self::send`] nuốt luôn chính nó.
pub struct Sender<T>
{
    pub(crate) inner: Arc<Inner<T>>,
}

impl<T> Sender<T>
{
    /// Gửi giá trị và đánh thức người đang chờ, nếu có ai chờ.
    ///
    /// Không bao giờ panic, và không bao giờ chặn. Đầu nhận có thể đã biến mất, và khi đó giá trị
    /// nằm lại trong kênh cho tới lúc kênh bị thả: người gửi không có việc gì phải bận tâm chuyện
    /// đó, nhất là khi người gửi là một job đang chạy trong pool.
    pub fn send(self, value: T)
    {
        // Safety: đây là đầu gửi duy nhất và nó chỉ gửi được một lần, nên không ai khác ghi vào ô
        // này; đầu nhận chỉ đọc sau khi thấy `ready`.
        self.inner.value.with_mut(|slot| unsafe { (*slot).write(value) });
        self.inner.ready.store(true, Ordering::Release);

        if let Some(waiter) = ignore_poison(self.inner.waiter.lock()).take()
        {
            waiter.wake();
        }
    }
}

impl<T> Drop for Sender<T>
{
    fn drop(&mut self)
    {
        if self.inner.ready.load(Ordering::Acquire)
        {
            return;
        }

        // Đầu gửi đi mà không gửi gì: người chờ phải biết, không thì họ chờ tới hết đời tiến trình.
        // Chuyện này xảy ra thật khi một job panic giữa chừng.
        self.inner.dropped.store(true, Ordering::Release);

        if let Some(waiter) = ignore_poison(self.inner.waiter.lock()).take()
        {
            waiter.wake();
        }
    }
}

impl<T> std::fmt::Debug for Sender<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("channel::Sender").finish_non_exhaustive()
    }
}
