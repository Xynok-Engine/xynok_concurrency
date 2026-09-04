use std::collections::VecDeque;

use crate::utils::queue_batching::queue_batching::QueueBatching;

/// Tấm vé chứng minh đang giữ quyền vào [`QueueBatching`], và cũng là đường vào hàng đợi bên trong.
///
/// Cầm vé thì làm được mọi thứ mà một hàng đợi hai đầu thường làm được, gom bao nhiêu việc vào một
/// lần chiếm quyền cũng được. Vé rơi khỏi tầm nhìn là quyền tự trả lại.
///
/// > [!IMPORTANT]
/// > Còn giữ vé là mọi thread khác còn phải chờ. Lấy đủ việc cần rồi thả ra sớm.
pub struct QueueBatchingGuard<'a, T>
{
    pub(super) queue_batching: &'a QueueBatching<T>,
}

unsafe impl<T: Sync> Sync for QueueBatchingGuard<'_, T> {}

impl<T> Drop for QueueBatchingGuard<'_, T>
{
    fn drop(&mut self)
    {
        // Chốt lại độ dài ngay trước lúc nhả quyền. Đây là chỗ duy nhất làm việc đó: vé cho mượn
        // thẳng hàng đợi bên trong nên không ai đoán trước được người cầm vé đã đổi những gì, chỉ
        // biết chắc là lúc này thì họ xong rồi.
        let len = self.len();
        self.queue_batching.release(len);
    }
}

impl<T> std::ops::Deref for QueueBatchingGuard<'_, T>
{
    type Target = VecDeque<T>;

    #[inline]
    fn deref(&self) -> &Self::Target
    {
        self.queue_batching.elements.with(|p| unsafe { &*p })
    }
}

impl<T> std::ops::DerefMut for QueueBatchingGuard<'_, T>
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        self.queue_batching.elements.with_mut(|p| unsafe { &mut *p })
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for QueueBatchingGuard<'_, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        std::fmt::Debug::fmt(&**self, f)
    }
}
