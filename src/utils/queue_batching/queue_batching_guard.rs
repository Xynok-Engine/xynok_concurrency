use std::collections::VecDeque;

use crate::utils::queue_batching::queue_batching::QueueBatching;
pub struct QueueBatchingGuard<'a, T>
{
    pub(super) queue_batching: &'a QueueBatching<T>,
}

unsafe impl<T: Sync> Sync for QueueBatchingGuard<'_, T> {}

impl<T> Drop for QueueBatchingGuard<'_, T>
{
    fn drop(&mut self)
    {
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
