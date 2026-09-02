use crate::collection::ring_buffer::spmc_lifo_produce_fifo_consume::SpmcRingBufferLifoProduceFifoConsume;
use crate::utils::steal::Steal;

/// Số việc vét mỗi lần khi người gọi không nói rõ muốn lấy bao nhiêu.
const DEFAULT_STEAL_BATCH: usize = 10;

pub struct Consumer<'a, T>
{
    src: &'a SpmcRingBufferLifoProduceFifoConsume<T>,
    dst: &'a SpmcRingBufferLifoProduceFifoConsume<T>,
}
impl<'a, T> Consumer<'a, T>
{
    pub(crate) fn new(src: &'a SpmcRingBufferLifoProduceFifoConsume<T>, dst: &'a SpmcRingBufferLifoProduceFifoConsume<T>) -> Self
    {
        Self { src, dst }
    }

    /// Vét một lô từ nạn nhân sang deque của mình, kèm lý do khi lấy hụt.
    ///
    /// [`Steal::Empty`] là nạn nhân cạn thật, nên đi tìm chỗ khác. [`Steal::Busy`] là bị chen ngang
    /// hoặc deque của mình hết chỗ, quay lại sau vẫn còn hàng.
    #[inline]
    pub fn try_steal(&self) -> Steal<usize>
    {
        self.try_steal_batch(DEFAULT_STEAL_BATCH)
    }

    /// Như [`Self::try_steal`] nhưng tự chọn trần cho lô.
    #[inline]
    pub fn try_steal_batch(&self, max: usize) -> Steal<usize>
    {
        self.src.try_steal_batch_to(max, self.dst)
    }

    /// Trộm đúng một việc và cầm luôn giá trị về, không đi qua deque đích.
    #[inline]
    pub fn try_steal_one(&self) -> Steal<T>
    {
        self.src.try_steal_one()
    }

    /// Bản gọn của [`Self::try_steal_one`] cho ai không quan tâm vì sao lấy hụt.
    #[inline]
    pub fn steal_one(&self) -> Option<T>
    {
        self.try_steal_one().success()
    }

    /// Bản gọn của [`Self::try_steal`], chỉ trả về số phần tử lấy được.
    #[inline]
    pub fn pop_batch(&self) -> usize
    {
        self.try_steal().success().unwrap_or(0)
    }
}
impl<T> Clone for Consumer<'_, T>
{
    #[inline]
    fn clone(&self) -> Self
    {
        *self
    }
}
impl<T> Copy for Consumer<'_, T> {}
