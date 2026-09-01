use crate::collection::ring_buffer::spmc_lifo_produce_fifo_consume::SpmcRingBufferLifoProduceFifoConsume;

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

    #[inline]
    pub fn pop_batch(&self) -> usize
    {
        self.src.consumer_pop_batch_to(10, self.dst)
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
