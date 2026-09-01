use crate::collection::ring_buffer::params::WriteableBuffer;
use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;

pub struct Consumer<'a, T>
{
    ring: &'a SpmcRingBufferFifo<T>,
}
impl<'a, T> Consumer<'a, T>
{
    pub(crate) fn new(r: &'a SpmcRingBufferFifo<T>) -> Self
    {
        Self { ring: r }
    }

    #[inline]
    pub fn pop(&self) -> Option<T>
    {
        self.ring.pop()
    }

    #[inline]
    pub fn pop_batch(&self, max: usize, dst: &mut Vec<T>) -> usize
    {
        self.ring.pop_batch(max, dst)
    }
    #[inline]
    pub fn pop_batch_to(&self, dst: WriteableBuffer<T>) -> usize
    {
        self.ring.pop_batch_to(dst)
    }

    #[inline]
    pub fn claim(&self) {}
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
