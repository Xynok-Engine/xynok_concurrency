use crate::collection::ring_buffer::spmc::SpmcRingBuffer;

pub struct Consumer<'a, T>
{
    ring: &'a SpmcRingBuffer<T>,
}
impl<'a, T> Consumer<'a, T>
{
    pub(crate) fn new(r: &'a SpmcRingBuffer<T>) -> Self
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
