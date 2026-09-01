use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;
use std::cell::Cell;
use std::marker::PhantomData;

pub struct Producer<'a, T>
{
    ring:     &'a SpmcRingBufferFifo<T>,
    not_sync: PhantomData<Cell<T>>,
}
impl<'a, T> Producer<'a, T>
{
    pub(crate) fn new(r: &'a SpmcRingBufferFifo<T>) -> Self
    {
        Self {
            ring:     r,
            not_sync: PhantomData,
        }
    }

    #[inline]
    pub fn push(&self, val: T) -> Result<(), T>
    {
        self.ring.push(val)
    }

    #[inline]
    pub fn push_batch(&self, max: usize, vals: &mut Vec<T>) -> usize
    {
        self.ring.push_batch(max, vals)
    }
}
