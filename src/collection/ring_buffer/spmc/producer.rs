use crate::collection::ring_buffer::spmc::SpmcRingBuffer;
use std::cell::Cell;
use std::marker::PhantomData;

pub struct Producer<'a, T>
{
    ring:     &'a SpmcRingBuffer<T>,
    not_sync: PhantomData<Cell<T>>,
}
impl<'a, T> Producer<'a, T>
{
    pub(crate) fn new(r: &'a SpmcRingBuffer<T>) -> Self
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
