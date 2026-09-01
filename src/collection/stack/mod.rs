use crate::sync::AtomicU32;
use crate::sync::Ordering::{Acquire, Relaxed, Release};
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedRingBuffer;
use crate::utils::packed::Packed;
pub struct SpmcStack<T>
{
    buffer: FixedRingBuffer<T>,
    head:   Packed,
    tail:   CachePadded<AtomicU32>,
}

unsafe impl<T: Send> Send for SpmcStack<T> {}
unsafe impl<T: Send> Sync for SpmcStack<T> {}

impl<T> SpmcStack<T>
{
    pub fn new(capacity: usize) -> Self
    {
        Self {
            buffer: FixedRingBuffer::new(capacity),
            head:   Packed::new(0, 0),
            tail:   CachePadded::new(AtomicU32::new(0)),
        }
    }
}
impl<T> SpmcStack<T>
{
    pub(crate) fn push(&self, val: T) -> Result<(), T>
    {
        let (next_push, next_pop) = self.head.load_unpack(Acquire);

        todo!()
    }
}
impl<T> SpmcStack<T> {}
