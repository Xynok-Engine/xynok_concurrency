use crate::collection::fixed_buffer::FixedBuffer;
use crate::sync::AtomicU32;
use crate::utils::cache_padded::CachePadded;
pub struct SpmcStack<T>
{
    buffer: FixedBuffer<T>,
    tail:   CachePadded<AtomicU32>,
}

unsafe impl<T: Send> Send for SpmcStack<T> {}
unsafe impl<T: Send> Sync for SpmcStack<T> {}

impl<T> SpmcStack<T>
{
    pub fn new(capacity: usize) -> Self
    {
        Self {
            buffer: FixedBuffer::new(capacity),
            tail:   CachePadded::new(AtomicU32::new(0)),
        }
    }
}
impl<T> SpmcStack<T>
{
    pub(crate) fn push(&self, val: T) -> Result<(), T>
    {
        todo!()
    }
}
