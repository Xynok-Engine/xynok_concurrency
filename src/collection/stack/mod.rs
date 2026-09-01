use crate::sync::Ordering::{Acquire, Relaxed, Release};
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::packed::Packed;
pub struct SpmcStack<T>
{
    buffer: FixedBuffer<T>,
    cursor: Packed,
}

unsafe impl<T: Send> Send for SpmcStack<T> {}
unsafe impl<T: Send> Sync for SpmcStack<T> {}

impl<T> SpmcStack<T>
{
    pub fn new(capacity: usize) -> Self
    {
        Self {
            buffer: FixedBuffer::new(capacity),
            cursor: Packed::new(0, 0),
        }
    }
}
impl<T> SpmcStack<T>
{
    pub(crate) fn push(&self, val: T) -> Result<(), T>
    {
        let (next_push, next_pop) = self.cursor.load_unpack(Acquire);

        todo!()
    }
}
impl<T> SpmcStack<T> {}
