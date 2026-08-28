use crate::collection::ring_buffer::consts::MAX_CAPACITY;
use crate::collection::ring_buffer::cursors::Cursors;
use crate::collection::ring_buffer::fixed_buffer::FixedBuffer;
use crate::collection::ring_buffer::head::Head;
use crate::sync::AtomicU32;
use crate::sync::Ordering::{Acquire, Relaxed, Release};
use crate::utils::cache_padded::CachePadded;

pub struct RingBuffer<T>
{
    head:   Head,
    tail:   CachePadded<AtomicU32>,
    buffer: FixedBuffer<T>,
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T>
{
    #[track_caller]
    pub fn new(capacity: usize) -> Self
    {
        debug_assert!(
            capacity.is_power_of_two() && capacity > 0,
            "`{}` capacity `{}` must be power of 2 and greater than 0 !",
            std::any::type_name::<Self>(),
            capacity
        );
        debug_assert!(
            capacity < MAX_CAPACITY,
            "`{}` capacity `{}` exceeds `{}` limit for wrapping u32 indices",
            std::any::type_name::<Self>(),
            capacity,
            MAX_CAPACITY
        );
        Self {
            head:   Head::new(0, 0),
            tail:   CachePadded::new(AtomicU32::new(0)),
            buffer: FixedBuffer::new(capacity as u32),
        }
    }

    #[inline]
    pub fn push(&mut self, val: T) -> Result<(), T>
    {
        let cursors = self.cursors();
        if cursors.available_slots() < 1
        {
            return Err(val);
        }
        unsafe {
            self.buffer.write(cursors.tail, val);
        }

        self.tail.store(cursors.tail.wrapping_add(1), Release);
        Ok(())
    }

    /// Takes up to `max` elements from `vals`. Elements are taken in order from left to right, starting from index `0` to `N`.
    /// - return: the number of elements successfully taken.
    #[inline]
    pub fn push_batch(&mut self, max: usize, vals: &mut Vec<T>) -> usize
    {
        let cursors = self.cursors();
        let take_amount = cursors.available_slots().min(vals.len()).min(max);
        if take_amount < 1
        {
            return 0;
        }
        for (offset, e) in vals.drain(..take_amount).enumerate()
        {
            let write_idx = cursors.tail + (offset as u32);
            unsafe {
                self.buffer.write(write_idx, e);
            }
        }

        self.tail.store(cursors.tail.wrapping_add(take_amount as u32), Release);
        take_amount
    }
}

impl<T> RingBuffer<T>
{
    #[inline]
    fn cursors(&self) -> Cursors
    {
        let (stolen, in_progress) = self.head.load_unpack(Acquire);
        let tail = self.tail.load(Relaxed);

        Cursors {
            stolen:      stolen,
            in_progress: in_progress,
            tail:        tail,
        }
    }
}
#[cfg(test)]
mod test
{
    #[test]
    fn test_drain()
    {
        let mut vals = vec![0, 1, 2, 3, 4, 5];
        let take_amount = 2;
        let mut drained = Vec::new();
        for e in vals.drain(..take_amount)
        {
            drained.push(e);
        }
        println!("vals:    {:?}", vals);
        println!("drained: {:?}", drained);
    }
}
