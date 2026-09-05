use crate::ring_buffer_fifo::consts::MAX_SLOTS;
use crate::ring_buffer_fifo::owner::Producer;
use crate::ring_buffer_fifo::thief::Consumer;
use crate::sync::{AtomicU32, AtomicU64, Ordering};
use crate::utils::bits::{pack, unpack};
use crate::utils::cache_padded::CachePadded;
use crate::utils::slots::Slots;

/// Ring vào trước ra trước, một người ghi và nhiều người trộm.
///
/// Chủ ring đẩy vào một đầu, kẻ trộm bốc từ đầu kia, nên hai bên hiếm khi đụng vào cùng một dòng
/// cache. Con trỏ đầu ring gói hai số vào chung một ô nhớ: chỗ kẻ trộm đang bốc dở, và chỗ thật sự
/// còn hàng.
pub struct RingBufferFifo<T>
{
    pub(crate) head:  CachePadded<AtomicU64>,
    pub(crate) tail:  CachePadded<AtomicU32>,
    pub(crate) slots: Slots<T>,
}

unsafe impl<T: Send> Send for RingBufferFifo<T> {}
unsafe impl<T: Send> Sync for RingBufferFifo<T> {}

impl<T> RingBufferFifo<T>
{
    #[track_caller]
    pub fn new(total_slots: u32) -> Self
    {
        assert!(
            total_slots <= MAX_SLOTS,
            "total_slots {total_slots} exceeds the 2^31 limit for wrapping u32 indices"
        );

        Self {
            head:  CachePadded::new(AtomicU64::new(pack(0, 0))),
            tail:  CachePadded::new(AtomicU32::new(0)),
            slots: Slots::new(total_slots),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.slots.capacity()
    }

    #[inline]
    pub fn split(&mut self) -> (Producer<'_, T>, Consumer<'_, T>)
    {
        let this = &*self;
        (Producer::new(this), Consumer::new(this))
    }

    #[inline]
    pub fn consumer(&self) -> Consumer<'_, T>
    {
        Consumer::new(self)
    }

    #[inline]
    pub unsafe fn producer(&self) -> Producer<'_, T>
    {
        Producer::new(self)
    }

    #[inline]
    pub fn occupied(&self) -> usize
    {
        let (steal, _) = unpack(self.head.load(Ordering::Acquire));
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(steal) as usize
    }

    #[inline]
    pub fn available(&self) -> usize
    {
        let (_, real) = unpack(self.head.load(Ordering::Acquire));
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(real) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.available() == 0
    }

    #[inline]
    pub(crate) unsafe fn drain_claimed_with(&self, start: u32, n: u32, mut sink: impl FnMut(T))
    {
        for offset in 0..n
        {
            sink(unsafe { self.slots.read(start.wrapping_add(offset)) });
        }
    }
}

impl<T> Drop for RingBufferFifo<T>
{
    fn drop(&mut self)
    {
        let (_, real) = unpack(self.head.load(Ordering::Relaxed));
        let tail = self.tail.load(Ordering::Relaxed);

        for offset in 0..tail.wrapping_sub(real)
        {
            unsafe { self.slots.drop_at(real.wrapping_add(offset)) };
        }
    }
}

impl<T> std::fmt::Debug for RingBufferFifo<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        let (steal, real) = unpack(self.head.load(Ordering::Relaxed));
        f.debug_struct("RingBufferFifo")
            .field("capacity", &self.capacity())
            .field("steal", &steal)
            .field("real", &real)
            .field("tail", &self.tail.load(Ordering::Relaxed))
            .finish()
    }
}
