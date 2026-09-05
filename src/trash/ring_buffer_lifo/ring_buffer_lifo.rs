//! src: <https://www.di.ens.fr/~zappa/readings/ppopp13.pdf>
//! src: <https://github.com/crossbeam-rs/crossbeam/blob/main/crossbeam-deque/src/deque.rs>

use crate::ring_buffer_lifo::consts::MAX_SLOTS;
use crate::ring_buffer_lifo::owner::Producer;
use crate::ring_buffer_lifo::thief::Consumer;
use crate::sync::{AtomicU32, AtomicU64, Ordering};
use crate::utils::bits::{pack, unpack};
use crate::utils::cache_padded::CachePadded;
use crate::utils::slots::Slots;

/// Ring mà chủ lấy việc mới nhất trước, còn kẻ trộm lấy việc cũ nhất trước.
///
/// Chủ ring đẩy vào và lấy ra ở cùng một đầu, nên việc vừa đẻ ra được chạy ngay lúc cache còn nóng.
/// Kẻ trộm bốc từ đầu kia, tức là những việc cũ nhất, thường cũng là những việc to nhất.
pub struct RingBufferLifo<T>
{
    pub(crate) top:    CachePadded<AtomicU64>,
    pub(crate) bottom: CachePadded<AtomicU32>,
    pub(crate) slots:  Slots<T>,
}

unsafe impl<T: Send> Send for RingBufferLifo<T> {}
unsafe impl<T: Send> Sync for RingBufferLifo<T> {}

impl<T> RingBufferLifo<T>
{
    #[track_caller]
    pub fn new(total_slots: u32) -> Self
    {
        assert!(
            total_slots <= MAX_SLOTS,
            "total_slots {total_slots} exceeds the 2^30 limit for signed u32 index math"
        );

        Self {
            top:    CachePadded::new(AtomicU64::new(pack(0, 0))),
            bottom: CachePadded::new(AtomicU32::new(0)),
            slots:  Slots::new(total_slots),
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
        let (free, _) = unpack(self.top.load(Ordering::Acquire));
        let bottom = self.bottom.load(Ordering::Acquire);
        (bottom.wrapping_sub(free) as i32).clamp(0, self.capacity() as i32) as usize
    }

    #[inline]
    pub fn available(&self) -> usize
    {
        let (_, claim) = unpack(self.top.load(Ordering::Acquire));
        let bottom = self.bottom.load(Ordering::Acquire);
        (bottom.wrapping_sub(claim) as i32).max(0) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.available() == 0
    }
}

impl<T> Drop for RingBufferLifo<T>
{
    fn drop(&mut self)
    {
        let (_, claim) = unpack(self.top.load(Ordering::Relaxed));
        let bottom = self.bottom.load(Ordering::Relaxed);
        let live = (bottom.wrapping_sub(claim) as i32).max(0) as u32;

        for offset in 0..live
        {
            unsafe { self.slots.drop_at(claim.wrapping_add(offset)) };
        }
    }
}

impl<T> std::fmt::Debug for RingBufferLifo<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        let (free, claim) = unpack(self.top.load(Ordering::Relaxed));
        f.debug_struct("RingBufferLifo")
            .field("capacity", &self.capacity())
            .field("free", &free)
            .field("claim", &claim)
            .field("bottom", &self.bottom.load(Ordering::Relaxed))
            .finish()
    }
}
