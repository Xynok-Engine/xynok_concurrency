//! src: <https://www.di.ens.fr/~zappa/readings/ppopp13.pdf>
//! src: <https://github.com/crossbeam-rs/crossbeam/blob/main/crossbeam-deque/src/deque.rs>

use crate::sync::{AtomicU32, AtomicU64, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::slots::Slots;
use crate::utils::{pack, unpack};

pub mod owner;
pub mod thief;

pub use crate::utils::steal::Steal;
pub use owner::Producer;
pub use thief::Consumer;

pub const MAX_SLOTS: u32 = 1 << 30;

pub struct RingBufferLifo<T>
{
    top:    CachePadded<AtomicU64>,
    bottom: CachePadded<AtomicU32>,
    slots:  Slots<T>,
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

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
