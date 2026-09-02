use super::owner::Producer;
use super::{RingBufferFifo, Steal};
use crate::sync::Ordering;
use crate::utils::bits::{pack, unpack};

pub struct Consumer<'a, T>
{
    ring: &'a RingBufferFifo<T>,
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

impl<'a, T> Consumer<'a, T>
{
    #[inline]
    pub(super) fn new(ring: &'a RingBufferFifo<T>) -> Self
    {
        Self { ring: ring }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.ring.capacity()
    }

    #[inline]
    pub fn available(&self) -> usize
    {
        self.ring.available()
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.ring.is_empty()
    }

    fn claim(&self, max: u32) -> Steal<(u32, u32)>
    {
        if max == 0
        {
            return Steal::Busy;
        }

        let mut head = self.ring.head.load(Ordering::Acquire);

        loop
        {
            let (steal, real) = unpack(head);
            if steal != real
            {
                return Steal::Busy;
            }

            let tail = self.ring.tail.load(Ordering::Acquire);
            let avail = tail.wrapping_sub(real);
            if avail == 0
            {
                return Steal::Empty;
            }

            let n = avail.min(max);
            match self
                .ring
                .head
                .compare_exchange_weak(head, pack(real, real.wrapping_add(n)), Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Steal::Success((real, n)),
                Err(actual) => head = actual,
            }
        }
    }

    fn release(&self)
    {
        let mut head = self.ring.head.load(Ordering::Relaxed);

        loop
        {
            let (_, real) = unpack(head);
            match self
                .ring
                .head
                .compare_exchange_weak(head, pack(real, real), Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    pub fn try_steal(&self) -> Steal<T>
    {
        let (start, _) = match self.claim(1)
        {
            Steal::Success(claimed) => claimed,
            Steal::Empty => return Steal::Empty,
            Steal::Busy => return Steal::Busy,
        };

        let val = unsafe { self.ring.slots.read(start) };
        self.release();
        Steal::Success(val)
    }

    #[inline]
    pub fn steal(&self) -> Option<T>
    {
        self.try_steal().success()
    }

    pub fn try_steal_with(&self, max: usize, sink: impl FnMut(T)) -> Steal<usize>
    {
        let max = max.min(u32::MAX as usize) as u32;

        let (start, n) = match self.claim(max)
        {
            Steal::Success(claimed) => claimed,
            Steal::Empty => return Steal::Empty,
            Steal::Busy => return Steal::Busy,
        };

        unsafe { self.ring.drain_claimed_with(start, n, sink) };
        self.release();
        Steal::Success(n as usize)
    }

    pub fn try_steal_batch(&self, dst: &mut Vec<T>, max: usize) -> Steal<usize>
    {
        dst.reserve(max.min(self.ring.available()));
        self.try_steal_with(max, |val| dst.push(val))
    }

    #[inline]
    pub fn steal_batch(&self, dst: &mut Vec<T>, max: usize) -> usize
    {
        self.try_steal_batch(dst, max).success().unwrap_or(0)
    }

    #[inline]
    pub fn try_steal_half(&self, dst: &mut Vec<T>) -> Steal<usize>
    {
        let avail = self.ring.available();
        match avail
        {
            0 => Steal::Empty,
            _ => self.try_steal_batch(dst, avail - avail / 2),
        }
    }

    #[inline]
    pub fn steal_half(&self, dst: &mut Vec<T>) -> usize
    {
        self.try_steal_half(dst).success().unwrap_or(0)
    }

    pub fn try_steal_into(&self, dst: &mut Producer<'_, T>) -> Steal<usize>
    {
        let room = dst.free_slots();
        if room == 0
        {
            return Steal::Busy;
        }

        let avail = self.ring.available() as u32;
        if avail == 0
        {
            return Steal::Empty;
        }

        let want = (avail - avail / 2).min(room);
        let (start, n) = match self.claim(want)
        {
            Steal::Success(claimed) => claimed,
            Steal::Empty => return Steal::Empty,
            Steal::Busy => return Steal::Busy,
        };

        let mut offset = 0;
        unsafe {
            self.ring.drain_claimed_with(start, n, |val| {
                dst.write_at(offset, val);
                offset += 1;
            })
        };
        unsafe { dst.publish(n) };
        self.release();

        Steal::Success(n as usize)
    }

    #[inline]
    pub fn steal_into(&self, dst: &mut Producer<'_, T>) -> usize
    {
        self.try_steal_into(dst).success().unwrap_or(0)
    }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/thief.rs"]
mod test;
