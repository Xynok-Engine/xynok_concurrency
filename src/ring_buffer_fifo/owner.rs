use std::cell::Cell;
use std::marker::PhantomData;

use super::RingBufferFifo;
use crate::sync::Ordering;
use crate::utils::{pack, unpack};

pub struct Producer<'a, T>
{
    ring:  &'a RingBufferFifo<T>,
    steal: Cell<u32>,
    _lone: PhantomData<Cell<T>>,
}

impl<'a, T> Producer<'a, T>
{
    #[inline]
    pub(super) fn new(ring: &'a RingBufferFifo<T>) -> Self
    {
        let (steal, _) = unpack(ring.head.load(Ordering::Acquire));
        Self {
            ring:  ring,
            steal: Cell::new(steal),
            _lone: PhantomData,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.ring.capacity()
    }

    #[inline]
    pub fn occupied(&self) -> usize
    {
        self.ring.occupied()
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

    #[inline]
    pub fn is_full(&self) -> bool
    {
        self.remaining() == 0
    }

    #[inline]
    pub fn remaining(&self) -> usize
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        self.refresh(tail) as usize
    }

    #[inline]
    fn refresh(&self, tail: u32) -> u32
    {
        let (steal, _) = unpack(self.ring.head.load(Ordering::Acquire));
        self.steal.set(steal);
        self.ring.capacity() as u32 - tail.wrapping_sub(steal)
    }

    #[inline]
    fn free_from(&self, tail: u32) -> u32
    {
        let used = tail.wrapping_sub(self.steal.get());
        let capacity = self.ring.capacity() as u32;

        match used < capacity
        {
            true => capacity - used,
            false => self.refresh(tail),
        }
    }

    #[inline]
    pub(super) fn free_slots(&self) -> u32
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        self.refresh(tail)
    }

    #[inline]
    pub(super) unsafe fn write_at(&mut self, offset: u32, val: T)
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        unsafe { self.ring.slots.write(tail.wrapping_add(offset), val) };
    }

    #[inline]
    pub(super) unsafe fn publish(&mut self, n: u32)
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        self.ring.tail.store(tail.wrapping_add(n), Ordering::Release);
    }

    pub fn push(&mut self, val: T) -> Result<(), T>
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);

        if self.free_from(tail) == 0
        {
            return Err(val);
        }

        unsafe { self.ring.slots.write(tail, val) };
        self.ring.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    pub fn push_batch(&mut self, vals: &mut Vec<T>) -> usize
    {
        if vals.is_empty()
        {
            return 0;
        }

        let tail = self.ring.tail.load(Ordering::Relaxed);
        let n = (self.refresh(tail) as usize).min(vals.len());
        if n == 0
        {
            return 0;
        }

        for (offset, val) in vals.drain(..n).enumerate()
        {
            unsafe { self.ring.slots.write(tail.wrapping_add(offset as u32), val) };
        }

        self.ring.tail.store(tail.wrapping_add(n as u32), Ordering::Release);
        n
    }

    pub fn push_iter<I>(&mut self, vals: I) -> usize
    where I: IntoIterator<Item = T>
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let free = self.refresh(tail);

        let mut n = 0;
        for val in vals.into_iter().take(free as usize)
        {
            unsafe { self.ring.slots.write(tail.wrapping_add(n), val) };
            n += 1;
        }

        if n > 0
        {
            self.ring.tail.store(tail.wrapping_add(n), Ordering::Release);
        }
        n as usize
    }

    pub fn pop(&mut self) -> Option<T>
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let mut head = self.ring.head.load(Ordering::Acquire);

        let real = loop
        {
            let (steal, real) = unpack(head);
            if real == tail
            {
                self.steal.set(steal);
                return None;
            }

            let next_real = real.wrapping_add(1);
            let next = match steal == real
            {
                true => pack(next_real, next_real),
                false => pack(steal, next_real),
            };

            match self.ring.head.compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) =>
                {
                    self.steal.set(unpack(next).0);
                    break real;
                }
                Err(actual) => head = actual,
            }
        };

        Some(unsafe { self.ring.slots.read(real) })
    }

    pub fn pop_batch_with(&mut self, max: usize, sink: impl FnMut(T)) -> usize
    {
        if max == 0
        {
            return 0;
        }
        let max = max.min(u32::MAX as usize) as u32;

        let tail = self.ring.tail.load(Ordering::Relaxed);
        let mut head = self.ring.head.load(Ordering::Acquire);

        let (start, n) = loop
        {
            let (steal, real) = unpack(head);
            let avail = tail.wrapping_sub(real);
            if avail == 0
            {
                self.steal.set(steal);
                return 0;
            }

            let n = avail.min(max);
            let next_real = real.wrapping_add(n);
            let next = match steal == real
            {
                true => pack(next_real, next_real),
                false => pack(steal, next_real),
            };

            match self.ring.head.compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) =>
                {
                    self.steal.set(unpack(next).0);
                    break (real, n);
                }
                Err(actual) => head = actual,
            }
        };

        unsafe { self.ring.drain_claimed_with(start, n, sink) };
        n as usize
    }

    #[inline]
    pub fn pop_batch(&mut self, out: &mut Vec<T>, max: usize) -> usize
    {
        out.reserve(max.min(self.ring.available()));
        self.pop_batch_with(max, |val| out.push(val))
    }

    #[inline]
    pub fn spill_half(&mut self, out: &mut Vec<T>) -> usize
    {
        let avail = self.ring.available();
        match avail
        {
            0 => 0,
            _ => self.pop_batch(out, avail - avail / 2),
        }
    }

    #[inline]
    pub fn drain(&mut self, out: &mut Vec<T>) -> usize
    {
        let mut total = 0;
        loop
        {
            let taken = self.pop_batch(out, u32::MAX as usize);
            if taken == 0
            {
                return total;
            }
            total += taken;
        }
    }
}
