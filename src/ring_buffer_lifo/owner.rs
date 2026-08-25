use std::cell::Cell;
use std::marker::PhantomData;

use super::RingBufferLifo;
use crate::sync::{Ordering, fence};
use crate::utils::{pack, unpack};

pub struct Producer<'a, T>
{
    ring:  &'a RingBufferLifo<T>,
    free:  Cell<u32>,
    _lone: PhantomData<Cell<T>>,
}

impl<'a, T> Producer<'a, T>
{
    #[inline]
    pub(super) fn new(ring: &'a RingBufferLifo<T>) -> Self
    {
        let (free, _) = unpack(ring.top.load(Ordering::Acquire));
        Self {
            ring:  ring,
            free:  Cell::new(free),
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
        let bottom = self.ring.bottom.load(Ordering::Relaxed);
        self.refresh(bottom) as usize
    }

    #[inline]
    fn refresh(&self, bottom: u32) -> u32
    {
        let (free, _) = unpack(self.ring.top.load(Ordering::Acquire));
        self.free.set(free);
        self.ring.capacity() as u32 - bottom.wrapping_sub(free)
    }

    #[inline]
    fn free_from(&self, bottom: u32) -> u32
    {
        let used = bottom.wrapping_sub(self.free.get());
        let capacity = self.ring.capacity() as u32;

        match used < capacity
        {
            true => capacity - used,
            false => self.refresh(bottom),
        }
    }

    #[inline]
    pub(super) fn free_slots(&self) -> u32
    {
        let bottom = self.ring.bottom.load(Ordering::Relaxed);
        self.refresh(bottom)
    }

    #[inline]
    pub(super) unsafe fn write_at(&mut self, offset: u32, val: T)
    {
        let bottom = self.ring.bottom.load(Ordering::Relaxed);
        unsafe { self.ring.slots.write(bottom.wrapping_add(offset), val) };
    }

    #[inline]
    pub(super) unsafe fn publish(&mut self, n: u32)
    {
        let bottom = self.ring.bottom.load(Ordering::Relaxed);
        self.ring.bottom.store(bottom.wrapping_add(n), Ordering::Release);
    }

    pub fn push(&mut self, val: T) -> Result<(), T>
    {
        let bottom = self.ring.bottom.load(Ordering::Relaxed);

        if self.free_from(bottom) == 0
        {
            return Err(val);
        }

        {
            let (rf, rc) = unpack(self.ring.top.load(Ordering::SeqCst));
            assert!(
                bottom.wrapping_sub(rf) < self.ring.capacity() as u32,
                "PUSH OVERWRITE bottom={bottom} free={rf} claim={rc} cap={}",
                self.ring.capacity()
            );
        }
        unsafe { self.ring.slots.write(bottom, val) };
        self.ring.bottom.store(bottom.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    pub fn push_batch(&mut self, vals: &mut Vec<T>) -> usize
    {
        if vals.is_empty()
        {
            return 0;
        }

        let bottom = self.ring.bottom.load(Ordering::Relaxed);
        let n = (self.refresh(bottom) as usize).min(vals.len());
        if n == 0
        {
            return 0;
        }

        for (offset, val) in vals.drain(..n).enumerate()
        {
            let idx = bottom.wrapping_add(offset as u32);
            let (rf, rc) = unpack(self.ring.top.load(Ordering::SeqCst));
            assert!(
                idx.wrapping_sub(rf) < self.ring.capacity() as u32,
                "PUSH_BATCH OVERWRITE idx={idx} bottom={bottom} n={n} free={rf} claim={rc}"
            );
            unsafe { self.ring.slots.write(idx, val) };
        }

        self.ring.bottom.store(bottom.wrapping_add(n as u32), Ordering::Release);
        n
    }

    pub fn push_iter<I>(&mut self, vals: I) -> usize
    where I: IntoIterator<Item = T>
    {
        let bottom = self.ring.bottom.load(Ordering::Relaxed);
        let free = self.refresh(bottom);

        let mut n = 0;
        for val in vals.into_iter().take(free as usize)
        {
            unsafe { self.ring.slots.write(bottom.wrapping_add(n), val) };
            n += 1;
        }

        if n > 0
        {
            self.ring.bottom.store(bottom.wrapping_add(n), Ordering::Release);
        }
        n as usize
    }

    pub fn pop(&mut self) -> Option<T>
    {
        let bottom = self.ring.bottom.load(Ordering::Relaxed);
        let next = bottom.wrapping_sub(1);
        self.ring.bottom.store(next, Ordering::Relaxed);
        fence(Ordering::SeqCst);

        let mut head = self.ring.top.load(Ordering::SeqCst);

        loop
        {
            let (free, claim) = unpack(head);
            self.free.set(free);
            let size = next.wrapping_sub(claim) as i32;

            if size < 0
            {
                self.ring.bottom.store(bottom, Ordering::Relaxed);
                return None;
            }
            if size > 0
            {
                return Some(unsafe { self.ring.slots.read(next) });
            }

            let taken = claim.wrapping_add(1);
            let advanced = match free == claim
            {
                true => pack(taken, taken),
                false => pack(free, taken),
            };

            match self.ring.top.compare_exchange_weak(head, advanced, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) =>
                {
                    self.ring.bottom.store(bottom, Ordering::Relaxed);
                    return Some(unsafe { self.ring.slots.read(next) });
                }
                Err(actual) => head = actual,
            }
        }
    }

    pub fn pop_batch_with(&mut self, max: usize, mut sink: impl FnMut(T)) -> usize
    {
        let mut taken = 0;
        while taken < max
        {
            match self.pop()
            {
                Some(val) => sink(val),
                None => break,
            }
            taken += 1;
        }
        taken
    }

    #[inline]
    pub fn pop_batch(&mut self, out: &mut Vec<T>, max: usize) -> usize
    {
        out.reserve(max.min(self.ring.available()));
        self.pop_batch_with(max, |val| out.push(val))
    }

    #[inline]
    pub fn drain(&mut self, out: &mut Vec<T>) -> usize
    {
        self.pop_batch_with(usize::MAX, |val| out.push(val))
    }
}
