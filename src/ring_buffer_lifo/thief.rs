use super::owner::Producer;
use super::{RingBufferLifo, Steal};
use crate::sync::{Ordering, fence};
use crate::utils::{pack, unpack};

pub struct Consumer<'a, T>
{
    ring: &'a RingBufferLifo<T>,
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
    pub(super) fn new(ring: &'a RingBufferLifo<T>) -> Self
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

    fn claim(&self) -> Steal<u32>
    {
        let head = self.ring.top.load(Ordering::SeqCst);
        let (free, claim) = unpack(head);
        if free != claim
        {
            return Steal::Busy;
        }

        fence(Ordering::SeqCst);

        let bottom = self.ring.bottom.load(Ordering::SeqCst);
        if (bottom.wrapping_sub(claim) as i32) <= 0
        {
            return Steal::Empty;
        }

        let next = claim.wrapping_add(1);
        match self.ring.top.compare_exchange(head, pack(claim, next), Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => Steal::Success(claim),
            Err(_) => Steal::Busy,
        }
    }

    fn release(&self)
    {
        let mut head = self.ring.top.load(Ordering::SeqCst);

        loop
        {
            let (_, claim) = unpack(head);
            match self
                .ring
                .top
                .compare_exchange_weak(head, pack(claim, claim), Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    pub fn try_steal(&self) -> Steal<T>
    {
        let start = match self.claim()
        {
            Steal::Success(start) => start,
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

    pub fn try_steal_into(&self, dst: &mut Producer<'_, T>) -> Steal<usize>
    {
        if dst.free_slots() == 0
        {
            return Steal::Busy;
        }

        let start = match self.claim()
        {
            Steal::Success(start) => start,
            Steal::Empty => return Steal::Empty,
            Steal::Busy => return Steal::Busy,
        };

        unsafe {
            let val = self.ring.slots.read(start);
            dst.write_at(0, val);
            dst.publish(1);
        }
        self.release();

        Steal::Success(1)
    }

    #[inline]
    pub fn steal_into(&self, dst: &mut Producer<'_, T>) -> usize
    {
        self.try_steal_into(dst).success().unwrap_or(0)
    }
}
