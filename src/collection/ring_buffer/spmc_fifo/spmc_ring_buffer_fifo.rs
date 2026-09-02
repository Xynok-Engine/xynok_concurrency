use crate::collection::ring_buffer::params::{ParamsCasForPopBatch, WriteableBuffer};
use crate::sync::AtomicU32;
use crate::sync::Ordering::{Acquire, Relaxed, Release};
use crate::utils::backoff::Backoff;
use crate::utils::bits::{pack, unpack};
use crate::utils::cache_padded::CachePadded;
use crate::utils::cursors::CursorData;
use crate::utils::fixed_buffer::FixedRingBuffer;
use crate::utils::packed::Packed;

use crate::collection::ring_buffer::spmc_fifo::consumer::Consumer;
use crate::collection::ring_buffer::spmc_fifo::producer::Producer;

/// Single-Producer, Multiple-Consumer (SPMC) ring buffer implementing First-In, First-Out (FIFO) semantics.
/// producer and consumers operate using FIFO ordering.
pub struct SpmcRingBufferFifo<T>
{
    head:   Packed,
    /// Note: the tail points to the next available slot where the next push occurs
    tail:   CachePadded<AtomicU32>,
    buffer: FixedRingBuffer<T>,
}

unsafe impl<T: Send> Send for SpmcRingBufferFifo<T> {}
unsafe impl<T: Send> Sync for SpmcRingBufferFifo<T> {}

impl<T> Drop for SpmcRingBufferFifo<T>
{
    fn drop(&mut self)
    {
        let (_, real) = unpack(self.head.load(Relaxed));
        let tail = self.tail.load(Relaxed);

        for offset in 0..tail.wrapping_sub(real)
        {
            unsafe { self.buffer.drop_at(real.wrapping_add(offset)) };
        }
    }
}

impl<T> SpmcRingBufferFifo<T>
{
    /// The capacity must be a power of 2. If it is not, the code will panic.
    pub fn new(capacity: usize) -> Self
    {
        Self {
            head:   Packed::new(0, 0),
            tail:   CachePadded::new(AtomicU32::new(0)),
            buffer: FixedRingBuffer::new(capacity),
        }
    }
    pub fn split(&self) -> (Producer<'_, T>, Consumer<'_, T>)
    {
        (self.producer(), self.consumer())
    }
    pub fn producer(&self) -> Producer<'_, T>
    {
        Producer::new(self)
    }

    pub fn consumer(&self) -> Consumer<'_, T>
    {
        Consumer::new(self)
    }
    #[inline]
    pub fn len(&self) -> usize
    {
        let data = self.cursor_data();
        data.filled_slots()
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.len() < 1
    }
}

impl<T> SpmcRingBufferFifo<T>
{
    // There is only ever one producer, so `tail` has no contention: no CAS is needed here, just a
    // plain load and a plain store. What matters is the order: write the data first, publish the
    // new `tail` (Release) after, so an consumer that observes the new `tail` (Acquire, see
    // `cursor_data`) is guaranteed to also observe the write that just happened.
    #[inline]
    pub(crate) fn push(&self, val: T) -> Result<(), T>
    {
        let cursors = self.cursor_data();
        if cursors.empty_slots() < 1
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
    pub(crate) fn push_batch(&self, max: usize, vals: &mut Vec<T>) -> usize
    {
        debug_assert!(max > 0, "The push batch size must be greater than zero.");
        debug_assert!(!vals.is_empty(), "cannot take data from an empty source");
        let cursors = self.cursor_data();
        let take_amount = cursors.empty_slots().min(max).min(vals.len());
        if take_amount < 1
        {
            return 0;
        }

        for (offset, e) in vals.drain(..take_amount).enumerate()
        {
            let write_idx = cursors.tail.wrapping_add(offset as u32);
            unsafe {
                self.buffer.write(write_idx, e);
            }
        }

        self.tail.store(cursors.tail.wrapping_add(take_amount as u32), Release);
        take_amount
    }
    pub(crate) fn pop(&self) -> Option<T>
    {
        let mut cursor_data = self.cursor_data();

        let params = ParamsCasForPopBatch {
            cursor_data:           &mut cursor_data,
            pop_amount:            1,
            success_order:         Release,
            fail_order:            Acquire,
            fetch_after_cas_order: Relaxed,
        };

        self.try_cas_for_pop_batch(params)?;

        let claim_start = cursor_data.blocked;
        let result = unsafe { Some(self.buffer.take_at(claim_start)) };

        self.publish_stolen(claim_start, 1);

        result
    }
    #[inline]
    pub(crate) fn pop_batch(&self, max: usize, dst: &mut Vec<T>) -> usize
    {
        let mut cursor_data = self.cursor_data();

        let params = ParamsCasForPopBatch {
            cursor_data:           &mut cursor_data,
            pop_amount:            max,
            success_order:         Release,
            fail_order:            Acquire,
            fetch_after_cas_order: Relaxed,
        };

        let pop_amount = match self.try_cas_for_pop_batch(params)
        {
            Some(r) => r,
            None => return 0,
        };
        let claim_start = cursor_data.blocked;
        for offset in 0..pop_amount
        {
            let cursor_idx = claim_start.wrapping_add(offset as u32);
            let val = unsafe { self.buffer.take_at(cursor_idx) };
            dst.push(val);
        }
        self.publish_stolen(claim_start, pop_amount as u32);
        pop_amount
    }
    #[inline]
    pub(crate) fn pop_batch_to(&self, dst: WriteableBuffer<T>) -> usize
    {
        let mut cursor_data = self.cursor_data();

        let params = ParamsCasForPopBatch {
            cursor_data:           &mut cursor_data,
            pop_amount:            dst.max_write_count,
            success_order:         Release,
            fail_order:            Acquire,
            fetch_after_cas_order: Relaxed,
        };

        let pop_amount = match self.try_cas_for_pop_batch(params)
        {
            Some(r) => r,
            None => return 0,
        };
        let claim_start = cursor_data.blocked;
        for offset in 0..pop_amount
        {
            let cursor_idx = claim_start.wrapping_add(offset as u32);
            unsafe {
                let val = self.buffer.take_at(cursor_idx);
                dst.buffer.write(dst.write_start_cursor.wrapping_add(offset as u32), val);
            }
        }
        self.publish_stolen(claim_start, pop_amount as u32);
        pop_amount
    }
}
impl<T> SpmcRingBufferFifo<T>
{
    /// calculates the maximum number of elements that can be moved from the buffer and updates the head cursor
    /// Note: Since this is an SPMC implementation, we do not increment the stolen count during the CAS operation. The caller must handle this after successfully popping the value.
    #[cold]
    fn try_cas_for_pop_batch(&self, mut cas_data: ParamsCasForPopBatch) -> Option<usize>
    {
        debug_assert!(cas_data.pop_amount > 0, "pop amount must > 0");
        loop
        {
            let filled_slots = cas_data.cursor_data.filled_slots();
            if filled_slots < 1
            {
                return None;
            }
            cas_data.pop_amount = cas_data.pop_amount.min(filled_slots);

            let current_head = pack(cas_data.cursor_data.stolen, cas_data.cursor_data.blocked);

            let next_head = pack(
                cas_data.cursor_data.stolen,
                // reserve a slot for the pop operation, creating a barrier for other consumers
                cas_data.cursor_data.blocked.wrapping_add(cas_data.pop_amount as u32),
            );

            match self
                .head
                .compare_exchange_weak(current_head, next_head, cas_data.success_order, cas_data.fail_order)
            {
                Ok(_) => return Some(cas_data.pop_amount),
                Err(c) =>
                {
                    let tail = self.tail.load(cas_data.fetch_after_cas_order);
                    let (stolen, in_progress) = unpack(c);
                    cas_data.cursor_data.stolen = stolen;
                    cas_data.cursor_data.blocked = in_progress;
                    cas_data.cursor_data.tail = tail;
                }
            }
        }
    }

    #[cold]
    fn publish_stolen(&self, start: u32, amount: u32)
    {
        let mut backoff = Backoff::new();
        loop
        {
            let current = self.head.load(Acquire);
            let (stolen, in_stealing) = unpack(current);
            if stolen != start
            {
                backoff.snooze();
                continue;
            }
            let next = pack(stolen.wrapping_add(amount), in_stealing);
            match self.head.compare_exchange_weak(current, next, Release, Relaxed)
            {
                Ok(_) => return,
                Err(_) => backoff.snooze(),
            }
        }
    }

    #[inline]
    fn cursor_data(&self) -> CursorData
    {
        let (stolen, in_progress) = self.head.load_unpack(Acquire);
        let tail = self.tail.load(Relaxed);

        CursorData {
            stolen:   stolen,
            blocked:  in_progress,
            tail:     tail,
            capacity: self.buffer.capacity() as u32,
            mask:     self.buffer.mask(),
        }
    }
}
