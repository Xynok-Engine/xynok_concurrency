use crate::collection::ring_buffer::params::ParamsCasForPopBatch;
use crate::sync::AtomicU32;
use crate::sync::Ordering::{Acquire, Relaxed, Release};
use crate::utils::backoff::Backoff;
use crate::utils::bits::{pack, unpack};
use crate::utils::cache_padded::CachePadded;
use crate::utils::cursors::CursorData;
use crate::utils::fixed_ring_buffer::FixedRingBuffer;
use crate::utils::packed::Packed;
use crate::utils::queue_batching::QueueBatching;
use crate::utils::steal::Steal;

/// Single producer, multiple consumers.
/// When the producer pops, it retrieves elements using LIFO ordering.
/// When consumers pop, they retrieve elements using FIFO ordering.
pub struct WorkerQueue<T>
{
    pub buffer: FixedRingBuffer<T>,
    anchor:     Packed,
    stolen:     CachePadded<AtomicU32>,
}

unsafe impl<T: Send> Send for WorkerQueue<T> {}
unsafe impl<T: Send> Sync for WorkerQueue<T> {}

impl<T> Drop for WorkerQueue<T>
{
    fn drop(&mut self)
    {
        // Only the range `[in_stealing, tail)` contains valid items. The `[stolen, in_stealing)`
        // range has already been stolen, so dropping these would result in a double free.
        let (tail, in_stealing) = self.anchor.load_unpack(Relaxed);
        for offset in 0..tail.wrapping_sub(in_stealing)
        {
            unsafe { self.buffer.drop_at(in_stealing.wrapping_add(offset)) };
        }
    }
}

impl<T> WorkerQueue<T>
{
    pub fn new(capacity: usize) -> Self
    {
        Self {
            buffer: FixedRingBuffer::new(capacity),
            anchor: Packed::new(0, 0),
            stolen: CachePadded::new(AtomicU32::new(0)),
        }
    }
    #[inline]
    pub fn buffer(&self) -> &FixedRingBuffer<T>
    {
        &self.buffer
    }
    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.buffer.capacity()
    }
    #[inline]
    pub fn consumer<'a>(&'a self, dst: &'a WorkerQueue<T>) -> Consumer<'a, T>
    {
        Consumer::new(self, dst)
    }
}
impl<T> WorkerQueue<T>
{
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T>
    {
        let cursors = self.cursor_data();
        if cursors.empty_slots() < 1
        {
            return Err(val);
        }

        unsafe {
            self.buffer.write(cursors.tail, val);
        }

        self.anchor.fetch_add(1 << 32, Release);
        Ok(())
    }

    #[inline]
    pub fn push_batch_by_taking_from_queue(&self, max: usize, src: &QueueBatching<T>) -> usize
    {
        debug_assert!(max > 0, "The push batch size must be greater than zero.");
        let cursor_data = self.cursor_data();
        let take_amount = cursor_data.empty_slots().min(max).min(src.len());
        if take_amount < 1
        {
            return 0;
        }

        let moved = src.drain_into_buffer(take_amount, &self.buffer, cursor_data.tail);
        if moved < 1
        {
            return 0;
        }

        self.anchor.fetch_add((moved as u64) << 32, Release);
        moved
    }

    #[inline]
    pub fn pop(&self) -> Option<T>
    {
        let mut cursor_data = self.cursor_data();
        loop
        {
            if cursor_data.filled_slots() < 1
            {
                return None;
            }

            let take_cursor = cursor_data.tail.wrapping_sub(1);
            let current = pack(cursor_data.tail, cursor_data.blocked);
            let next = pack(take_cursor, cursor_data.blocked);

            match self.anchor.compare_exchange_weak(current, next, Release, Acquire)
            {
                Ok(_) => return unsafe { Some(self.buffer.take_at(take_cursor)) },
                Err(c) =>
                {
                    let (tail, in_stealing) = unpack(c);
                    cursor_data.tail = tail;
                    cursor_data.blocked = in_stealing;
                }
            }
        }
    }
    /// Drains a batch of tasks from this ring into `dst`, while preserving the original FIFO order.
    ///
    /// Returns a [`Steal`] instead of a simple count, because "stealing 0" could mean two very different things:
    /// - [`Steal::Empty`]: The source is genuinely empty, so the thief should probably look for another victim.
    /// - [`Steal::Busy`]: The source still has items, so coming back later should work.
    /// - [`Steal::Success`]: This includes the number of elements moved, which is always greater than 0.
    ///
    /// The CAS attempt to grab a batch happens only once. If it fails, we return [`Steal::Busy`] instead of spinning: it is faster to let the caller decide whether to retry here or move on elsewhere rather than forcing them to wait.
    #[inline]
    pub fn try_steal_batch_to(&self, max: usize, dst: &WorkerQueue<T>) -> Steal<usize>
    {
        debug_assert!(max > 0, "The steal batch size must be greater than zero.");
        let dst_cursor_data = dst.cursor_data();
        let mut my_cursor_data = self.cursor_data();

        let free_slots = dst_cursor_data.empty_slots();
        debug_assert!(free_slots > 0, "a stealer shouldn't have any tasks while trying to steal");

        let take_amount = free_slots.min(max).min(my_cursor_data.filled_slots());
        if take_amount < 1
        {
            return Steal::Empty;
        }

        let params = ParamsCasForPopBatch {
            cursor_data:           &mut my_cursor_data,
            pop_amount:            take_amount,
            success_order:         Release,
            fail_order:            Acquire,
            fetch_after_cas_order: Relaxed,
        };

        let pop_amount = match self.fifo_try_cas_for_pop_batch(params)
        {
            Steal::Success(amount) => amount,
            Steal::Empty => return Steal::Empty,
            Steal::Busy => return Steal::Busy,
        };
        let claim_start = my_cursor_data.blocked;
        for offset in 0..pop_amount
        {
            let cursor_idx = claim_start.wrapping_add(offset as u32);
            unsafe {
                let val = self.buffer.take_at(cursor_idx);
                dst.buffer.write(dst_cursor_data.tail.wrapping_add(offset as u32), val);
            }
        }
        self.consumer_publish_stolen(claim_start, pop_amount as u32);

        // `dst` is the deque belonging to the current thread, so no one else can push to it concurrently. Even so, I still use `fetch_add` to avoid interfering with `in_stealing` if another thief is currently incrementing it.
        dst.anchor.fetch_add((pop_amount as u64) << 32, Release);
        Steal::Success(pop_amount)
    }
}

impl<T> WorkerQueue<T>
{
    /// Note: Since this is an SPMC implementation, we do not increment the stolen count during the
    /// CAS operation. The caller must handle this after successfully popping the value.
    #[cold]
    fn fifo_try_cas_for_pop_batch(&self, mut cas_data: ParamsCasForPopBatch) -> Steal<usize>
    {
        debug_assert!(cas_data.pop_amount > 0, "pop amount must > 0");
        let filled_slots = cas_data.cursor_data.filled_slots();
        if filled_slots < 1
        {
            return Steal::Empty;
        }
        cas_data.pop_amount = cas_data.pop_amount.min(filled_slots);

        let current = pack(cas_data.cursor_data.tail, cas_data.cursor_data.blocked);
        let next = pack(cas_data.cursor_data.tail, cas_data.cursor_data.blocked.wrapping_add(cas_data.pop_amount as u32));

        // I'm using the `strong` version here. Since it only attempts the operation once, a stray failure from `weak` might be mistakenly interpreted as contention.
        match self.anchor.compare_exchange(current, next, cas_data.success_order, cas_data.fail_order)
        {
            Ok(_) => Steal::Success(cas_data.pop_amount),
            Err(_) => Steal::Busy,
        }
    }

    #[cold]
    fn consumer_publish_stolen(&self, start: u32, amount: u32)
    {
        let mut backoff = Backoff::new();
        loop
        {
            let current = self.stolen.load(Acquire);
            if current != start
            {
                backoff.snooze();
                continue;
            }
            match self.stolen.compare_exchange_weak(current, current.wrapping_add(amount), Release, Relaxed)
            {
                Ok(_) => return,
                Err(_) => backoff.snooze(),
            }
        }
    }

    #[inline]
    fn cursor_data(&self) -> CursorData
    {
        let (tail, in_stealing) = self.anchor.load_unpack(Acquire);
        let stolen = self.stolen.load(Acquire);

        CursorData {
            stolen:   stolen,
            blocked:  in_stealing,
            tail:     tail,
            capacity: self.buffer.capacity() as u32,
            mask:     self.buffer.mask(),
        }
    }
}

pub struct Consumer<'a, T>
{
    src: &'a WorkerQueue<T>,
    dst: &'a WorkerQueue<T>,
}
impl<'a, T> Consumer<'a, T>
{
    pub fn new(src: &'a WorkerQueue<T>, dst: &'a WorkerQueue<T>) -> Self
    {
        Self { src, dst }
    }

    /// Vét một lô từ nạn nhân sang deque của mình, kèm lý do khi lấy hụt.
    ///
    /// [`Steal::Empty`] là nạn nhân cạn thật, nên đi tìm chỗ khác. [`Steal::Busy`] là bị chen ngang
    /// hoặc deque của mình hết chỗ, quay lại sau vẫn còn hàng.
    #[inline]
    pub fn try_steal_half(&self) -> Steal<usize>
    {
        let amount = self.src.buffer.capacity() / 2;
        self.try_steal_batch(amount)
    }

    /// Như [`Self::try_steal`] nhưng tự chọn trần cho lô.
    #[inline]
    pub fn try_steal_batch(&self, max: usize) -> Steal<usize>
    {
        self.src.try_steal_batch_to(max, self.dst)
    }
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
