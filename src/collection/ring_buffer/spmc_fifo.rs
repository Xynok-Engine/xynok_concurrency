use crate::collection::ring_buffer::consts::MAX_CAPACITY;
use crate::collection::ring_buffer::cursors::CursorData;
use crate::collection::ring_buffer::fixed_buffer::FixedBuffer;
use crate::collection::ring_buffer::head::Head;
use crate::collection::ring_buffer::params::{ParamsCasForPopBatch, ParamsCasTailForPushBatch};
use crate::sync::AtomicU32;
use crate::sync::Ordering::{Acquire, Relaxed, Release};
use crate::utils::cache_padded::CachePadded;
use crate::utils::{pack, unpack};

/// Single Producer - Multiple Consumers, Ring Buffer, Fist In - Firt Out
pub struct SpmcRingBufferFifo<T>
{
    head:   Head,
    /// Note: the tail points to the next available slot where the next push occurs
    tail:   CachePadded<AtomicU32>,
    buffer: FixedBuffer<T>,
}

unsafe impl<T: Send> Send for SpmcRingBufferFifo<T> {}
unsafe impl<T: Send> Sync for SpmcRingBufferFifo<T> {}

impl<T> SpmcRingBufferFifo<T>
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
        let mut cursors = self.cursor_data();

        let cas_params = ParamsCasTailForPushBatch {
            cursor_data:      &mut cursors,
            src_amount:       1,
            push_amount:      1,
            success_order:    Release,
            fail_order:       Acquire,
            fetch_head_order: Relaxed,
        };
        if self.try_cas_tail_for_push(cas_params).is_none()
        {
            return Err(val);
        }

        unsafe {
            self.buffer.write(cursors.tail, val);
        }

        Ok(())
    }

    /// Takes up to `max` elements from `vals`. Elements are taken in order from left to right, starting from index `0` to `N`.
    /// - return: the number of elements successfully taken.
    #[inline]
    #[track_caller]
    pub fn push_batch(&mut self, max: usize, vals: &mut Vec<T>) -> usize
    {
        debug_assert!(max > 0, "The push batch size must be greater than zero.");
        let mut cursors = self.cursor_data();

        let cas_params = ParamsCasTailForPushBatch {
            cursor_data:      &mut cursors,
            src_amount:       vals.len(),
            push_amount:      max,
            success_order:    Release,
            fail_order:       Acquire,
            fetch_head_order: Relaxed,
        };
        let take_amount = match self.try_cas_tail_for_push(cas_params)
        {
            Some(r) => r,
            None => return 0,
        };
        for (offset, e) in vals.drain(..take_amount).enumerate()
        {
            let write_idx = cursors.tail.wrapping_add(offset as u32);
            unsafe {
                self.buffer.write(write_idx, e);
            }
        }

        take_amount
    }
    pub fn pop(&mut self) -> Option<T>
    {
        let mut cursor_data = self.cursor_data();

        let params = ParamsCasForPopBatch {
            cursor_data:      &mut cursor_data,
            pop_amount:       1,
            success_order:    Release,
            fail_order:       Acquire,
            fetch_tail_order: Relaxed,
        };

        self.try_cas_for_pop_batch(params)?;

        let result = unsafe { Some(self.buffer.take_at(cursor_data.in_progress)) };

        // notify consumers that I've finished the task
        self.head.store_pack((cursor_data.stolen.wrapping_add(1), cursor_data.in_progress), Release);

        result
    }
    pub fn pop_batch(&mut self, max: usize, dst: &mut Vec<T>) -> usize
    {
        let mut cursor_data = self.cursor_data();

        let params = ParamsCasForPopBatch {
            cursor_data:      &mut cursor_data,
            pop_amount:       max,
            success_order:    Release,
            fail_order:       Acquire,
            fetch_tail_order: Relaxed,
        };

        let pop_amount = match self.try_cas_for_pop_batch(params)
        {
            Some(r) => r,
            None => return 0,
        };
        for offset in 0..pop_amount
        {
            let cursor_idx = cursor_data.stolen.wrapping_add(offset as u32);
            let val = unsafe { self.buffer.take_at(cursor_idx) };
            dst.push(val);
        }
        // notify consumers that I've finished the task
        self.head
            .store_pack((cursor_data.stolen.wrapping_add(pop_amount as u32), cursor_data.in_progress), Release);
        pop_amount
    }
}

impl<T> SpmcRingBufferFifo<T>
{
    /// calculates the maximum number of elements that can be moved from the source into the available empty slots,
    /// and stores the new tail cursor
    #[cold]
    fn try_cas_tail_for_push(&self, mut cas_data: ParamsCasTailForPushBatch) -> Option<usize>
    {
        debug_assert!(cas_data.src_amount > 0, "cannot take data from an empty source");
        loop
        {
            let available_slots = cas_data.cursor_data.available_slots();
            if available_slots < 1
            {
                return None;
            }
            cas_data.push_amount = cas_data.push_amount.min(available_slots).min(cas_data.src_amount);

            let current_tail = cas_data.cursor_data.tail;
            let next_tail = current_tail.wrapping_add(cas_data.push_amount as u32);

            match self
                .tail
                .compare_exchange_weak(current_tail, next_tail, cas_data.success_order, cas_data.fail_order)
            {
                Ok(_) => return Some(cas_data.push_amount),
                Err(c) =>
                {
                    let (stolen, in_progress) = self.head.load_unpack(cas_data.fetch_head_order);
                    cas_data.cursor_data.tail = c;
                    cas_data.cursor_data.stolen = stolen;
                    cas_data.cursor_data.in_progress = in_progress;
                }
            }
        }
    }

    /// Calculates the maximum number of elements that can be moved from the buffer and updates the head cursor
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

            let current_head = pack(cas_data.cursor_data.stolen, cas_data.cursor_data.in_progress);

            let next_head = pack(
                cas_data.cursor_data.stolen,
                cas_data.cursor_data.in_progress.wrapping_add(cas_data.pop_amount as u32),
            );

            match self
                .head
                .compare_exchange_weak(current_head, next_head, cas_data.success_order, cas_data.fail_order)
            {
                Ok(_) => return Some(cas_data.pop_amount),
                Err(c) =>
                {
                    let tail = self.tail.load(cas_data.fetch_tail_order);
                    let (stolen, in_progress) = unpack(c);
                    cas_data.cursor_data.stolen = stolen;
                    cas_data.cursor_data.in_progress = in_progress;
                    cas_data.cursor_data.tail = tail;
                }
            }
        }
    }

    #[inline]
    fn cursor_data(&self) -> CursorData
    {
        let (stolen, in_progress) = self.head.load_unpack(Acquire);
        let tail = self.tail.load(Relaxed);

        CursorData {
            stolen:      stolen,
            in_progress: in_progress,
            tail:        tail,
            capacity:    self.buffer.capacity() as u32,
            mask:        self.buffer.mask(),
        }
    }
}
#[cfg(test)]
mod test
{
    use super::*;

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

    // --- new() must reject invalid capacity ---

    #[test]
    #[should_panic(expected = "power of 2")]
    fn capacity_khong_phai_luy_thua_hai_thi_panic()
    {
        let _ = SpmcRingBufferFifo::<u8>::new(3);
    }

    #[test]
    #[should_panic(expected = "power of 2")]
    fn capacity_bang_khong_thi_panic()
    {
        let _ = SpmcRingBufferFifo::<u8>::new(0);
    }

    #[test]
    #[should_panic(expected = "exceeds")]
    fn capacity_vuot_gioi_han_thi_panic()
    {
        let _ = SpmcRingBufferFifo::<u8>::new(MAX_CAPACITY);
    }

    // --- push_batch / pop_batch reject nonsensical arguments ---

    #[test]
    #[should_panic(expected = "greater than zero")]
    fn push_batch_voi_max_bang_khong_thi_panic()
    {
        let mut ring = SpmcRingBufferFifo::new(4);
        let mut vals = vec![1u32];
        let _ = ring.push_batch(0, &mut vals);
    }

    #[test]
    #[should_panic(expected = "empty source")]
    fn push_batch_voi_vals_rong_thi_panic()
    {
        let mut ring = SpmcRingBufferFifo::new(4);
        let mut vals: Vec<u32> = Vec::new();
        let _ = ring.push_batch(4, &mut vals);
    }

    #[test]
    #[should_panic(expected = "pop amount must > 0")]
    fn pop_batch_voi_max_bang_khong_thi_panic()
    {
        let mut ring = SpmcRingBufferFifo::<u32>::new(4);
        let mut out = Vec::new();
        let _ = ring.pop_batch(0, &mut out);
    }

    // --- basic push / pop ---

    #[test]
    fn ring_moi_khoi_tao_thi_pop_tra_ve_none()
    {
        let mut ring = SpmcRingBufferFifo::<u32>::new(4);
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn push_va_pop_giu_dung_thu_tu_fifo()
    {
        let mut ring = SpmcRingBufferFifo::new(4);
        for i in 0..4u32
        {
            assert_eq!(ring.push(i), Ok(()));
        }
        for i in 0..4u32
        {
            assert_eq!(ring.pop(), Some(i));
        }
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn day_khi_da_day_thi_tra_lai_gia_tri_chu_khong_nuot()
    {
        let mut ring = SpmcRingBufferFifo::new(2);
        assert_eq!(ring.push(1), Ok(()));
        assert_eq!(ring.push(2), Ok(()));
        assert_eq!(ring.push(3), Err(3));

        assert_eq!(ring.pop(), Some(1));
        assert_eq!(ring.push(3), Ok(()));
    }

    #[test]
    fn chi_so_quan_qua_cuoi_mang_van_giu_dung_thu_tu()
    {
        let mut ring = SpmcRingBufferFifo::new(4);
        for round in 0..10u32
        {
            for i in 0..4u32
            {
                assert_eq!(ring.push(round * 4 + i), Ok(()));
            }
            for i in 0..4u32
            {
                assert_eq!(ring.pop(), Some(round * 4 + i));
            }
        }
    }

    // --- push_batch ---

    #[test]
    fn push_batch_bi_gioi_han_boi_cho_trong_va_giu_lai_phan_thua()
    {
        let mut ring = SpmcRingBufferFifo::new(4);
        let mut vals: Vec<u32> = (0..10).collect();

        assert_eq!(ring.push_batch(10, &mut vals), 4);
        assert_eq!(vals, (4..10).collect::<Vec<_>>());

        assert_eq!(ring.push_batch(10, &mut vals), 0);
        assert_eq!(vals.len(), 6);
    }

    #[test]
    fn push_batch_bi_gioi_han_boi_tham_so_max()
    {
        let mut ring = SpmcRingBufferFifo::new(8);
        let mut vals: Vec<u32> = (0..8).collect();

        assert_eq!(ring.push_batch(3, &mut vals), 3);
        assert_eq!(vals, (3..8).collect::<Vec<_>>());
    }

    #[test]
    fn push_batch_bi_gioi_han_boi_do_dai_vals()
    {
        let mut ring = SpmcRingBufferFifo::new(8);
        let mut vals: Vec<u32> = (0..3).collect();

        assert_eq!(ring.push_batch(8, &mut vals), 3);
        assert!(vals.is_empty());
    }

    // --- pop_batch ---

    #[test]
    fn pop_batch_tra_dung_thu_tu_va_bi_chan_boi_so_phan_tu_dang_co()
    {
        let mut ring = SpmcRingBufferFifo::new(8);
        let mut vals: Vec<u32> = (0..5).collect();
        assert_eq!(ring.push_batch(5, &mut vals), 5);

        let mut out = Vec::new();
        assert_eq!(ring.pop_batch(100, &mut out), 5);
        assert_eq!(out, vec![0, 1, 2, 3, 4]);
        assert_eq!(ring.pop_batch(100, &mut out), 0);
    }

    #[test]
    fn pop_batch_gioi_han_boi_tham_so_max()
    {
        let mut ring = SpmcRingBufferFifo::new(8);
        let mut vals: Vec<u32> = (0..8).collect();
        assert_eq!(ring.push_batch(8, &mut vals), 8);

        let mut out = Vec::new();
        assert_eq!(ring.pop_batch(3, &mut out), 3);
        assert_eq!(out, vec![0, 1, 2]);
        assert_eq!(ring.pop_batch(3, &mut out), 3);
        assert_eq!(out, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn push_batch_va_pop_batch_van_dung_khi_quan_qua_cuoi_mang()
    {
        let mut ring = SpmcRingBufferFifo::new(4);
        for round in 0..20u32
        {
            let mut vals: Vec<u32> = (0..3).map(|i| round * 3 + i).collect();
            assert_eq!(ring.push_batch(3, &mut vals), 3);

            let mut out = Vec::new();
            assert_eq!(ring.pop_batch(3, &mut out), 3);
            assert_eq!(out, (0..3).map(|i| round * 3 + i).collect::<Vec<_>>());
        }
    }

    // --- one producer, many consumers pushing/popping concurrently (the intended SPMC model) ---
    //
    // Only one thread ever calls `push`, but multiple threads call `pop`/`pop_batch` at the same
    // time, matching how `worker_pool` is meant to access the ring through `HeapPtr::as_ref_mut`
    // from several threads (bypassing the borrow checker with a raw pointer). This test recreates
    // that same access pattern to check that the CAS on `head` never loses or duplicates elements
    // when consumers race each other.
    //
    // NOTE: currently FAILS with the current `try_cas_for_pop_batch` implementation, because the
    // CAS advances `head` (publishing "already taken") before the data is actually read out of the
    // buffer, so two racing consumers can step on the same in-flight range. Keeping this test as
    // the regression target for fixing `pop`'s claim/publish logic.
    #[cfg(not(loom))]
    #[test]
    fn mot_producer_nhieu_consumer_khong_mat_khong_nhan_doi()
    {
        use crate::sync::AtomicBool;
        use crate::sync::Ordering::{Acquire as SyncAcquire, Release as SyncRelease};
        use crate::utils::backoff::Backoff;

        const CAP: usize = 64;
        const CONSUMERS: u32 = 4;
        const TOTAL: u32 = 20_000;
        const CHUNK: usize = 16;

        #[derive(Clone, Copy)]
        struct RawPtr(*mut SpmcRingBufferFifo<u32>);
        unsafe impl Send for RawPtr {}
        unsafe impl Sync for RawPtr {}

        let mut boxed = Box::new(SpmcRingBufferFifo::<u32>::new(CAP));
        let raw = RawPtr(boxed.as_mut() as *mut _);
        let done = AtomicBool::new(false);

        let collected = std::thread::scope(|scope| {
            let consumers: Vec<_> = (0..CONSUMERS)
                .map(|_| {
                    let done = &done;
                    scope.spawn(move || {
                        let raw = raw; // force the closure to capture the whole `RawPtr`, not just the `.0` field
                        let ring: &mut SpmcRingBufferFifo<u32> = unsafe { &mut *raw.0 };
                        let mut out = Vec::new();
                        let mut backoff = Backoff::new();
                        loop
                        {
                            let mut chunk = Vec::new();
                            if ring.pop_batch(CHUNK, &mut chunk) == 0
                            {
                                if done.load(SyncAcquire)
                                {
                                    break out;
                                }
                                backoff.snooze();
                            }
                            else
                            {
                                backoff.reset();
                                out.extend(chunk);
                            }
                        }
                    })
                })
                .collect();

            let producer = scope.spawn(move || {
                let raw = raw; // same as above: force capturing the whole `RawPtr`
                let ring: &mut SpmcRingBufferFifo<u32> = unsafe { &mut *raw.0 };
                let mut backoff = Backoff::new();
                for val in 0..TOTAL
                {
                    loop
                    {
                        match ring.push(val)
                        {
                            Ok(()) =>
                            {
                                backoff.reset();
                                break;
                            }
                            Err(_) => backoff.snooze(),
                        }
                    }
                }
            });
            producer.join().unwrap();
            // the producer has already pushed all TOTAL elements by this line, so it's safe to flag
            // done here: a consumer seeing done=true together with an empty pop means the ring is
            // truly drained, not just momentarily empty.
            done.store(true, SyncRelease);

            let mut all = Vec::new();
            for c in consumers
            {
                all.extend(c.join().unwrap());
            }
            all
        });

        let mut collected = collected;
        collected.sort_unstable();
        assert_eq!(collected.len(), TOTAL as usize, "tổng số phần tử phải khớp: mất hoặc nhân đôi là hỏng");
        assert!(collected.iter().copied().eq(0..TOTAL), "phải là đúng dãy 0..TOTAL, mỗi số một lần");
    }
}
