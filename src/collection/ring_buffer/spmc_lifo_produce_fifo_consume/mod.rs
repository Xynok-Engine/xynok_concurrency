use crate::collection::ring_buffer::params::{ParamsCasForPopBatch, WriteableBuffer};
use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;
use crate::sync::AtomicU32;
use crate::sync::Ordering::{Acquire, Relaxed, Release};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::cursors::CursorData;
use crate::utils::fixed_buffer::FixedRingBuffer;
use crate::utils::packed::Packed;
use crate::utils::{pack, unpack};

use consumer::Consumer;
pub mod consumer;

/// Single producer, multiple consumers.
/// When the producer pops, it retrieves elements using LIFO ordering.
/// When consumers pop, they retrieve elements using FIFO ordering.
pub struct SpmcRingBufferLifoProduceFifoConsume<T>
{
    buffer: FixedRingBuffer<T>,
    head:   Packed,
    tail:   CachePadded<AtomicU32>,

    #[cfg(debug_assertions)]
    owner_thread: crate::sync::thread::Thread,
}

unsafe impl<T: Send> Send for SpmcRingBufferLifoProduceFifoConsume<T> {}
unsafe impl<T: Send> Sync for SpmcRingBufferLifoProduceFifoConsume<T> {}
impl<T> Drop for SpmcRingBufferLifoProduceFifoConsume<T>
{
    fn drop(&mut self)
    {
        let (_, in_stealing) = unpack(self.head.load(Relaxed));
        let tail = self.tail.load(Relaxed);
        for offset in 0..tail.wrapping_sub(in_stealing)
        {
            unsafe { self.buffer.drop_at(in_stealing.wrapping_add(offset)) };
        }
    }
}
impl<T> SpmcRingBufferLifoProduceFifoConsume<T>
{
    pub fn new(capacity: usize) -> Self
    {
        Self {
            buffer: FixedRingBuffer::new(capacity),
            head: Packed::new(0, 0),
            tail: CachePadded::new(AtomicU32::new(0)),
            #[cfg(debug_assertions)]
            owner_thread: crate::sync::thread::current(),
        }
    }

    pub fn consumer<'a>(&'a self, dst: &'a SpmcRingBufferLifoProduceFifoConsume<T>) -> Consumer<'a, T>
    {
        Consumer::new(self, dst)
    }
}
impl<T> SpmcRingBufferLifoProduceFifoConsume<T>
{
    /// only call by the owner thread
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T>
    {
        debug_assert!(
            crate::sync::thread::current().id() == self.owner_thread.id(),
            "push must be called from owner thread"
        );
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

    #[inline]
    pub(crate) fn push_batch_by_taking_from(&self, max: usize, src: &SpmcRingBufferFifo<T>) -> usize
    {
        debug_assert!(
            crate::sync::thread::current().id() == self.owner_thread.id(),
            "push must be called from owner thread"
        );
        let src_len = src.len();
        debug_assert!(max > 0, "The push batch size must be greater than zero.");
        let cursor_data = self.cursor_data();
        let take_amount = cursor_data.empty_slots().min(max).min(src_len);
        if take_amount < 1
        {
            return 0;
        }

        let param = WriteableBuffer {
            buffer:             &self.buffer,
            write_start_cursor: cursor_data.tail,
            max_write_count:    take_amount,
        };
        src.pop_batch_to(param);
        self.tail.store(cursor_data.tail.wrapping_add(take_amount as u32), Release);
        take_amount
    }

    #[inline]
    pub(crate) fn pop_lifo(&self) -> Option<T>
    {
        debug_assert!(
            crate::sync::thread::current().id() == self.owner_thread.id(),
            "pop lifo must be called from owner thread"
        );
        let mut cursor_data = self.cursor_data();

        if cursor_data.filled_slots() < 1
        {
            return None;
        }

        match cursor_data.filled_slots() == 1
        {
            true =>
            {
                let params = ParamsCasForPopBatch {
                    cursor_data:           &mut cursor_data,
                    pop_amount:            1,
                    success_order:         Release,
                    fail_order:            Acquire,
                    fetch_after_cas_order: Relaxed,
                };

                let pop_amount = self.fifo_try_cas_for_pop_batch(params)?;
                let claim_start = cursor_data.in_stealing;
                let result = unsafe { Some(self.buffer.take_at(cursor_data.in_stealing)) };

                self.consumer_publish_stolen(claim_start, pop_amount as u32);
                result
            }
            false =>
            {
                self.tail.fetch_sub(1, Release);
                let take_cursor = cursor_data.tail.wrapping_sub(1);
                unsafe { Some(self.buffer.take_at(take_cursor)) }
            }
        }
    }
    #[inline]
    pub(crate) fn consumer_pop_batch_to(&self, max: usize, other: &SpmcRingBufferLifoProduceFifoConsume<T>) -> usize
    {
        let other_cusror_data = other.cursor_data();
        let mut my_cursor_data = self.cursor_data();

        let take_amount = other_cusror_data.empty_slots().min(max).min(my_cursor_data.filled_slots());
        if take_amount < 1
        {
            return 0;
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
            Some(r) => r,
            None => return 0,
        };
        let claim_start = my_cursor_data.in_stealing;
        for offset in 0..pop_amount
        {
            let cursor_idx = claim_start.wrapping_add(offset as u32);
            unsafe {
                let val = self.buffer.take_at(cursor_idx);
                other.buffer.write(other_cusror_data.tail.wrapping_add(offset as u32), val);
            }
        }
        self.consumer_publish_stolen(claim_start, pop_amount as u32);
        other.tail.store(other_cusror_data.tail.wrapping_add(pop_amount as u32), Release);
        pop_amount
    }
}
impl<T> SpmcRingBufferLifoProduceFifoConsume<T>
{
    #[cold]
    fn lifo_try_cas_for_pop_batch(&self, mut cas_data: ParamsCasForPopBatch) -> Option<usize>
    {
        debug_assert!(cas_data.pop_amount == 1, "pop lifo amount must be 1");
        loop
        {
            let filled_slots = cas_data.cursor_data.filled_slots();
            if filled_slots < 1
            {
                return None;
            }
            cas_data.pop_amount = cas_data.pop_amount.min(filled_slots);

            match self.tail.compare_exchange_weak(
                cas_data.cursor_data.tail,
                cas_data.cursor_data.tail.wrapping_sub(1),
                cas_data.success_order,
                cas_data.fail_order,
            )
            {
                Ok(_) => return Some(cas_data.pop_amount),
                Err(c) =>
                {
                    let (stolen, in_progress) = self.head.load_unpack(cas_data.fetch_after_cas_order);
                    cas_data.cursor_data.stolen = stolen;
                    cas_data.cursor_data.in_stealing = in_progress;
                    cas_data.cursor_data.tail = c;
                }
            }
        }
    }
    #[cold]
    fn fifo_try_cas_for_pop_batch(&self, mut cas_data: ParamsCasForPopBatch) -> Option<usize>
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

            let current_head = pack(cas_data.cursor_data.stolen, cas_data.cursor_data.in_stealing);

            let next_head = pack(
                cas_data.cursor_data.stolen,
                // reserve a slot for the pop operation, creating a barrier for other consumers
                cas_data.cursor_data.in_stealing.wrapping_add(cas_data.pop_amount as u32),
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
                    cas_data.cursor_data.in_stealing = in_progress;
                    cas_data.cursor_data.tail = tail;
                }
            }
        }
    }
    #[cold]
    fn consumer_publish_stolen(&self, start: u32, amount: u32)
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
            stolen:      stolen,
            in_stealing: in_progress,
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
    use crate::collection::ring_buffer::consts::MAX_CAPACITY;
    use std::cell::Cell;
    use std::rc::Rc;

    type Ring<T> = SpmcRingBufferLifoProduceFifoConsume<T>;

    /// Đọc trộm một ô trong buffer để kiểm tra kết quả.
    ///
    /// Ring này chưa có đường đọc nào an toàn ngoài `pop_lifo` và `consumer_pop_batch_to`, nên vài
    /// test cần nhìn thẳng vào buffer. `take_at` chỉ là một phép copy bit, với kiểu `Copy` thì ô
    /// vẫn còn nguyên sau khi đọc, không có gì bị hỏng.
    fn peek<T: Copy>(ring: &Ring<T>, cursor: u32) -> T
    {
        unsafe { ring.buffer.take_at(cursor) }
    }

    /// Gom các con trỏ của ring thành bộ ba dễ so sánh: (stolen, in_stealing, tail).
    fn cursors<T>(ring: &Ring<T>) -> (u32, u32, u32)
    {
        let c = ring.cursor_data();
        (c.stolen, c.in_stealing, c.tail)
    }

    /// Chạy `f` trên một thread khác rồi trả về thông điệp panic, hoặc `None` nếu chạy trót lọt.
    ///
    /// Phải `join` thủ công thay vì để `thread::scope` tự dọn, nếu không scope sẽ ném lại panic của
    /// mình ("a scoped thread panicked") và nuốt mất thông điệp gốc mà test cần soi.
    fn panic_tu_thread_khac<F: FnOnce() + Send>(f: F) -> Option<String>
    {
        let ket_qua = std::thread::scope(|scope| scope.spawn(f).join());
        ket_qua.err().map(|e| match e.downcast_ref::<&str>()
        {
            Some(s) => (*s).to_string(),
            None => e.downcast_ref::<String>().cloned().unwrap_or_default(),
        })
    }

    /// Dựng sẵn một `SpmcRingBufferFifo` đã nạp `0..n` để làm nguồn cho `push_batch_by_taking_from`.
    fn fifo_source(capacity: usize, n: u32) -> SpmcRingBufferFifo<u32>
    {
        let src = SpmcRingBufferFifo::new(capacity);
        for i in 0..n
        {
            assert_eq!(src.push(i), Ok(()), "nguồn phải đủ chỗ cho {} phần tử", n);
        }
        src
    }

    // --- new() phải chặn sức chứa không hợp lệ ---

    #[test]
    #[should_panic(expected = "power of 2")]
    fn suc_chua_khong_phai_luy_thua_hai_thi_panic()
    {
        let _ = Ring::<u8>::new(3);
    }

    #[test]
    #[should_panic]
    fn suc_chua_bang_khong_thi_panic()
    {
        let _ = Ring::<u8>::new(0);
    }

    #[test]
    #[should_panic(expected = "exceeds")]
    fn suc_chua_vuot_gioi_han_thi_panic()
    {
        let _ = Ring::<u8>::new(MAX_CAPACITY);
    }

    #[test]
    fn ring_moi_tao_thi_moi_con_tro_deu_bang_khong()
    {
        let ring = Ring::<u32>::new(8);
        assert_eq!(cursors(&ring), (0, 0, 0));
        assert_eq!(ring.cursor_data().filled_slots(), 0);
        assert_eq!(ring.cursor_data().empty_slots(), 8);
    }

    #[test]
    fn hai_quyen_co_dung_bo_trait_can_thiet()
    {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<Ring<u32>>();
        assert_sync::<Ring<u32>>();
        assert_send::<Consumer<'_, u32>>();
        assert_sync::<Consumer<'_, u32>>();
    }

    // --- push: chỉ chủ sở hữu gọi, ghi theo thứ tự tăng dần của tail ---

    #[test]
    fn push_ghi_lan_luot_va_day_tail_len()
    {
        let ring = Ring::new(4);
        for i in 0..4u32
        {
            assert_eq!(ring.push(i * 10), Ok(()));
            assert_eq!(cursors(&ring), (0, 0, i + 1));
        }
        for i in 0..4u32
        {
            assert_eq!(peek(&ring, i), i * 10, "ô {} phải giữ đúng giá trị đã push", i);
        }
    }

    #[test]
    fn push_khi_day_thi_tra_lai_gia_tri_chu_khong_nuot()
    {
        let ring = Ring::new(2);
        assert_eq!(ring.push(1), Ok(()));
        assert_eq!(ring.push(2), Ok(()));
        assert_eq!(ring.push(3), Err(3), "ring đầy thì phải trả lại giá trị cho người gọi");
        assert_eq!(cursors(&ring), (0, 0, 2), "lần push hỏng không được đụng vào tail");
    }

    #[test]
    fn push_tu_thread_khac_thi_panic()
    {
        let ring = Ring::<u32>::new(4);
        let thong_diep = panic_tu_thread_khac(|| {
            let _ = ring.push(1);
        });
        assert!(
            thong_diep.as_deref().is_some_and(|m| m.contains("owner thread")),
            "push từ thread lạ phải panic vì sai chủ sở hữu, nhận được: {:?}",
            thong_diep
        );
    }

    // --- pop_lifo: chủ sở hữu lấy phần tử mới nhất trước ---

    #[test]
    fn ring_rong_thi_pop_lifo_tra_ve_none()
    {
        let ring = Ring::<u32>::new(4);
        assert!(ring.pop_lifo().is_none());
        assert_eq!(cursors(&ring), (0, 0, 0), "pop hụt không được đụng vào con trỏ");
    }

    #[test]
    fn pop_lifo_tu_thread_khac_thi_panic()
    {
        let ring = Ring::<u32>::new(4);
        assert_eq!(ring.push(1), Ok(()));
        let thong_diep = panic_tu_thread_khac(|| {
            let _ = ring.pop_lifo();
        });
        assert!(
            thong_diep.as_deref().is_some_and(|m| m.contains("owner thread")),
            "pop_lifo từ thread lạ phải panic vì sai chủ sở hữu, nhận được: {:?}",
            thong_diep
        );
    }

    #[test]
    fn pop_lifo_lay_phan_tu_moi_nhat_truoc()
    {
        // `tail` luôn trỏ vào ô trống kế tiếp, nên phần tử vừa push nằm ở `tail - 1`.
        let ring = Ring::new(4);
        for i in 0..4u32
        {
            assert_eq!(ring.push(i), Ok(()));
        }
        assert_eq!(ring.pop_lifo(), Some(3));
        assert_eq!(ring.pop_lifo(), Some(2));
        assert_eq!(ring.pop_lifo(), Some(1));
        assert_eq!(ring.pop_lifo(), Some(0));
        assert_eq!(ring.pop_lifo(), None);
    }

    #[test]
    fn pop_lifo_xen_ke_push_van_dung_thu_tu()
    {
        let ring = Ring::new(4);
        assert_eq!(ring.push(1), Ok(()));
        assert_eq!(ring.push(2), Ok(()));
        assert_eq!(ring.pop_lifo(), Some(2));
        assert_eq!(ring.push(3), Ok(()));
        assert_eq!(ring.pop_lifo(), Some(3));
        assert_eq!(ring.pop_lifo(), Some(1));
        assert_eq!(ring.pop_lifo(), None);
    }

    #[test]
    fn pop_lifo_chay_qua_diem_vong_lai_van_dung()
    {
        let ring = Ring::new(4);
        for round in 0..20u32
        {
            for i in 0..4u32
            {
                assert_eq!(ring.push(round * 4 + i), Ok(()));
            }
            for i in (0..4u32).rev()
            {
                assert_eq!(ring.pop_lifo(), Some(round * 4 + i), "vòng {}", round);
            }
        }
    }

    // --- push_batch_by_taking_from: hút việc từ một ring FIFO ---

    #[test]
    #[should_panic(expected = "greater than zero")]
    fn push_batch_voi_max_bang_khong_thi_panic()
    {
        let ring = Ring::<u32>::new(4);
        let src = fifo_source(4, 2);
        let _ = ring.push_batch_by_taking_from(0, &src);
    }

    #[test]
    fn push_batch_tu_thread_khac_thi_panic()
    {
        let ring = Ring::<u32>::new(4);
        let src = fifo_source(4, 2);
        let thong_diep = panic_tu_thread_khac(|| {
            let _ = ring.push_batch_by_taking_from(2, &src);
        });
        assert!(
            thong_diep.as_deref().is_some_and(|m| m.contains("owner thread")),
            "push_batch từ thread lạ phải panic vì sai chủ sở hữu, nhận được: {:?}",
            thong_diep
        );
    }

    #[test]
    fn push_batch_tu_nguon_rong_thi_khong_lay_gi()
    {
        let ring = Ring::<u32>::new(4);
        let src = SpmcRingBufferFifo::<u32>::new(4);
        assert_eq!(ring.push_batch_by_taking_from(4, &src), 0);
        assert_eq!(cursors(&ring), (0, 0, 0));
    }

    #[test]
    fn push_batch_giu_nguyen_thu_tu_fifo_cua_nguon()
    {
        let ring = Ring::new(8);
        let src = fifo_source(8, 5);

        assert_eq!(ring.push_batch_by_taking_from(5, &src), 5);
        assert_eq!(cursors(&ring), (0, 0, 5));
        assert!(src.is_empty(), "nguồn phải bị hút cạn");

        for i in 0..5u32
        {
            assert_eq!(peek(&ring, i), i, "phần tử phải nằm đúng thứ tự FIFO của nguồn");
        }
    }

    #[test]
    fn push_batch_bi_gioi_han_boi_so_o_trong()
    {
        let ring = Ring::new(4);
        let src = fifo_source(16, 10);

        assert_eq!(ring.push_batch_by_taking_from(10, &src), 4, "chỉ còn 4 ô nên chỉ lấy được 4");
        assert_eq!(cursors(&ring), (0, 0, 4));
        assert_eq!(src.len(), 6, "phần còn lại phải nằm nguyên bên nguồn");

        assert_eq!(ring.push_batch_by_taking_from(10, &src), 0, "ring đã đầy thì không lấy thêm");
        assert_eq!(src.len(), 6);
    }

    #[test]
    fn push_batch_bi_gioi_han_boi_max()
    {
        let ring = Ring::new(8);
        let src = fifo_source(8, 8);

        assert_eq!(ring.push_batch_by_taking_from(3, &src), 3);
        assert_eq!(cursors(&ring), (0, 0, 3));
        assert_eq!(src.len(), 5);
    }

    #[test]
    fn push_batch_bi_gioi_han_boi_so_luong_ben_nguon()
    {
        let ring = Ring::new(8);
        let src = fifo_source(8, 3);

        assert_eq!(ring.push_batch_by_taking_from(8, &src), 3);
        assert_eq!(cursors(&ring), (0, 0, 3));
        assert!(src.is_empty());
    }

    // --- consumer_pop_batch_to: consumer khác lấy việc theo thứ tự FIFO ---

    #[test]
    fn steal_tu_ring_rong_thi_tra_ve_khong()
    {
        let src = Ring::<u32>::new(4);
        let dst = Ring::<u32>::new(4);
        assert_eq!(src.consumer_pop_batch_to(4, &dst), 0);
        assert_eq!(cursors(&src), (0, 0, 0));
        assert_eq!(cursors(&dst), (0, 0, 0));
    }

    #[test]
    fn steal_khi_dich_da_day_thi_tra_ve_khong()
    {
        let src = Ring::new(4);
        let dst = Ring::new(2);
        for i in 0..4u32
        {
            assert_eq!(src.push(i), Ok(()));
        }
        for i in 0..2u32
        {
            assert_eq!(dst.push(100 + i), Ok(()));
        }

        assert_eq!(src.consumer_pop_batch_to(4, &dst), 0, "đích không còn ô trống thì không lấy gì");
        assert_eq!(cursors(&src), (0, 0, 4), "lần steal hụt không được đụng vào con trỏ nguồn");
    }

    #[test]
    fn steal_cap_nhat_con_tro_cua_nguon()
    {
        let src = Ring::new(8);
        let dst = Ring::<u32>::new(8);
        for i in 0..5u32
        {
            assert_eq!(src.push(i), Ok(()));
        }

        assert_eq!(src.consumer_pop_batch_to(5, &dst), 5);
        assert_eq!(cursors(&src), (5, 5, 5), "steal xong thì stolen và in_stealing phải cùng đuổi kịp tail");
        assert_eq!(src.cursor_data().filled_slots(), 0);
        assert_eq!(src.cursor_data().empty_slots(), 8, "các ô đã bị lấy phải được trả lại cho producer");
    }

    #[test]
    fn steal_bi_gioi_han_boi_max()
    {
        let src = Ring::new(8);
        let dst = Ring::<u32>::new(16);
        for i in 0..8u32
        {
            assert_eq!(src.push(i), Ok(()));
        }

        assert_eq!(src.consumer_pop_batch_to(3, &dst), 3);
        assert_eq!(cursors(&src), (3, 3, 8));
        assert_eq!(src.cursor_data().filled_slots(), 5, "phần chưa lấy vẫn còn bên nguồn");
    }

    #[test]
    fn steal_bi_gioi_han_boi_so_o_trong_cua_dich()
    {
        let src = Ring::new(8);
        let dst = Ring::new(4);
        for i in 0..8u32
        {
            assert_eq!(src.push(i), Ok(()));
        }
        assert_eq!(dst.push(99), Ok(()));

        assert_eq!(src.consumer_pop_batch_to(8, &dst), 3, "đích chỉ còn 3 ô trống");
    }

    #[test]
    fn steal_chuyen_du_phan_tu_sang_dich()
    {
        // `consumer_pop_batch_to` ghi dữ liệu vào `other.buffer` nhưng không hề `store` lại
        // `other.tail`, nên bên đích vẫn tưởng mình rỗng và lần push kế tiếp sẽ đè lên việc vừa lấy.
        let src = Ring::new(8);
        let dst = Ring::new(8);
        for i in 0..5u32
        {
            assert_eq!(src.push(i), Ok(()));
        }

        assert_eq!(src.consumer_pop_batch_to(5, &dst), 5);
        assert_eq!(cursors(&dst), (0, 0, 5), "đích phải đẩy tail lên đúng số phần tử đã nhận");
        assert_eq!(dst.cursor_data().filled_slots(), 5);
    }

    #[test]
    fn steal_giu_thu_tu_fifo()
    {
        let src = Ring::new(8);
        let dst = Ring::new(8);
        for i in 0..4u32
        {
            assert_eq!(src.push(i), Ok(()));
        }

        assert_eq!(src.consumer_pop_batch_to(4, &dst), 4);
        // Bên đích, `pop_lifo` phải nhả ra theo thứ tự ngược lại vì phần tử được xếp FIFO khi chuyển sang.
        assert_eq!(dst.pop_lifo(), Some(3));
        assert_eq!(dst.pop_lifo(), Some(2));
        assert_eq!(dst.pop_lifo(), Some(1));
        assert_eq!(dst.pop_lifo(), Some(0));
    }

    #[test]
    fn steal_nhieu_lan_khong_ghi_de_len_nhau()
    {
        let src = Ring::new(8);
        let dst = Ring::new(8);
        for i in 0..4u32
        {
            assert_eq!(src.push(i), Ok(()));
        }

        assert_eq!(src.consumer_pop_batch_to(2, &dst), 2);
        assert_eq!(src.consumer_pop_batch_to(2, &dst), 2);
        assert_eq!(cursors(&dst), (0, 0, 4), "hai lần steal phải nối tiếp nhau, không đè lên nhau");
        for i in 0..4u32
        {
            assert_eq!(peek(&dst, i), i);
        }
    }

    // --- Consumer ---

    #[test]
    fn consumer_pop_batch_lay_toi_da_muoi_phan_tu_mot_lan()
    {
        let src = Ring::new(16);
        let dst = Ring::<u32>::new(16);
        for i in 0..16u32
        {
            assert_eq!(src.push(i), Ok(()));
        }

        let consumer = src.consumer(&dst);
        assert_eq!(consumer.pop_batch(), 10, "Consumer::pop_batch cố định trần ở 10");
        assert_eq!(src.cursor_data().filled_slots(), 6);
    }

    #[test]
    fn consumer_pop_batch_tren_ring_rong_thi_tra_ve_khong()
    {
        let src = Ring::<u32>::new(16);
        let dst = Ring::<u32>::new(16);
        assert_eq!(src.consumer(&dst).pop_batch(), 0);
    }

    // --- một chủ, nhiều kẻ trộm chạy song song ---

    // Đúng mô hình mà `worker_pool` nhắm tới: chỉ chủ sở hữu push vào deque của mình, các worker
    // khác vét việc sang deque riêng rồi tự `pop_lifo` ra xử lý.
    //
    // Chủ cố tình KHÔNG gọi `pop_lifo` trên deque của chính nó ở đây. Đường đó còn tranh chấp
    // phần tử cuối với kẻ trộm, trộn vào thì test sẽ chết vì lỗi khác chứ không đo được giao thức
    // CAS/publish mà nó định đo.
    #[cfg(not(loom))]
    #[test]
    fn mot_chu_nhieu_ke_trom_khong_mat_khong_trung()
    {
        use crate::sync::AtomicBool;
        use crate::sync::Ordering::{Acquire as AcqXong, Release as RelXong};

        const CAP: usize = 64;
        const KE_TROM: u32 = 4;
        const TONG: u32 = 20_000;
        const CHUNK: usize = 8;
        /// Trần cứng cho vòng gom. Nếu ring hỏng và nhả ra vô hạn thì test phải chết ngay tại đây,
        /// đừng để nó gặm hết RAM rồi mới chịu dừng.
        const TRAN_GOM: usize = TONG as usize * 2;

        let victim = Ring::<u32>::new(CAP);
        let xong = AtomicBool::new(false);

        let mut gom = std::thread::scope(|scope| {
            let ke_trom: Vec<_> = (0..KE_TROM)
                .map(|_| {
                    let victim = &victim;
                    let xong = &xong;
                    scope.spawn(move || {
                        // Deque riêng, tạo ngay trong thread này để chính nó làm chủ sở hữu,
                        // nhờ vậy `pop_lifo` bên dưới qua được debug_assert.
                        let cua_toi = Ring::<u32>::new(CAP);
                        let mut ket_qua = Vec::new();
                        let mut backoff = Backoff::new();
                        loop
                        {
                            if victim.consumer_pop_batch_to(CHUNK, &cua_toi) == 0
                            {
                                if xong.load(AcqXong)
                                {
                                    break ket_qua;
                                }
                                backoff.snooze();
                                continue;
                            }
                            backoff.reset();
                            while let Some(v) = cua_toi.pop_lifo()
                            {
                                ket_qua.push(v);
                                assert!(ket_qua.len() <= TRAN_GOM, "gom vượt trần, ring đang nhả ra rác");
                            }
                        }
                    })
                })
                .collect();

            let mut backoff = Backoff::new();
            for val in 0..TONG
            {
                let mut cho_day = val;
                while let Err(tra_lai) = victim.push(cho_day)
                {
                    cho_day = tra_lai;
                    backoff.snooze();
                }
                backoff.reset();
            }
            // Tới dòng này chủ đã push đủ TONG phần tử, nên kẻ trộm nào thấy xong=true kèm một lần
            // steal hụt thì chắc chắn ring đã cạn thật, không phải cạn tạm thời.
            xong.store(true, RelXong);

            let mut tat_ca = Vec::new();
            for h in ke_trom
            {
                tat_ca.extend(h.join().unwrap());
            }
            tat_ca
        });

        gom.sort_unstable();
        assert_eq!(gom.len(), TONG as usize, "không được mất hay nhân bản phần tử nào");
        assert!(gom.iter().copied().eq(0..TONG), "phải đúng dãy 0..TONG, mỗi số xuất hiện một lần");
    }

    // Chọc thẳng vào cửa sổ tranh chấp giữa `pop_lifo` của chủ và kiểu vét theo lô của kẻ trộm.
    //
    // Nhánh nhanh của `pop_lifo` (khi `filled >= 2`) hạ `tail` rồi lấy ô `tail - 1` mà không hỏi
    // `head`. Nó chỉ chắc chân nếu kẻ trộm mỗi lần chỉ nhích `in_stealing` lên một, như Chase-Lev.
    // `consumer_pop_batch_to` thì vét cả lô, nên một lô có thể trùm luôn ô `tail - 1` mà chủ đang
    // định lấy:
    //
    //     in_stealing=0, tail=2
    //     chủ:   snapshot filled=2, rẽ vào nhánh nhanh
    //     trộm:  CAS in_stealing 0→2, ôm cả ô 0 lẫn ô 1
    //     chủ:   fetch_sub tail→1, take_at(1)   ← trùng ô với trộm
    //
    // Ring cố tình để bé và cho kẻ trộm vét trọn lô, để `filled` luôn quanh quẩn 1..3, đúng chỗ
    // cửa sổ này hay mở ra nhất.
    #[cfg(not(loom))]
    #[test]
    //#[ignore = "đang lỗi: nhánh nhanh của pop_lifo còn tranh chấp với lô của kẻ trộm, xem ghi chú trên test"]
    fn chu_vua_pop_lifo_vua_bi_trom_lay_lo()
    {
        use crate::sync::AtomicBool;
        use crate::sync::Ordering::{Acquire as AcqXong, Release as RelXong};

        const CAP: usize = 8;
        const KE_TROM: u32 = 3;
        const TONG: u32 = 2_000;
        /// Race kiểu này không nổ đều, chạy lại nhiều vòng cho nó có cơ hội.
        const VONG: u32 = 20;
        /// Kẻ trộm vét trọn ring, để lô của nó chạm tới sát `tail`.
        const CHUNK: usize = CAP;
        /// Trần cứng cho mọi vòng gom, ring hỏng thì test phải chết ngay chứ đừng gặm hết RAM.
        const TRAN_GOM: usize = TONG as usize * 2;
        /// Trần cho vòng chờ push, phòng khi con trỏ hỏng làm ring vừa báo đầy vừa báo rỗng.
        const TRAN_CHO: u32 = 100_000;

        for vong in 0..VONG
        {
            let victim = Ring::<u32>::new(CAP);
            let xong = AtomicBool::new(false);

            let mut gom = std::thread::scope(|scope| {
                let ke_trom: Vec<_> = (0..KE_TROM)
                    .map(|_| {
                        let victim = &victim;
                        let xong = &xong;
                        scope.spawn(move || {
                            let cua_toi = Ring::<u32>::new(CAP);
                            let mut ket_qua = Vec::new();
                            let mut backoff = Backoff::new();
                            loop
                            {
                                if victim.consumer_pop_batch_to(CHUNK, &cua_toi) == 0
                                {
                                    if xong.load(AcqXong)
                                    {
                                        break ket_qua;
                                    }
                                    backoff.snooze();
                                    continue;
                                }
                                backoff.reset();
                                while let Some(v) = cua_toi.pop_lifo()
                                {
                                    ket_qua.push(v);
                                    assert!(ket_qua.len() <= TRAN_GOM, "gom vượt trần, ring đang nhả ra rác");
                                }
                            }
                        })
                    })
                    .collect();

                // Chủ mà panic giữa chừng thì cờ `xong` bên dưới không bao giờ chạy tới, kẻ trộm sẽ
                // treo mãi và `thread::scope` ngồi chờ theo. Guard này đảm bảo cờ được dựng cả trên
                // đường unwind. Nó khai báo sau `ke_trom` nên khi unwind sẽ rụng trước, kịp báo cho
                // kẻ trộm trước lúc scope đứng ra chờ.
                struct CoXong<'a>(&'a AtomicBool);
                impl Drop for CoXong<'_>
                {
                    fn drop(&mut self)
                    {
                        self.0.store(true, RelXong);
                    }
                }
                let _co_xong = CoXong(&xong);

                // Chủ vừa nạp việc vừa tự rút việc ra làm, đúng kiểu một worker chạy job của chính nó.
                let mut cua_chu = Vec::new();
                let mut backoff = Backoff::new();
                for val in 0..TONG
                {
                    let mut cho_day = val;
                    let mut cho = 0u32;
                    while let Err(tra_lai) = victim.push(cho_day)
                    {
                        cho_day = tra_lai;
                        // Ring đầy thì chủ tự rút bớt một việc, vừa khỏi kẹt vừa mở thêm cửa sổ tranh chấp.
                        if let Some(v) = victim.pop_lifo()
                        {
                            cua_chu.push(v);
                        }
                        cho += 1;
                        assert!(cho < TRAN_CHO, "kẹt ở push quá lâu, con trỏ ring có vấn đề");
                        backoff.snooze();
                    }
                    backoff.reset();

                    if val % 2 == 0
                    {
                        if let Some(v) = victim.pop_lifo()
                        {
                            cua_chu.push(v);
                        }
                    }
                }
                // Vét nốt phần chủ còn ôm rồi mới báo xong, để kẻ trộm nào thấy cờ kèm một lần vét
                // hụt thì biết chắc ring đã cạn thật.
                while let Some(v) = victim.pop_lifo()
                {
                    cua_chu.push(v);
                    assert!(cua_chu.len() <= TRAN_GOM, "gom vượt trần, ring đang nhả ra rác");
                }
                xong.store(true, RelXong);

                for h in ke_trom
                {
                    cua_chu.extend(h.join().unwrap());
                }
                cua_chu
            });

            gom.sort_unstable();
            assert_eq!(gom.len(), TONG as usize, "vòng {}: mất hoặc nhân bản phần tử", vong);
            assert!(
                gom.iter().copied().eq(0..TONG),
                "vòng {}: phải đúng dãy 0..TONG, mỗi số xuất hiện một lần",
                vong
            );
        }
    }

    // --- Drop ---

    struct DemHuy(Rc<Cell<usize>>);
    impl Drop for DemHuy
    {
        fn drop(&mut self)
        {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn ring_bi_huy_thi_tha_cac_phan_tu_con_lai()
    {
        // `SpmcRingBufferFifo` có `impl Drop` dọn khoảng `[in_stealing, tail)`, ring này thì chưa,
        // nên mọi phần tử chưa kịp lấy ra sẽ không bao giờ chạy destructor.
        let dem = Rc::new(Cell::new(0));
        {
            let ring = Ring::new(4);
            for _ in 0..3
            {
                assert!(ring.push(DemHuy(dem.clone())).is_ok());
            }
            assert_eq!(dem.get(), 0, "chưa huỷ ring thì chưa có gì bị thả");
        }
        assert_eq!(dem.get(), 3, "huỷ ring phải thả hết 3 phần tử còn sót");
    }
}
