use crate::collection::ring_buffer::params::{ParamsCasForPopBatch, WriteableBuffer};
use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;
use crate::sync::AtomicU32;
use crate::sync::Ordering::{Acquire, Relaxed, Release};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::cursors::CursorData;
use crate::utils::fixed_buffer::FixedRingBuffer;
use crate::utils::packed::Packed;
use crate::utils::steal::Steal;
use crate::utils::{pack, unpack};

use consumer::Consumer;
pub mod consumer;

/// Single producer, multiple consumers.
/// When the producer pops, it retrieves elements using LIFO ordering.
/// When consumers pop, they retrieve elements using FIFO ordering.
pub struct SpmcRingBufferLifoProduceFifoConsume<T>
{
    buffer: FixedRingBuffer<T>,
    anchor: Packed,
    stolen: CachePadded<AtomicU32>,

    #[cfg(debug_assertions)]
    owner_thread: crate::sync::thread::Thread,
}

unsafe impl<T: Send> Send for SpmcRingBufferLifoProduceFifoConsume<T> {}
unsafe impl<T: Send> Sync for SpmcRingBufferLifoProduceFifoConsume<T> {}

impl<T> Drop for SpmcRingBufferLifoProduceFifoConsume<T>
{
    fn drop(&mut self)
    {
        // Chỉ khoảng `[in_stealing, tail)` là còn hàng thật. Phần `[stolen, in_stealing)` đã bị kẻ
        // trộm lấy đi rồi, drop nữa là double free.
        let (tail, in_stealing) = self.anchor.load_unpack(Relaxed);
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
            anchor: Packed::new(0, 0),
            stolen: CachePadded::new(AtomicU32::new(0)),
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
        #[cfg(debug_assertions)]
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

        // Ghi dữ liệu xong mới công bố `tail`, và chỉ nhích nửa cao nên `in_stealing` của kẻ trộm
        // không hề bị đụng tới.
        self.anchor.fetch_add(1 << 32, Release);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_batch_by_taking_from(&self, max: usize, src: &SpmcRingBufferFifo<T>) -> usize
    {
        #[cfg(debug_assertions)]
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
        // `take_amount` is just what we requested, but the actual amount we can take depends on `src`:
        // other thieves might snatch a portion in the meantime.
        // Advancing `tail` by the requested amount would expose unwritten slots, causing the next thief to read garbage data. Advance it by the actual amount instead.
        let moved = src.pop_batch_to(param);
        if moved < 1
        {
            return 0;
        }

        self.anchor.fetch_add((moved as u64) << 32, Release);
        moved
    }

    /// Pops the newest element. Only the owner is permitted to call this.
    #[inline]
    pub(crate) fn pop_lifo(&self) -> Option<T>
    {
        #[cfg(debug_assertions)]
        debug_assert!(
            crate::sync::thread::current().id() == self.owner_thread.id(),
            "pop lifo must be called from owner thread"
        );
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

    /// Vét một lô việc từ ring này sang `dst`, giữ nguyên thứ tự FIFO của nguồn.
    ///
    /// Trả về [`Steal`] thay vì một con số, vì "lấy được 0" gộp chung hai chuyện rất khác nhau:
    /// - [`Steal::Empty`]: nguồn đang cạn thật, kẻ trộm nên đi tìm nạn nhân khác.
    /// - [`Steal::Busy`]: có người chen ngang, hoặc `dst` hết chỗ. Nguồn vẫn còn hàng, quay lại sau
    ///   là lấy được.
    /// - [`Steal::Success`]: kèm theo số phần tử đã chuyển, luôn lớn hơn 0.
    ///
    /// Cú CAS giành lô chỉ thử đúng một lần. Thua thì nhả ra [`Steal::Busy`] chứ không ngồi xoay
    /// vòng: người gọi tự quyết là thử lại chỗ này hay bỏ đi nơi khác, vẫn nhanh hơn đứng chờ.
    #[inline]
    pub(crate) fn try_steal_batch_to(&self, max: usize, dst: &SpmcRingBufferLifoProduceFifoConsume<T>) -> Steal<usize>
    {
        debug_assert!(max > 0, "The steal batch size must be greater than zero.");
        let dst_cursor_data = dst.cursor_data();
        let mut my_cursor_data = self.cursor_data();

        // Đích hết chỗ thì đó là chuyện của đích, không phải nguồn cạn. Báo `Busy` để người gọi
        // biết là dọn bớt deque của mình rồi quay lại, đừng bỏ nạn nhân này đi.
        let free_slots = dst_cursor_data.empty_slots();
        if free_slots < 1
        {
            return Steal::Busy;
        }

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
        // `dst` là deque của chính thread đang gọi nên không có ai push song song, nhưng vẫn dùng
        // `fetch_add` để khỏi giẫm lên `in_stealing` mà kẻ trộm khác đang nhích.
        dst.anchor.fetch_add((pop_amount as u64) << 32, Release);
        Steal::Success(pop_amount)
    }

    /// Trộm đúng một việc rồi cầm luôn giá trị về, không cần deque đích.
    ///
    /// Hợp với lúc worker chỉ muốn một việc để làm ngay. Vẫn là đầu FIFO của nạn nhân, nên chủ và
    /// kẻ trộm ăn từ hai đầu khác nhau.
    #[inline]
    pub(crate) fn try_steal_one(&self) -> Steal<T>
    {
        let mut my_cursor_data = self.cursor_data();

        let params = ParamsCasForPopBatch {
            cursor_data:           &mut my_cursor_data,
            pop_amount:            1,
            success_order:         Release,
            fail_order:            Acquire,
            fetch_after_cas_order: Relaxed,
        };

        match self.fifo_try_cas_for_pop_batch(params)
        {
            Steal::Success(_) =>
            {}
            Steal::Empty => return Steal::Empty,
            Steal::Busy => return Steal::Busy,
        }

        let claim_start = my_cursor_data.blocked;
        let val = unsafe { self.buffer.take_at(claim_start) };
        self.consumer_publish_stolen(claim_start, 1);
        Steal::Success(val)
    }

    /// Bản gọn của [`Self::try_steal_batch_to`] cho chỗ nào chỉ cần biết lấy được bao nhiêu.
    #[inline]
    pub(crate) fn consumer_pop_batch_to(&self, max: usize, other: &SpmcRingBufferLifoProduceFifoConsume<T>) -> usize
    {
        self.try_steal_batch_to(max, other).success().unwrap_or(0)
    }
}
impl<T> SpmcRingBufferLifoProduceFifoConsume<T>
{
    /// Giành trước một lô ở đầu `in_stealing`, thử đúng một lần.
    ///
    /// Không có vòng xoay ở đây: CAS thua nghĩa là vừa có người đụng vào `anchor`, ảnh chụp con trỏ
    /// trong tay đã cũ. Thay vì thử lại với số liệu cũ, trả [`Steal::Busy`] để người gọi chụp lại từ
    /// đầu hoặc chuyển sang nạn nhân khác.
    ///
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
        // `tail` về cùng nhịp với `in_stealing` nên không thể lệch nhau. Đây đúng là chỗ giữ cho
        // `pop_amount` không bao giờ ôm nhầm ô của chủ.
        cas_data.pop_amount = cas_data.pop_amount.min(filled_slots);

        let current = pack(cas_data.cursor_data.tail, cas_data.cursor_data.blocked);
        let next = pack(
            cas_data.cursor_data.tail,
            // reserve a slot for the pop operation, creating a barrier for other consumers
            cas_data.cursor_data.blocked.wrapping_add(cas_data.pop_amount as u32),
        );

        // Dùng bản `strong`: chỉ thử một lần nên một cú trượt vu vơ của `weak` sẽ bị hiểu nhầm thành
        // có người tranh chấp.
        match self.anchor.compare_exchange(current, next, cas_data.success_order, cas_data.fail_order)
        {
            Ok(_) => Steal::Success(cas_data.pop_amount),
            Err(_) => Steal::Busy,
        }
    }

    /// Công bố phần vừa lấy xong, theo đúng thứ tự đã giành.
    ///
    /// Ai giành trước công bố trước, nên người tới sau phải chờ tới lượt. Nhờ vậy `stolen` không
    /// bao giờ nhảy qua một lô còn đang dở, và producer nhìn vào `stolen` là biết chắc ô nào đã
    /// hết người đọc.
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
        (c.stolen, c.blocked, c.tail)
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

    /// Vá cho cái lỗi: `push_batch_by_taking_from` từng nhích `tail` theo số ô nó **xin**, chứ
    /// không theo số ô nó **lấy được**. Khi một kẻ trộm khác cướp mất một phần của nguồn ngay giữa
    /// lúc đọc `len()` và lúc CAS, phần chênh lệch trở thành những ô chưa ai ghi mà `tail` vẫn nói
    /// là có hàng. Người trộm kế tiếp đọc trúng rác.
    ///
    /// Cửa sổ race chỉ hé ra khi nguồn còn ít hơn một lô, nên chỉ thả một mớ vào rồi cùng rút thì
    /// hoạ hoằn mới bắt được. Ở đây nguồn được giữ nông: một thread bơm vào từng cái một, ba tay
    /// cùng rút ra, nên gần như lần nào `take_amount` cũng đúng bằng số hàng đang có và bất kỳ cú
    /// trộm nào chen vào cũng làm lệch.
    ///
    /// Cộng sổ ở cuối: số ô `tail` của ring cộng phần kẻ trộm gom được phải đúng bằng số phần tử
    /// đã bơm. Lệch lên nghĩa là `tail` đã đi quá phần thực có.
    #[cfg(not(loom))]
    #[test]
    fn push_batch_khong_duoc_nhich_tail_qua_so_o_thuc_su_lay_duoc()
    {
        use crate::sync::Ordering::{Acquire as SyncAcquire, Relaxed as SyncRelaxed, Release as SyncRelease};
        use crate::sync::{AtomicBool, AtomicUsize};

        const TONG: u32 = 8_192;
        /// Nguồn cố tình nhỏ, để nó luôn gần cạn và cửa sổ race luôn mở.
        const SUC_CHUA_NGUON: usize = 16;
        const KE_TROM: usize = 2;
        const LO: usize = 8;
        /// Trần cứng cho mọi vòng, để một ring hỏng không kéo test chạy mãi.
        const TRAN_VONG_LAP: usize = 2_000_000;

        let src = SpmcRingBufferFifo::<u32>::new(SUC_CHUA_NGUON);
        let ring = Ring::<u32>::new(TONG as usize * 2);
        let ke_trom_gom = AtomicUsize::new(0);
        let da_bom_xong = AtomicBool::new(false);

        std::thread::scope(|scope| {
            scope.spawn(|| {
                let mut con_lai = 0..TONG;
                let mut val = con_lai.next();
                for _ in 0..TRAN_VONG_LAP
                {
                    let Some(v) = val
                    else
                    {
                        break;
                    };
                    match src.push(v)
                    {
                        Ok(()) => val = con_lai.next(),
                        Err(_) => std::hint::spin_loop(),
                    }
                }
                assert!(val.is_none(), "producer phải bơm hết {} phần tử", TONG);
                da_bom_xong.store(true, SyncRelease);
            });

            for _ in 0..KE_TROM
            {
                scope.spawn(|| {
                    let mut thung = Vec::new();
                    for _ in 0..TRAN_VONG_LAP
                    {
                        let lay = src.pop_batch(LO, &mut thung);
                        if lay > 0
                        {
                            ke_trom_gom.fetch_add(lay, SyncRelaxed);
                            thung.clear();
                        }
                        else if da_bom_xong.load(SyncAcquire) && src.is_empty()
                        {
                            break;
                        }
                        else
                        {
                            std::hint::spin_loop();
                        }
                    }
                });
            }

            // Chủ sở hữu của `ring` phải là chính thread đã tạo nó, nên phần hút việc chạy ở đây
            // chứ không đẩy sang thread con.
            for _ in 0..TRAN_VONG_LAP
            {
                if src.is_empty()
                {
                    if da_bom_xong.load(SyncAcquire)
                    {
                        break;
                    }
                    std::hint::spin_loop();
                    continue;
                }
                if ring.push_batch_by_taking_from(LO, &src) == 0
                {
                    std::hint::spin_loop();
                }
            }
        });

        let (_, _, tail) = cursors(&ring);
        let da_gom = ke_trom_gom.load(SyncRelaxed);
        assert!(src.is_empty(), "nguồn phải bị rút cạn, còn lại {}", src.len());
        assert_eq!(
            tail as usize + da_gom,
            TONG as usize,
            "tail={} cộng phần kẻ trộm gom={} phải bằng {}, lệch lên nghĩa là tail đã nhích quá phần thực sự lấy được",
            tail,
            da_gom,
            TONG
        );
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

    // --- Steal: lấy hụt phải nói rõ vì sao ---

    #[test]
    #[should_panic(expected = "greater than zero")]
    fn steal_voi_max_bang_khong_thi_panic()
    {
        let src = Ring::<u32>::new(4);
        let dst = Ring::<u32>::new(4);
        let _ = src.try_steal_batch_to(0, &dst);
    }

    #[test]
    fn steal_tu_ring_rong_thi_bao_empty()
    {
        let src = Ring::<u32>::new(4);
        let dst = Ring::<u32>::new(4);
        assert_eq!(src.try_steal_batch_to(4, &dst), Steal::Empty);
    }

    #[test]
    fn steal_khi_dich_day_thi_bao_busy_chu_khong_bao_empty()
    {
        // Nguồn vẫn còn nguyên hàng, chỉ là đích không có chỗ nhận. Kẻ trộm cần phân biệt được hai
        // chuyện này để khỏi gạch tên một nạn nhân đang đầy việc.
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

        assert_eq!(src.try_steal_batch_to(4, &dst), Steal::Busy);
        assert_eq!(cursors(&src), (0, 0, 4), "lần steal hụt không được đụng vào con trỏ nguồn");
    }

    #[test]
    fn steal_thanh_cong_thi_bao_dung_so_luong()
    {
        let src = Ring::new(8);
        let dst = Ring::<u32>::new(8);
        for i in 0..5u32
        {
            assert_eq!(src.push(i), Ok(()));
        }

        assert_eq!(src.try_steal_batch_to(3, &dst), Steal::Success(3));
        assert_eq!(src.try_steal_batch_to(8, &dst), Steal::Success(2), "chỉ còn 2 phần tử thì lấy 2");
        assert_eq!(src.try_steal_batch_to(8, &dst), Steal::Empty);
        for i in 0..5u32
        {
            assert_eq!(peek(&dst, i), i, "thứ tự FIFO của nguồn phải được giữ nguyên");
        }
    }

    // --- try_steal_one: trộm một việc, cầm luôn giá trị về ---

    #[test]
    fn steal_one_tren_ring_rong_thi_bao_empty()
    {
        let ring = Ring::<u32>::new(4);
        assert_eq!(ring.try_steal_one(), Steal::Empty);
        assert_eq!(cursors(&ring), (0, 0, 0), "steal hụt không được đụng vào con trỏ");
    }

    #[test]
    fn steal_one_lay_phan_tu_cu_nhat_truoc()
    {
        // Chủ ăn từ đầu `tail`, kẻ trộm ăn từ đầu `in_stealing`, nên hai bên không giẫm chân nhau.
        let ring = Ring::new(4);
        for i in 0..4u32
        {
            assert_eq!(ring.push(i), Ok(()));
        }

        assert_eq!(ring.try_steal_one(), Steal::Success(0));
        assert_eq!(ring.try_steal_one(), Steal::Success(1));
        assert_eq!(cursors(&ring), (2, 2, 4), "stolen và in_stealing phải nhích cùng nhịp");
        assert_eq!(ring.pop_lifo(), Some(3), "chủ vẫn lấy được phần tử mới nhất");
        assert_eq!(ring.try_steal_one(), Steal::Success(2));
        assert_eq!(ring.try_steal_one(), Steal::Empty);
    }

    #[test]
    fn steal_one_tra_lai_o_trong_cho_producer()
    {
        let ring = Ring::new(2);
        assert_eq!(ring.push(1), Ok(()));
        assert_eq!(ring.push(2), Ok(()));
        assert_eq!(ring.push(3), Err(3), "ring đang đầy");

        assert_eq!(ring.try_steal_one(), Steal::Success(1));
        assert_eq!(ring.push(3), Ok(()), "trộm xong thì ô vừa trống phải dùng lại được");
        assert_eq!(ring.try_steal_one(), Steal::Success(2));
        assert_eq!(ring.try_steal_one(), Steal::Success(3));
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

    #[test]
    fn consumer_try_steal_noi_ro_ly_do_khi_lay_hut()
    {
        let src = Ring::new(16);
        let dst = Ring::<u32>::new(16);
        assert_eq!(src.consumer(&dst).try_steal(), Steal::Empty, "nguồn cạn thì phải là Empty");

        for i in 0..4u32
        {
            assert_eq!(src.push(i), Ok(()));
        }
        assert_eq!(src.consumer(&dst).try_steal(), Steal::Success(4));

        let dst_day = Ring::<u32>::new(1);
        assert_eq!(dst_day.push(9), Ok(()));
        for i in 0..4u32
        {
            assert_eq!(src.push(i), Ok(()));
        }
        assert_eq!(src.consumer(&dst_day).try_steal(), Steal::Busy, "đích hết chỗ thì phải là Busy");
    }

    #[test]
    fn consumer_steal_one_lay_dung_mot_viec()
    {
        let src = Ring::new(8);
        let dst = Ring::<u32>::new(8);
        for i in 0..3u32
        {
            assert_eq!(src.push(i), Ok(()));
        }

        let consumer = src.consumer(&dst);
        assert_eq!(consumer.steal_one(), Some(0));
        assert_eq!(consumer.try_steal_one(), Steal::Success(1));
        assert_eq!(cursors(&dst), (0, 0, 0), "steal_one không đi qua deque đích");
        assert_eq!(src.cursor_data().filled_slots(), 1);
        assert_eq!(consumer.steal_one(), Some(2));
        assert_eq!(consumer.steal_one(), None);
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

                    // Chủ tự rút một việc sau mỗi hai lần nạp, giữ cho hàng luôn quanh quẩn 1..3
                    // phần tử, đúng chỗ hai đầu chạm nhau.
                    if val % 2 == 0
                        && let Some(v) = victim.pop_lifo()
                    {
                        cua_chu.push(v);
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
