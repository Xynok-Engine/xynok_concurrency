use crate::collection::ring_buffer::params::{ParamsCasForPopBatch, WriteableBuffer};
use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;
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

use crate::collection::ring_buffer::spmc_lifo_produce_fifo_consume::consumer::Consumer;

/// Single producer, multiple consumers.
/// When the producer pops, it retrieves elements using LIFO ordering.
/// When consumers pop, they retrieve elements using FIFO ordering.
pub struct SpmcRingBufferLifoProduceFifoConsume<T>
{
    pub(crate) buffer: FixedRingBuffer<T>,
    anchor:            Packed,
    stolen:            CachePadded<AtomicU32>,
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
        }
    }
    #[inline]
    pub fn buffer(&self) -> &FixedRingBuffer<T>
    {
        &self.buffer
    }

    pub fn consumer<'a>(&'a self, dst: &'a SpmcRingBufferLifoProduceFifoConsume<T>) -> Consumer<'a, T>
    {
        Consumer::new(self, dst)
    }
    #[inline]
    pub fn tail(&self) -> u32
    {
        let (tail, _) = self.anchor.load_unpack(Relaxed);
        tail
    }
}
impl<T> SpmcRingBufferLifoProduceFifoConsume<T>
{
    /// only call by the owner thread
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

        // Ghi dữ liệu xong mới công bố `tail`, và chỉ nhích nửa cao nên `in_stealing` của kẻ trộm
        // không hề bị đụng tới.
        self.anchor.fetch_add(1 << 32, Release);
        Ok(())
    }

    /// Nạp một lô việc từ hàng đợi chung `src` vào ring này, trả về số phần tử thật sự nạp được.
    ///
    /// Chỉ chủ ring được gọi, vì nó là người duy nhất có quyền nhích `tail`.
    #[inline]
    pub(crate) fn push_batch_by_taking_from_queue(&self, max: usize, src: &QueueBatching<T>) -> usize
    {
        debug_assert!(max > 0, "The push batch size must be greater than zero.");
        let cursor_data = self.cursor_data();
        let take_amount = cursor_data.empty_slots().min(max).min(src.len());
        if take_amount < 1
        {
            return 0;
        }

        // `src.len()` chỉ là ảnh chụp không khoá, người khác có thể vét trước nên số lấy về thật sự
        // có thể ít hơn. Nhích `tail` theo số thật, nhích theo số đã hỏi là hở ra mấy ô chưa ghi cho
        // kẻ trộm kế tiếp đọc phải rác.
        let moved = src.drain_into_buffer(take_amount, &self.buffer, cursor_data.tail);
        if moved < 1
        {
            return 0;
        }

        // Ghi xong hết mới công bố `tail`, và chỉ đụng nửa cao nên `in_stealing` của kẻ trộm vẫn nguyên.
        self.anchor.fetch_add((moved as u64) << 32, Release);
        moved
    }
    #[inline]
    pub(crate) fn push_batch_by_taking_from(&self, max: usize, src: &SpmcRingBufferFifo<T>) -> usize
    {
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
    pub(crate) fn cursor_data(&self) -> CursorData
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
