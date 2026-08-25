//! Đầu ghi của [`RingBuffer`]. Đúng một thread, không sao chép được.

use std::cell::Cell;
use std::marker::PhantomData;

use super::RingBuffer;
use crate::sync::Ordering;
use crate::utils::{pack, unpack};

/// Quyền của chủ ring buffer. Không `Clone`, không `Sync` — kiểu dữ liệu tự bảo đảm "chỉ đúng một thread
/// được ghi ở đầu này", thay cho một lời hứa suông trong tài liệu.
pub struct Producer<'a, T>
{
    ring:  &'a RingBuffer<T>,
    /// `Cell` là `Send` nhưng không `Sync`: `Producer` truyền được sang thread worker, nhưng
    /// `&Producer` thì không chia sẻ được giữa hai thread — và chính điều đó làm cho `tail` chỉ có
    /// một người ghi.
    _lone: PhantomData<Cell<T>>,
}

impl<'a, T> Producer<'a, T>
{
    #[inline]
    pub(super) fn new(ring: &'a RingBuffer<T>) -> Self
    {
        Self { ring, _lone: PhantomData }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.ring.capacity()
    }

    /// Số ô đang bị chiếm, kể cả vùng đang bị bê — xem [`RingBuffer::occupied`].
    #[inline]
    pub fn occupied(&self) -> usize
    {
        self.ring.occupied()
    }

    /// Số job [`pop`](Self::pop) còn lấy được — xem [`RingBuffer::available`].
    #[inline]
    pub fn available(&self) -> usize
    {
        self.ring.available()
    }

    /// Không còn gì để [`pop`](Self::pop). Cặp với [`available`](Self::available), **không** với
    /// [`occupied`](Self::occupied) — xem [`RingBuffer::occupied`].
    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.ring.is_empty()
    }

    /// Số ô còn trống — con số cho [`push`](Self::push). Mốc là `steal`, nên đây là hàm dựng trên
    /// [`occupied`](Self::occupied) chứ không phải [`available`](Self::available).
    ///
    /// Xấp xỉ **thiếu**, không bao giờ thừa: `steal` chỉ tiến, nên một giá trị đọc lệch nhịp chỉ
    /// làm con số này nhỏ hơn sự thật.
    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.capacity() - self.occupied().min(self.capacity())
    }

    /// Số ô còn trống, tính từ một `tail` mà chỗ gọi đã cầm sẵn.
    ///
    /// Mốc phải là `steal`, **không** phải `real`: ô trong `[steal, real)` đang có chủ nên vẫn tính
    /// là bị chiếm — dùng `real` là ghi đè lên tay kẻ trộm.
    #[inline]
    fn free_from(&self, tail: u32) -> u32
    {
        let (steal, _) = unpack(self.ring.head.load(Ordering::Acquire));
        self.ring.capacity() as u32 - tail.wrapping_sub(steal)
    }

    /// Đặt một job vào `tail`. Đầy → trả lại `val` để chỗ gọi đẩy xuống kho chung.
    ///
    /// Ghi ô trước, công bố `tail` sau. Không CAS, không giành gì — đó là toàn bộ khoản lời so với
    /// khoá, và là lý do `tail` phải chỉ có một người ghi.
    pub fn push(&mut self, val: T) -> Result<(), T>
    {
        // `tail` chỉ mình ta ghi nên giá trị này luôn chính xác; `Relaxed` là đủ.
        let tail = self.ring.tail.load(Ordering::Relaxed);

        if self.free_from(tail) == 0
        {
            return Err(val);
        }

        // Ghi ô trước — mask nằm trong `slot()`, `tail` giữ nguyên là bộ đếm vô hạn.
        self.ring.slot(tail).with_mut(|p| unsafe {
            (*p).write(val);
        });
        // ...công bố sau. `Release` ở đây ghép với `Acquire` khi người đọc nạp `tail`: thấy chỉ số
        // mới là chắc chắn thấy cả job vừa ghi.
        self.ring.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Đặt cả một lô, chỉ công bố `tail` **một lần** cho toàn lô.
    ///
    /// Lấy từ đầu `vals` và xoá đi phần đã nhận; phần không vừa còn nguyên tại chỗ để chỗ gọi đẩy
    /// xuống kho chung. Trả về số job đã nhận.
    ///
    /// Khoản lời so với gọi [`push`](Self::push) `n` lần: một lần đọc `head` và một lần ghi `tail`
    /// thay vì `n` lần. Với `n` lớn thì đây là khác biệt giữa `n` lần chạm cache line dùng chung và
    /// đúng một lần.
    pub fn push_batch(&mut self, vals: &mut Vec<T>) -> usize
    {
        if vals.is_empty()
        {
            return 0;
        }

        let tail = self.ring.tail.load(Ordering::Relaxed);
        let n = (self.free_from(tail) as usize).min(vals.len());
        if n == 0
        {
            return 0;
        }

        for (offset, val) in vals.drain(..n).enumerate()
        {
            self.ring.slot(tail.wrapping_add(offset as u32)).with_mut(|p| unsafe {
                (*p).write(val);
            });
        }

        // Một lần công bố duy nhất cho cả lô. Cho tới dòng này, với mọi người khác thì các job vừa
        // ghi **chưa tồn tại** — nên không ai có thể bốc nhầm một ô mới ghi được nửa chừng.
        self.ring.tail.store(tail.wrapping_add(n as u32), Ordering::Release);
        n
    }

    /// Lấy một job ở `real` — FIFO, cùng đầu với [`Consumer`](super::Consumer).
    ///
    /// Một lần `compare_exchange` phân xử với mọi kẻ trộm. Xem phần đầu module về vì sao đường này
    /// không phải là LIFO ở `tail`.
    pub fn pop(&mut self) -> Option<T>
    {
        // `tail` chỉ mình ta ghi, và trong lúc ở đây thì không ai push — giá trị này chính xác.
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let mut head = self.ring.head.load(Ordering::Acquire);

        let real = loop
        {
            let (steal, real) = unpack(head);
            if real == tail
            {
                return None;
            }

            let next_real = real.wrapping_add(1);
            // `steal == real` → không ai đang bê, kéo cả hai lên để ô được trả lại cho `push` ngay.
            // `steal != real` → một kẻ trộm đang bê `[steal, real)`; `steal` phải nằm yên cho tới
            //                   khi nó chép xong, nên chỉ đẩy `real`.
            let next = match steal == real
            {
                true => pack(next_real, next_real),
                false => pack(steal, next_real),
            };

            match self.ring.head.compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break real,
                Err(actual) => head = actual,
            }
        };

        // CAS thắng rồi thì ô `real` là của riêng ta, không ai đọc hay ghi vào đó nữa.
        Some(self.ring.slot(real).with(|p| unsafe { (*p).assume_init_read() }))
    }

    /// Lấy tới `max` job vào `out`. Trả về số job lấy được.
    ///
    /// **Một** lần CAS cho cả lô, chứ không phải `max` lần — đó là toàn bộ lý do hàm này tồn tại.
    /// Nhận được cả vùng `[real, real + n)` trong một thao tác nguyên tử thì phần chép ra sau đó
    /// không còn ai giành nữa.
    pub fn pop_batch(&mut self, out: &mut Vec<T>, max: usize) -> usize
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
                Ok(_) => break (real, n),
                Err(actual) => head = actual,
            }
        };

        // SAFETY: CAS trên đã nhận trọn `[start, start + n)`, và không đường nào khác đọc lại vùng
        // đó — `real` đã vượt qua nó, `steal` thì không bao giờ vượt `real`.
        unsafe { self.ring.drain_claimed(start, n, out) };
        n as usize
    }
}
