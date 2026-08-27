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
            unsafe { self.ring.slots.write(bottom.wrapping_add(offset as u32), val) };
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
        self.ring.bottom.store(next, Ordering::SeqCst);
        fence(Ordering::SeqCst);

        let mut head = self.ring.top.load(Ordering::SeqCst);

        loop
        {
            let (free, claim) = unpack(head);
            self.free.set(free);
            let size = next.wrapping_sub(claim) as i32;

            if size < 0
            {
                self.ring.bottom.store(bottom, Ordering::SeqCst);
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
                    self.ring.bottom.store(bottom, Ordering::SeqCst);
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

    /// Xả nửa cũ của ring xuống `out`, giữ lại nửa mới cho chủ.
    ///
    /// Đây là cửa thoát khi ring đầy: thay vì để `push` trả `Err`, chủ đẩy phần cũ sang lane queue
    /// rồi push tiếp. Nửa cũ đi chứ không phải nửa mới, vì với LIFO thì job mới nhất là job nóng
    /// nhất trong cache của chính chủ, còn job cũ nằm ở đáy có thể đã nguội và ai chạy cũng như
    /// nhau.
    ///
    /// # Vì sao chủ bốc được cả lô còn kẻ trộm thì không
    ///
    /// Ràng buộc `n = 1` ở [`Consumer::try_steal`](super::thief::Consumer::try_steal) sinh ra từ
    /// việc kẻ trộm chỉ có một bản chụp `bottom` đã cũ, nên vùng nó nhận có thể trùm qua đúng ô mà
    /// chủ đang `pop` (xem mục 4 của `docs_internal/ring-buffer-lifo.md`). Ở đây người bốc lô
    /// **chính là** chủ: `bottom` không thể đổi dưới lưng nó, và không có `pop` nào chạy song song
    /// vì `pop` cũng đòi `&mut self`. Kẽ hở đó không tồn tại, nên lấy `n` ô một lượt là an toàn.
    ///
    /// Trả `0` khi ring rỗng, hoặc khi có kẻ trộm đang giữ một ô (`free != claim`). Trường hợp sau
    /// không phải lỗi: kẻ trộm sắp lấy bớt việc đi, gọi lại một nhịp nữa là xong.
    pub fn spill_half(&mut self, out: &mut Vec<T>) -> usize
    {
        // Chủ là người duy nhất ghi `bottom`, nên `Relaxed` là đủ và giá trị này không cũ đi được.
        let bottom = self.ring.bottom.load(Ordering::Relaxed);
        let mut head = self.ring.top.load(Ordering::SeqCst);

        let (start, n) = loop
        {
            let (free, claim) = unpack(head);
            self.free.set(free);

            if free != claim
            {
                return 0;
            }

            let size = bottom.wrapping_sub(claim) as i32;
            if size <= 0
            {
                return 0;
            }

            // Làm tròn lên: còn đúng một job thì job đó ở lại với chủ, không xả đi.
            let size = size as u32;
            let n = size - size / 2;
            let next = claim.wrapping_add(n);

            match self.ring.top.compare_exchange_weak(head, pack(claim, next), Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break (claim, n),
                Err(actual) => head = actual,
            }
        };

        out.reserve(n as usize);
        for offset in 0..n
        {
            out.push(unsafe { self.ring.slots.read(start.wrapping_add(offset)) });
        }

        // Nhả vùng đã đọc xong: `free` đuổi kịp `claim`. Trong lúc mình giữ, không kẻ trộm nào
        // claim thêm được (họ thấy `free != claim` và bỏ đi), nên `claim` đọc lại ở đây vẫn là cái
        // mình vừa đặt.
        let mut head = self.ring.top.load(Ordering::SeqCst);
        loop
        {
            let (_, claim) = unpack(head);
            match self
                .ring
                .top
                .compare_exchange_weak(head, pack(claim, claim), Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) =>
                {
                    self.free.set(claim);
                    break;
                }
                Err(actual) => head = actual,
            }
        }

        n as usize
    }

    #[inline]
    pub fn drain(&mut self, out: &mut Vec<T>) -> usize
    {
        self.pop_batch_with(usize::MAX, |val| out.push(val))
    }
}
