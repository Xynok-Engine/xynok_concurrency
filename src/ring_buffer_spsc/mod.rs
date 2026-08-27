//! Ring có trần cho đúng một người ghi và đúng một người đọc.
//!
//! Hai ring kia trong crate này ([`ring_buffer_fifo`](crate::ring_buffer_fifo) và
//! [`ring_buffer_lifo`](crate::ring_buffer_lifo)) đều là SPMC: một chủ, nhiều kẻ trộm. Chỗ này cần
//! thứ ngược lại, và người đặt hàng là **thread audio**.
//!
//! # Vì sao audio không dùng chung đường với mọi thứ khác
//!
//! Thread audio bị driver gọi lại mỗi vài mili giây và phải trả xong bộ đệm trước hạn, lần nào cũng
//! vậy. Trong callback đó nó **không được** cấp phát, không được lấy khoá, không được park. Một lần
//! trễ hạn không phải là một frame tụt xuống 58 FPS, nó là một tiếng "pop" nghe rõ mồn một.
//!
//! Nên nó không bao giờ là worker của pool. Lane A gửi lệnh cho nó (phát tiếng này, đổi volume kia)
//! qua đúng cấu trúc này, và trong callback nó chỉ đọc:
//!
//! ```text
//!   lane A (một worker)                    thread audio
//!        │ push(lệnh)                           │ pop() trong callback
//!        ▼                                      ▼
//!   ┌──────────────────────────────────────────────┐
//!   │ [x][x][x][ ][ ][ ][ ][ ]                     │
//!   └──────────────────────────────────────────────┘
//!        tail (chỉ người ghi đụng)   head (chỉ người đọc đụng)
//! ```
//!
//! # Vì sao hai bên mỗi bên một chỉ số
//!
//! Người ghi chỉ ghi `tail`, người đọc chỉ ghi `head`. Không có CAS ở đâu cả, nên cả `push` lẫn
//! `pop` đều là wait-free: chúng chạy trong một số bước cố định bất kể phía bên kia đang làm gì.
//! Đó là điều kiện thật sự của một callback realtime, chứ "lock-free" thôi thì chưa đủ, một vòng
//! CAS vẫn có thể quay lại nhiều lần.
//!
//! Mỗi bên còn giữ thêm một **bản chụp** chỉ số của bên kia. Nhờ nó, đường nóng chỉ đọc cache line
//! của chính mình: người ghi chỉ đi hỏi `head` thật khi bản chụp nói là ring đã đầy, và phần lớn
//! thời gian thì nó không đầy. Không có bản chụp này thì mỗi lần push là một lần kéo cache line của
//! người đọc về, và hai core đá qua đá lại một dòng cache suốt cả frame.

use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicU32, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::slots::Slots;

/// Trần số ô, để phép trừ vòng của chỉ số `u32` không bao giờ nhập nhằng.
pub const MAX_SLOTS: u32 = 1 << 31;

pub struct RingBufferSpsc<T>
{
    /// Ô kế tiếp người đọc sẽ lấy. Chỉ người đọc ghi.
    head:  CachePadded<AtomicU32>,
    /// Ô kế tiếp người ghi sẽ ghi. Chỉ người ghi ghi.
    tail:  CachePadded<AtomicU32>,
    slots: Slots<T>,
}

unsafe impl<T: Send> Send for RingBufferSpsc<T> {}
unsafe impl<T: Send> Sync for RingBufferSpsc<T> {}

/// Đầu ghi. Chỉ một, và không `Sync`: hai thread cùng ghi là phá vỡ toàn bộ lập luận ở trên.
pub struct Sender<'a, T>
{
    ring: &'a RingBufferSpsc<T>,
    /// Bản chụp `head`, chỉ người ghi đụng tới.
    head: UnsafeCell<u32>,
}

/// Đầu đọc. Chỉ một, và cũng không `Sync`.
pub struct Receiver<'a, T>
{
    ring: &'a RingBufferSpsc<T>,
    /// Bản chụp `tail`, chỉ người đọc đụng tới.
    tail: UnsafeCell<u32>,
}

/// Người ghi được gửi sang thread khác, nhưng không được dùng từ hai thread cùng lúc.
unsafe impl<T: Send> Send for Sender<'_, T> {}
unsafe impl<T: Send> Send for Receiver<'_, T> {}

impl<T> RingBufferSpsc<T>
{
    /// Ring với ít nhất `total_slots` ô, làm tròn lên luỹ thừa hai.
    #[track_caller]
    pub fn new(total_slots: u32) -> Self
    {
        assert!(total_slots <= MAX_SLOTS, "total_slots {total_slots} vượt trần 2^31 của chỉ số u32 quay vòng");

        Self {
            head:  CachePadded::new(AtomicU32::new(0)),
            tail:  CachePadded::new(AtomicU32::new(0)),
            slots: Slots::new(total_slots),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.slots.capacity()
    }

    /// Tách thành hai đầu. `&mut self` là bằng chứng chưa có ai cầm đầu nào cả.
    #[inline]
    pub fn split(&mut self) -> (Sender<'_, T>, Receiver<'_, T>)
    {
        let this = &*self;
        (
            Sender {
                ring: this,
                head: UnsafeCell::new(this.head.load(Ordering::Acquire)),
            },
            Receiver {
                ring: this,
                tail: UnsafeCell::new(this.tail.load(Ordering::Acquire)),
            },
        )
    }

    /// Đầu ghi, cho trường hợp ring nằm sau một `Arc` chứ không phải một biến cục bộ.
    ///
    /// # Safety
    ///
    /// Chỉ được tồn tại đúng một `Sender` tại một thời điểm, và nó chỉ được dùng từ một thread.
    /// [`Self::split`] bảo đảm điều đó bằng `&mut self`; ở đây người gọi tự bảo đảm.
    #[inline]
    pub unsafe fn sender(&self) -> Sender<'_, T>
    {
        Sender {
            ring: self,
            head: UnsafeCell::new(self.head.load(Ordering::Acquire)),
        }
    }

    /// Đầu đọc. Cùng điều kiện với [`Self::sender`].
    ///
    /// # Safety
    ///
    /// Chỉ được tồn tại đúng một `Receiver` tại một thời điểm, và nó chỉ được dùng từ một thread.
    #[inline]
    pub unsafe fn receiver(&self) -> Receiver<'_, T>
    {
        Receiver {
            ring: self,
            tail: UnsafeCell::new(self.tail.load(Ordering::Acquire)),
        }
    }

    /// Số phần tử đang nằm trong ring. Ảnh chụp, và bên nào đọc cũng chỉ đúng tại thời điểm đọc.
    #[inline]
    pub fn len(&self) -> usize
    {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        tail.wrapping_sub(head) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.len() == 0
    }
}

impl<T> Sender<'_, T>
{
    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.ring.capacity()
    }

    /// Còn chỗ cho bao nhiêu phần tử nữa. Có thể là con số cũ, và cũ theo hướng an toàn: người đọc
    /// chỉ làm nó tăng lên.
    #[inline]
    pub fn remaining(&self) -> usize
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        self.free_from(tail) as usize
    }

    #[inline]
    pub fn is_full(&self) -> bool
    {
        self.remaining() == 0
    }

    /// Đọc lại `head` thật và cập nhật bản chụp.
    #[inline]
    fn refresh(&self, tail: u32) -> u32
    {
        let head = self.ring.head.load(Ordering::Acquire);
        self.head.with_mut(|cached| unsafe { *cached = head });
        self.ring.capacity() as u32 - tail.wrapping_sub(head)
    }

    /// Chỗ trống theo bản chụp, và chỉ đi hỏi thật khi bản chụp nói là hết chỗ.
    #[inline]
    fn free_from(&self, tail: u32) -> u32
    {
        let cached = self.head.with(|cached| unsafe { *cached });
        let used = tail.wrapping_sub(cached);
        let capacity = self.ring.capacity() as u32;

        match used < capacity
        {
            true => capacity - used,
            false => self.refresh(tail),
        }
    }

    /// Đẩy một phần tử vào, trả nó lại nếu ring đã đầy.
    ///
    /// Trả lại chứ không nuốt, và cũng không chờ: bên gửi là lane A, mà lane A thì không được đứng
    /// lại vì thread audio chưa kịp tiêu thụ. Đầy nghĩa là audio đang tụt lại, và người gọi mới là
    /// người biết nên bỏ lệnh nào.
    pub fn push(&mut self, val: T) -> Result<(), T>
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);

        if self.free_from(tail) == 0
        {
            return Err(val);
        }

        // Safety: ô này nằm ngoài vùng người đọc được phép đọc, vì `tail` chưa công bố nó.
        unsafe { self.ring.slots.write(tail, val) };
        // `Release`: người đọc thấy `tail` mới thì cũng thấy nội dung vừa ghi vào ô.
        self.ring.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Đẩy cả cụm và công bố **một lần**, trả về số phần tử đã vào.
    ///
    /// Một lần công bố cho cả cụm nghĩa là người đọc hoặc thấy cả cụm hoặc chưa thấy gì, và cũng
    /// nghĩa là chỉ một lần chạm vào cache line mà bên kia đang theo dõi.
    pub fn push_iter<I>(&mut self, vals: I) -> usize
    where I: IntoIterator<Item = T>
    {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let free = self.free_from(tail);

        let mut n = 0;
        for val in vals.into_iter().take(free as usize)
        {
            unsafe { self.ring.slots.write(tail.wrapping_add(n), val) };
            n += 1;
        }

        if n > 0
        {
            self.ring.tail.store(tail.wrapping_add(n), Ordering::Release);
        }
        n as usize
    }

    /// Số phần tử người đọc chưa lấy. Ảnh chụp.
    #[inline]
    pub fn len(&self) -> usize
    {
        self.ring.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.ring.is_empty()
    }
}

impl<T> Receiver<'_, T>
{
    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.ring.capacity()
    }

    /// Đọc lại `tail` thật và cập nhật bản chụp.
    #[inline]
    fn refresh(&self, head: u32) -> u32
    {
        let tail = self.ring.tail.load(Ordering::Acquire);
        self.tail.with_mut(|cached| unsafe { *cached = tail });
        tail.wrapping_sub(head)
    }

    /// Số phần tử đọc được theo bản chụp, và chỉ đi hỏi thật khi bản chụp nói là hết.
    #[inline]
    fn ready_from(&self, head: u32) -> u32
    {
        let cached = self.tail.with(|cached| unsafe { *cached });
        let ready = cached.wrapping_sub(head);

        match ready > 0
        {
            true => ready,
            false => self.refresh(head),
        }
    }

    /// Lấy một phần tử. Wait-free: không vòng lặp, không CAS, không cấp phát.
    ///
    /// Đây là hàm mà callback audio gọi, nên nó không được phép làm gì ngoài mấy dòng dưới đây.
    pub fn pop(&mut self) -> Option<T>
    {
        let head = self.ring.head.load(Ordering::Relaxed);

        if self.ready_from(head) == 0
        {
            return None;
        }

        // Safety: `tail` đã công bố ô này, và người ghi không đụng lại nó cho tới khi `head` đi qua.
        let val = unsafe { self.ring.slots.read(head) };
        // `Release`: người ghi thấy `head` mới thì ô này đã đọc xong, ghi đè được.
        self.ring.head.store(head.wrapping_add(1), Ordering::Release);
        Some(val)
    }

    /// Lấy tối đa `max` phần tử, đưa từng cái cho `sink`, và trả `head` **một lần** ở cuối.
    ///
    /// Một lần trả cho cả cụm thay vì mỗi phần tử một lần: đúng thứ một callback audio muốn khi nó
    /// vét hết hàng lệnh vào đầu mỗi lần được gọi.
    pub fn pop_batch_with(&mut self, max: usize, mut sink: impl FnMut(T)) -> usize
    {
        if max == 0
        {
            return 0;
        }

        let head = self.ring.head.load(Ordering::Relaxed);
        let n = self.ready_from(head).min(max.min(u32::MAX as usize) as u32);
        if n == 0
        {
            return 0;
        }

        for offset in 0..n
        {
            sink(unsafe { self.ring.slots.read(head.wrapping_add(offset)) });
        }
        self.ring.head.store(head.wrapping_add(n), Ordering::Release);
        n as usize
    }

    /// [`Self::pop_batch_with`] đổ vào một `Vec`. Không dành cho callback audio: `Vec` có thể cấp
    /// phát, mà cấp phát trong callback là thứ cả cấu trúc này sinh ra để tránh.
    #[inline]
    pub fn pop_batch(&mut self, out: &mut Vec<T>, max: usize) -> usize
    {
        out.reserve(max.min(self.ring.len()));
        self.pop_batch_with(max, |val| out.push(val))
    }

    /// Vét sạch những gì đang có.
    #[inline]
    pub fn drain_with(&mut self, sink: impl FnMut(T)) -> usize
    {
        self.pop_batch_with(u32::MAX as usize, sink)
    }

    /// Số phần tử đang chờ. Ảnh chụp.
    #[inline]
    pub fn len(&self) -> usize
    {
        self.ring.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.ring.is_empty()
    }
}

impl<T> Drop for RingBufferSpsc<T>
{
    fn drop(&mut self)
    {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);

        for offset in 0..tail.wrapping_sub(head)
        {
            unsafe { self.slots.drop_at(head.wrapping_add(offset)) };
        }
    }
}

impl<T> std::fmt::Debug for RingBufferSpsc<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("RingBufferSpsc")
            .field("capacity", &self.capacity())
            .field("head", &self.head.load(Ordering::Relaxed))
            .field("tail", &self.tail.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
