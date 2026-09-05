use crate::ring_buffer_spsc::ring_buffer_spsc::RingBufferSpsc;
use crate::sync::Ordering;
use crate::sync::cell::UnsafeCell;

/// Đầu ghi. Chỉ một, và không `Sync`: hai thread cùng ghi là phá vỡ toàn bộ lập luận ở trên.
pub struct Sender<'a, T>
{
    pub(crate) ring: &'a RingBufferSpsc<T>,
    /// Bản chụp `head`, chỉ người ghi đụng tới.
    pub(crate) head: UnsafeCell<u32>,
}

/// Người ghi được gửi sang thread khác, nhưng không được dùng từ hai thread cùng lúc.
unsafe impl<T: Send> Send for Sender<'_, T> {}

impl<'a, T> Sender<'a, T>
{
    #[inline]
    pub(crate) fn new(ring: &'a RingBufferSpsc<T>) -> Self
    {
        Self {
            ring: ring,
            head: UnsafeCell::new(ring.head.load(Ordering::Acquire)),
        }
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
