use crate::ring_buffer_spsc::ring_buffer_spsc::RingBufferSpsc;
use crate::sync::Ordering;
use crate::sync::cell::UnsafeCell;

/// Đầu đọc. Chỉ một, và cũng không `Sync`.
pub struct Receiver<'a, T>
{
    pub(crate) ring: &'a RingBufferSpsc<T>,
    /// Bản chụp `tail`, chỉ người đọc đụng tới.
    pub(crate) tail: UnsafeCell<u32>,
}

/// Người đọc cũng vậy: gửi đi được, dùng chung thì không.
unsafe impl<T: Send> Send for Receiver<'_, T> {}

impl<'a, T> Receiver<'a, T>
{
    #[inline]
    pub(crate) fn new(ring: &'a RingBufferSpsc<T>) -> Self
    {
        Self {
            ring: ring,
            tail: UnsafeCell::new(ring.tail.load(Ordering::Acquire)),
        }
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
