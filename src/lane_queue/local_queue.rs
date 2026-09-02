/// Ring local nhìn từ phía [`LaneQueue`].
///
/// Hai ring của crate ([`ring_buffer_fifo`](crate::ring_buffer_fifo) và
/// [`ring_buffer_lifo`](crate::ring_buffer_lifo)) có cùng bốn hàm này với cùng ý nghĩa, nên
/// `LaneQueue` không cần biết mình đang nạp vào loại nào. Pool chọn loại ring, `LaneQueue` chỉ đổ
/// job vào.
pub trait LocalQueue<T>
{
    /// Tổng số ô, cố định từ lúc khởi tạo.
    fn capacity(&self) -> usize;
    /// Số ô còn trống. Chỉ có thể tăng khi không ai khác đang push, và chủ ring là người duy nhất
    /// được push, nên con số này an toàn để chia cụm dựa trên nó.
    fn remaining(&self) -> usize;
    fn push(&mut self, val: T) -> Result<(), T>;
    /// Ghi cả cụm rồi publish **một lần**. Đây là lý do `LaneQueue` không cần `Vec` trung gian.
    fn push_iter<I: IntoIterator<Item = T>>(&mut self, vals: I) -> usize;
}

macro_rules! impl_local_queue {
    ($ring:path) => {
        impl<T> LocalQueue<T> for $ring
        {
            #[inline]
            fn capacity(&self) -> usize
            {
                Self::capacity(self)
            }
            #[inline]
            fn remaining(&self) -> usize
            {
                Self::remaining(self)
            }
            #[inline]
            fn push(&mut self, val: T) -> Result<(), T>
            {
                Self::push(self, val)
            }
            #[inline]
            fn push_iter<I: IntoIterator<Item = T>>(&mut self, vals: I) -> usize
            {
                Self::push_iter(self, vals)
            }
        }
    };
}

impl_local_queue!(crate::ring_buffer_fifo::Producer<'_, T>);
impl_local_queue!(crate::ring_buffer_lifo::Producer<'_, T>);
