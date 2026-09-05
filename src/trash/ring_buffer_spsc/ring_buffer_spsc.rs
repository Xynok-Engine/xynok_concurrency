use crate::ring_buffer_spsc::consts::MAX_SLOTS;
use crate::ring_buffer_spsc::receiver::Receiver;
use crate::ring_buffer_spsc::sender::Sender;
use crate::sync::{AtomicU32, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::slots::Slots;

pub struct RingBufferSpsc<T>
{
    /// Ô kế tiếp người đọc sẽ lấy. Chỉ người đọc ghi.
    pub(crate) head:  CachePadded<AtomicU32>,
    /// Ô kế tiếp người ghi sẽ ghi. Chỉ người ghi ghi.
    pub(crate) tail:  CachePadded<AtomicU32>,
    pub(crate) slots: Slots<T>,
}

unsafe impl<T: Send> Send for RingBufferSpsc<T> {}
unsafe impl<T: Send> Sync for RingBufferSpsc<T> {}

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
        (Sender::new(this), Receiver::new(this))
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
        Sender::new(self)
    }

    /// Đầu đọc. Cùng điều kiện với [`Self::sender`].
    ///
    /// # Safety
    ///
    /// Chỉ được tồn tại đúng một `Receiver` tại một thời điểm, và nó chỉ được dùng từ một thread.
    #[inline]
    pub unsafe fn receiver(&self) -> Receiver<'_, T>
    {
        Receiver::new(self)
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
