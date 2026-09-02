use std::collections::VecDeque;

use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicBool, Ordering};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedRingBuffer;
use crate::utils::queue_batching::queue_batching_guard::QueueBatchingGuard;

/// Hàng đợi vào trước ra trước cho nhiều người đẩy và nhiều người rút, sức chứa tự nới.
///
/// Mọi thao tác đều có bản gom lô đi kèm, để chi phí chiếm quyền được chia cho cả cụm phần tử thay
/// vì trả lại từ đầu cho từng phần tử một.
pub struct QueueBatching<T>
{
    pub(super) elements: UnsafeCell<VecDeque<T>>,
    pub(super) locked:   CachePadded<AtomicBool>,
}

unsafe impl<T: Send> Send for QueueBatching<T> {}
unsafe impl<T: Send> Sync for QueueBatching<T> {}

impl<T> QueueBatching<T>
{
    pub fn new() -> Self
    {
        Self::from_queue(VecDeque::new())
    }

    pub fn with_capacity(capacity: usize) -> Self
    {
        Self::from_queue(VecDeque::with_capacity(capacity))
    }

    fn from_queue(queue: VecDeque<T>) -> Self
    {
        Self {
            elements: UnsafeCell::new(queue),
            locked:   CachePadded::new(AtomicBool::new(false)),
        }
    }
}

impl<T> QueueBatching<T>
{
    /// Chờ tới khi mượn được hàng đợi bên trong.
    #[inline]
    pub fn get(&self) -> QueueBatchingGuard<'_, T>
    {
        if self.try_lock_weak()
        {
            return QueueBatchingGuard { queue_batching: self };
        }
        self.cas_get()
    }

    /// Thử mượn đúng một lần, đang có người giữ thì trả về `None` chứ không chờ.
    #[inline]
    pub fn try_get(&self) -> Option<QueueBatchingGuard<'_, T>>
    {
        match self.try_lock()
        {
            true => Some(QueueBatchingGuard { queue_batching: self }),
            false => None,
        }
    }

    /// Mượn thẳng hàng đợi khi đã cầm tham chiếu độc quyền, khỏi cần đụng tới cờ.
    #[inline]
    pub fn get_mut(&mut self) -> &mut VecDeque<T>
    {
        self.elements.with_mut(|p| unsafe { &mut *p })
    }

    /// Tháo vỏ ra, lấy lại hàng đợi bên trong.
    pub fn take(self) -> VecDeque<T>
    {
        let this = std::mem::ManuallyDrop::new(self);
        this.elements.with_mut(|p| unsafe { p.read() })
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn push(&self, val: T)
    {
        self.get().push_back(val);
    }

    /// Đẩy cả cụm phần tử vào trong một lần chiếm quyền.
    #[inline]
    pub fn push_batch<I>(&self, values: I)
    where I: IntoIterator<Item = T>
    {
        let mut elements = self.get();
        elements.extend(values);
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn pop(&self) -> Option<T>
    {
        self.get().pop_front()
    }

    /// Rút tối đa `limit` phần tử, nối thêm vào `out`, trả về số phần tử thật sự lấy được.
    pub fn pop_batch(&self, out: &mut Vec<T>, limit: usize) -> usize
    {
        if limit == 0
        {
            return 0;
        }

        let mut elements = self.get();
        let taken = limit.min(elements.len());
        out.extend(elements.drain(..taken));
        taken
    }

    #[inline]
    pub fn drain_into(&self, out: &mut Vec<T>) -> usize
    {
        self.pop_batch(out, usize::MAX)
    }

    /// Rút tối đa `max` phần tử và ghi thẳng vào `dst`, bắt đầu từ ô `write_start_cursor`.
    ///
    /// Trả về số phần tử thật sự chuyển được, có thể ít hơn `max` nếu hàng đợi cạn trước.
    #[inline]
    pub fn drain_into_buffer(&self, max: usize, dst: &FixedRingBuffer<T>, write_start_cursor: u32) -> usize
    {
        if max == 0
        {
            return 0;
        }
        debug_assert!(
            max <= dst.capacity(),
            "`{}` chỉ có `{}` ô, không chứa nổi `{}` phần tử",
            std::any::type_name::<FixedRingBuffer<T>>(),
            dst.capacity(),
            max
        );

        let mut elements = self.get();
        let moved = max.min(elements.len());
        for offset in 0..moved
        {
            let val = match elements.pop_front()
            {
                Some(val) => val,
                None => return offset,
            };
            unsafe {
                dst.write(write_start_cursor.wrapping_add(offset as u32), val);
            }
        }
        moved
    }

    pub fn clear(&self) -> usize
    {
        let mut elements = self.get();
        let cleared = elements.len();
        elements.clear();
        cleared
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn len(&self) -> usize
    {
        self.get().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.get().is_empty()
    }

    #[inline]
    pub fn is_locked(&self) -> bool
    {
        self.locked.load(Ordering::Relaxed)
    }
}

impl<T> QueueBatching<T>
{
    #[cold]
    fn cas_get(&self) -> QueueBatchingGuard<'_, T>
    {
        let mut backoff = Backoff::new();
        loop
        {
            if !self.is_locked() && self.try_lock_weak()
            {
                return QueueBatchingGuard { queue_batching: self };
            }
            backoff.snooze();
        }
    }

    #[inline]
    fn try_lock(&self) -> bool
    {
        self.locked.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }

    #[inline]
    fn try_lock_weak(&self) -> bool
    {
        self.locked.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }

    #[inline]
    pub(super) fn unlock(&self)
    {
        self.locked.store(false, Ordering::Release);
    }
}

impl<T> Default for QueueBatching<T>
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl<T> From<VecDeque<T>> for QueueBatching<T>
{
    fn from(value: VecDeque<T>) -> Self
    {
        Self::from_queue(value)
    }
}

impl<T> FromIterator<T> for QueueBatching<T>
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self
    {
        Self::from_queue(VecDeque::from_iter(iter))
    }
}

impl<T> Extend<T> for QueueBatching<T>
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I)
    {
        self.get_mut().extend(iter);
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for QueueBatching<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        let mut builder = f.debug_struct("QueueBatching");
        match self.try_get()
        {
            Some(elements) => builder.field("elements", &*elements),
            None => builder.field("elements", &format_args!("<locked>")),
        }
        .finish()
    }
}
