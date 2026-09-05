//! Hai chỗ chứa job riêng của một người tham gia: ô LIFO và ring local.

use crate::custom_type::Job;
use crate::ring_buffer_fifo::RingBufferFifo;
use crate::sync::cell::UnsafeCell;

/// Hai chỗ chứa job của một người tham gia pool.
pub(super) struct Local
{
    /// Job vừa được spawn ra, giữ nguyên ở đây thay vì đẩy xuống ring.
    ///
    /// Chỉ chủ của ô này chạm vào: thread đang mang đúng `index` đó, và chỉ khi nó đang ở trong
    /// vòng chạy job. Không ai trộm được ô LIFO, nên nó phải được vét sạch trước khi chủ rời vòng,
    /// nếu không job nằm đó không ai chạy.
    pub(super) lifo: UnsafeCell<Option<Job>>,
    /// Việc của người này, ai cũng trộm được.
    pub(super) ring: RingBufferFifo<Job>,
}

/// An toàn vì bất biến ở [`Local::lifo`]: đúng một thread chạm vào ô đó, và nó chạm khi đang giữ
/// `index` tương ứng. `ring` thì tự nó đã `Sync`.
unsafe impl Sync for Local {}

impl Local
{
    /// Lấy job trong ô LIFO ra, nếu có.
    ///
    /// # Safety
    ///
    /// Chỉ được gọi từ thread đang mang `index` của chính `Local` này.
    #[inline]
    pub(super) unsafe fn take_lifo(&self) -> Option<Job>
    {
        self.lifo.with_mut(|slot| unsafe { (*slot).take() })
    }

    /// Đặt job vào ô LIFO, trả lại job cũ đang nằm đó để người gọi đẩy xuống ring.
    ///
    /// # Safety
    ///
    /// Như [`Self::take_lifo`].
    #[inline]
    pub(super) unsafe fn swap_lifo(&self, job: Job) -> Option<Job>
    {
        self.lifo.with_mut(|slot| unsafe { (*slot).replace(job) })
    }
}
