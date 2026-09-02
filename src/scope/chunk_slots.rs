use crate::sync::cell::UnsafeCell;

/// Cho một lát các ô kết quả theo lô đi được vào thread worker.
///
/// Ghi qua [`Self::set`] chứ không qua trường, vì một closure chạm thẳng `slots.0` sẽ bắt lấy cái
/// lát trần, mà lát trần thì không `Sync`.
pub struct ChunkSlots<'a, T>(pub(crate) &'a [UnsafeCell<Option<T>>]);

/// An toàn vì mỗi ô chỉ có đúng một thread chạm vào: thread nào giành được chỉ số lô đó từ bộ đếm.
unsafe impl<T: Send> Sync for ChunkSlots<'_, T> {}

impl<T> ChunkSlots<'_, T>
{
    /// # Safety
    ///
    /// Không thread nào khác được cầm cùng `chunk` này.
    #[inline]
    pub unsafe fn set(&self, chunk: usize, value: T)
    {
        self.0[chunk].with_mut(|slot| unsafe { *slot = Some(value) });
    }
}
