//! src: https://doc.rust-lang.org/std/task/struct.RawWakerVTable.html
use std::mem::ManuallyDrop;

use crate::sync::cell::UnsafeCell;
use crate::utils::inline_fn::consts::INLINE_BYTES;
use crate::utils::inline_fn::fn_buffer::FnBuffer;
use crate::utils::inline_fn::runnable::Runnable;
use crate::utils::inline_fn::unbound_v_table::UnboundVTable;
use crate::utils::inline_fn::v_table::VTable;
use crate::utils::inline_fn::v_table_alias::VTableAlias;

/// Một việc đã được gói lại, kích thước cố định vừa một dòng cache.
///
/// Closure đủ nhỏ thì nằm thẳng bên trong, khỏi cấp phát trên heap. Closure to hơn thì tự động lùi
/// về đường heap, và chỗ gọi không cần phân biệt hai đường đó.
#[repr(C)]
pub struct InlineFn
{
    vtable:    &'static VTable,
    fn_buffer: UnsafeCell<FnBuffer>,
}

unsafe impl Send for InlineFn {}

impl InlineFn
{
    /// Hỏi trước xem closure kiểu `F` có nằm gọn trong buffer hay phải đi đường heap.
    pub const fn is_fit<F>() -> bool
    {
        size_of::<F>() <= INLINE_BYTES && align_of::<F>() <= align_of::<FnBuffer>()
    }

    #[inline]
    pub fn new<F: Runnable>(f: F) -> Self
    {
        let mut f_box = FnBuffer::new();

        let v_table = unsafe {
            match Self::is_fit::<F>()
            {
                true =>
                {
                    f_box.as_mut_ptr().cast::<F>().write(f);
                    VTableAlias::<F>::INLINE
                }
                false =>
                {
                    f_box.as_mut_ptr().cast::<Box<F>>().write(Box::new(f));
                    VTableAlias::<F>::BOXED
                }
            }
        };
        Self {
            vtable:    v_table,
            fn_buffer: UnsafeCell::new(f_box),
        }
    }

    /// Như [`Self::new`] nhưng nhận cả closure **không** `'static`.
    ///
    /// Dành cho [`Scope`](crate::scope::Scope): job của một scope mượn stack của người mở scope,
    /// nên nó không thể `'static`, mà không có hàm này thì đường duy nhất là bọc closure vào một
    /// `Box<dyn FnOnce() + Send + 'scope>` rồi xoá lifetime của cái box đó. Cách ấy đúng, và tốn
    /// một lần cấp phát cho mỗi job spawn trong scope, tức là đúng cái mà [`InlineFn`] sinh ra để
    /// tránh.
    ///
    /// # Safety
    ///
    /// Người gọi phải bảo đảm job này chạy xong **trước khi** bất cứ thứ gì closure mượn bị thả.
    /// `Scope` bảo đảm điều đó bằng cách không trả về cho tới khi mọi job của nó báo xong, kể cả
    /// khi đang unwind vì panic. Không có bảo đảm đó thì đây là một use-after-free đợi sẵn.
    #[inline]
    pub unsafe fn new_unbound<F>(f: F) -> Self
    where F: FnOnce() + Send
    {
        let mut f_box = FnBuffer::new();

        let v_table = unsafe {
            match Self::is_fit::<F>()
            {
                true =>
                {
                    f_box.as_mut_ptr().cast::<F>().write(f);
                    UnboundVTable::<F>::INLINE
                }
                false =>
                {
                    f_box.as_mut_ptr().cast::<Box<F>>().write(Box::new(f));
                    UnboundVTable::<F>::BOXED
                }
            }
        };
        Self {
            vtable:    v_table,
            fn_buffer: UnsafeCell::new(f_box),
        }
    }

    /// Chạy việc và tiêu luôn job, nên không có đường chạy hai lần.
    #[inline]
    pub fn run_once(self)
    {
        let this = ManuallyDrop::new(self);
        (this.vtable.runner)(this.fn_buffer.with_mut(|p| p))
    }
}

impl Drop for InlineFn
{
    fn drop(&mut self)
    {
        (self.vtable.dropper)(self.fn_buffer.with_mut(|p| p));
    }
}

impl std::fmt::Debug for InlineFn
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("InlineFn").finish_non_exhaustive()
    }
}
