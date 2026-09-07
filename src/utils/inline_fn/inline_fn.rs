use crate::sync::UnsafeCell;
use crate::utils::inline_fn::fn_buffer::FnBuffer;
use crate::utils::inline_fn::runnable::Runnable;
use crate::utils::inline_fn::v_table::VTable;
use crate::utils::inline_fn::unbound_v_table::UnboundVTable;
use std::mem::ManuallyDrop;

/// src: https://doc.rust-lang.org/std/task/struct.RawWakerVTable.html
#[repr(C)]
pub struct InlineFn
{
    vtable:    &'static VTable,
    fn_buffer: UnsafeCell<FnBuffer>,
}
pub const INLINE_BYTES: usize = 48;

unsafe impl Send for InlineFn {}

impl InlineFn
{
    pub const fn is_fit<F>() -> bool
    {
        size_of::<F>() <= INLINE_BYTES && align_of::<F>() <= align_of::<FnBuffer>()
    }

    #[inline]
    pub fn new<F: Runnable>(f: F) -> Self
    {
        // SAFETY: Runnable requires 'static, so the closure cannot outlive its borrows.
        unsafe { Self::new_scoped(f) }
    }

    /// # Safety
    /// The job must be run or dropped before anything captured by reference expires.
    #[inline]
    pub(crate) unsafe fn new_scoped<F: FnOnce() + Send>(f: F) -> Self
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
