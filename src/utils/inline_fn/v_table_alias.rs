use std::marker::PhantomData;

use crate::utils::inline_fn::fn_buffer::FnBuffer;
use crate::utils::inline_fn::runnable::Runnable;
use crate::utils::inline_fn::v_table::VTable;

/// Chỗ treo bảng hàm tĩnh cho từng kiểu closure của đường thường.
///
/// Bản thân nó không chứa gì, chỉ mượn hệ thống kiểu để mỗi `F` có một bảng riêng nằm sẵn trong
/// binary, khỏi dựng lúc chạy.
pub struct VTableAlias<F>(PhantomData<F>);

impl<F: Runnable> VTableAlias<F>
{
    /// Bảng cho closure nằm thẳng trong buffer.
    pub const INLINE: &'static VTable = &VTable {
        runner:  Self::run_inline,
        dropper: Self::drop_inline,
    };
    /// Bảng cho closure phải đi đường heap vì không lọt vào buffer.
    pub const BOXED: &'static VTable = &VTable {
        runner:  Self::run_boxed,
        dropper: Self::drop_boxed,
    };

    fn run_inline(f_box: *mut FnBuffer)
    {
        let f = unsafe { f_box.cast::<F>().read() };
        f();
    }
    fn drop_inline(f_box: *mut FnBuffer)
    {
        unsafe {
            f_box.cast::<F>().drop_in_place();
        }
    }
    fn run_boxed(f_box: *mut FnBuffer)
    {
        let f = unsafe { f_box.cast::<Box<F>>().read() };
        f();
    }
    fn drop_boxed(f_box: *mut FnBuffer)
    {
        unsafe {
            f_box.cast::<Box<F>>().drop_in_place();
        }
    }
}
