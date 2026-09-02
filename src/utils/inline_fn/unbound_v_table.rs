use std::marker::PhantomData;

use crate::utils::inline_fn::fn_buffer::FnBuffer;
use crate::utils::inline_fn::v_table::VTable;

/// Bản sao của [`VTableAlias`](crate::utils::inline_fn::v_table_alias::VTableAlias) cho closure có
/// mượn dữ liệu bên ngoài.
///
/// Phải là một kiểu riêng vì bảng của đường thường đòi closure không mượn gì cả. Bốn hàm bên dưới
/// không hề chạm tới thời hạn của `F`: chúng chỉ đọc, gọi, và thả một giá trị nằm trong buffer, nên
/// chúng hợp lệ với `F` mượn dữ liệu có thời hạn bất kỳ.
///
/// Cái phải bảo đảm bằng tay là job chạy xong trước khi thứ nó mượn biến mất, và đó là hợp đồng của
/// [`InlineFn::new_unbound`](crate::utils::inline_fn::InlineFn::new_unbound).
pub struct UnboundVTable<F>(PhantomData<F>);

impl<F: FnOnce() + Send> UnboundVTable<F>
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
