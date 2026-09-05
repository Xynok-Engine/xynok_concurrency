use std::marker::PhantomData;

use crate::utils::inline_fn::fn_buffer::FnBuffer;
use crate::utils::inline_fn::v_table::VTable;

pub struct UnboundVTable<F>(PhantomData<F>);

impl<F: FnOnce() + Send> UnboundVTable<F>
{
    pub const INLINE: &'static VTable = &VTable {
        runner:  Self::run_inline,
        dropper: Self::drop_inline,
    };
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
