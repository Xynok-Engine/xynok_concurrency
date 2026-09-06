use crate::utils::inline_fn::fn_buffer::FnBuffer;

pub struct VTable
{
    pub runner:  fn(*mut FnBuffer),
    pub dropper: fn(*mut FnBuffer),
}
