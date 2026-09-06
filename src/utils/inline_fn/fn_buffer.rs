use std::mem::MaybeUninit;

use crate::utils::inline_fn::inline_fn::INLINE_BYTES;

#[repr(C, align(16))]
pub struct FnBuffer
{
    buffer: [MaybeUninit<u8>; INLINE_BYTES],
}

impl FnBuffer
{
    #[inline]
    pub fn new() -> Self
    {
        Self {
            buffer: [MaybeUninit::uninit(); INLINE_BYTES],
        }
    }

    #[inline]
    pub(super) fn as_mut_ptr(&mut self) -> *mut u8
    {
        self.buffer.as_mut_ptr().cast()
    }
}

impl Default for FnBuffer
{
    fn default() -> Self
    {
        Self::new()
    }
}
