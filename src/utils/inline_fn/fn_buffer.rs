use std::mem::MaybeUninit;

use crate::utils::inline_fn::consts::INLINE_BYTES;

/// Vùng byte thô nơi closure nằm, canh lề 16 để chứa được hầu hết mọi kiểu thông thường.
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
