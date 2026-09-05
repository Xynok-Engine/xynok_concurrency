use crate::pool::sleep::consts::{COUNT_MASK, SLEEPER_BITS};

/// Tách ô `state` thành `(events, searching, unparked)`.
#[inline]
pub(crate) const fn split(state: u64) -> (u32, u32, u32)
{
    (
        (state >> (2 * SLEEPER_BITS)) as u32,
        ((state >> SLEEPER_BITS) & COUNT_MASK) as u32,
        (state & COUNT_MASK) as u32,
    )
}
