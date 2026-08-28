/// Represents a snapshot of the Ring Buffer indices, loaded atomically to ensure consistent CPU-side ordering.
///
/// When working with a Ring Buffer, we often need to synchronize the loading of index values during a single call. This ensures that the CPU maintains a consistent and sequential order of operations.
///
/// ### The Problem
/// In practice, checking indices or retrieving values requires repetitive code. Because these index operations are scattered across various methods, the implementation quickly becomes verbose and difficult to maintain.
///
/// To address this, I introduced a dedicated `Cursors` struct. By loading all necessary index information at once, we eliminate the need for repeated, fragmented lookups.
#[derive(Clone, Copy)]
pub struct Cursors
{
    pub stolen:      u32,
    pub in_progress: u32,
    pub tail:        u32,
}

impl Cursors
{
    #[inline]
    pub fn increse_tail(&self, val: u32) -> u32
    {
        self.tail.wrapping_add(val)
    }
    #[inline]
    pub fn available_slots(&self) -> usize
    {
        (self.tail - self.stolen) as usize
    }
}
