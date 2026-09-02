/// Represents a snapshot of the [`FixedBuffer`] indices, loaded atomically to ensure consistent CPU-side ordering.
///
/// When working with a [`FixedBuffer`], we often need to synchronize the loading of index values during a single call. This ensures that the CPU maintains a consistent and sequential order of operations.
///
/// ### The Problem
/// In practice, checking indices or retrieving values requires repetitive code. Because these index operations are scattered across various methods, the implementation quickly becomes verbose and difficult to maintain.
///
/// To address this, I introduced a dedicated struct. By loading all necessary index information at once, we eliminate the need for repeated, fragmented lookups.
#[derive(Clone, Copy)]
pub struct CursorData
{
    pub stolen:   u32,
    pub blocked:  u32,
    pub tail:     u32,
    pub capacity: u32,
    pub mask:     u32,
}

impl CursorData
{
    /// the number of guaranteed empty slots
    #[inline]
    pub fn empty_slots(&self) -> usize
    {
        self.capacity.wrapping_sub(self.tail).wrapping_add(self.stolen) as usize
    }

    /// the number of guaranteed filled slots
    #[inline]
    pub fn filled_slots(&self) -> usize
    {
        self.tail.wrapping_sub(self.blocked) as usize
    }
}
#[cfg(test)]
mod test
{
    fn fn_available_slots(capacity: u32, tail: u32, stolen: u32, result: u32)
    {
        let available = capacity.wrapping_sub(tail).wrapping_add(stolen);
        //let available = available & (capacity - 1);
        assert!(available == result, "{} != {}", available, result);
    }

    #[test]
    fn test_available_slots()
    {
        fn_available_slots(8, 6, 1, 3);
        fn_available_slots(8, 6, 5, 7);
        fn_available_slots(8, 8, 8, 8);
        fn_available_slots(8, 12, 12, 8);
        fn_available_slots(8, 12, 5, 1);
    }
}
