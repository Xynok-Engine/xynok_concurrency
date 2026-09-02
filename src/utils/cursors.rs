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
#[cfg(all(test, not(loom)))]
mod test
{
    use super::*;

    fn cursor(capacity: u32, tail: u32, stolen: u32, blocked: u32) -> CursorData
    {
        CursorData {
            stolen:   stolen,
            blocked:  blocked,
            tail:     tail,
            capacity: capacity,
            mask:     capacity - 1,
        }
    }

    #[test]
    fn t0_dem_dung_so_o_con_trong()
    {
        assert_eq!(cursor(8, 6, 1, 0).empty_slots(), 3);
        assert_eq!(cursor(8, 6, 5, 0).empty_slots(), 7);
        assert_eq!(cursor(8, 8, 8, 0).empty_slots(), 8);
    }

    #[test]
    fn t1_o_con_trong_van_dung_khi_con_tro_da_quan_vong()
    {
        assert_eq!(cursor(8, 12, 12, 0).empty_slots(), 8);
        assert_eq!(cursor(8, 12, 5, 0).empty_slots(), 1);
    }

    #[test]
    fn t2_dem_dung_so_o_da_co_hang()
    {
        assert_eq!(cursor(8, 6, 0, 2).filled_slots(), 4);
        assert_eq!(cursor(8, 6, 0, 6).filled_slots(), 0);
        // Tail quấn qua 0 mà blocked chưa quấn: hiệu vẫn phải ra đúng.
        assert_eq!(cursor(8, 1, 0, u32::MAX).filled_slots(), 2);
    }
}
