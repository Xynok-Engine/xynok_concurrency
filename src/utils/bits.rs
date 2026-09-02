/// Nhét hai số 32 bit vào chung một từ nhớ 64 bit.
///
/// `a` nằm ở nửa cao, `b` nằm ở nửa thấp. Ghép lại như vậy để hai con trỏ luôn được đọc và ghi
/// trong cùng một thao tác nguyên tử, khỏi phải lo hai lần load lệch nhau.
#[inline]
pub const fn pack(a: u32, b: u32) -> u64
{
    (a as u64) << 32 | (b as u64)
}

/// Tách lại cặp số mà [`pack`] đã ghép, theo đúng thứ tự `(a, b)`.
#[inline]
pub const fn unpack(val: u64) -> (u32, u32)
{
    ((val >> 32) as u32, val as u32)
}

#[cfg(all(test, not(loom)))]
mod test
{
    use super::*;

    #[test]
    fn t0_ghep_roi_tach_ra_thi_duoc_cap_so_ban_dau()
    {
        let a: u32 = 23;
        let b: u32 = 1;
        let (left, right) = unpack(pack(a, b));
        assert_eq!(left, a);
        assert_eq!(right, b);
    }

    #[test]
    fn t1_gia_tri_bien_van_giu_nguyen()
    {
        for (a, b) in [(0, 0), (u32::MAX, 0), (0, u32::MAX), (u32::MAX, u32::MAX)]
        {
            assert_eq!(unpack(pack(a, b)), (a, b));
        }
    }

    #[test]
    fn t2_nua_cao_va_nua_thap_khong_lan_sang_nhau()
    {
        assert_eq!(pack(1, 0), 1 << 32);
        assert_eq!(pack(0, 1), 1);
    }
}
