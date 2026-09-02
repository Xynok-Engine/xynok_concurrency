use crate::bump::Bump;

#[test]
fn t0_suc_chua_lam_tron_len_boi_cua_64()
{
    assert_eq!(Bump::with_capacity(1).capacity(), 64);
    assert_eq!(Bump::with_capacity(64).capacity(), 64);
    assert_eq!(Bump::with_capacity(65).capacity(), 128);
}

#[test]
fn t1_hai_lan_cap_phat_cung_song_va_khong_de_len_nhau()
{
    let arena = Bump::with_capacity(1 << 10);

    let first = arena.alloc(7u32).expect("arena mới mà đã hết chỗ");
    let second = arena.alloc(9u32).expect("arena mới mà đã hết chỗ");

    *first += 1;
    *second += 1;

    assert_eq!(*first, 8);
    assert_eq!(*second, 10);
}

#[test]
fn t2_lat_duoc_ghi_day_truoc_khi_phat_ra()
{
    let arena = Bump::with_capacity(1 << 12);
    let slice = arena.alloc_slice(128, 3u16).expect("arena hết chỗ");

    assert_eq!(slice.len(), 128);
    assert!(slice.iter().all(|value| *value == 3));

    slice[10] = 99;
    assert_eq!(slice[10], 99);
}

#[test]
fn t3_cap_phat_giu_dung_canh_le()
{
    let arena = Bump::with_capacity(1 << 10);

    let _byte = arena.alloc(1u8).unwrap();
    let wide = arena.alloc(1u64).unwrap();

    assert_eq!((wide as *mut u64 as usize) % align_of::<u64>(), 0, "u64 bị đặt lệch canh lề");
}

#[test]
fn t4_het_cho_thi_tra_none_chu_khong_no_ra()
{
    let arena = Bump::with_capacity(64);
    assert!(arena.alloc_slice(64, 0u8).is_some());
    assert!(arena.alloc(0u8).is_none(), "arena đầy mà vẫn phát tiếp");
    assert_eq!(arena.remaining(), 0);
}

#[test]
fn t5_reset_tra_lai_toan_bo_cho()
{
    let mut arena = Bump::with_capacity(256);
    let _ = arena.alloc_slice(200, 0u8).unwrap();
    assert!(arena.used() >= 200);

    arena.reset();
    assert_eq!(arena.used(), 0);
    assert!(arena.alloc_slice(200, 0u8).is_some(), "reset rồi mà vẫn không cấp lại được");
}

#[test]
#[should_panic(expected = "canh lề")]
fn t6_canh_le_qua_rong_thi_panic()
{
    #[repr(align(128))]
    #[derive(Clone, Copy)]
    struct TooWide(#[allow(dead_code)] u8);

    let arena = Bump::with_capacity(1 << 12);
    let _ = arena.alloc(TooWide(0));
}
