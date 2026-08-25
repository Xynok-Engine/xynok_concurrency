use super::*;
use crate::sync::{Arc, AtomicUsize};

fn drain(ring: &RingBufferFifo<u32>, start: u32, n: u32) -> Vec<u32>
{
    let mut out = Vec::new();
    unsafe { ring.drain_claimed_with(start, n, |val| out.push(val)) };
    out
}

#[test]
fn test_wrapping_sub()
{
    let a: u32 = 0;
    let b: u32 = u32::MAX;
    assert!(a.wrapping_sub(b) == 1);
}

#[test]
fn test_mask()
{
    let capacity: u32 = 8;
    assert!(capacity.is_power_of_two());
    let mask = capacity - 1;
    for i in 0..1024
    {
        assert!(i & mask == i % capacity);
    }
}

#[test]
fn suc_chua_lam_tron_len_luy_thua_hai()
{
    assert_eq!(RingBufferFifo::<u8>::new(0).capacity(), 2);
    assert_eq!(RingBufferFifo::<u8>::new(1).capacity(), 2);
    assert_eq!(RingBufferFifo::<u8>::new(2).capacity(), 2);
    assert_eq!(RingBufferFifo::<u8>::new(3).capacity(), 4);
    assert_eq!(RingBufferFifo::<u8>::new(256).capacity(), 256);
    assert_eq!(RingBufferFifo::<u8>::new(257).capacity(), 512);
}

#[test]
#[should_panic(expected = "2^31")]
fn suc_chua_vuot_tran_thi_panic()
{
    let _ = RingBufferFifo::<u8>::new(MAX_SLOTS + 1);
}

#[test]
fn ring_buffer_rong_thi_moi_con_so_deu_bang_khong()
{
    let ring = RingBufferFifo::<u8>::new(8);
    assert_eq!(ring.occupied(), 0);
    assert_eq!(ring.available(), 0);
    assert!(ring.is_empty());
}

#[test]
fn occupied_la_hieu_hai_dau_ke_ca_khi_tran_so()
{
    let ring = RingBufferFifo::<u8>::new(8);

    ring.head.store(pack(u32::MAX - 1, u32::MAX - 1), Ordering::Relaxed);
    ring.tail.store(2, Ordering::Relaxed);
    assert_eq!(ring.occupied(), 4);

    ring.head.store(pack(0, 3), Ordering::Relaxed);
    ring.tail.store(5, Ordering::Relaxed);
    assert_eq!(ring.occupied(), 5);
    assert_eq!(ring.available(), 2);
}

#[test]
fn occupied_va_is_empty_duoc_phep_lech_nhau()
{
    let ring = RingBufferFifo::<u32>::new(4);
    let mut tx = unsafe { ring.producer() };

    let mut vals: Vec<u32> = (0..4).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);

    ring.head.store(pack(0, 4), Ordering::Relaxed);

    assert_eq!(ring.occupied(), 4);
    assert_eq!(tx.remaining(), 0);
    assert!(tx.is_full());
    assert!(ring.is_empty());
    assert_eq!(ring.available(), 0);
    assert_eq!(tx.pop(), None);

    let _ = drain(&ring, 0, 4);
    ring.head.store(pack(4, 4), Ordering::Relaxed);
}

#[test]
fn drop_tha_moi_job_chua_lay()
{
    let alive = Arc::new(AtomicUsize::new(0));
    {
        let mut ring = RingBufferFifo::new(4);
        let (mut tx, _) = ring.split();
        for _ in 0..3
        {
            tx.push(Arc::clone(&alive)).unwrap();
        }
        assert_eq!(Arc::strong_count(&alive), 4);
    }
    assert_eq!(Arc::strong_count(&alive), 1);
}

#[test]
fn drop_di_dung_vong_khi_chi_so_quan_qua_cuoi_mang()
{
    let alive = Arc::new(AtomicUsize::new(0));
    {
        let mut ring = RingBufferFifo::new(4);
        let (mut tx, _) = ring.split();

        for _ in 0..3
        {
            tx.push(Arc::new(AtomicUsize::new(0))).unwrap();
            tx.pop().unwrap();
        }
        for _ in 0..3
        {
            tx.push(Arc::clone(&alive)).unwrap();
        }
        assert_eq!(Arc::strong_count(&alive), 4);
    }
    assert_eq!(Arc::strong_count(&alive), 1);
}

#[test]
fn drop_khong_dung_toi_vung_da_co_chu()
{
    let alive = Arc::new(AtomicUsize::new(0));
    {
        let mut ring = RingBufferFifo::new(8);
        let (mut tx, _) = ring.split();
        for _ in 0..4
        {
            tx.push(Arc::clone(&alive)).unwrap();
        }

        let mut taken = Vec::new();
        unsafe { ring.drain_claimed_with(0, 2, |val| taken.push(val)) };
        ring.head.store(pack(0, 2), Ordering::Relaxed);

        assert_eq!(Arc::strong_count(&alive), 5);
        drop(taken);
        assert_eq!(Arc::strong_count(&alive), 3);
    }
    assert_eq!(Arc::strong_count(&alive), 1);
}

#[test]
fn hai_quyen_co_dung_bo_trait_can_thiet()
{
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<RingBufferFifo<u8>>();
    assert_sync::<RingBufferFifo<u8>>();
    assert_send::<Producer<'_, u8>>();
    assert_send::<Consumer<'_, u8>>();
    assert_sync::<Consumer<'_, u8>>();
}

#[test]
fn push_va_pop_giu_dung_thu_tu_fifo()
{
    let mut ring = RingBufferFifo::new(4);
    let (mut tx, _) = ring.split();

    for i in 0..4u32
    {
        assert_eq!(tx.push(i), Ok(()));
    }
    assert_eq!(tx.occupied(), 4);
    assert_eq!(tx.remaining(), 0);

    for i in 0..4u32
    {
        assert_eq!(tx.pop(), Some(i));
    }
    assert_eq!(tx.pop(), None);
}

#[test]
fn day_thi_tra_lai_job_chu_khong_nuot()
{
    let mut ring = RingBufferFifo::new(2);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push(1u8), Ok(()));
    assert_eq!(tx.push(2u8), Ok(()));
    assert_eq!(tx.push(3u8), Err(3));

    assert_eq!(tx.pop(), Some(1));
    assert_eq!(tx.push(3u8), Ok(()));
}

#[test]
fn chi_so_quan_qua_cuoi_mang_van_dung_thu_tu()
{
    let mut ring = RingBufferFifo::new(4);
    let (mut tx, _) = ring.split();

    for round in 0..10u32
    {
        for i in 0..4u32
        {
            assert_eq!(tx.push(round * 4 + i), Ok(()));
        }
        for i in 0..4u32
        {
            assert_eq!(tx.pop(), Some(round * 4 + i));
        }
    }
}

#[test]
fn push_batch_nhan_phan_vua_va_giu_nguyen_phan_thua()
{
    let mut ring = RingBufferFifo::new(4);
    let (mut tx, _) = ring.split();

    let mut vals: Vec<u32> = (0..10).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);
    assert_eq!(vals, (4..10).collect::<Vec<_>>());

    assert_eq!(tx.push_batch(&mut vals), 0);
    assert_eq!(vals.len(), 6);

    let mut out = Vec::new();
    assert_eq!(tx.pop_batch(&mut out, 2), 2);
    assert_eq!(out, vec![0, 1]);
    assert_eq!(tx.push_batch(&mut vals), 2);
    assert_eq!(vals, vec![6, 7, 8, 9]);
}

#[test]
fn push_iter_khong_can_vec_trung_gian()
{
    let mut ring = RingBufferFifo::new(4);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push_iter(0..10u32), 4);
    assert_eq!(tx.remaining(), 0);
    assert_eq!(tx.push_iter(std::iter::empty::<u32>()), 0);

    let mut out = Vec::new();
    assert_eq!(tx.drain(&mut out), 4);
    assert_eq!(out, vec![0, 1, 2, 3]);
}

#[test]
fn pop_batch_bi_chan_boi_so_job_dang_co()
{
    let mut ring = RingBufferFifo::new(8);
    let (mut tx, _) = ring.split();

    let mut vals: Vec<u32> = (0..5).collect();
    assert_eq!(tx.push_batch(&mut vals), 5);

    let mut out = Vec::new();
    assert_eq!(tx.pop_batch(&mut out, 100), 5);
    assert_eq!(out, vec![0, 1, 2, 3, 4]);
    assert_eq!(tx.pop_batch(&mut out, 100), 0);
    assert_eq!(tx.pop_batch(&mut out, 0), 0);
}

#[test]
fn spill_half_nha_nua_hang_doi_ra_ngoai()
{
    let mut ring = RingBufferFifo::new(8);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push_iter(0..5u32), 5);

    let mut out = Vec::new();
    assert_eq!(tx.spill_half(&mut out), 3);
    assert_eq!(out, vec![0, 1, 2]);
    assert_eq!(tx.available(), 2);
}

#[test]
fn ke_trom_boc_tu_dau_cu_nhat()
{
    let mut ring = RingBufferFifo::new(8);
    let (mut tx, rx) = ring.split();

    let mut vals: Vec<u32> = (0..6).collect();
    assert_eq!(tx.push_batch(&mut vals), 6);

    assert_eq!(rx.steal(), Some(0));

    let mut got = Vec::new();
    assert_eq!(rx.steal_batch(&mut got, 2), 2);
    assert_eq!(got, vec![1, 2]);

    assert_eq!(tx.pop(), Some(3));
}

#[test]
fn steal_half_lay_nua_lam_tron_len()
{
    let mut ring = RingBufferFifo::new(8);
    let (mut tx, rx) = ring.split();

    let mut vals: Vec<u32> = (0..5).collect();
    assert_eq!(tx.push_batch(&mut vals), 5);

    let mut got = Vec::new();
    assert_eq!(rx.steal_half(&mut got), 3);
    assert_eq!(got, vec![0, 1, 2]);
    assert_eq!(rx.available(), 2);

    got.clear();
    assert_eq!(rx.steal_half(&mut got), 1);
    assert_eq!(got, vec![3]);

    got.clear();
    assert_eq!(rx.steal_half(&mut got), 1);
    assert_eq!(rx.steal_half(&mut got), 0);
}

#[test]
fn rong_va_ban_la_hai_ket_qua_khac_nhau()
{
    let ring = RingBufferFifo::<u32>::new(8);
    let mut tx = unsafe { ring.producer() };
    let rx = ring.consumer();

    assert_eq!(rx.try_steal(), Steal::Empty);
    assert_eq!(rx.try_steal_batch(&mut Vec::new(), 4), Steal::Empty);

    assert_eq!(tx.push_iter(0..4u32), 4);
    ring.head.store(pack(0, 2), Ordering::Relaxed);

    assert_eq!(rx.try_steal(), Steal::Busy);
    assert_eq!(rx.try_steal_batch(&mut Vec::new(), 4), Steal::Busy);

    let _ = drain(&ring, 0, 2);
    ring.head.store(pack(2, 2), Ordering::Relaxed);
    assert_eq!(rx.try_steal(), Steal::Success(2));
}

#[test]
fn steal_into_chuyen_thang_sang_ring_khac_khong_cap_phat()
{
    let mut victim = RingBufferFifo::new(8);
    let mut thief = RingBufferFifo::new(8);

    let (mut vtx, vrx) = victim.split();
    let (mut ttx, _trx) = thief.split();

    assert_eq!(vtx.push_iter(0..6u32), 6);

    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Success(3));
    assert_eq!(vtx.available(), 3);
    assert_eq!(ttx.available(), 3);

    let mut out = Vec::new();
    assert_eq!(ttx.drain(&mut out), 3);
    assert_eq!(out, vec![0, 1, 2]);
    assert_eq!(vtx.pop(), Some(3));
}

#[test]
fn steal_into_khong_vuot_qua_cho_trong_cua_dich()
{
    let mut victim = RingBufferFifo::new(16);
    let mut thief = RingBufferFifo::new(2);

    let (mut vtx, vrx) = victim.split();
    let (mut ttx, _trx) = thief.split();

    assert_eq!(vtx.push_iter(0..16u32), 16);
    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Success(2));
    assert_eq!(ttx.remaining(), 0);
    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Busy);

    let mut out = Vec::new();
    assert_eq!(ttx.drain(&mut out), 2);
    assert_eq!(out, vec![0, 1]);
}

#[test]
fn ke_trom_thu_hai_bo_di_khi_da_co_nguoi_dang_be()
{
    let ring = RingBufferFifo::new(8);
    let mut tx = unsafe { ring.producer() };
    let rx = ring.consumer();

    let mut vals: Vec<u32> = (0..4).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);

    ring.head.store(pack(0, 2), Ordering::Relaxed);

    let mut got = Vec::new();
    assert_eq!(rx.steal_batch(&mut got, 4), 0);
    assert!(got.is_empty());

    assert_eq!(tx.pop(), Some(2));
    assert_eq!(tx.remaining(), 4);

    let _ = drain(&ring, 0, 2);
    ring.head.store(pack(4, 4), Ordering::Relaxed);
}

#[test]
fn pop_khong_keo_steal_qua_vung_dang_bi_be()
{
    let ring = RingBufferFifo::new(8);
    let mut tx = unsafe { ring.producer() };

    let mut vals: Vec<u32> = (0..4).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);
    ring.head.store(pack(0, 2), Ordering::Relaxed);

    assert_eq!(tx.pop(), Some(2));
    let (steal, real) = unpack(ring.head.load(Ordering::Relaxed));
    assert_eq!((steal, real), (0, 3));

    let _ = drain(&ring, 0, 2);
    ring.head.store(pack(3, 3), Ordering::Relaxed);
}

#[test]
fn cache_steal_cua_chu_khong_bao_gio_bao_thua_cho_trong()
{
    let ring = RingBufferFifo::<u32>::new(4);
    let mut tx = unsafe { ring.producer() };
    let rx = ring.consumer();

    assert_eq!(tx.push_iter(0..4u32), 4);
    assert_eq!(tx.push(99), Err(99));

    let mut got = Vec::new();
    assert_eq!(rx.steal_batch(&mut got, 4), 4);
    assert_eq!(got, vec![0, 1, 2, 3]);

    assert_eq!(tx.push(99), Ok(()));
    assert_eq!(tx.pop(), Some(99));

    for round in 0..64u32
    {
        assert_eq!(tx.push_iter(0..4u32), 4);
        got.clear();
        assert_eq!(rx.steal_batch(&mut got, 4), 4, "vòng {round}");
    }
}

#[test]
fn push_theo_lo_khong_bi_cache_cu_cat_ngan()
{
    let mut ring = RingBufferFifo::new(4);
    let (mut tx, rx) = ring.split();

    assert_eq!(tx.push_iter(0..4u32), 4);
    assert_eq!(tx.pop(), Some(0));

    let mut got = Vec::new();
    assert_eq!(rx.steal_batch(&mut got, 3), 3);
    assert_eq!(got, vec![1, 2, 3]);

    let mut vals: Vec<u32> = (10..14).collect();
    assert_eq!(tx.push_batch(&mut vals), 4, "cache `steal` cũ không được cắt ngắn một lô");
    assert!(vals.is_empty());
    assert_eq!(tx.push_iter(20..24u32), 0);
}
