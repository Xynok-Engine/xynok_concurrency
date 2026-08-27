use super::*;
use crate::sync::{Arc, AtomicUsize};

#[test]
fn suc_chua_lam_tron_len_luy_thua_hai()
{
    assert_eq!(RingBufferLifo::<u8>::new(0).capacity(), 2);
    assert_eq!(RingBufferLifo::<u8>::new(3).capacity(), 4);
    assert_eq!(RingBufferLifo::<u8>::new(256).capacity(), 256);
    assert_eq!(RingBufferLifo::<u8>::new(257).capacity(), 512);
}

#[test]
#[should_panic(expected = "2^30")]
fn suc_chua_vuot_tran_thi_panic()
{
    let _ = RingBufferLifo::<u8>::new(MAX_SLOTS + 1);
}

#[test]
fn ring_buffer_rong_thi_moi_con_so_deu_bang_khong()
{
    let ring = RingBufferLifo::<u8>::new(8);
    assert_eq!(ring.occupied(), 0);
    assert_eq!(ring.available(), 0);
    assert!(ring.is_empty());
}

#[test]
fn hai_quyen_co_dung_bo_trait_can_thiet()
{
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<RingBufferLifo<u8>>();
    assert_sync::<RingBufferLifo<u8>>();
    assert_send::<Producer<'_, u8>>();
    assert_send::<Consumer<'_, u8>>();
    assert_sync::<Consumer<'_, u8>>();
}

#[test]
fn chu_lay_job_moi_nhat_truoc()
{
    let mut ring = RingBufferLifo::new(8);
    let (mut tx, _) = ring.split();

    for i in 0..4u32
    {
        assert_eq!(tx.push(i), Ok(()));
    }
    assert_eq!(tx.available(), 4);

    assert_eq!(tx.pop(), Some(3));
    assert_eq!(tx.pop(), Some(2));
    assert_eq!(tx.pop(), Some(1));
    assert_eq!(tx.pop(), Some(0));
    assert_eq!(tx.pop(), None);
    assert_eq!(tx.pop(), None);
}

#[test]
fn ke_trom_lay_job_cu_nhat_truoc()
{
    let mut ring = RingBufferLifo::new(8);
    let (mut tx, rx) = ring.split();

    assert_eq!(tx.push_iter(0..4u32), 4);

    assert_eq!(rx.steal(), Some(0));
    assert_eq!(rx.steal(), Some(1));
    assert_eq!(tx.pop(), Some(3));
    assert_eq!(rx.steal(), Some(2));
    assert_eq!(rx.steal(), None);
    assert_eq!(tx.pop(), None);
}

#[test]
fn day_thi_tra_lai_job_chu_khong_nuot()
{
    let mut ring = RingBufferLifo::new(2);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push(1u8), Ok(()));
    assert_eq!(tx.push(2u8), Ok(()));
    assert!(tx.is_full());
    assert_eq!(tx.push(3u8), Err(3));

    assert_eq!(tx.pop(), Some(2));
    assert_eq!(tx.push(3u8), Ok(()));
    assert_eq!(tx.pop(), Some(3));
    assert_eq!(tx.pop(), Some(1));
}

#[test]
fn chi_so_quan_qua_cuoi_mang_van_dung_thu_tu()
{
    let mut ring = RingBufferLifo::new(4);
    let (mut tx, rx) = ring.split();

    for round in 0..32u32
    {
        assert_eq!(tx.push_iter(round * 4..round * 4 + 4), 4);
        assert_eq!(rx.steal(), Some(round * 4));
        assert_eq!(tx.pop(), Some(round * 4 + 3));
        assert_eq!(tx.pop(), Some(round * 4 + 2));
        assert_eq!(rx.steal(), Some(round * 4 + 1));
        assert!(tx.is_empty());
    }
}

#[test]
fn pop_batch_tra_ve_theo_thu_tu_nguoc()
{
    let mut ring = RingBufferLifo::new(8);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push_iter(0..5u32), 5);

    let mut out = Vec::new();
    assert_eq!(tx.pop_batch(&mut out, 3), 3);
    assert_eq!(out, vec![4, 3, 2]);

    out.clear();
    assert_eq!(tx.drain(&mut out), 2);
    assert_eq!(out, vec![1, 0]);
    assert_eq!(tx.drain(&mut out), 0);
}

#[test]
fn push_batch_nhan_phan_vua_va_giu_nguyen_phan_thua()
{
    let mut ring = RingBufferLifo::new(4);
    let (mut tx, _) = ring.split();

    let mut vals: Vec<u32> = (0..10).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);
    assert_eq!(vals, (4..10).collect::<Vec<_>>());
    assert_eq!(tx.push_batch(&mut vals), 0);

    assert_eq!(tx.pop(), Some(3));
    assert_eq!(tx.push_batch(&mut vals), 1);
    assert_eq!(vals, (5..10).collect::<Vec<_>>());
    assert_eq!(tx.pop(), Some(4));
}

#[test]
fn rong_va_ban_la_hai_ket_qua_khac_nhau()
{
    let ring = RingBufferLifo::<u32>::new(8);
    let mut tx = unsafe { ring.producer() };
    let rx = ring.consumer();

    assert_eq!(rx.try_steal(), Steal::Empty);

    assert_eq!(tx.push_iter(0..4u32), 4);
    ring.top.store(pack(0, 1), Ordering::Relaxed);

    assert_eq!(rx.try_steal(), Steal::Busy);

    let taken = unsafe { ring.slots.read(0) };
    assert_eq!(taken, 0);
    ring.top.store(pack(1, 1), Ordering::Relaxed);
    assert_eq!(rx.try_steal(), Steal::Success(1));
}

#[test]
fn chu_van_lay_duoc_khi_ke_trom_dang_be()
{
    let ring = RingBufferLifo::<u32>::new(8);
    let mut tx = unsafe { ring.producer() };

    assert_eq!(tx.push_iter(0..4u32), 4);
    ring.top.store(pack(0, 1), Ordering::Relaxed);

    assert_eq!(tx.pop(), Some(3));
    assert_eq!(tx.remaining(), 5, "vùng đang bị bê vẫn tính là bị chiếm");

    let taken = unsafe { ring.slots.read(0) };
    assert_eq!(taken, 0);
    ring.top.store(pack(1, 1), Ordering::Relaxed);
    assert_eq!(tx.remaining(), 6);
}

#[test]
fn chu_gianh_duoc_job_cuoi_cung_thi_giu_nguyen_vung_dang_be()
{
    let ring = RingBufferLifo::<u32>::new(8);
    let mut tx = unsafe { ring.producer() };

    assert_eq!(tx.push_iter(0..2u32), 2);
    ring.top.store(pack(0, 1), Ordering::Relaxed);

    assert_eq!(tx.pop(), Some(1));
    let (free, claim) = unpack(ring.top.load(Ordering::Relaxed));
    assert_eq!((free, claim), (0, 2));

    let taken = unsafe { ring.slots.read(0) };
    assert_eq!(taken, 0);
    ring.top.store(pack(2, 2), Ordering::Relaxed);
    assert_eq!(tx.remaining(), 8);
}

#[test]
fn steal_into_chuyen_thang_sang_ring_khac()
{
    let mut victim = RingBufferLifo::new(8);
    let mut thief = RingBufferLifo::new(8);

    let (mut vtx, vrx) = victim.split();
    let (mut ttx, _trx) = thief.split();

    assert_eq!(vtx.push_iter(0..3u32), 3);

    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Success(1));
    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Success(1));
    assert_eq!(vtx.available(), 1);

    let mut out = Vec::new();
    assert_eq!(ttx.drain(&mut out), 2);
    assert_eq!(out, vec![1, 0]);

    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Success(1));
    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Empty);
}

#[test]
fn steal_into_bao_ban_khi_dich_da_day()
{
    let mut victim = RingBufferLifo::new(8);
    let mut thief = RingBufferLifo::new(2);

    let (mut vtx, vrx) = victim.split();
    let (mut ttx, _trx) = thief.split();

    assert_eq!(vtx.push_iter(0..8u32), 8);
    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Success(1));
    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Success(1));
    assert_eq!(vrx.try_steal_into(&mut ttx), Steal::Busy);
}

#[test]
fn drop_tha_moi_job_chua_lay()
{
    let alive = Arc::new(AtomicUsize::new(0));
    {
        let mut ring = RingBufferLifo::new(4);
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
        let mut ring = RingBufferLifo::new(4);
        let (mut tx, rx) = ring.split();

        for _ in 0..3
        {
            tx.push(Arc::new(AtomicUsize::new(0))).unwrap();
            rx.steal().unwrap();
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
        let mut ring = RingBufferLifo::new(8);
        let (mut tx, _) = ring.split();
        for _ in 0..4
        {
            tx.push(Arc::clone(&alive)).unwrap();
        }

        let taken = unsafe { ring.slots.read(0) };
        ring.top.store(pack(0, 1), Ordering::Relaxed);

        assert_eq!(Arc::strong_count(&alive), 5);
        drop(taken);
        assert_eq!(Arc::strong_count(&alive), 4);
    }
    assert_eq!(Arc::strong_count(&alive), 1);
}

#[test]
fn push_theo_lo_khong_bi_cache_cu_cat_ngan()
{
    let mut ring = RingBufferLifo::new(4);
    let (mut tx, rx) = ring.split();

    assert_eq!(tx.push_iter(0..4u32), 4);
    assert_eq!(tx.pop(), Some(3));
    assert_eq!(rx.steal(), Some(0));
    assert_eq!(rx.steal(), Some(1));
    assert_eq!(rx.steal(), Some(2));

    let mut vals: Vec<u32> = (10..14).collect();
    assert_eq!(tx.push_batch(&mut vals), 4, "cache `free` cũ không được cắt ngắn một lô");
    assert!(vals.is_empty());
    assert_eq!(tx.push_iter(20..24u32), 0);
}

#[test]
fn spill_half_nha_nua_cu_va_giu_nua_moi()
{
    let mut ring = RingBufferLifo::new(8);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push_iter(0..5u32), 5);

    let mut out = Vec::new();
    assert_eq!(tx.spill_half(&mut out), 3);
    // Nửa cũ ra ngoài theo đúng thứ tự đẩy vào, để lane queue giữ được tính FIFO của nó.
    assert_eq!(out, vec![0, 1, 2]);
    assert_eq!(tx.available(), 2);

    // Nửa mới ở lại, và chủ vẫn lấy chúng theo LIFO.
    assert_eq!(tx.pop(), Some(4));
    assert_eq!(tx.pop(), Some(3));
    assert_eq!(tx.pop(), None);
}

#[test]
fn spill_half_chua_lai_job_cuoi_cung_cho_chu()
{
    let mut ring = RingBufferLifo::new(8);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push(7u32), Ok(()));

    let mut out = Vec::new();
    assert_eq!(tx.spill_half(&mut out), 1);
    assert_eq!(out, vec![7]);
    assert!(tx.is_empty());

    // Ring rỗng thì không có gì để xả, và cũng không được đụng vào ô nào.
    assert_eq!(tx.spill_half(&mut out), 0);
    assert_eq!(out, vec![7]);
}

#[test]
fn spill_half_nhuong_duong_khi_ke_trom_dang_be()
{
    let mut ring = RingBufferLifo::new(8);
    let (mut tx, rx) = ring.split();

    assert_eq!(tx.push_iter(0..4u32), 4);

    // Giành lấy một ô nhưng chưa nhả: `free != claim`, đúng cửa sổ mà kẻ trộm đang bê job đi.
    let stolen = rx.steal();
    assert_eq!(stolen, Some(0));

    // Kẻ trộm đã nhả xong nên chủ xả được bình thường.
    let mut out = Vec::new();
    assert_eq!(tx.spill_half(&mut out), 2);
    assert_eq!(out, vec![1, 2]);
    assert_eq!(tx.available(), 1);
}

#[test]
fn spill_half_roi_push_tiep_thi_ring_khong_con_day()
{
    let mut ring = RingBufferLifo::new(4);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push_iter(0..4u32), 4);
    assert!(tx.is_full());
    assert_eq!(tx.push(99), Err(99));

    let mut out = Vec::new();
    assert_eq!(tx.spill_half(&mut out), 2);
    assert_eq!(out, vec![0, 1]);

    // Đây là toàn bộ lý do `spill_half` tồn tại: biên cứng của ring thành ngưỡng xả.
    assert_eq!(tx.push(99), Ok(()));
    assert_eq!(tx.pop(), Some(99));
    assert_eq!(tx.pop(), Some(3));
    assert_eq!(tx.pop(), Some(2));
    assert_eq!(tx.pop(), None);
}

#[test]
fn spill_half_quan_qua_cuoi_mang_van_dung_thu_tu()
{
    let mut ring = RingBufferLifo::new(4);
    let (mut tx, _) = ring.split();

    // Đẩy chỉ số chạy gần hết một vòng trước khi kiểm tra.
    for _ in 0..3
    {
        assert_eq!(tx.push_iter(0..4u32), 4);
        let mut sink = Vec::new();
        assert_eq!(tx.drain(&mut sink), 4);
    }

    assert_eq!(tx.push_iter(10..14u32), 4);
    let mut out = Vec::new();
    assert_eq!(tx.spill_half(&mut out), 2);
    assert_eq!(out, vec![10, 11]);
    assert_eq!(tx.pop(), Some(13));
    assert_eq!(tx.pop(), Some(12));
}
