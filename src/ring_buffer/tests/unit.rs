//! Test đơn vị cho [`RingBuffer`]. Để ở đây thay vì trong `mod.rs` cho phần cài đặt đọc được
//! liền một mạch; vẫn là module con của `ring_buffer` nên chạm được `head`, `tail` và
//! `drain_claimed`.

use super::*;
use crate::sync::{Arc, AtomicUsize};

#[test]
fn test_wrapping_sub()
{
    let a: u32 = 0;
    let b: u32 = u32::MAX;
    let result = a.wrapping_sub(b);
    println!("a.wrapping_sub(b): {}", result);
    assert!(result == 1);
}

#[test]
fn test_mask()
{
    let capacity: u32 = 8;
    assert!(capacity.is_power_of_two());
    let mask = capacity - 1;
    for i in 0..1024
    {
        let idx = i & mask;
        assert!(idx == i % capacity);
    }
}

#[test]
fn suc_chua_lam_tron_len_luy_thua_hai()
{
    assert_eq!(RingBuffer::<u8>::new(0).capacity(), 2, "0 và 1 đều phải nâng lên 2");
    assert_eq!(RingBuffer::<u8>::new(1).capacity(), 2);
    assert_eq!(RingBuffer::<u8>::new(2).capacity(), 2);
    assert_eq!(RingBuffer::<u8>::new(3).capacity(), 4);
    assert_eq!(RingBuffer::<u8>::new(256).capacity(), 256);
    assert_eq!(RingBuffer::<u8>::new(257).capacity(), 512);
}

#[test]
#[should_panic(expected = "2^31")]
fn suc_chua_vuot_tran_thi_panic()
{
    let _ = RingBuffer::<u8>::new((1 << 31) + 1);
}

#[test]
fn ring_buffer_rong_thi_moi_con_so_deu_bang_khong()
{
    let ring = RingBuffer::<u8>::new(8);
    assert_eq!(ring.occupied(), 0);
    assert_eq!(ring.available(), 0);
    assert!(ring.is_empty());
}

#[test]
fn occupied_la_hieu_hai_dau_ke_ca_khi_tran_so()
{
    let ring = RingBuffer::<u8>::new(8);

    ring.head.store(pack(u32::MAX - 1, u32::MAX - 1), Ordering::Relaxed);
    ring.tail.store(2, Ordering::Relaxed);
    assert_eq!(ring.occupied(), 4, "chỉ số tràn u32 là chuyện bình thường, hiệu wrapping vẫn đúng");

    // `len` lấy mốc dưới là `steal`, `available` lấy `real`.
    ring.head.store(pack(0, 3), Ordering::Relaxed);
    ring.tail.store(5, Ordering::Relaxed);
    assert_eq!(ring.occupied(), 5, "ô đang bị bê dở vẫn tính là bị chiếm");
    assert_eq!(ring.available(), 2, "nhưng không còn lấy được nữa");
}

/// Hợp đồng mà cái tên `occupied` sinh ra để bảo vệ: hai con số **được phép** lệch nhau, nên
/// `is_empty` cặp với `available` chứ không cặp với `occupied`. Nếu ai đó đổi `occupied` về
/// `len` thì test này là chỗ nhắc rằng lệ `len() == 0 ⟺ is_empty()` của Rust bị vi phạm.
#[test]
fn occupied_va_is_empty_duoc_phep_lech_nhau()
{
    let ring = RingBuffer::<u32>::new(4);
    // SAFETY: đúng một `Producer`.
    let mut tx = unsafe { ring.producer() };

    let mut vals: Vec<u32> = (0..4).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);

    // Một kẻ trộm dán biển lên trọn cả ring buffer và chưa gỡ.
    ring.head.store(pack(0, 4), Ordering::Relaxed);

    assert_eq!(ring.occupied(), 4, "ô vẫn bị chiếm: `push` không được đụng vào");
    assert_eq!(tx.remaining(), 0);
    assert!(ring.is_empty(), "nhưng không còn gì để lấy: `pop`/`steal` phải đi chỗ khác");
    assert_eq!(ring.available(), 0);
    assert_eq!(tx.pop(), None);

    // Dọn lại cho `Drop` khỏi thấy một trạng thái không thể xảy ra thật.
    let mut leftover = Vec::new();
    unsafe { ring.drain_claimed(0, 4, &mut leftover) };
    ring.head.store(pack(4, 4), Ordering::Relaxed);
}

#[test]
fn drop_tha_moi_job_chua_lay()
{
    let alive = Arc::new(AtomicUsize::new(0));
    {
        let mut ring = RingBuffer::new(4);
        let (mut tx, _) = ring.split();
        for _ in 0..3
        {
            tx.push(Arc::clone(&alive)).unwrap();
        }
        assert_eq!(Arc::strong_count(&alive), 4);
    }
    assert_eq!(Arc::strong_count(&alive), 1, "job chưa lấy vẫn phải được thả");
}

#[test]
fn drop_di_dung_vong_khi_chi_so_quan_qua_cuoi_mang()
{
    let alive = Arc::new(AtomicUsize::new(0));
    {
        let mut ring = RingBuffer::new(4);
        let (mut tx, _) = ring.split();

        // Đẩy ring buffer chạy tới chỉ số 3 rồi mới nạp, để vùng job vắt qua cuối mảng.
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
        let mut ring = RingBuffer::new(8);
        let (mut tx, _) = ring.split();
        for _ in 0..4
        {
            tx.push(Arc::clone(&alive)).unwrap();
        }
        // Giả lập: một kẻ trộm đã nhận [0, 2), chép xong nhưng chưa kịp gỡ biển.
        let mut taken = Vec::new();
        unsafe { ring.drain_claimed(0, 2, &mut taken) };
        ring.head.store(pack(0, 2), Ordering::Relaxed);

        assert_eq!(Arc::strong_count(&alive), 5, "4 trong ring buffer + 1 gốc");
        drop(taken);
        assert_eq!(Arc::strong_count(&alive), 3, "2 job đã ra khỏi ring buffer");
    }
    assert_eq!(Arc::strong_count(&alive), 1, "Drop chỉ được thả [real, tail), không đụng [steal, real)");
}

#[test]
fn hai_quyen_co_dung_bo_trait_can_thiet()
{
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<RingBuffer<u8>>();
    assert_sync::<RingBuffer<u8>>();

    // `Producer` phải chuyển được sang thread worker...
    assert_send::<Producer<'_, u8>>();
    // ...còn `Consumer` thì thoải mái chia sẻ cho mọi worker khác.
    assert_send::<Consumer<'_, u8>>();
    assert_sync::<Consumer<'_, u8>>();

    // `Producer` **không** được `Sync`. Không khẳng định được điều đó bằng một dòng như trên,
    // nên bỏ dấu chú thích dưới đây ra là biên dịch phải hỏng — đó chính là phép thử.
    // assert_sync::<Producer<'_, u8>>();
}

#[test]
fn push_va_pop_giu_dung_thu_tu_fifo()
{
    let mut ring = RingBuffer::new(4);
    let (mut tx, _) = ring.split();

    for i in 0..4u32
    {
        assert_eq!(tx.push(i), Ok(()));
    }
    assert_eq!(tx.occupied(), 4);
    assert_eq!(tx.remaining(), 0);

    for i in 0..4u32
    {
        assert_eq!(tx.pop(), Some(i), "job vào trước phải ra trước");
    }
    assert_eq!(tx.pop(), None);
}

#[test]
fn day_thi_tra_lai_job_chu_khong_nuot()
{
    let mut ring = RingBuffer::new(2);
    let (mut tx, _) = ring.split();

    assert_eq!(tx.push(1u8), Ok(()));
    assert_eq!(tx.push(2u8), Ok(()));
    assert_eq!(tx.push(3u8), Err(3), "job không vừa phải quay về tay chỗ gọi");

    // Lấy một cái ra là lại có chỗ ngay — `pop` kéo cả `steal` lên khi không ai đang bê.
    assert_eq!(tx.pop(), Some(1));
    assert_eq!(tx.push(3u8), Ok(()));
}

#[test]
fn chi_so_quan_qua_cuoi_mang_van_dung_thu_tu()
{
    let mut ring = RingBuffer::new(4);
    let (mut tx, _) = ring.split();

    // Chạy 10 vòng quanh một ring buffer 4 ô: chỉ số đi tới 40, ô thì quay lại đầu liên tục.
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
    let mut ring = RingBuffer::new(4);
    let (mut tx, _) = ring.split();

    let mut vals: Vec<u32> = (0..10).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);
    assert_eq!(vals, (4..10).collect::<Vec<_>>(), "phần thừa phải còn nguyên, theo đúng thứ tự");

    assert_eq!(tx.push_batch(&mut vals), 0, "ring buffer đầy thì không nhận thêm ô nào");
    assert_eq!(vals.len(), 6);

    let mut out = Vec::new();
    assert_eq!(tx.pop_batch(&mut out, 2), 2);
    assert_eq!(out, vec![0, 1]);
    assert_eq!(tx.push_batch(&mut vals), 2);
    assert_eq!(vals, vec![6, 7, 8, 9]);
}

#[test]
fn pop_batch_bi_chan_boi_so_job_dang_co()
{
    let mut ring = RingBuffer::new(8);
    let (mut tx, _) = ring.split();

    let mut vals: Vec<u32> = (0..5).collect();
    assert_eq!(tx.push_batch(&mut vals), 5);

    let mut out = Vec::new();
    assert_eq!(tx.pop_batch(&mut out, 100), 5, "xin 100 nhưng chỉ có 5");
    assert_eq!(out, vec![0, 1, 2, 3, 4]);
    assert_eq!(tx.pop_batch(&mut out, 100), 0);
    assert_eq!(tx.pop_batch(&mut out, 0), 0);
}

#[test]
fn ke_trom_boc_tu_dau_cu_nhat()
{
    let mut ring = RingBuffer::new(8);
    let (mut tx, rx) = ring.split();

    let mut vals: Vec<u32> = (0..6).collect();
    assert_eq!(tx.push_batch(&mut vals), 6);

    assert_eq!(rx.steal(), Some(0), "kẻ trộm ăn ở đầu cũ");

    let mut got = Vec::new();
    assert_eq!(rx.steal_batch(&mut got, 2), 2);
    assert_eq!(got, vec![1, 2]);

    // Chủ ring buffer và kẻ trộm cùng ăn ở một đầu, nên chủ nhận tiếp từ chỗ kẻ trộm bỏ lại.
    assert_eq!(tx.pop(), Some(3));
}

#[test]
fn steal_half_lay_nua_lam_tron_len()
{
    let mut ring = RingBuffer::new(8);
    let (mut tx, rx) = ring.split();

    let mut vals: Vec<u32> = (0..5).collect();
    assert_eq!(tx.push_batch(&mut vals), 5);

    let mut got = Vec::new();
    assert_eq!(rx.steal_half(&mut got), 3, "5 job thì bốc 3, để lại 2");
    assert_eq!(got, vec![0, 1, 2]);
    assert_eq!(rx.available(), 2);

    got.clear();
    assert_eq!(rx.steal_half(&mut got), 1);
    assert_eq!(got, vec![3]);

    got.clear();
    assert_eq!(rx.steal_half(&mut got), 1);
    assert_eq!(rx.steal_half(&mut got), 0, "ring buffer rỗng thì không bốc gì");
}

#[test]
fn ke_trom_thu_hai_bo_di_khi_da_co_nguoi_dang_be()
{
    let ring = RingBuffer::new(8);
    // SAFETY: đúng một `Producer`, và test này cần chọc thẳng vào `head` trong lúc hai quyền
    // còn sống nên không dùng được `split` an toàn.
    let mut tx = unsafe { ring.producer() };
    let rx = ring.consumer();

    let mut vals: Vec<u32> = (0..4).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);

    // Dựng tay trạng thái "một kẻ trộm đã dán biển lên [0, 2) và chưa gỡ".
    ring.head.store(pack(0, 2), Ordering::Relaxed);

    let mut got = Vec::new();
    assert_eq!(rx.steal_batch(&mut got, 4), 0, "thấy steal != real là đi tìm ring buffer khác, không đợi");
    assert!(got.is_empty());

    // ...nhưng chủ ring buffer vẫn lấy tiếp được, ở ngay sau tấm biển.
    assert_eq!(tx.pop(), Some(2));

    // Và `push` vẫn coi vùng đang bị bê là bị chiếm: 8 ô, `tail - steal` = 4 → còn 4 chỗ.
    assert_eq!(tx.remaining(), 4);

    // Dọn lại cho `Drop` khỏi thấy một trạng thái không thể xảy ra thật.
    let mut leftover = Vec::new();
    unsafe { ring.drain_claimed(0, 2, &mut leftover) };
    ring.head.store(pack(4, 4), Ordering::Relaxed);
}

#[test]
fn pop_khong_keo_steal_qua_vung_dang_bi_be()
{
    let ring = RingBuffer::new(8);
    // SAFETY: đúng một `Producer`; xem test trên.
    let mut tx = unsafe { ring.producer() };

    let mut vals: Vec<u32> = (0..4).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);
    ring.head.store(pack(0, 2), Ordering::Relaxed);

    assert_eq!(tx.pop(), Some(2));
    let (steal, real) = unpack(ring.head.load(Ordering::Relaxed));
    assert_eq!((steal, real), (0, 3), "`real` tiến, `steal` phải nằm yên tại chỗ tấm biển");

    let mut leftover = Vec::new();
    unsafe { ring.drain_claimed(0, 2, &mut leftover) };
    ring.head.store(pack(3, 3), Ordering::Relaxed);
}
