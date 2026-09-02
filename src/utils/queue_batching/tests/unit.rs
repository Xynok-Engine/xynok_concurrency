use std::sync::Arc;

use crate::sync::AtomicUsize;
use crate::utils::fixed_buffer::FixedRingBuffer;
use crate::utils::queue_batching::QueueBatching;

/// Rút `count` phần tử ra khỏi ring buffer để so sánh. Rút hẳn ra nên phần tử coi như đã bị
/// tiêu thụ, gọi hai lần trên cùng một khoảng là đọc lại ô đã trống.
fn read_back<T>(buffer: &FixedRingBuffer<T>, start: u32, count: usize) -> Vec<T>
{
    (0..count as u32).map(|offset| unsafe { buffer.take_at(start.wrapping_add(offset)) }).collect()
}

#[test]
fn t0_day_va_rut_giu_dung_thu_tu()
{
    let queue = QueueBatching::new();
    assert!(queue.is_empty());
    assert_eq!(queue.pop(), None);

    queue.push(1);
    queue.push(2);
    queue.push(3);

    assert_eq!(queue.len(), 3);
    assert_eq!(queue.pop(), Some(1));
    assert_eq!(queue.pop(), Some(2));
    assert_eq!(queue.pop(), Some(3));
    assert_eq!(queue.pop(), None);
    assert!(queue.is_empty());
}

#[test]
fn t1_push_batch_dem_dung_so_phan_tu()
{
    let queue = QueueBatching::with_capacity(8);
    queue.push_batch(0..5);
    queue.push_batch(std::iter::empty::<i32>());
    queue.push_batch(vec![5, 6]);

    let mut out = Vec::new();
    assert_eq!(queue.drain_into(&mut out), 7);
    assert_eq!(out, vec![0, 1, 2, 3, 4, 5, 6]);
}

#[test]
fn t2_pop_batch_noi_them_va_ton_trong_limit()
{
    let queue = QueueBatching::new();
    queue.push_batch(0..10);

    let mut out = vec![-1];
    assert_eq!(queue.pop_batch(&mut out, 4), 4);
    assert_eq!(out, vec![-1, 0, 1, 2, 3]);

    assert_eq!(queue.pop_batch(&mut out, 0), 0, "limit 0 không được chạm vào gì");
    assert_eq!(out.len(), 5);

    assert_eq!(queue.pop_batch(&mut out, usize::MAX), 6);
    assert_eq!(out, vec![-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    assert_eq!(queue.pop_batch(&mut out, 4), 0, "rỗng thì trả 0");
}

#[test]
fn t3_drain_into_buffer_ghi_dung_thu_tu_va_ton_trong_max()
{
    let queue = QueueBatching::new();
    queue.push_batch(0..10);

    let buffer = FixedRingBuffer::new(8);
    let moved = queue.drain_into_buffer(4, &buffer, 0);

    assert_eq!(moved, 4);
    assert_eq!(read_back(&buffer, 0, moved), vec![0, 1, 2, 3]);
    assert_eq!(queue.len(), 6, "phần còn lại vẫn nằm trong hàng đợi");

    assert_eq!(queue.drain_into_buffer(0, &buffer, 0), 0, "max 0 không được chạm vào gì");
    assert_eq!(queue.len(), 6);
}

#[test]
fn t4_drain_into_buffer_dung_lai_khi_hang_doi_can()
{
    let queue = QueueBatching::new();
    queue.push_batch(0..3);

    let buffer = FixedRingBuffer::new(8);
    let moved = queue.drain_into_buffer(8, &buffer, 0);

    assert_eq!(moved, 3, "hàng đợi chỉ có 3 thì chỉ chuyển được 3");
    assert_eq!(read_back(&buffer, 0, moved), vec![0, 1, 2]);
    assert!(queue.is_empty());

    assert_eq!(queue.drain_into_buffer(4, &buffer, 3), 0, "rỗng thì trả 0");
}

#[test]
fn t5_drain_into_buffer_ghi_tiep_tu_cursor_dang_do()
{
    let queue = QueueBatching::new();
    queue.push_batch(0..6);

    let buffer = FixedRingBuffer::new(8);
    assert_eq!(queue.drain_into_buffer(2, &buffer, 0), 2);
    // Đợt sau nối ngay sau đợt trước, giống lúc worker nhích cursor rồi bơm tiếp.
    assert_eq!(queue.drain_into_buffer(3, &buffer, 2), 3);

    assert_eq!(read_back(&buffer, 0, 5), vec![0, 1, 2, 3, 4]);
    assert_eq!(queue.len(), 1);
}

#[test]
fn t6_drain_into_buffer_chay_vong_qua_cuoi_buffer()
{
    let queue = QueueBatching::new();
    queue.push_batch(0..4);

    let buffer = FixedRingBuffer::new(4);
    // Bắt đầu ngay sát điểm quấn của `u32` để chắc chắn `wrapping_add` không trượt ô.
    let start = u32::MAX - 1;
    let moved = queue.drain_into_buffer(4, &buffer, start);

    assert_eq!(moved, 4);
    assert_eq!(read_back(&buffer, start, moved), vec![0, 1, 2, 3]);
}

#[test]
fn t7_drain_into_buffer_chuyen_han_quyen_so_huu()
{
    let alive = Arc::new(AtomicUsize::new(0));
    let queue = QueueBatching::new();
    queue.push_batch((0..3).map(|_| Arc::clone(&alive)));

    let buffer = FixedRingBuffer::new(4);
    let moved = queue.drain_into_buffer(3, &buffer, 0);

    assert_eq!(moved, 3);
    assert_eq!(Arc::strong_count(&alive), 4, "thả hàng đợi rồi thì buffer là chủ mới");

    drop(queue);
    assert_eq!(Arc::strong_count(&alive), 4);

    // `FixedRingBuffer` không tự drop phần tử, phải rút tay ra cho hết.
    for offset in 0..moved as u32
    {
        unsafe { buffer.drop_at(offset) };
    }
    assert_eq!(Arc::strong_count(&alive), 1);
}

#[test]
fn t8_guard_giu_khoa_cho_toi_khi_bi_tha()
{
    let queue = QueueBatching::new();
    {
        let mut elements = queue.get();
        assert!(queue.is_locked());
        assert!(queue.try_get().is_none(), "khoá không đệ quy");

        elements.push_back(7);
        elements.push_back(8);
        assert_eq!(elements.front(), Some(&7));
        assert_eq!(elements.len(), 2);
    }
    assert!(!queue.is_locked(), "thả guard phải nhả khoá");
    assert!(queue.try_get().is_some());
    assert_eq!(queue.len(), 2);
}

#[test]
fn t9_panic_giua_push_batch_van_nha_khoa()
{
    let queue = QueueBatching::new();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        queue.push_batch((0..10).map(|i| match i
        {
            5 => panic!("iterator nổ giữa chừng"),
            i => i,
        }));
    }));

    assert!(result.is_err());
    assert!(!queue.is_locked(), "tháo stack vì panic vẫn phải nhả khoá");
    assert_eq!(queue.len(), 5, "những phần tử đã kịp vào thì vẫn còn nguyên");
}

#[test]
fn t10_get_mut_va_take_khong_qua_khoa()
{
    let mut queue = QueueBatching::new();
    queue.extend(0..3);
    queue.get_mut().push_back(3);

    assert!(!queue.is_locked());

    let mut inner = queue.take();
    assert_eq!(inner.len(), 4);
    assert_eq!(inner.pop_front(), Some(0));
}

#[test]
fn t11_from_iter_va_debug()
{
    let queue: QueueBatching<i32> = (0..3).collect();
    assert_eq!(queue.len(), 3);
    assert!(format!("{:?}", queue).contains("[0, 1, 2]"));

    let held = queue.get();
    assert!(format!("{:?}", queue).contains("<locked>"), "`Debug` không được đợi khoá");
    drop(held);
}

#[test]
fn t12_tha_hang_doi_keo_theo_phan_tu_chua_rut()
{
    let alive = Arc::new(AtomicUsize::new(0));
    {
        let queue = QueueBatching::new();
        queue.push_batch((0..8).map(|_| Arc::clone(&alive)));
        assert_eq!(Arc::strong_count(&alive), 9);
    }
    assert_eq!(Arc::strong_count(&alive), 1, "phần tử chưa rút vẫn phải được thả sạch");
}
