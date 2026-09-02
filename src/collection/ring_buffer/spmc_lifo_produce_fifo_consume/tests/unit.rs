use std::cell::Cell;
use std::rc::Rc;

use crate::collection::ring_buffer::consts::MAX_CAPACITY;
use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;
use crate::collection::ring_buffer::spmc_lifo_produce_fifo_consume::{Consumer, SpmcRingBufferLifoProduceFifoConsume};
use crate::utils::steal::Steal;

type Ring<T> = SpmcRingBufferLifoProduceFifoConsume<T>;

/// Đọc trộm một ô trong buffer để kiểm tra kết quả.
///
/// Ring này chưa có đường đọc nào an toàn ngoài `pop_lifo` và `consumer_pop_batch_to`, nên vài
/// test cần nhìn thẳng vào buffer. `take_at` chỉ là một phép copy bit, với kiểu `Copy` thì ô
/// vẫn còn nguyên sau khi đọc, không có gì bị hỏng.
fn peek<T: Copy>(ring: &Ring<T>, cursor: u32) -> T
{
    unsafe { ring.buffer.take_at(cursor) }
}

/// Gom các con trỏ của ring thành bộ ba dễ so sánh: (stolen, in_stealing, tail).
fn cursors<T>(ring: &Ring<T>) -> (u32, u32, u32)
{
    let c = ring.cursor_data();
    (c.stolen, c.blocked, c.tail)
}

/// Chạy `f` trên một thread khác rồi trả về thông điệp panic, hoặc `None` nếu chạy trót lọt.
///
/// Phải `join` thủ công thay vì để `thread::scope` tự dọn, nếu không scope sẽ ném lại panic của
/// mình ("a scoped thread panicked") và nuốt mất thông điệp gốc mà test cần soi.
fn panic_tu_thread_khac<F: FnOnce() + Send>(f: F) -> Option<String>
{
    let ket_qua = std::thread::scope(|scope| scope.spawn(f).join());
    ket_qua.err().map(|e| match e.downcast_ref::<&str>()
    {
        Some(s) => (*s).to_string(),
        None => e.downcast_ref::<String>().cloned().unwrap_or_default(),
    })
}

/// Dựng sẵn một `SpmcRingBufferFifo` đã nạp `0..n` để làm nguồn cho `push_batch_by_taking_from`.
fn fifo_source(capacity: usize, n: u32) -> SpmcRingBufferFifo<u32>
{
    let src = SpmcRingBufferFifo::new(capacity);
    for i in 0..n
    {
        assert_eq!(src.push(i), Ok(()), "nguồn phải đủ chỗ cho {} phần tử", n);
    }
    src
}
// --- new() phải chặn sức chứa không hợp lệ ---

#[test]
#[should_panic(expected = "power of 2")]
fn t0_suc_chua_khong_phai_luy_thua_hai_thi_panic()
{
    let _ = Ring::<u8>::new(3);
}

#[test]
#[should_panic]
fn t1_suc_chua_bang_khong_thi_panic()
{
    let _ = Ring::<u8>::new(0);
}

#[test]
#[should_panic(expected = "exceeds")]
fn t2_suc_chua_vuot_gioi_han_thi_panic()
{
    let _ = Ring::<u8>::new(MAX_CAPACITY);
}

#[test]
fn t3_ring_moi_tao_thi_moi_con_tro_deu_bang_khong()
{
    let ring = Ring::<u32>::new(8);
    assert_eq!(cursors(&ring), (0, 0, 0));
    assert_eq!(ring.cursor_data().filled_slots(), 0);
    assert_eq!(ring.cursor_data().empty_slots(), 8);
}

#[test]
fn t4_hai_quyen_co_dung_bo_trait_can_thiet()
{
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Ring<u32>>();
    assert_sync::<Ring<u32>>();
    assert_send::<Consumer<'_, u32>>();
    assert_sync::<Consumer<'_, u32>>();
}

// --- push: chỉ chủ sở hữu gọi, ghi theo thứ tự tăng dần của tail ---

#[test]
fn t5_push_ghi_lan_luot_va_day_tail_len()
{
    let ring = Ring::new(4);
    for i in 0..4u32
    {
        assert_eq!(ring.push(i * 10), Ok(()));
        assert_eq!(cursors(&ring), (0, 0, i + 1));
    }
    for i in 0..4u32
    {
        assert_eq!(peek(&ring, i), i * 10, "ô {} phải giữ đúng giá trị đã push", i);
    }
}

#[test]
fn t6_push_khi_day_thi_tra_lai_gia_tri_chu_khong_nuot()
{
    let ring = Ring::new(2);
    assert_eq!(ring.push(1), Ok(()));
    assert_eq!(ring.push(2), Ok(()));
    assert_eq!(ring.push(3), Err(3), "ring đầy thì phải trả lại giá trị cho người gọi");
    assert_eq!(cursors(&ring), (0, 0, 2), "lần push hỏng không được đụng vào tail");
}

#[test]
fn t7_push_tu_thread_khac_thi_panic()
{
    let ring = Ring::<u32>::new(4);
    let thong_diep = panic_tu_thread_khac(|| {
        let _ = ring.push(1);
    });
    assert!(
        thong_diep.as_deref().is_some_and(|m| m.contains("owner thread")),
        "push từ thread lạ phải panic vì sai chủ sở hữu, nhận được: {:?}",
        thong_diep
    );
}

// --- pop_lifo: chủ sở hữu lấy phần tử mới nhất trước ---

#[test]
fn t8_ring_rong_thi_pop_lifo_tra_ve_none()
{
    let ring = Ring::<u32>::new(4);
    assert!(ring.pop_lifo().is_none());
    assert_eq!(cursors(&ring), (0, 0, 0), "pop hụt không được đụng vào con trỏ");
}

#[test]
fn t9_pop_lifo_tu_thread_khac_thi_panic()
{
    let ring = Ring::<u32>::new(4);
    assert_eq!(ring.push(1), Ok(()));
    let thong_diep = panic_tu_thread_khac(|| {
        let _ = ring.pop_lifo();
    });
    assert!(
        thong_diep.as_deref().is_some_and(|m| m.contains("owner thread")),
        "pop_lifo từ thread lạ phải panic vì sai chủ sở hữu, nhận được: {:?}",
        thong_diep
    );
}

#[test]
fn t10_pop_lifo_lay_phan_tu_moi_nhat_truoc()
{
    // `tail` luôn trỏ vào ô trống kế tiếp, nên phần tử vừa push nằm ở `tail - 1`.
    let ring = Ring::new(4);
    for i in 0..4u32
    {
        assert_eq!(ring.push(i), Ok(()));
    }
    assert_eq!(ring.pop_lifo(), Some(3));
    assert_eq!(ring.pop_lifo(), Some(2));
    assert_eq!(ring.pop_lifo(), Some(1));
    assert_eq!(ring.pop_lifo(), Some(0));
    assert_eq!(ring.pop_lifo(), None);
}

#[test]
fn t11_pop_lifo_xen_ke_push_van_dung_thu_tu()
{
    let ring = Ring::new(4);
    assert_eq!(ring.push(1), Ok(()));
    assert_eq!(ring.push(2), Ok(()));
    assert_eq!(ring.pop_lifo(), Some(2));
    assert_eq!(ring.push(3), Ok(()));
    assert_eq!(ring.pop_lifo(), Some(3));
    assert_eq!(ring.pop_lifo(), Some(1));
    assert_eq!(ring.pop_lifo(), None);
}

#[test]
fn t12_pop_lifo_chay_qua_diem_vong_lai_van_dung()
{
    let ring = Ring::new(4);
    for round in 0..20u32
    {
        for i in 0..4u32
        {
            assert_eq!(ring.push(round * 4 + i), Ok(()));
        }
        for i in (0..4u32).rev()
        {
            assert_eq!(ring.pop_lifo(), Some(round * 4 + i), "vòng {}", round);
        }
    }
}

// --- push_batch_by_taking_from: hút việc từ một ring FIFO ---

#[test]
#[should_panic(expected = "greater than zero")]
fn t13_push_batch_voi_max_bang_khong_thi_panic()
{
    let ring = Ring::<u32>::new(4);
    let src = fifo_source(4, 2);
    let _ = ring.push_batch_by_taking_from(0, &src);
}

#[test]
fn t14_push_batch_tu_thread_khac_thi_panic()
{
    let ring = Ring::<u32>::new(4);
    let src = fifo_source(4, 2);
    let thong_diep = panic_tu_thread_khac(|| {
        let _ = ring.push_batch_by_taking_from(2, &src);
    });
    assert!(
        thong_diep.as_deref().is_some_and(|m| m.contains("owner thread")),
        "push_batch từ thread lạ phải panic vì sai chủ sở hữu, nhận được: {:?}",
        thong_diep
    );
}

#[test]
fn t15_push_batch_tu_nguon_rong_thi_khong_lay_gi()
{
    let ring = Ring::<u32>::new(4);
    let src = SpmcRingBufferFifo::<u32>::new(4);
    assert_eq!(ring.push_batch_by_taking_from(4, &src), 0);
    assert_eq!(cursors(&ring), (0, 0, 0));
}

#[test]
fn t16_push_batch_giu_nguyen_thu_tu_fifo_cua_nguon()
{
    let ring = Ring::new(8);
    let src = fifo_source(8, 5);

    assert_eq!(ring.push_batch_by_taking_from(5, &src), 5);
    assert_eq!(cursors(&ring), (0, 0, 5));
    assert!(src.is_empty(), "nguồn phải bị hút cạn");

    for i in 0..5u32
    {
        assert_eq!(peek(&ring, i), i, "phần tử phải nằm đúng thứ tự FIFO của nguồn");
    }
}

#[test]
fn t17_push_batch_bi_gioi_han_boi_so_o_trong()
{
    let ring = Ring::new(4);
    let src = fifo_source(16, 10);

    assert_eq!(ring.push_batch_by_taking_from(10, &src), 4, "chỉ còn 4 ô nên chỉ lấy được 4");
    assert_eq!(cursors(&ring), (0, 0, 4));
    assert_eq!(src.len(), 6, "phần còn lại phải nằm nguyên bên nguồn");

    assert_eq!(ring.push_batch_by_taking_from(10, &src), 0, "ring đã đầy thì không lấy thêm");
    assert_eq!(src.len(), 6);
}

#[test]
fn t18_push_batch_bi_gioi_han_boi_max()
{
    let ring = Ring::new(8);
    let src = fifo_source(8, 8);

    assert_eq!(ring.push_batch_by_taking_from(3, &src), 3);
    assert_eq!(cursors(&ring), (0, 0, 3));
    assert_eq!(src.len(), 5);
}

#[test]
fn t19_push_batch_bi_gioi_han_boi_so_luong_ben_nguon()
{
    let ring = Ring::new(8);
    let src = fifo_source(8, 3);

    assert_eq!(ring.push_batch_by_taking_from(8, &src), 3);
    assert_eq!(cursors(&ring), (0, 0, 3));
    assert!(src.is_empty());
}

/// Vá cho cái lỗi: `push_batch_by_taking_from` từng nhích `tail` theo số ô nó **xin**, chứ
/// không theo số ô nó **lấy được**. Khi một kẻ trộm khác cướp mất một phần của nguồn ngay giữa
/// lúc đọc `len()` và lúc CAS, phần chênh lệch trở thành những ô chưa ai ghi mà `tail` vẫn nói
/// là có hàng. Người trộm kế tiếp đọc trúng rác.
///
/// Cửa sổ race chỉ hé ra khi nguồn còn ít hơn một lô, nên chỉ thả một mớ vào rồi cùng rút thì
/// hoạ hoằn mới bắt được. Ở đây nguồn được giữ nông: một thread bơm vào từng cái một, ba tay
/// cùng rút ra, nên gần như lần nào `take_amount` cũng đúng bằng số hàng đang có và bất kỳ cú
/// trộm nào chen vào cũng làm lệch.
///
/// Cộng sổ ở cuối: số ô `tail` của ring cộng phần kẻ trộm gom được phải đúng bằng số phần tử
/// đã bơm. Lệch lên nghĩa là `tail` đã đi quá phần thực có.
#[cfg(not(loom))]
#[test]
fn t20_push_batch_khong_duoc_nhich_tail_qua_so_o_thuc_su_lay_duoc()
{
    use crate::sync::Ordering::{Acquire as SyncAcquire, Relaxed as SyncRelaxed, Release as SyncRelease};
    use crate::sync::{AtomicBool, AtomicUsize};

    const TONG: u32 = 8_192;
    /// Nguồn cố tình nhỏ, để nó luôn gần cạn và cửa sổ race luôn mở.
    const SUC_CHUA_NGUON: usize = 16;
    const KE_TROM: usize = 2;
    const LO: usize = 8;
    /// Trần cứng cho mọi vòng, để một ring hỏng không kéo test chạy mãi.
    const TRAN_VONG_LAP: usize = 2_000_000;

    let src = SpmcRingBufferFifo::<u32>::new(SUC_CHUA_NGUON);
    let ring = Ring::<u32>::new(TONG as usize * 2);
    let ke_trom_gom = AtomicUsize::new(0);
    let da_bom_xong = AtomicBool::new(false);

    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut con_lai = 0..TONG;
            let mut val = con_lai.next();
            for _ in 0..TRAN_VONG_LAP
            {
                let Some(v) = val
                else
                {
                    break;
                };
                match src.push(v)
                {
                    Ok(()) => val = con_lai.next(),
                    Err(_) => std::hint::spin_loop(),
                }
            }
            assert!(val.is_none(), "producer phải bơm hết {} phần tử", TONG);
            da_bom_xong.store(true, SyncRelease);
        });

        for _ in 0..KE_TROM
        {
            scope.spawn(|| {
                let mut thung = Vec::new();
                for _ in 0..TRAN_VONG_LAP
                {
                    let lay = src.pop_batch(LO, &mut thung);
                    if lay > 0
                    {
                        ke_trom_gom.fetch_add(lay, SyncRelaxed);
                        thung.clear();
                    }
                    else if da_bom_xong.load(SyncAcquire) && src.is_empty()
                    {
                        break;
                    }
                    else
                    {
                        std::hint::spin_loop();
                    }
                }
            });
        }

        // Chủ sở hữu của `ring` phải là chính thread đã tạo nó, nên phần hút việc chạy ở đây
        // chứ không đẩy sang thread con.
        for _ in 0..TRAN_VONG_LAP
        {
            if src.is_empty()
            {
                if da_bom_xong.load(SyncAcquire)
                {
                    break;
                }
                std::hint::spin_loop();
                continue;
            }
            if ring.push_batch_by_taking_from(LO, &src) == 0
            {
                std::hint::spin_loop();
            }
        }
    });

    let (_, _, tail) = cursors(&ring);
    let da_gom = ke_trom_gom.load(SyncRelaxed);
    assert!(src.is_empty(), "nguồn phải bị rút cạn, còn lại {}", src.len());
    assert_eq!(
        tail as usize + da_gom,
        TONG as usize,
        "tail={} cộng phần kẻ trộm gom={} phải bằng {}, lệch lên nghĩa là tail đã nhích quá phần thực sự lấy được",
        tail,
        da_gom,
        TONG
    );
}

// --- consumer_pop_batch_to: consumer khác lấy việc theo thứ tự FIFO ---

#[test]
fn t21_steal_tu_ring_rong_thi_tra_ve_khong()
{
    let src = Ring::<u32>::new(4);
    let dst = Ring::<u32>::new(4);
    assert_eq!(src.consumer_pop_batch_to(4, &dst), 0);
    assert_eq!(cursors(&src), (0, 0, 0));
    assert_eq!(cursors(&dst), (0, 0, 0));
}

#[test]
fn t22_steal_khi_dich_da_day_thi_tra_ve_khong()
{
    let src = Ring::new(4);
    let dst = Ring::new(2);
    for i in 0..4u32
    {
        assert_eq!(src.push(i), Ok(()));
    }
    for i in 0..2u32
    {
        assert_eq!(dst.push(100 + i), Ok(()));
    }

    assert_eq!(src.consumer_pop_batch_to(4, &dst), 0, "đích không còn ô trống thì không lấy gì");
    assert_eq!(cursors(&src), (0, 0, 4), "lần steal hụt không được đụng vào con trỏ nguồn");
}

#[test]
fn t23_steal_cap_nhat_con_tro_cua_nguon()
{
    let src = Ring::new(8);
    let dst = Ring::<u32>::new(8);
    for i in 0..5u32
    {
        assert_eq!(src.push(i), Ok(()));
    }

    assert_eq!(src.consumer_pop_batch_to(5, &dst), 5);
    assert_eq!(cursors(&src), (5, 5, 5), "steal xong thì stolen và in_stealing phải cùng đuổi kịp tail");
    assert_eq!(src.cursor_data().filled_slots(), 0);
    assert_eq!(src.cursor_data().empty_slots(), 8, "các ô đã bị lấy phải được trả lại cho producer");
}

#[test]
fn t24_steal_bi_gioi_han_boi_max()
{
    let src = Ring::new(8);
    let dst = Ring::<u32>::new(16);
    for i in 0..8u32
    {
        assert_eq!(src.push(i), Ok(()));
    }

    assert_eq!(src.consumer_pop_batch_to(3, &dst), 3);
    assert_eq!(cursors(&src), (3, 3, 8));
    assert_eq!(src.cursor_data().filled_slots(), 5, "phần chưa lấy vẫn còn bên nguồn");
}

#[test]
fn t25_steal_bi_gioi_han_boi_so_o_trong_cua_dich()
{
    let src = Ring::new(8);
    let dst = Ring::new(4);
    for i in 0..8u32
    {
        assert_eq!(src.push(i), Ok(()));
    }
    assert_eq!(dst.push(99), Ok(()));

    assert_eq!(src.consumer_pop_batch_to(8, &dst), 3, "đích chỉ còn 3 ô trống");
}

#[test]
fn t26_steal_chuyen_du_phan_tu_sang_dich()
{
    // `consumer_pop_batch_to` ghi dữ liệu vào `other.buffer` nhưng không hề `store` lại
    // `other.tail`, nên bên đích vẫn tưởng mình rỗng và lần push kế tiếp sẽ đè lên việc vừa lấy.
    let src = Ring::new(8);
    let dst = Ring::new(8);
    for i in 0..5u32
    {
        assert_eq!(src.push(i), Ok(()));
    }

    assert_eq!(src.consumer_pop_batch_to(5, &dst), 5);
    assert_eq!(cursors(&dst), (0, 0, 5), "đích phải đẩy tail lên đúng số phần tử đã nhận");
    assert_eq!(dst.cursor_data().filled_slots(), 5);
}

#[test]
fn t27_steal_giu_thu_tu_fifo()
{
    let src = Ring::new(8);
    let dst = Ring::new(8);
    for i in 0..4u32
    {
        assert_eq!(src.push(i), Ok(()));
    }

    assert_eq!(src.consumer_pop_batch_to(4, &dst), 4);
    // Bên đích, `pop_lifo` phải nhả ra theo thứ tự ngược lại vì phần tử được xếp FIFO khi chuyển sang.
    assert_eq!(dst.pop_lifo(), Some(3));
    assert_eq!(dst.pop_lifo(), Some(2));
    assert_eq!(dst.pop_lifo(), Some(1));
    assert_eq!(dst.pop_lifo(), Some(0));
}

#[test]
fn t28_steal_nhieu_lan_khong_ghi_de_len_nhau()
{
    let src = Ring::new(8);
    let dst = Ring::new(8);
    for i in 0..4u32
    {
        assert_eq!(src.push(i), Ok(()));
    }

    assert_eq!(src.consumer_pop_batch_to(2, &dst), 2);
    assert_eq!(src.consumer_pop_batch_to(2, &dst), 2);
    assert_eq!(cursors(&dst), (0, 0, 4), "hai lần steal phải nối tiếp nhau, không đè lên nhau");
    for i in 0..4u32
    {
        assert_eq!(peek(&dst, i), i);
    }
}

// --- Steal: lấy hụt phải nói rõ vì sao ---

#[test]
#[should_panic(expected = "greater than zero")]
fn t29_steal_voi_max_bang_khong_thi_panic()
{
    let src = Ring::<u32>::new(4);
    let dst = Ring::<u32>::new(4);
    let _ = src.try_steal_batch_to(0, &dst);
}

#[test]
fn t30_steal_tu_ring_rong_thi_bao_empty()
{
    let src = Ring::<u32>::new(4);
    let dst = Ring::<u32>::new(4);
    assert_eq!(src.try_steal_batch_to(4, &dst), Steal::Empty);
}

#[test]
fn t31_steal_khi_dich_day_thi_bao_busy_chu_khong_bao_empty()
{
    // Nguồn vẫn còn nguyên hàng, chỉ là đích không có chỗ nhận. Kẻ trộm cần phân biệt được hai
    // chuyện này để khỏi gạch tên một nạn nhân đang đầy việc.
    let src = Ring::new(4);
    let dst = Ring::new(2);
    for i in 0..4u32
    {
        assert_eq!(src.push(i), Ok(()));
    }
    for i in 0..2u32
    {
        assert_eq!(dst.push(100 + i), Ok(()));
    }

    assert_eq!(src.try_steal_batch_to(4, &dst), Steal::Busy);
    assert_eq!(cursors(&src), (0, 0, 4), "lần steal hụt không được đụng vào con trỏ nguồn");
}

#[test]
fn t32_steal_thanh_cong_thi_bao_dung_so_luong()
{
    let src = Ring::new(8);
    let dst = Ring::<u32>::new(8);
    for i in 0..5u32
    {
        assert_eq!(src.push(i), Ok(()));
    }

    assert_eq!(src.try_steal_batch_to(3, &dst), Steal::Success(3));
    assert_eq!(src.try_steal_batch_to(8, &dst), Steal::Success(2), "chỉ còn 2 phần tử thì lấy 2");
    assert_eq!(src.try_steal_batch_to(8, &dst), Steal::Empty);
    for i in 0..5u32
    {
        assert_eq!(peek(&dst, i), i, "thứ tự FIFO của nguồn phải được giữ nguyên");
    }
}

// --- try_steal_one: trộm một việc, cầm luôn giá trị về ---

#[test]
fn t33_steal_one_tren_ring_rong_thi_bao_empty()
{
    let ring = Ring::<u32>::new(4);
    assert_eq!(ring.try_steal_one(), Steal::Empty);
    assert_eq!(cursors(&ring), (0, 0, 0), "steal hụt không được đụng vào con trỏ");
}

#[test]
fn t34_steal_one_lay_phan_tu_cu_nhat_truoc()
{
    // Chủ ăn từ đầu `tail`, kẻ trộm ăn từ đầu `in_stealing`, nên hai bên không giẫm chân nhau.
    let ring = Ring::new(4);
    for i in 0..4u32
    {
        assert_eq!(ring.push(i), Ok(()));
    }

    assert_eq!(ring.try_steal_one(), Steal::Success(0));
    assert_eq!(ring.try_steal_one(), Steal::Success(1));
    assert_eq!(cursors(&ring), (2, 2, 4), "stolen và in_stealing phải nhích cùng nhịp");
    assert_eq!(ring.pop_lifo(), Some(3), "chủ vẫn lấy được phần tử mới nhất");
    assert_eq!(ring.try_steal_one(), Steal::Success(2));
    assert_eq!(ring.try_steal_one(), Steal::Empty);
}

#[test]
fn t35_steal_one_tra_lai_o_trong_cho_producer()
{
    let ring = Ring::new(2);
    assert_eq!(ring.push(1), Ok(()));
    assert_eq!(ring.push(2), Ok(()));
    assert_eq!(ring.push(3), Err(3), "ring đang đầy");

    assert_eq!(ring.try_steal_one(), Steal::Success(1));
    assert_eq!(ring.push(3), Ok(()), "trộm xong thì ô vừa trống phải dùng lại được");
    assert_eq!(ring.try_steal_one(), Steal::Success(2));
    assert_eq!(ring.try_steal_one(), Steal::Success(3));
}

// --- Consumer ---

#[test]
fn t36_consumer_pop_batch_lay_toi_da_muoi_phan_tu_mot_lan()
{
    let src = Ring::new(16);
    let dst = Ring::<u32>::new(16);
    for i in 0..16u32
    {
        assert_eq!(src.push(i), Ok(()));
    }

    let consumer = src.consumer(&dst);
    assert_eq!(consumer.pop_batch(), 10, "Consumer::pop_batch cố định trần ở 10");
    assert_eq!(src.cursor_data().filled_slots(), 6);
}

#[test]
fn t37_consumer_pop_batch_tren_ring_rong_thi_tra_ve_khong()
{
    let src = Ring::<u32>::new(16);
    let dst = Ring::<u32>::new(16);
    assert_eq!(src.consumer(&dst).pop_batch(), 0);
}

#[test]
fn t38_consumer_try_steal_noi_ro_ly_do_khi_lay_hut()
{
    let src = Ring::new(16);
    let dst = Ring::<u32>::new(16);
    assert_eq!(src.consumer(&dst).try_steal(), Steal::Empty, "nguồn cạn thì phải là Empty");

    for i in 0..4u32
    {
        assert_eq!(src.push(i), Ok(()));
    }
    assert_eq!(src.consumer(&dst).try_steal(), Steal::Success(4));

    let dst_day = Ring::<u32>::new(1);
    assert_eq!(dst_day.push(9), Ok(()));
    for i in 0..4u32
    {
        assert_eq!(src.push(i), Ok(()));
    }
    assert_eq!(src.consumer(&dst_day).try_steal(), Steal::Busy, "đích hết chỗ thì phải là Busy");
}

#[test]
fn t39_consumer_steal_one_lay_dung_mot_viec()
{
    let src = Ring::new(8);
    let dst = Ring::<u32>::new(8);
    for i in 0..3u32
    {
        assert_eq!(src.push(i), Ok(()));
    }

    let consumer = src.consumer(&dst);
    assert_eq!(consumer.steal_one(), Some(0));
    assert_eq!(consumer.try_steal_one(), Steal::Success(1));
    assert_eq!(cursors(&dst), (0, 0, 0), "steal_one không đi qua deque đích");
    assert_eq!(src.cursor_data().filled_slots(), 1);
    assert_eq!(consumer.steal_one(), Some(2));
    assert_eq!(consumer.steal_one(), None);
}

// --- Drop ---

struct DemHuy(Rc<Cell<usize>>);
impl Drop for DemHuy
{
    fn drop(&mut self)
    {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn t42_ring_bi_huy_thi_tha_cac_phan_tu_con_lai()
{
    // `SpmcRingBufferFifo` có `impl Drop` dọn khoảng `[in_stealing, tail)`, ring này thì chưa,
    // nên mọi phần tử chưa kịp lấy ra sẽ không bao giờ chạy destructor.
    let dem = Rc::new(Cell::new(0));
    {
        let ring = Ring::new(4);
        for _ in 0..3
        {
            assert!(ring.push(DemHuy(dem.clone())).is_ok());
        }
        assert_eq!(dem.get(), 0, "chưa huỷ ring thì chưa có gì bị thả");
    }
    assert_eq!(dem.get(), 3, "huỷ ring phải thả hết 3 phần tử còn sót");
}
