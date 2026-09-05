//! Loom cho ring SPSC.
//!
//! Cấu trúc này không có CAS nào cả, nên chỗ duy nhất có thể sai là thứ tự bộ nhớ: người đọc thấy
//! `tail` mới mà chưa thấy nội dung ô, hoặc người ghi thấy `head` mới rồi đè lên ô mà người đọc còn
//! đang đọc. Loại lỗi đó gần như không bao giờ lộ ra trên x86 và lộ ra rất hiếm trên ARM, nên nó
//! đúng là thứ phải để loom kiểm chứ không phải để stress test đoán.
//!
//! Mô hình cố tình bé: ring hai ô, hai tới ba phần tử. Ring bé thì mọi lần push đều chạm biên và
//! phải đợi người đọc nhả chỗ, tức là đúng cái nhánh đáng ngờ, mà số interleaving vẫn hữu hạn.

use loom::sync::Arc;
use loom::thread;

use crate::ring_buffer_spsc::RingBufferSpsc;

/// Người đọc phải thấy đủ mọi phần tử, đúng thứ tự.
#[test]
fn t0_khong_mat_phan_tu_va_khong_dao_thu_tu()
{
    loom::model(|| {
        const TOTAL: u32 = 3;

        let ring = Arc::new(RingBufferSpsc::<u32>::new(2));

        let writer = Arc::clone(&ring);
        let sender = thread::spawn(move || {
            // Safety: đây là `Sender` duy nhất, và nó không rời thread này.
            let mut tx = unsafe { writer.sender() };
            let mut next = 0;
            while next < TOTAL
            {
                match tx.push(next)
                {
                    Ok(()) => next += 1,
                    Err(_) => thread::yield_now(),
                }
            }
        });

        // Safety: đây là `Receiver` duy nhất, và nó không rời thread này.
        let mut rx = unsafe { ring.receiver() };
        let mut got = Vec::new();
        while (got.len() as u32) < TOTAL
        {
            match rx.pop()
            {
                Some(val) => got.push(val),
                None => thread::yield_now(),
            }
        }

        sender.join().unwrap();
        assert_eq!(got, (0..TOTAL).collect::<Vec<_>>());
    });
}

/// Nội dung ô phải hiện ra cùng lúc với `tail`, không được đến sau.
///
/// Mỗi phần tử mang một dấu cố định ở nửa cao và chỉ số ở nửa thấp, nên đọc phải một ô chưa ghi
/// xong là lộ ra ngay, chứ không lẫn vào một giá trị hợp lệ nào khác.
#[test]
fn t1_noi_dung_o_hien_ra_cung_luc_voi_chi_so()
{
    loom::model(|| {
        const TOTAL: u64 = 2;
        const MARK: u64 = 0xA5A5_0000;

        let ring = Arc::new(RingBufferSpsc::<u64>::new(2));

        let writer = Arc::clone(&ring);
        let sender = thread::spawn(move || {
            // Safety: `Sender` duy nhất, không rời thread này.
            let mut tx = unsafe { writer.sender() };
            for i in 0..TOTAL
            {
                while tx.push(MARK | i).is_err()
                {
                    thread::yield_now();
                }
            }
        });

        // Safety: `Receiver` duy nhất, không rời thread này.
        let mut rx = unsafe { ring.receiver() };
        let mut seen = 0u64;
        while seen < TOTAL
        {
            match rx.pop()
            {
                Some(val) =>
                {
                    assert_eq!(val & 0xFFFF_0000, MARK, "đọc phải một ô chưa ghi xong");
                    assert_eq!(val & 0xFFFF, seen, "phần tử tới sai thứ tự");
                    seen += 1;
                }
                None => thread::yield_now(),
            }
        }

        sender.join().unwrap();
    });
}

/// Người ghi không được đè lên ô mà người đọc chưa lấy xong.
///
/// Ring hai ô và bốn phần tử: người ghi buộc phải quay lại đúng những ô cũ, nên nếu nó đọc `head`
/// quá sớm thì nó ghi đè, và người đọc sẽ thấy một giá trị của tương lai.
#[test]
fn t2_nguoi_ghi_khong_de_len_o_nguoi_doc_chua_lay()
{
    loom::model(|| {
        const TOTAL: u32 = 4;

        let ring = Arc::new(RingBufferSpsc::<u32>::new(2));

        let writer = Arc::clone(&ring);
        let sender = thread::spawn(move || {
            // Safety: `Sender` duy nhất, không rời thread này.
            let mut tx = unsafe { writer.sender() };
            let mut next = 0;
            while next < TOTAL
            {
                match tx.push(next)
                {
                    Ok(()) => next += 1,
                    Err(_) => thread::yield_now(),
                }
            }
        });

        // Safety: `Receiver` duy nhất, không rời thread này.
        let mut rx = unsafe { ring.receiver() };
        let mut expected = 0;
        while expected < TOTAL
        {
            match rx.pop()
            {
                Some(val) =>
                {
                    assert_eq!(val, expected, "một ô bị ghi đè trước khi người đọc lấy xong");
                    expected += 1;
                }
                None => thread::yield_now(),
            }
        }

        sender.join().unwrap();
    });
}
