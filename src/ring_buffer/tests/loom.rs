//! Loom test: duyệt **mọi** thứ tự đan xen của một mô hình bé. Đây là chỗ bắt lỗi `Ordering`
//! và lỗi gỡ biển sớm — những thứ stress test chạy cả ngày cũng có thể không đụng tới.

use super::*;

/// Chủ ring buffer và một kẻ trộm cùng nhắm một ring buffer có **hai** job — ca hẹp nhất mà cả hai đều có
/// thể thắng, và cũng là ca duy nhất mà một `Ordering` sai sẽ lộ ra.
#[test]
fn chu_va_trom_cung_an_mot_dau_khong_ai_lay_trung()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBuffer::<u32>::new(2));

        // SAFETY: đúng một `Producer` được dựng, và nó ở lại thread này.
        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();
        tx.push(2).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || {
            let rx = other.consumer();
            let mut got = Vec::new();
            rx.steal_half(&mut got);
            got
        });

        let mut mine = Vec::new();
        tx.pop_batch(&mut mine, 2);

        mine.extend(thief.join().unwrap());
        mine.sort_unstable();
        assert_eq!(mine, vec![1, 2], "hai job, không được mất và không được nhân đôi");
    });
}

/// Người ghi và kẻ trộm chạy song song trên một ring buffer rỗng: kiểm tra cặp `Release`/`Acquire`
/// giữa `tail` và `head` — kẻ trộm thấy chỉ số mới thì phải thấy cả job đằng sau nó.
#[test]
fn trom_thay_tail_moi_thi_thay_ca_job()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBuffer::<u32>::new(2));

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || {
            let rx = other.consumer();
            rx.steal()
        });

        // SAFETY: đúng một `Producer`.
        let mut tx = unsafe { ring.producer() };
        tx.push(7).unwrap();

        match thief.join().unwrap()
        {
            Some(val) => assert_eq!(val, 7),
            None => assert_eq!(tx.pop(), Some(7), "trộm không kịp thì job phải còn nguyên"),
        }
    });
}
/// Ca mà cả module này xoay quanh: **gỡ biển sớm**.
///
/// Kẻ trộm nhận ô 0, và chủ ring buffer lấy nốt ô 1 rồi lập tức muốn đặt job mới — mà ô trống duy
/// nhất lúc đó chính là ô 0. Chừng nào `steal` còn nằm yên thì `push` thấy ring buffer đầy và không
/// đụng vào. Đổi thứ tự `release()` lên trước vòng chép là loom báo "concurrent read and write
/// accesses" ngay tại đây — thứ mà stress test chạy cả ngày cũng chưa chắc bắt được.
#[test]
fn chu_khong_ghi_de_len_o_ke_trom_dang_be()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBuffer::<u32>::new(2));

        // SAFETY: đúng một `Producer`, ở lại thread này.
        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();
        tx.push(2).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || {
            let rx = other.consumer();
            let mut got = Vec::new();
            rx.steal_half(&mut got);
            got
        });

        let mut mine = Vec::new();
        tx.pop_batch(&mut mine, 2);

        // Thử vài nhịp thay vì quay vòng vô hạn — loom không kết thúc được một vòng lặp bận.
        for _ in 0..2
        {
            if tx.push(3).is_ok()
            {
                break;
            }
            loom::thread::yield_now();
        }

        mine.extend(thief.join().unwrap());
        mine.sort_unstable();
        // Tuỳ nhịp mà kẻ trộm có bốc được cả job `3` vừa đặt hay không — cả hai đều hợp lệ.
        // Điều **không** được phép là thiếu `1`/`2` hoặc có một số xuất hiện hai lần.
        assert!(
            mine == vec![1, 2] || mine == vec![1, 2, 3],
            "job cũ không được mất, không được nhân đôi: {mine:?}"
        );
    });
}
