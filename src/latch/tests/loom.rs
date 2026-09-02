use loom::sync::Arc;
use loom::thread;

use crate::latch::Latch;

/// Mọi interleaving của hai job và một người chờ đều phải kết thúc bằng việc `wait` trả về.
///
/// Đây là test bắt được lỗi mất wake-up nếu vé và người chờ không thống nhất được ai chịu trách
/// nhiệm đọc lại bộ đếm. Nó cũng là chỗ dựa cho `Scope`, vốn chỉ là latch này cộng một cách phát
/// closure ra pool.
#[test]
fn t0_cho_luon_ket_thuc_du_thu_tu_the_nao()
{
    loom::model(|| {
        let latch = Arc::new(Latch::new(0));

        let tickets: Vec<_> = (0..2).map(|_| latch.ticket()).collect();
        let handles: Vec<_> = tickets
            .into_iter()
            .map(|ticket| {
                thread::spawn(move || {
                    drop(ticket);
                })
            })
            .collect();

        latch.wait();
        assert_eq!(latch.remaining(), 0);

        for handle in handles
        {
            handle.join().unwrap();
        }
    });
}

/// Vé cuối cùng phải gọi được người chờ dậy kể cả khi nó thả **trước** lúc người kia kịp ngủ.
#[test]
fn t1_ve_tha_truoc_khi_nguoi_cho_kip_ngu_thi_khong_mat()
{
    loom::model(|| {
        let latch = Arc::new(Latch::new(0));
        let ticket = latch.ticket();

        let worker = thread::spawn(move || {
            drop(ticket);
        });

        // Nhường lượt trước khi chờ, để loom thử cả cảnh vé về đích trước lẫn cảnh nó về sau.
        thread::yield_now();
        latch.wait();

        worker.join().unwrap();
        assert!(latch.is_done());
    });
}
