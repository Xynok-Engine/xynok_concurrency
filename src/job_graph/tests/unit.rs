use crate::pool::{Config, ThreadPool};

use std::sync::atomic::{AtomicUsize, Ordering};

/// Miri diễn giải từng lệnh một, nên số vòng lặp ở đây phải nhỏ hẳn khi chạy dưới nó.
const fn rounds() -> usize
{
    match cfg!(miri)
    {
        true => 2,
        false => 100,
    }
}

fn pool_with(threads: usize) -> ThreadPool
{
    ThreadPool::new(Config {
        threads: threads,
        ring_capacity: 16,
        thread_name: "graph-test".to_string(),
        ..Config::default()
    })
}

#[test]
fn t0_ke_phu_thuoc_chay_sau_thu_no_phu_thuoc()
{
    for threads in [0usize, 1, 3]
    {
        let pool = pool_with(threads);
        let ticket = AtomicUsize::new(0);

        for round in 0..rounds()
        {
            let first = AtomicUsize::new(usize::MAX);
            let second = AtomicUsize::new(usize::MAX);

            pool.scope(|s| {
                let a = s.spawn_with_handle(|| {
                    first.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                });
                s.spawn_after(&[&a], || {
                    second.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                });
            });

            assert!(
                first.load(Ordering::SeqCst) < second.load(Ordering::SeqCst),
                "{threads} thread, vòng {round}: kẻ phụ thuộc chạy trước"
            );
        }
    }
}

#[test]
fn t1_cho_du_moi_phu_thuoc_chu_khong_phai_cai_dau_tien()
{
    let pool = pool_with(4);

    for _ in 0..rounds()
    {
        let done = AtomicUsize::new(0);
        let seen_by_last = AtomicUsize::new(usize::MAX);

        pool.scope(|s| {
            let deps: Vec<_> = (0..8)
                .map(|_| {
                    s.spawn_with_handle(|| {
                        done.fetch_add(1, Ordering::SeqCst);
                    })
                })
                .collect();

            let borrowed: Vec<&_> = deps.iter().collect();
            s.spawn_after(&borrowed, || {
                seen_by_last.store(done.load(Ordering::SeqCst), Ordering::SeqCst);
            });
        });

        assert_eq!(seen_by_last.load(Ordering::SeqCst), 8, "job cuối chạy khi chưa đủ 8 phụ thuộc");
    }
}

#[test]
fn t2_phu_thuoc_da_xong_thi_khong_phai_cho()
{
    let pool = pool_with(2);
    let done = AtomicUsize::new(0);

    pool.scope(|s| {
        let first = s.spawn_with_handle(|| {
            done.fetch_add(1, Ordering::SeqCst);
        });

        // Cố tình để phụ thuộc kịp xong hẳn trước khi có ai bám vào nó.
        std::thread::sleep(std::time::Duration::from_millis(20));

        s.spawn_after(&[&first], || {
            done.fetch_add(1, Ordering::SeqCst);
        });
    });

    assert_eq!(done.load(Ordering::SeqCst), 2);
}

#[test]
fn t3_mot_chuoi_dai_van_giu_dung_thu_tu()
{
    let length = match cfg!(miri)
    {
        true => 4,
        false => 64,
    };

    let pool = pool_with(4);
    let order: Vec<AtomicUsize> = (0..length).map(|_| AtomicUsize::new(usize::MAX)).collect();
    let ticket = AtomicUsize::new(0);

    pool.scope(|s| {
        let mut previous = s.spawn_with_handle(|| {
            order[0].store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
        });

        for step in 1..length
        {
            let order = &order;
            let ticket = &ticket;
            previous = s.spawn_after(&[&previous], move || {
                order[step].store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
            });
        }
    });

    let stamps: Vec<usize> = order.iter().map(|slot| slot.load(Ordering::SeqCst)).collect();
    assert_eq!(stamps, (0..length).collect::<Vec<_>>(), "chuỗi phụ thuộc chạy sai thứ tự");
}

#[test]
fn t4_hinh_kim_cuong_chay_dung()
{
    let pool = pool_with(4);

    for _ in 0..rounds()
    {
        let ticket = AtomicUsize::new(0);
        let root = AtomicUsize::new(usize::MAX);
        let left = AtomicUsize::new(usize::MAX);
        let right = AtomicUsize::new(usize::MAX);
        let leaf = AtomicUsize::new(usize::MAX);

        pool.scope(|s| {
            let a = s.spawn_with_handle(|| {
                root.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
            });
            let b = s.spawn_after(&[&a], || {
                left.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
            });
            let c = s.spawn_after(&[&a], || {
                right.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
            });
            s.spawn_after(&[&b, &c], || {
                leaf.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
            });
        });

        let (a, b, c, d) = (
            root.load(Ordering::SeqCst),
            left.load(Ordering::SeqCst),
            right.load(Ordering::SeqCst),
            leaf.load(Ordering::SeqCst),
        );
        assert!(a < b && a < c, "hai nhánh giữa chạy trước gốc");
        assert!(b < d && c < d, "nút cuối chạy trước một nhánh giữa");
    }
}

/// Thả tay cầm cuối cùng của pool ngay sau khi scope trả về.
///
/// Đây là kịch bản mà miri bắt được với đúng bốn chữ "trying to join itself". Trình tự: job của
/// một nút chạy xong và thả cái vé latch của nó, scope thấy bộ đếm về 0 nên trả về, thread gọi
/// thả nốt tay cầm pool, và worker thì vẫn đang ở trong `Node::run` thêm một nhịp nữa để dọn
/// danh sách kế nhiệm. Nếu nút giữ một `ThreadPool` thay vì phần dùng chung của nó, cái `Arc`
/// mà worker thả ở nhịp ấy là tay cầm cuối cùng, và `Drop` của nó đi join chính worker đang
/// chạy dòng đó.
#[test]
fn t5_tha_pool_ngay_sau_scope_khong_lam_worker_join_chinh_no()
{
    for _ in 0..rounds()
    {
        let pool = pool_with(2);
        let done = AtomicUsize::new(0);

        pool.scope(|s| {
            let first = s.spawn_with_handle(|| {});
            let second = s.spawn_after(&[&first], || {});
            s.spawn_after(&[&second], || {
                done.fetch_add(1, Ordering::SeqCst);
            });
        });

        assert_eq!(done.load(Ordering::SeqCst), 1);
        drop(pool);
    }
}

#[test]
fn t6_phu_thuoc_panic_thi_ke_ke_nhiem_van_duoc_tha_ra()
{
    let pool = pool_with(2);
    let ran = AtomicUsize::new(0);

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.scope(|s| {
            let broken = s.spawn_with_handle(|| panic!("phụ thuộc chết"));
            s.spawn_after(&[&broken], || {
                ran.fetch_add(1, Ordering::SeqCst);
            });
        });
    }));

    std::panic::set_hook(previous);
    assert!(outcome.is_err(), "panic của phụ thuộc không tới được điểm join");
    assert_eq!(ran.load(Ordering::SeqCst), 1, "kẻ kế nhiệm bị bỏ rơi, scope này lẽ ra đã treo");
}
