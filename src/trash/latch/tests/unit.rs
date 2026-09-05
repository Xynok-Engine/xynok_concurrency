use crate::latch::Latch;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pool::{Config, ThreadPool};

#[test]
fn t0_cho_toi_khi_moi_ve_deu_da_duoc_tha()
{
    const JOBS: usize = 1_000;

    let pool = ThreadPool::new(Config {
        threads: 3,
        ..Config::default()
    });
    let latch = Latch::new(0);
    let done = Arc::new(AtomicUsize::new(0));

    for _ in 0..JOBS
    {
        let ticket = latch.ticket();
        let done = Arc::clone(&done);
        pool.spawn(move || {
            done.fetch_add(1, Ordering::Relaxed);
            drop(ticket);
        });
    }

    latch.wait_in(&pool);
    assert_eq!(latch.remaining(), 0);
    assert_eq!(done.load(Ordering::Acquire), JOBS);
}

#[test]
fn t1_ve_van_bao_ve_khi_job_panic()
{
    let pool = ThreadPool::new(Config {
        threads: 2,
        ..Config::default()
    });
    let latch = Latch::new(0);

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    for _ in 0..16
    {
        let ticket = latch.ticket();
        pool.spawn(move || {
            let _ticket = ticket;
            panic!("job này chết giữa chừng");
        });
    }

    latch.wait_in(&pool);
    std::panic::set_hook(previous);
    assert_eq!(latch.remaining(), 0, "một job panic đã mang cái vé của nó xuống mồ");
}

#[test]
fn t2_latch_rong_thi_khong_cho_gi_ca()
{
    let latch = Latch::new(0);
    assert!(latch.is_done());
    latch.wait();
}

#[test]
fn t3_thread_ngoai_pool_thi_ngu_cho()
{
    let pool = ThreadPool::new(Config {
        threads: 2,
        ..Config::default()
    });
    let latch = Latch::new(0);
    let done = Arc::new(AtomicUsize::new(0));

    for _ in 0..64
    {
        let ticket = latch.ticket();
        let done = Arc::clone(&done);
        pool.spawn(move || {
            done.fetch_add(1, Ordering::Relaxed);
            drop(ticket);
        });
    }

    // Không chạy giúp ai cả, chỉ nằm chờ. Vé cuối cùng phải gọi được thread này dậy.
    latch.wait();
    assert_eq!(done.load(Ordering::Acquire), 64);
}

#[test]
fn t4_cho_nham_thread_thi_panic()
{
    let latch = Arc::new(Latch::new(1));
    let foreign = Arc::clone(&latch);
    let outcome = std::thread::spawn(move || foreign.wait()).join();

    assert!(outcome.is_err(), "chờ trên thread lạ mà vẫn trả về");
    drop(latch.ticket());
    // Bù lại cái vé vừa thả, để latch về đúng 0 rồi mới bỏ đi.
    assert_eq!(latch.remaining(), 1);
}

#[test]
fn t5_huy_thi_job_chua_chay_bo_qua_phan_than()
{
    const JOBS: usize = 256;

    let pool = ThreadPool::new(Config {
        threads: 3,
        ..Config::default()
    });
    let latch = Latch::new(0);
    let done = Arc::new(AtomicUsize::new(0));

    // Huỷ trước khi phát việc, nên không job nào kịp thấy cờ ở trạng thái tắt.
    assert!(latch.cancel(), "lần bật cờ đầu tiên phải trả về true");

    for _ in 0..JOBS
    {
        let ticket = latch.ticket();
        let done = Arc::clone(&done);
        pool.spawn(move || {
            if ticket.is_cancelled()
            {
                return;
            }
            done.fetch_add(1, Ordering::Relaxed);
        });
    }

    latch.wait_in(&pool);
    assert_eq!(done.load(Ordering::Acquire), 0, "job vẫn chạy phần thân dù nhóm việc đã bị huỷ");
    // Bỏ qua phần thân không có nghĩa là bỏ luôn cái vé. Người chờ vẫn phải đếm đủ.
    assert_eq!(latch.remaining(), 0);
}

#[test]
fn t6_chi_mot_nguoi_thang_lan_bat_co()
{
    let latch = Latch::new(0);

    assert!(!latch.is_cancelled());
    assert!(latch.cancel(), "người bật cờ đầu tiên phải nhận true");
    assert!(!latch.cancel(), "người tới sau mà cũng nhận true thì lý do huỷ sẽ bị ghi đè");
    assert!(latch.is_cancelled());
}

#[test]
fn t7_huy_khong_lam_lenh_cho_tra_ve_som()
{
    let latch = Latch::new(0);
    let ticket = latch.ticket();

    latch.cancel();
    assert!(
        !latch.is_done(),
        "vé còn sống mà latch đã báo xong, người chờ sẽ bỏ đi trong lúc vé vẫn trỏ vào stack của nó"
    );

    drop(ticket);
    assert!(latch.is_done());
    latch.wait();
}
