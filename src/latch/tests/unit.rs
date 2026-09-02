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
