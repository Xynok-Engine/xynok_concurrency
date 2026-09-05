use crate::per_worker::PerWorker;
use crate::pool::ThreadPool;

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pool::Config;

fn pool_with(threads: usize) -> ThreadPool
{
    ThreadPool::new(Config {
        threads: threads,
        ..Config::default()
    })
}

#[test]
fn t0_moi_o_duoc_dung_bang_chi_so_cua_no()
{
    let mut per = PerWorker::with_len(4, |i| i * 10);
    assert_eq!(per.len(), 4);
    assert_eq!(per.iter_mut().map(|slot| *slot).collect::<Vec<_>>(), vec![0, 10, 20, 30]);
}

#[test]
fn t1_moi_worker_ghi_vao_o_cua_rieng_no()
{
    const JOBS: usize = 5_000;

    let pool = pool_with(4);
    let mut counts: PerWorker<usize> = PerWorker::for_pool(&pool, |_| 0);
    assert_eq!(counts.len(), pool.worker_count());

    pool.parallel_for(JOBS, 16, |_| {
        counts.with_mut(&pool, |n| *n += 1);
    });

    // Gộp theo thứ tự ô, không theo thứ tự ai xong trước.
    assert_eq!(counts.iter_mut().map(|n| *n).sum::<usize>(), JOBS);
}

#[test]
#[should_panic(expected = "đang được chính thread này mượn")]
fn t2_muon_hai_lan_cung_mot_o_thi_panic()
{
    let pool = pool_with(1);
    let per: PerWorker<usize> = PerWorker::for_pool(&pool, |_| 0);

    per.with_mut(&pool, |_| {
        per.with_mut(&pool, |_| {});
    });
}

#[test]
#[should_panic(expected = "nằm ngoài một PerWorker")]
fn t3_chi_so_ngoai_khoang_thi_panic()
{
    let per: PerWorker<usize> = PerWorker::with_len(2, |_| 0);
    per.with(5, |_| {});
}

#[test]
fn t4_for_each_unchecked_bat_duoc_o_dang_muon()
{
    let pool = pool_with(1);
    let per: PerWorker<usize> = PerWorker::for_pool(&pool, |_| 0);

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        per.with_mut(&pool, |_| unsafe {
            per.for_each_unchecked(|_| {});
        });
    }));

    assert!(outcome.is_err(), "reset trong lúc một ô còn đang được mượn mà vẫn trót lọt");
}

#[test]
fn t5_du_lieu_khong_sync_van_qua_duoc()
{
    // `Cell` là `Send` nhưng không `Sync`: đúng loại dữ liệu mà `PerWorker` phải mang được, vì
    // mỗi ô chỉ có một thread chạm vào.
    let pool = pool_with(2);
    let mut cells: PerWorker<std::cell::Cell<usize>> = PerWorker::for_pool(&pool, |_| std::cell::Cell::new(0));
    let total = AtomicUsize::new(0);

    pool.parallel_for(1_000, 32, |_| {
        cells.with_mut(&pool, |cell| cell.set(cell.get() + 1));
    });

    for cell in cells.iter_mut()
    {
        total.fetch_add(cell.get(), Ordering::Relaxed);
    }
    assert_eq!(total.load(Ordering::Acquire), 1_000);
}
