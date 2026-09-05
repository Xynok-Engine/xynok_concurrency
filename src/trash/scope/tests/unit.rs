use crate::scope::ParamsParReduce;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::pool::{Config, ThreadPool};

/// Miri diễn giải từng lệnh một, nên mọi con số lớn ở đây đều chia cho hằng này khi chạy dưới nó.
#[cfg(miri)]
const SCALE: usize = 100;
#[cfg(not(miri))]
const SCALE: usize = 1;

const fn scaled(n: usize) -> usize
{
    if n / SCALE == 0 { 1 } else { n / SCALE }
}

fn pool_with(threads: usize) -> ThreadPool
{
    ThreadPool::new(Config {
        threads: threads,
        thread_name: "scope-test".to_string(),
        ..Config::default()
    })
}

#[test]
fn t0_scope_cho_moi_job_no_spawn_ra()
{
    let pool = pool_with(4);
    let mut totals = [0usize; 64];

    pool.scope(|s| {
        for (i, slot) in totals.iter_mut().enumerate()
        {
            s.spawn(move || *slot = i * i);
        }
    });

    for (i, total) in totals.iter().enumerate()
    {
        assert_eq!(*total, i * i);
    }
}

#[test]
fn t1_job_muon_duoc_stack_cua_nguoi_mo_scope()
{
    let pool = pool_with(3);
    let data: Vec<usize> = (0..scaled(1_000)).collect();
    let sum = AtomicUsize::new(0);

    pool.scope(|s| {
        for chunk in data.chunks(64)
        {
            let sum = &sum;
            s.spawn(move || {
                sum.fetch_add(chunk.iter().sum::<usize>(), Ordering::Relaxed);
            });
        }
    });

    assert_eq!(sum.load(Ordering::Acquire), data.iter().sum::<usize>());
}

#[test]
fn t2_scope_long_nhau_khong_deadlock()
{
    // Bốn job, mỗi job lại mở scope riêng. Nếu thread đang chờ mà ngủ thay vì chạy job giúp, đây
    // đúng là chỗ cả pool nằm chờ lẫn nhau.
    let pool = pool_with(4);
    let done = AtomicUsize::new(0);

    pool.scope(|outer| {
        for _ in 0..4
        {
            let pool = &pool;
            let done = &done;
            outer.spawn(move || {
                pool.scope(|inner| {
                    for _ in 0..16
                    {
                        inner.spawn(move || {
                            done.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                });
            });
        }
    });

    assert_eq!(done.load(Ordering::Acquire), 64);
}

#[test]
fn t3_scope_long_nhau_ba_tang_tren_mot_worker()
{
    // Một worker duy nhất và ba tầng join lồng nhau: mọi tầng đều phải tự chạy phần việc của mình
    // trong lúc chờ, không thì treo.
    let pool = pool_with(1);
    let done = AtomicUsize::new(0);

    pool.scope(|a| {
        for _ in 0..2
        {
            let pool = &pool;
            let done = &done;
            a.spawn(move || {
                pool.scope(|b| {
                    for _ in 0..2
                    {
                        b.spawn(move || {
                            pool.scope(|c| {
                                for _ in 0..2
                                {
                                    c.spawn(move || {
                                        done.fetch_add(1, Ordering::Relaxed);
                                    });
                                }
                            });
                        });
                    }
                });
            });
        }
    });

    assert_eq!(done.load(Ordering::Acquire), 8);
}

#[test]
fn t4_panic_trong_job_duoc_nem_lai_o_diem_join()
{
    let pool = pool_with(2);
    let done = Arc::new(AtomicUsize::new(0));

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let counter = Arc::clone(&done);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.scope(|s| {
            for i in 0..32
            {
                let counter = Arc::clone(&counter);
                s.spawn(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                    if i == 7
                    {
                        panic!("job số bảy");
                    }
                });
            }
        });
    }));

    std::panic::set_hook(previous);

    let payload = outcome.expect_err("panic của job không tới được điểm join");
    let message = payload.downcast_ref::<&str>().copied().unwrap_or_default();
    assert_eq!(message, "job số bảy");
    // Mọi job anh em vẫn phải chạy xong trước khi panic được ném lại.
    assert_eq!(done.load(Ordering::Acquire), 32);
}

#[test]
fn t5_panic_cua_than_scope_van_cho_job_chay_xong()
{
    let pool = pool_with(2);
    let done = Arc::new(AtomicUsize::new(0));

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let counter = Arc::clone(&done);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.scope(|s| {
            for _ in 0..16
            {
                let counter = Arc::clone(&counter);
                s.spawn(move || {
                    std::thread::yield_now();
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }
            panic!("thân scope chết");
        });
    }));

    std::panic::set_hook(previous);
    assert!(outcome.is_err());
    assert_eq!(done.load(Ordering::Acquire), 16, "scope panic mà bỏ mặc job đang mượn stack của nó");
}

#[test]
fn t6_join_tra_ve_ca_hai_ket_qua()
{
    let pool = pool_with(2);
    let (left, right) = pool.join(|| (1..=50u64).sum::<u64>(), || (51..=100u64).sum::<u64>());
    assert_eq!(left + right, 5050);
}

#[test]
fn t7_join_long_nhau_thanh_cay_de_quy()
{
    fn sum(pool: &ThreadPool, range: std::ops::Range<u64>) -> u64
    {
        let width = range.end - range.start;
        if width <= 64
        {
            return range.sum();
        }

        let middle = range.start + width / 2;
        let (left, right) = pool.join(|| sum(pool, range.start..middle), || sum(pool, middle..range.end));
        left + right
    }

    let pool = pool_with(4);
    let n = scaled(10_000) as u64;
    assert_eq!(sum(&pool, 0..n), (0..n).sum::<u64>());
}

#[test]
fn t8_parallel_for_cham_moi_chi_so_dung_mot_lan()
{
    let n = scaled(10_000);

    let pool = pool_with(4);
    let seen: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();

    pool.parallel_for(n, 64, |i| {
        seen[i].fetch_add(1, Ordering::Relaxed);
    });

    let wrong: Vec<usize> = (0..n).filter(|&i| seen[i].load(Ordering::Acquire) != 1).collect();
    assert!(wrong.is_empty(), "{} chỉ số sai, ví dụ {:?}", wrong.len(), &wrong[..wrong.len().min(8)]);
}

#[test]
fn t9_batch_lon_hon_n_thi_chay_tuan_tu_ngay_tai_cho()
{
    let pool = pool_with(4);
    let here = std::thread::current().id();
    let elsewhere = AtomicUsize::new(0);

    pool.parallel_for(scaled(100), 1_000, |_| {
        if std::thread::current().id() != here
        {
            elsewhere.fetch_add(1, Ordering::Relaxed);
        }
    });

    assert_eq!(elsewhere.load(Ordering::Acquire), 0, "batch >= n mà vẫn spawn job");
}

#[test]
fn t10_parallel_for_tren_pool_khong_worker_van_chay_du()
{
    let pool = ThreadPool::new(Config::inline());
    let seen: Vec<AtomicUsize> = (0..scaled(500)).map(|_| AtomicUsize::new(0)).collect();

    pool.parallel_for(scaled(500), 16, |i| {
        seen[i].fetch_add(1, Ordering::Relaxed);
    });

    assert!(seen.iter().all(|slot| slot.load(Ordering::Acquire) == 1));
}

#[test]
fn t11_par_reduce_cong_dung_tong()
{
    let pool = pool_with(4);
    let n = scaled(10_000);
    let total = pool.par_reduce(ParamsParReduce {
        n:        n,
        batch:    64,
        identity: || 0u64,
        fold:     |acc, i| acc + i as u64,
        join:     |a, b| a + b,
    });
    assert_eq!(total, (0..n as u64).sum::<u64>());
}

#[test]
fn t12_par_reduce_noi_theo_thu_tu_lo_nen_ket_qua_lap_lai_duoc()
{
    let pool = pool_with(4);

    // Phép nối không giao hoán: kết quả chỉ giữ nguyên nếu thứ tự nối là cố định.
    let n = scaled(1_000);
    let first = pool.par_reduce(ParamsParReduce {
        n:        n,
        batch:    16,
        identity: String::new,
        fold:     |mut acc: String, i: usize| {
            acc.push_str(&(i % 10).to_string());
            acc
        },
        join:     |a: String, b: String| a + &b,
    });

    for _ in 0..scaled(20)
    {
        let again = pool.par_reduce(ParamsParReduce {
            n:        n,
            batch:    16,
            identity: String::new,
            fold:     |mut acc: String, i: usize| {
                acc.push_str(&(i % 10).to_string());
                acc
            },
            join:     |a: String, b: String| a + &b,
        });
        assert_eq!(again, first, "par_reduce đổi kết quả giữa hai lần chạy");
    }

    let expected: String = (0..n as u32).map(|i| char::from_digit(i % 10, 10).unwrap()).collect();
    assert_eq!(first, expected);
}

#[test]
fn t13_scope_rong_khong_lam_gi_ca()
{
    let pool = pool_with(2);
    let value = pool.scope(|_| 42);
    assert_eq!(value, 42);
}

#[test]
fn t14_scope_tra_ve_gia_tri_cua_than_no()
{
    let pool = pool_with(2);
    let value = pool.scope(|s| {
        let mut side = 0;
        s.spawn(|| {});
        side += 1;
        side
    });
    assert_eq!(value, 1);
}

#[test]
fn t15_job_trong_scope_spawn_tiep_vao_chinh_scope_do()
{
    // `&Scope` phải đi được vào trong job, nếu không thì mọi thuật toán chia đôi đệ quy đều phải mở
    // một scope mới ở mỗi tầng, và mỗi scope mới là một điểm join nữa.
    let pool = pool_with(4);
    let done = AtomicUsize::new(0);

    pool.scope(|s| {
        for _ in 0..8
        {
            let done = &done;
            s.spawn_with(move |inner| {
                for _ in 0..8
                {
                    inner.spawn(move || {
                        done.fetch_add(1, Ordering::Relaxed);
                    });
                }
            });
        }
    });

    assert_eq!(done.load(Ordering::Acquire), 64, "job cháu bị bỏ rơi hoặc scope trả về quá sớm");
}

#[test]
fn t16_scope_tren_pool_dung_chung_giua_nhieu_thread()
{
    // Hai thread ngoài cùng mở scope trên một pool: chúng không được nhìn thấy job của nhau, và
    // không cái nào được trả về sớm vì job của cái kia còn chạy.
    let pool = Arc::new(pool_with(3));
    let done = Arc::new(AtomicUsize::new(0));

    let threads: Vec<_> = (0..2)
        .map(|_| {
            let pool = Arc::clone(&pool);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                pool.scope(|s| {
                    for _ in 0..scaled(100)
                    {
                        let done = Arc::clone(&done);
                        s.spawn(move || {
                            done.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                });
                // Ngay khi scope của mình trả về, phần của mình đã chạy xong hết.
                assert!(done.load(Ordering::Acquire) >= scaled(100));
            })
        })
        .collect();

    for thread in threads
    {
        thread.join().expect("một thread panic");
    }
    assert_eq!(done.load(Ordering::Acquire), scaled(100) * 2);
}

#[test]
fn t17_tha_pool_ngay_sau_scope_long_nhau_khong_lam_worker_join_chinh_no()
{
    // Cùng một cái bẫy với `job_graph`: một scope mở **bên trong** một job sẽ thả tay cầm của nó
    // trên chính thread worker. Nếu scope giữ một `ThreadPool` thay vì phần dùng chung của pool,
    // cái tay cầm ấy có thể là cái cuối cùng, và worker sẽ đi join chính nó.
    for _ in 0..scaled(50)
    {
        let pool = pool_with(2);
        let done = AtomicUsize::new(0);

        pool.scope(|outer| {
            let pool = &pool;
            let done = &done;
            outer.spawn(move || {
                pool.scope(|inner| {
                    inner.spawn(move || {
                        done.fetch_add(1, Ordering::Relaxed);
                    });
                });
            });
        });

        assert_eq!(done.load(Ordering::Acquire), 1);
        drop(pool);
    }
}
