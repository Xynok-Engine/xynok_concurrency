use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::time::Duration;

use super::*;

/// Chạy `body` trên thread riêng, để một lần treo thành lỗi test thay vì treo cả lần chạy.
///
/// Mọi thứ ở đây đều có thể deadlock nếu giao thức ngủ sai, và một test treo thì không nói được gì
/// ngoài việc "có gì đó hỏng", còn ở đây thì ít nhất biết là cái nào.
fn with_watchdog<F>(name: &'static str, body: F)
where F: FnOnce() + Send + 'static
{
    let (tx, rx) = channel::<()>();
    let scenario = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            body();
            let _ = tx.send(());
        })
        .expect("không spawn được thread cho kịch bản test");

    match rx.recv_timeout(Duration::from_secs(60))
    {
        Ok(()) | Err(RecvTimeoutError::Disconnected) =>
        {
            if let Err(payload) = scenario.join()
            {
                std::panic::resume_unwind(payload);
            }
        }
        Err(RecvTimeoutError::Timeout) => panic!("`{name}` treo quá 60 giây: nhiều khả năng là một lời đánh thức bị mất"),
    }
}

/// Miri diễn giải từng lệnh một, nên số vòng chạy vài giây trên máy thật sẽ chạy hàng giờ ở đó.
/// Mọi con số lớn trong file này đều chia cho hằng này.
#[cfg(miri)]
const SCALE: usize = 200;
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
        thread_name: "test-worker".to_string(),
        ..Config::default()
    })
}

#[test]
fn moi_job_deu_chay_dung_mot_lan()
{
    with_watchdog("moi_job_deu_chay_dung_mot_lan", || {
        let jobs = scaled(10_000);

        let pool = pool_with(4);
        let done = Arc::new(AtomicUsize::new(0));

        for _ in 0..jobs
        {
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Release);
            });
        }

        pool.run_until(|| done.load(Ordering::Acquire) == jobs);
        assert_eq!(done.load(Ordering::Acquire), jobs);
    });
}

#[test]
fn khong_co_worker_thi_job_chay_ngay_tai_cho()
{
    let pool = ThreadPool::new(Config::inline());
    assert_eq!(pool.worker_threads(), 0);
    assert_eq!(pool.worker_count(), 1);

    let here = std::thread::current().id();
    let ran_on = Arc::new(Mutex::new(None));

    let slot = Arc::clone(&ran_on);
    let done = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&done);
    pool.spawn(move || {
        *ignore_poison(slot.lock()) = Some(std::thread::current().id());
        counter.fetch_add(1, Ordering::Release);
    });

    // Đã chạy xong trước cả khi `spawn` trả về, nên không cần chờ gì cả.
    assert_eq!(done.load(Ordering::Acquire), 1);
    assert_eq!(*ignore_poison(ran_on.lock()), Some(here));
}

#[test]
fn job_de_ra_job_con_thi_cay_chay_het()
{
    with_watchdog("job_de_ra_job_con_thi_cay_chay_het", || {
        /// Cây nhị phân sâu 10 tầng: 1023 node, và mỗi node spawn từ trong một job.
        #[cfg(not(miri))]
        const DEPTH: usize = 10;
        #[cfg(miri)]
        const DEPTH: usize = 4;
        const NODES: usize = (1 << DEPTH) - 1;

        let pool = Arc::new(pool_with(4));
        let done = Arc::new(AtomicUsize::new(0));

        fn branch(pool: Arc<ThreadPool>, done: Arc<AtomicUsize>, depth: usize)
        {
            done.fetch_add(1, Ordering::Relaxed);
            if depth == 0
            {
                return;
            }

            for _ in 0..2
            {
                let pool_child = Arc::clone(&pool);
                let done_child = Arc::clone(&done);
                pool.spawn(move || branch(pool_child, done_child, depth - 1));
            }
        }

        let root_pool = Arc::clone(&pool);
        let root_done = Arc::clone(&done);
        pool.spawn(move || branch(root_pool, root_done, DEPTH - 1));

        pool.run_until(|| done.load(Ordering::Acquire) == NODES);
        assert_eq!(done.load(Ordering::Acquire), NODES);
    });
}

#[test]
fn job_tu_thread_ngoai_pool_van_toi_duoc_worker()
{
    with_watchdog("job_tu_thread_ngoai_pool_van_toi_duoc_worker", || {
        let jobs = scaled(2_000);

        let pool = Arc::new(pool_with(3));
        let done = Arc::new(AtomicUsize::new(0));

        // Thread này không phải host, cũng không phải worker: nó chỉ có mỗi lane queue để đẩy vào.
        let outsider = {
            let pool = Arc::clone(&pool);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                for _ in 0..jobs
                {
                    let done = Arc::clone(&done);
                    pool.spawn(move || {
                        done.fetch_add(1, Ordering::Relaxed);
                    });
                }
            })
        };

        outsider.join().expect("thread ngoài panic");
        pool.run_until(|| done.load(Ordering::Acquire) == jobs);
        assert_eq!(done.load(Ordering::Acquire), jobs);
    });
}

#[test]
fn ring_day_thi_job_tran_xuong_lane_queue_chu_khong_mat()
{
    with_watchdog("ring_day_thi_job_tran_xuong_lane_queue_chu_khong_mat", || {
        // Ring bé xíu so với số job: mọi job sau ô thứ tư đều phải đi đường xả.
        let jobs = scaled(5_000);

        let pool = ThreadPool::new(Config {
            threads: 2,
            ring_capacity: 4,
            ..Config::default()
        });
        let done = Arc::new(AtomicUsize::new(0));

        for _ in 0..jobs
        {
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }

        pool.run_until(|| done.load(Ordering::Acquire) == jobs);
        assert_eq!(done.load(Ordering::Acquire), jobs);
    });
}

#[test]
fn shutdown_chay_not_phan_con_xep_hang()
{
    with_watchdog("shutdown_chay_not_phan_con_xep_hang", || {
        let jobs = scaled(1_000);

        let done = Arc::new(AtomicUsize::new(0));
        {
            let pool = pool_with(2);
            for _ in 0..jobs
            {
                let done = Arc::clone(&done);
                pool.spawn(move || {
                    done.fetch_add(1, Ordering::Relaxed);
                });
            }
            pool.shutdown();
            // Sau khi `shutdown` trả về thì không còn worker nào, và cũng không còn job nào bị bỏ.
            assert_eq!(done.load(Ordering::Acquire), jobs);
        }
        assert_eq!(done.load(Ordering::Acquire), jobs);
    });
}

#[test]
fn tha_tay_cam_cuoi_cung_cung_tat_pool()
{
    with_watchdog("tha_tay_cam_cuoi_cung_cung_tat_pool", || {
        let done = Arc::new(AtomicUsize::new(0));
        {
            let pool = pool_with(2);
            let clone = pool.clone();
            for _ in 0..100
            {
                let done = Arc::clone(&done);
                clone.spawn(move || {
                    done.fetch_add(1, Ordering::Relaxed);
                });
            }
        }
        assert_eq!(done.load(Ordering::Acquire), 100);
    });
}

#[test]
fn shutdown_goi_nhieu_lan_khong_sao()
{
    with_watchdog("shutdown_goi_nhieu_lan_khong_sao", || {
        let pool = pool_with(2);
        pool.shutdown();
        pool.shutdown();
        pool.shutdown();
    });
}

#[test]
fn mot_job_panic_khong_giet_pool()
{
    with_watchdog("mot_job_panic_khong_giet_pool", || {
        let pool = pool_with(2);
        let done = Arc::new(AtomicUsize::new(0));

        // Panic hook mặc định sẽ in ra, nhưng test này quan tâm chuyện pool còn sống hay không.
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        pool.spawn(|| panic!("job này chết"));

        for _ in 0..scaled(200)
        {
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }

        pool.run_until(|| done.load(Ordering::Acquire) == scaled(200));
        std::panic::set_hook(previous);
        assert_eq!(done.load(Ordering::Acquire), scaled(200));
    });
}

#[test]
fn moi_nguoi_tham_gia_co_mot_chi_so_rieng()
{
    with_watchdog("moi_nguoi_tham_gia_co_mot_chi_so_rieng", || {
        const WORKERS: usize = 4;

        let pool = Arc::new(pool_with(WORKERS));
        assert_eq!(pool.worker_count(), WORKERS + 1);
        assert_eq!(pool.worker_index(), WORKERS, "host đứng ở ô ngay sau worker cuối");

        let jobs = scaled(2_000);
        let seen = Arc::new((0..pool.worker_count()).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let done = Arc::new(AtomicUsize::new(0));

        for _ in 0..jobs
        {
            let pool_job = Arc::clone(&pool);
            let seen = Arc::clone(&seen);
            let done = Arc::clone(&done);
            pool.spawn(move || {
                let index = pool_job.worker_index();
                assert!(index < pool_job.worker_count());
                seen[index].fetch_add(1, Ordering::Relaxed);
                done.fetch_add(1, Ordering::Release);
            });
        }

        pool.run_until(|| done.load(Ordering::Acquire) == jobs);

        // Tắt pool trước khi cộng: `join` của các worker là chỗ duy nhất bảo đảm mọi lần ghi
        // `Relaxed` của chúng đã hiện ra ở đây. Không có nó thì tổng đọc được có thể thiếu vài cái,
        // và đó không phải bug của pool mà là mô hình bộ nhớ đang làm đúng việc của nó.
        pool.shutdown();

        let total: usize = seen.iter().map(|slot| slot.load(Ordering::Acquire)).sum();
        assert_eq!(total, jobs);
    });
}

#[test]
#[should_panic(expected = "không phải worker của pool này")]
fn chi_so_worker_cua_thread_la_thi_panic()
{
    let pool = Arc::new(pool_with(1));
    let foreign = Arc::clone(&pool);
    let outcome = std::thread::spawn(move || foreign.worker_index()).join();

    match outcome
    {
        Ok(_) => panic!("thread lạ lấy được chỉ số worker"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[test]
fn worker_ngu_roi_van_thuc_day_khi_co_viec_moi()
{
    with_watchdog("worker_ngu_roi_van_thuc_day_khi_co_viec_moi", || {
        let pool = pool_with(4);

        for round in 0..scaled(50)
        {
            // Đủ lâu để mọi worker cạn backoff và park thật.
            std::thread::sleep(Duration::from_millis(2));

            let done = Arc::new(AtomicUsize::new(0));
            for _ in 0..64
            {
                let done = Arc::clone(&done);
                pool.spawn(move || {
                    done.fetch_add(1, Ordering::Relaxed);
                });
            }

            pool.run_until(|| done.load(Ordering::Acquire) == 64);
            assert_eq!(done.load(Ordering::Acquire), 64, "vòng {round}");
        }
    });
}

#[test]
fn cho_bang_run_until_thi_thread_goi_cung_chay_job()
{
    with_watchdog("cho_bang_run_until_thi_thread_goi_cung_chay_job", || {
        // Một worker duy nhất, và một job "chốt chặn" chỉ thoát khi đủ 500 job kia đã chạy xong.
        //
        // Ai bốc phải chốt chặn cũng được, và đó chính là chỗ hay: người còn lại buộc phải chạy 500
        // job kia thì cả hai mới thoát. Nếu thread gọi `run_until` chỉ ngồi chờ chứ không chạy job,
        // nhánh "worker bốc phải chốt chặn" sẽ treo, và cái treo đó là thứ test này đi tìm.
        let jobs = scaled(500);

        let pool = pool_with(1);
        let done = Arc::new(AtomicUsize::new(0));

        let gate = Arc::clone(&done);
        pool.spawn(move || {
            while gate.load(Ordering::Acquire) < jobs
            {
                std::hint::spin_loop();
            }
        });

        for _ in 0..jobs
        {
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }

        pool.run_until(|| done.load(Ordering::Acquire) == jobs);
        assert_eq!(done.load(Ordering::Acquire), jobs);
    });
}

#[test]
fn lane_queue_khong_bi_bo_doi_khi_worker_tu_nuoi_minh()
{
    with_watchdog("lane_queue_khong_bi_bo_doi_khi_worker_tu_nuoi_minh", || {
        // Một worker duy nhất, và nó có một chuỗi job tự đẻ ra nhau không dứt. Job mà thread ngoài
        // đẩy vào lane queue phải chạy được trong thời gian hữu hạn, đó là toàn bộ việc của luật
        // `LANE_QUEUE_TICK`.
        let pool = Arc::new(pool_with(1));
        let keep_going = Arc::new(AtomicUsize::new(1));
        let outsider_ran = Arc::new(AtomicUsize::new(0));

        fn feed(pool: Arc<ThreadPool>, keep_going: Arc<AtomicUsize>)
        {
            if keep_going.load(Ordering::Acquire) == 0
            {
                return;
            }
            let next_pool = Arc::clone(&pool);
            let next_flag = Arc::clone(&keep_going);
            pool.spawn(move || feed(next_pool, next_flag));
        }

        let seed_pool = Arc::clone(&pool);
        let seed_flag = Arc::clone(&keep_going);
        pool.spawn(move || feed(seed_pool, seed_flag));

        // Đẩy từ một thread ngoài, để job chắc chắn nằm trong lane queue chứ không phải ring nào.
        let outside = {
            let pool = Arc::clone(&pool);
            let ran = Arc::clone(&outsider_ran);
            std::thread::spawn(move || {
                pool.spawn(move || {
                    ran.fetch_add(1, Ordering::Release);
                });
            })
        };
        outside.join().expect("thread ngoài panic");

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while outsider_ran.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }

        keep_going.store(0, Ordering::Release);
        assert_eq!(outsider_ran.load(Ordering::Acquire), 1, "job trong lane queue bị bỏ đói");
    });
}

#[test]
fn arena_nhap_rieng_cho_tung_nguoi_tham_gia()
{
    with_watchdog("arena_nhap_rieng_cho_tung_nguoi_tham_gia", || {
        let pool = ThreadPool::new(Config {
            threads: 3,
            scratch_bytes: 4 << 10,
            ..Config::default()
        });

        let done = Arc::new(AtomicUsize::new(0));
        let bad = Arc::new(AtomicUsize::new(0));

        pool.scope(|s| {
            for value in 0..scaled(200) as u32
            {
                let pool = &pool;
                let done = Arc::clone(&done);
                let bad = Arc::clone(&bad);
                s.spawn(move || {
                    pool.scratch(|arena| {
                        // Ghi rồi đọc lại ngay: nếu hai thread dùng chung một arena thì con số này
                        // sẽ có lúc không khớp.
                        let slot = arena.alloc(value).expect("arena hết chỗ");
                        std::thread::yield_now();
                        if *slot != value
                        {
                            bad.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                    done.fetch_add(1, Ordering::Relaxed);
                });
            }
        });

        assert_eq!(done.load(Ordering::Acquire), scaled(200));
        assert_eq!(bad.load(Ordering::Acquire), 0, "hai thread cùng ghi vào một arena");

        // Sau frame thì mọi arena trống trở lại.
        pool.end_frame();
        pool.scratch(|arena| assert_eq!(arena.used(), 0));
    });
}

#[test]
fn end_frame_giua_luc_dang_muon_arena_thi_panic()
{
    let pool = ThreadPool::new(Config {
        threads: 1,
        ..Config::default()
    });

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.scratch(|_| pool.end_frame());
    }));

    assert!(outcome.is_err(), "dọn arena trong lúc còn người đang cầm nó mà vẫn trót lọt");
}

#[test]
fn bo_dem_ghi_lai_dung_so_job_da_chay()
{
    with_watchdog("bo_dem_ghi_lai_dung_so_job_da_chay", || {
        let jobs = scaled(2_000) as u64;

        let pool = pool_with(3);
        let done = Arc::new(AtomicUsize::new(0));

        for _ in 0..jobs
        {
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Release);
            });
        }

        pool.run_until(|| done.load(Ordering::Acquire) as u64 == jobs);

        // Bộ đếm được ghi `Relaxed` từ mỗi worker, nên chỉ sau khi join chúng thì con số mới khít.
        // Xem `counters::PoolCounters::total`.
        pool.shutdown();

        let counters = pool.counters();
        assert_eq!(counters.jobs_run, jobs, "số job đếm được không khớp số job đã chạy");
        assert_eq!(counters.queued, 0, "lane queue còn job sau khi mọi thứ đã xong");

        // Tổng của các ô riêng phải bằng tổng chung.
        let by_worker: u64 = (0..pool.worker_count()).map(|i| pool.counters_of(i).jobs_run).sum();
        assert_eq!(by_worker, jobs);
    });
}

#[test]
fn bo_dem_thay_duoc_lan_xa_khi_ring_qua_be()
{
    with_watchdog("bo_dem_thay_duoc_lan_xa_khi_ring_qua_be", || {
        let jobs = scaled(3_000);

        let pool = ThreadPool::new(Config {
            threads: 2,
            ring_capacity: 4,
            ..Config::default()
        });
        let done = Arc::new(AtomicUsize::new(0));

        for _ in 0..jobs
        {
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }

        pool.run_until(|| done.load(Ordering::Acquire) == jobs);

        let counters = pool.counters();
        assert!(counters.spills > 0, "ring 4 ô mà 3000 job vẫn không xả lần nào");
        assert!(counters.lane_pops > 0, "job xả xuống lane queue mà không ai lấy từ đó");
    });
}

#[test]
#[should_panic(expected = "nằm ngoài pool")]
fn bo_dem_cua_chi_so_khong_ton_tai_thi_panic()
{
    let pool = pool_with(1);
    let _ = pool.counters_of(99);
}

#[test]
fn tha_tay_cam_cuoi_cung_tu_trong_mot_job()
{
    with_watchdog("tha_tay_cam_cuoi_cung_tu_trong_mot_job", || {
        // Một job giữ tay cầm pool, và nó tình cờ là kẻ thả cái cuối cùng. Không ai cố ý viết ra
        // cảnh này, nhưng chỉ cần một `Arc<ThreadPool>` bị bắt vào closure là dính, và trước khi
        // `Owner::stop` biết nhìn xem mình đang đứng ở đâu thì đây là một lần join chính mình, tức
        // là hành vi không xác định. Miri gọi tên nó ra trong một test khác của chính file này.
        let done = Arc::new(AtomicUsize::new(0));

        {
            let pool = Arc::new(pool_with(2));
            let carried = Arc::clone(&pool);
            let counter = Arc::clone(&done);

            pool.spawn(move || {
                counter.fetch_add(1, Ordering::Release);
                // Tay cầm chết ở đây, trên chính worker đang chạy dòng này.
                drop(carried);
            });

            // Thread này buông trước, nên cái tay cầm cuối cùng nằm trong tay job ở trên. Từ đây
            // trở đi không còn ai giữ pool ngoài chính cái job đang chạy trên nó.
            drop(pool);
        }

        // Không có tay cầm nào để `run_until`, nên chờ bằng đồng hồ, và có trần cứng.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while done.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }

        assert_eq!(done.load(Ordering::Acquire), 1);
    });
}
