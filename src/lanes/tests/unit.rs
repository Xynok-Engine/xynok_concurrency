use crate::channel::oneshot;
use crate::lanes::{LaneId, Lanes, LanesConfig, block_in_place};
use crate::pool::Config;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn small_lanes() -> Lanes
{
    Lanes::new(LanesConfig {
        compute:    Config {
            threads: 2,
            ..Config::default()
        },
        async_lane: Config {
            threads: 2,
            ..LanesConfig::default().async_lane
        },
    })
}

#[test]
fn t0_job_di_dung_lane_duoc_chi_dinh()
{
    let lanes = small_lanes();
    let compute_done = Arc::new(AtomicUsize::new(0));
    let async_done = Arc::new(AtomicUsize::new(0));

    for _ in 0..100
    {
        let done = Arc::clone(&compute_done);
        lanes.spawn(LaneId::Compute, move || {
            done.fetch_add(1, Ordering::Relaxed);
        });

        let done = Arc::clone(&async_done);
        lanes.spawn(LaneId::Async, move || {
            done.fetch_add(1, Ordering::Relaxed);
        });
    }

    lanes.compute().run_until(|| compute_done.load(Ordering::Acquire) == 100);
    lanes.async_lane().run_until(|| async_done.load(Ordering::Acquire) == 100);

    assert_eq!(compute_done.load(Ordering::Acquire), 100);
    assert_eq!(async_done.load(Ordering::Acquire), 100);
}

#[test]
fn t1_job_cua_main_chi_chay_khi_main_vet()
{
    let lanes = small_lanes();
    let ran = Arc::new(AtomicUsize::new(0));
    let main_thread = std::thread::current().id();
    let wrong_thread = Arc::new(AtomicUsize::new(0));

    for _ in 0..8
    {
        let ran = Arc::clone(&ran);
        let wrong = Arc::clone(&wrong_thread);
        lanes.spawn_on_main(move || {
            if std::thread::current().id() != main_thread
            {
                wrong.fetch_add(1, Ordering::Relaxed);
            }
            ran.fetch_add(1, Ordering::Relaxed);
        });
    }

    // Đợi một lúc: không ai được phép chạy chúng thay main thread.
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(ran.load(Ordering::Acquire), 0, "job của main bị chạy trước khi main vét");
    assert_eq!(lanes.pending_on_main(), 8);

    assert_eq!(lanes.run_pending_on_main(), 8);
    assert_eq!(ran.load(Ordering::Acquire), 8);
    assert_eq!(wrong_thread.load(Ordering::Acquire), 0);
    assert_eq!(lanes.pending_on_main(), 0);
}

#[test]
fn t2_job_tu_lane_khac_van_xep_duoc_cho_main()
{
    let lanes = small_lanes();
    let ran = Arc::new(AtomicUsize::new(0));
    let queued = Arc::new(AtomicUsize::new(0));

    // Một job của lane compute xếp việc cho main thread, đúng hình dạng của "present phải chạy
    // trên main".
    for _ in 0..16
    {
        let ran = Arc::clone(&ran);
        let queued = Arc::clone(&queued);
        let lanes_ref = &lanes;
        lanes.compute().scope(|s| {
            s.spawn(move || {
                lanes_ref.spawn_on_main(move || {
                    ran.fetch_add(1, Ordering::Relaxed);
                });
                queued.fetch_add(1, Ordering::Relaxed);
            });
        });
    }

    assert_eq!(queued.load(Ordering::Acquire), 16);
    assert_eq!(lanes.run_pending_on_main(), 16);
    assert_eq!(ran.load(Ordering::Acquire), 16);
}

#[test]
fn t3_vet_main_khong_chay_job_do_chinh_dot_nay_de_ra()
{
    let ran = Arc::new(AtomicUsize::new(0));

    // Job này, khi chạy, lại xếp thêm một job cho main. Cái mới phải đợi lần vét sau.
    //
    // `Arc` chứ không phải tham chiếu: job phải `'static`, mà đây đúng là hình dạng thật của
    // engine, nơi `Lanes` sống trong một `Arc` dùng chung.
    let lanes = Arc::new(small_lanes());
    let ran_outer = Arc::clone(&ran);
    let lanes_inner = Arc::clone(&lanes);
    lanes.spawn_on_main(move || {
        ran_outer.fetch_add(1, Ordering::Relaxed);
        lanes_inner.spawn_on_main(|| {});
    });

    assert_eq!(lanes.run_pending_on_main(), 1);
    assert_eq!(ran.load(Ordering::Acquire), 1);
    assert_eq!(lanes.pending_on_main(), 1, "job do đợt này đẻ ra phải đợi lần vét sau");

    assert_eq!(lanes.run_pending_on_main(), 1);
    assert_eq!(lanes.pending_on_main(), 0);
}

#[test]
#[should_panic(expected = "phải chạy trên chính thread đã dựng Lanes")]
fn t4_vet_main_tu_thread_khac_thi_panic()
{
    let lanes = Arc::new(small_lanes());
    let foreign = Arc::clone(&lanes);

    let outcome = std::thread::spawn(move || foreign.run_pending_on_main()).join();
    match outcome
    {
        Ok(_) => panic!("thread lạ vét được hàng đợi của main"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[test]
fn t5_viec_chan_tra_ket_qua_ve_qua_kenh()
{
    let lanes = small_lanes();

    let reading = lanes.run_blocking(|| {
        std::thread::sleep(std::time::Duration::from_millis(10));
        "nội dung file".to_string()
    });

    // Trong lúc chờ, lane compute vẫn chạy việc của nó.
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..100
    {
        let done = Arc::clone(&done);
        lanes.compute().spawn(move || {
            done.fetch_add(1, Ordering::Relaxed);
        });
    }

    assert_eq!(reading.recv_in(lanes.compute()).as_deref(), Some("nội dung file"));
    lanes.compute().run_until(|| done.load(Ordering::Acquire) == 100);
}

#[test]
fn t6_task_cua_lane_async_await_duoc_viec_chan()
{
    let lanes = Arc::new(small_lanes());

    let inner = Arc::clone(&lanes);
    let loaded = lanes.spawn_async(async move {
        let text = inner
            .run_blocking(|| {
                std::thread::sleep(std::time::Duration::from_millis(10));
                "nội dung file".to_string()
            })
            .await?;
        Some(text.len())
    });

    assert_eq!(loaded.recv_in(lanes.compute()), Some(Some(15)));
}

#[test]
fn t7_task_dang_await_khong_giu_thread_nao_cua_lane()
{
    // Lane hai thread, ba task cùng chờ. Nếu "chờ" mà chiếm thread thì lane đã tắc, và mấy job
    // bên dưới sẽ không bao giờ chạy hết.
    let lanes = small_lanes();
    let mut senders = Vec::new();
    let mut tasks = Vec::new();

    for i in 0..3
    {
        let (tx, rx) = oneshot::<usize>();
        senders.push(tx);
        tasks.push(lanes.spawn_async(async move { rx.await.map(|value| value + i) }));
    }

    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..200
    {
        let done = Arc::clone(&done);
        lanes.spawn(LaneId::Async, move || {
            done.fetch_add(1, Ordering::Relaxed);
        });
    }
    lanes.async_lane().run_until(|| done.load(Ordering::Acquire) == 200);
    assert_eq!(done.load(Ordering::Acquire), 200);

    for (i, tx) in senders.into_iter().enumerate()
    {
        tx.send(i * 10);
    }
    for (i, task) in tasks.into_iter().enumerate()
    {
        assert_eq!(task.recv_in(lanes.compute()), Some(Some(i * 10 + i)));
    }
}

#[test]
fn t8_block_on_chay_job_lane_compute_trong_luc_cho()
{
    let lanes = Arc::new(small_lanes());

    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..200
    {
        let done = Arc::clone(&done);
        lanes.compute().spawn(move || {
            done.fetch_add(1, Ordering::Relaxed);
        });
    }

    let inner = Arc::clone(&lanes);
    let value = lanes.block_on(async move {
        inner
            .run_blocking(|| {
                std::thread::sleep(std::time::Duration::from_millis(10));
                7u32
            })
            .await
    });

    assert_eq!(value, Some(7));
    lanes.compute().run_until(|| done.load(Ordering::Acquire) == 200);
    assert_eq!(done.load(Ordering::Acquire), 200);
}

#[test]
fn t9_task_panic_thi_nguoi_cho_nhan_none()
{
    let lanes = small_lanes();
    let loaded = lanes.spawn_async(async {
        panic!("asset này hỏng");
    });
    assert_eq!(loaded.recv_in(lanes.compute()), None::<()>);

    // Lane vẫn nhận việc mới sau đó.
    let after = lanes.spawn_async(async { 7u32 });
    assert_eq!(after.recv_in(lanes.compute()), Some(7));
}

#[test]
fn t10_block_in_place_van_tra_ve_ket_qua()
{
    let lanes = small_lanes();
    let value = block_in_place(lanes.compute(), || {
        std::thread::sleep(std::time::Duration::from_millis(5));
        7u32
    });
    assert_eq!(value, 7);
}

#[test]
fn t11_shutdown_chay_not_viec_cua_main()
{
    let ran = Arc::new(AtomicUsize::new(0));
    {
        let lanes = small_lanes();
        for _ in 0..4
        {
            let ran = Arc::clone(&ran);
            lanes.spawn_on_main(move || {
                ran.fetch_add(1, Ordering::Relaxed);
            });
        }
        lanes.shutdown();
    }
    assert_eq!(ran.load(Ordering::Acquire), 4);
}
