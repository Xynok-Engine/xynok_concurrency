use crate::channel::oneshot;
use crate::pool::{Config, ThreadPool};

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};

fn pool_with(threads: usize) -> ThreadPool
{
    ThreadPool::new(Config {
        threads: threads,
        ..Config::default()
    })
}

#[test]
fn t0_gia_tri_di_tu_job_ve_nguoi_cho()
{
    let pool = pool_with(2);
    let (tx, rx) = oneshot();

    pool.spawn(move || tx.send(42u32));
    assert_eq!(rx.recv_in(&pool), Some(42));
}

#[test]
fn t1_thread_ngoai_pool_ngu_cho_ket_qua()
{
    let pool = pool_with(2);
    let (tx, rx) = oneshot();

    pool.spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        tx.send("xong".to_string());
    });

    assert_eq!(rx.recv().as_deref(), Some("xong"));
}

#[test]
fn t2_ket_qua_toi_truoc_khi_ai_cho_thi_khong_mat()
{
    let pool = pool_with(1);
    let (tx, rx) = oneshot();

    tx.send(7u8);
    std::thread::sleep(std::time::Duration::from_millis(5));

    assert!(rx.is_ready());
    assert_eq!(rx.recv_in(&pool), Some(7));
}

#[test]
fn t3_try_recv_tra_lai_dau_nhan_khi_chua_co_gi()
{
    let (tx, rx) = oneshot::<u8>();

    let rx = rx.try_recv().expect_err("chưa gửi mà đã có kết quả");
    tx.send(3);
    assert_eq!(rx.try_recv().ok(), Some(3));
}

#[test]
fn t4_dau_gui_bien_mat_thi_nguoi_cho_khong_treo()
{
    let pool = pool_with(2);
    let (tx, rx) = oneshot::<u32>();

    pool.spawn(move || {
        // Thả đầu gửi mà không gửi gì, đúng như một job panic giữa chừng.
        drop(tx);
    });

    assert_eq!(rx.recv_in(&pool), None);
}

#[test]
fn t5_job_panic_thi_nguoi_cho_nhan_duoc_none()
{
    let pool = pool_with(2);
    let (tx, rx) = oneshot::<u32>();

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    pool.spawn(move || {
        let _tx = tx;
        panic!("job chết trước khi gửi");
    });

    let got = rx.recv_in(&pool);
    std::panic::set_hook(previous);
    assert_eq!(got, None);
}

#[test]
fn t6_khong_ai_lay_thi_gia_tri_van_duoc_tha()
{
    let tracker = StdArc::new(AtomicUsize::new(0));

    struct Tracked(StdArc<AtomicUsize>);
    impl Drop for Tracked
    {
        fn drop(&mut self)
        {
            self.0.fetch_add(1, StdOrdering::Release);
        }
    }

    {
        let (tx, rx) = oneshot();
        tx.send(Tracked(StdArc::clone(&tracker)));
        drop(rx);
    }

    assert_eq!(tracker.load(StdOrdering::Acquire), 1, "giá trị không ai lấy mà cũng không được thả");
}

#[test]
fn t7_await_kenh_da_co_ket_qua_thi_khong_can_ai_goi_day()
{
    let (tx, rx) = oneshot();
    tx.send(42u32);

    // Kết quả tới trước cả lần poll đầu tiên: `poll` phải trả `Ready` ngay, không ghi waker nào,
    // vì sẽ không còn ai gọi nó nữa.
    assert_eq!(crate::task::block_on(rx), Some(42));
}

#[test]
fn t8_await_dau_gui_bien_mat_thi_nhan_none()
{
    let pool = pool_with(2);
    let (tx, rx) = oneshot::<u32>();

    pool.spawn(move || drop(tx));
    assert_eq!(crate::task::block_on(rx), None);
}

#[test]
fn t9_nhieu_kenh_chay_song_song_khong_lan_ket_qua()
{
    const JOBS: usize = 500;

    let pool = pool_with(4);
    let mut receivers = Vec::with_capacity(JOBS);

    for job in 0..JOBS
    {
        let (tx, rx) = oneshot();
        pool.spawn(move || tx.send(job * 3));
        receivers.push(rx);
    }

    for (job, rx) in receivers.into_iter().enumerate()
    {
        assert_eq!(rx.recv_in(&pool), Some(job * 3), "kênh của job {job} nhận nhầm kết quả");
    }
}
