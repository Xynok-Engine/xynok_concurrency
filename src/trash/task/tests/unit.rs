use crate::channel::{Receiver, oneshot};
use crate::pool::{Config, ThreadPool};
use crate::task::{block_on, block_on_in, spawn, yield_now};

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};
use std::time::Duration;

fn pool_with(threads: usize) -> ThreadPool
{
    ThreadPool::new(Config {
        threads: threads,
        spin_rounds: 1,
        ..Config::default()
    })
}

#[test]
fn t0_future_khong_cho_gi_thi_chay_mot_nhat()
{
    let pool = pool_with(2);
    let answer = spawn(&pool, async { 6 * 7 });
    assert_eq!(block_on(answer), Some(42));
}

#[test]
fn t1_await_ket_qua_cua_mot_task_khac()
{
    let pool = pool_with(2);

    let inner = spawn(&pool, async { "nội dung file".to_string() });
    let outer = spawn(&pool, async move {
        let text = inner.await.expect("task trong không trả kết quả");
        text.len()
    });

    assert_eq!(block_on(outer), Some(15));
}

#[test]
fn t2_task_ngu_roi_duoc_thread_ngoai_goi_day()
{
    let pool = pool_with(2);
    let (tx, rx) = oneshot::<u32>();

    let task = spawn(&pool, async move { rx.await.map(|value| value * 2) });

    // Task đã `Pending` và không giữ thread nào của lane: lane phải rảnh để chạy việc khác.
    let ran = StdArc::new(AtomicUsize::new(0));
    for _ in 0..100
    {
        let ran = StdArc::clone(&ran);
        pool.spawn(move || {
            ran.fetch_add(1, StdOrdering::Relaxed);
        });
    }
    pool.run_until(|| ran.load(StdOrdering::Acquire) == 100);

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        tx.send(21);
    });

    assert_eq!(block_on(task), Some(Some(42)));
}

#[test]
fn t3_goi_day_nhieu_lan_khong_lam_task_chay_hai_lan()
{
    let pool = pool_with(2);
    let polls = StdArc::new(AtomicUsize::new(0));
    let (tx, rx) = oneshot::<()>();

    let counted = StdArc::clone(&polls);
    let task = spawn(&pool, async move {
        counted.fetch_add(1, StdOrdering::Relaxed);
        let _ = rx.await;
        counted.fetch_add(1, StdOrdering::Relaxed);
    });

    // Đầu gửi bị thả: người chờ được gọi dậy đúng một lần dù có bao nhiêu waker đi nữa.
    drop(tx);
    assert_eq!(block_on(task), Some(()));
    assert_eq!(polls.load(StdOrdering::Acquire), 2);
}

#[test]
fn t4_yield_now_tra_thread_lai_cho_lane()
{
    let pool = pool_with(2);
    let order = StdArc::new(AtomicUsize::new(0));

    let ticket = StdArc::clone(&order);
    let long = spawn(&pool, async move {
        let mut seen = Vec::new();
        for _ in 0..4
        {
            seen.push(ticket.fetch_add(1, StdOrdering::Relaxed));
            yield_now().await;
        }
        seen
    });

    let seen = block_on(long).expect("task nhường lượt không trả kết quả");
    assert_eq!(seen.len(), 4);
}

#[test]
fn t5_task_panic_thi_nguoi_cho_nhan_none()
{
    let pool = pool_with(2);

    let task = spawn(&pool, async {
        panic!("task này chết giữa chừng");
    });

    assert_eq!(block_on(task), None::<()>);

    // Lane vẫn sống sau cú panic đó.
    let after = spawn(&pool, async { 7u32 });
    assert_eq!(block_on(after), Some(7));
}

#[test]
fn t6_block_on_in_chay_job_trong_luc_cho()
{
    let pool = pool_with(2);
    let ran = StdArc::new(AtomicUsize::new(0));
    let (tx, rx) = oneshot::<u32>();

    for _ in 0..200
    {
        let ran = StdArc::clone(&ran);
        pool.spawn(move || {
            ran.fetch_add(1, StdOrdering::Relaxed);
        });
    }

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        tx.send(9);
    });

    assert_eq!(block_on_in(&pool, rx), Some(9));
    pool.run_until(|| ran.load(StdOrdering::Acquire) == 200);
    assert_eq!(ran.load(StdOrdering::Acquire), 200);
}

/// Trần cứng cho mọi lần chờ trong nhóm test này. Một waker rơi mất thì task ngủ vĩnh viễn, và
/// nếu không có hạn giờ thì lần chạy test treo luôn chứ không báo hỏng.
const DEADLINE: Duration = Duration::from_secs(60);

/// Miri diễn giải từng lệnh một, nên số vòng chạy vài giây trên máy thật sẽ chạy hàng giờ ở đó.
#[cfg(miri)]
const SCALE: usize = 50;
#[cfg(not(miri))]
const SCALE: usize = 1;

const fn scaled(n: usize) -> usize
{
    if n / SCALE == 0 { 1 } else { n / SCALE }
}

/// Chờ kết quả nhưng có hạn giờ, xem [`DEADLINE`].
fn recv_before_deadline<T>(mut rx: Receiver<T>, what: &str) -> Option<T>
{
    let deadline = std::time::Instant::now() + DEADLINE;
    loop
    {
        match rx.try_recv()
        {
            Ok(value) => return Some(value),
            Err(back) =>
            {
                if back.is_cancelled()
                {
                    return None;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "{what}: quá {DEADLINE:?} mà task vẫn chưa xong, có waker bị rơi mất"
                );
                rx = back;
                std::thread::yield_now();
            }
        }
    }
}

/// Đúng cái mà ô trạng thái của task sinh ra để chịu: rất nhiều task cùng ngủ, cùng được gọi
/// dậy từ mấy thread khác nhau, rồi mỗi task còn tự xếp lại vào hàng thêm mấy lượt nữa.
///
/// Số task và số lượt nhường đều là hằng, không phải một vòng gom lớn dần: một test đo tính
/// đúng đắn không có lý do gì để ăn thêm bộ nhớ theo thời gian chạy.
#[test]
fn t7_stress_nhieu_task_cung_ngu_roi_cung_bi_goi_day()
{
    const TASKS: usize = 200;
    const YIELDS: usize = 16;
    const SENDERS: usize = 4;

    let tasks = scaled(TASKS);
    let pool = pool_with(2);

    let mut senders = Vec::with_capacity(tasks);
    let mut results = Vec::with_capacity(tasks);

    for i in 0..tasks
    {
        let (tx, rx) = oneshot::<usize>();
        senders.push(tx);
        results.push(spawn(&pool, async move {
            let seed = rx.await?;
            // Mỗi lượt nhường là một lần task tự xếp lại vào lane, tức là một lần đi qua đúng
            // cái đường mà waker gọi giữa lúc poll cũng đi.
            for _ in 0..YIELDS
            {
                yield_now().await;
            }
            Some(seed + i)
        }));
    }

    // Gửi từ nhiều thread một lúc: waker của task bị gọi từ ngoài lane, xen vào giữa lúc lane
    // đang poll những task khác.
    let mut chunks: Vec<Vec<(usize, crate::channel::Sender<usize>)>> = (0..SENDERS).map(|_| Vec::new()).collect();
    for (i, tx) in senders.into_iter().enumerate()
    {
        chunks[i % SENDERS].push((i, tx));
    }

    std::thread::scope(|s| {
        for chunk in chunks
        {
            s.spawn(move || {
                for (i, tx) in chunk
                {
                    if i % 7 == 0
                    {
                        std::thread::yield_now();
                    }
                    tx.send(i * 10);
                }
            });
        }
    });

    for (i, rx) in results.into_iter().enumerate()
    {
        assert_eq!(recv_before_deadline(rx, "stress task"), Some(Some(i * 10 + i)), "task {i} trả sai kết quả");
    }
}

#[test]
fn t8_lane_khong_co_worker_van_chay_duoc_task()
{
    // `threads: 0` chạy mọi job ngay trên thread gọi, và một task cũng chỉ là một job.
    let pool = pool_with(0);
    let answer = spawn(&pool, async { 1u32 + 1 });
    assert_eq!(block_on(answer), Some(2));
}
