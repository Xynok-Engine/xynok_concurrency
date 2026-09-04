use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::apis::priority::Priority;
use crate::custom_type::Job;
use crate::sync::{AtomicUsize, Ordering};
use crate::thread_pool::{CfgThreadPool, ThreadPool};

/// Trần cứng cho mọi vòng chờ trong file này. Chạm trần là hỏng, không phải chờ thêm.
const TIMEOUT: Duration = Duration::from_secs(10);

fn cfg(name: &str, worker_count: usize) -> CfgThreadPool
{
    CfgThreadPool {
        name:                     name.to_string(),
        priority:                 Priority::Frame,
        per_worker_task_capacity: 256,
        task_capacity:            1024,
        worker_count:             worker_count,
    }
}

/// Đợi tới khi `cond` đúng, nhưng không đợi quá [`TIMEOUT`].
fn wait_until(what: &str, cond: impl Fn() -> bool)
{
    let deadline = Instant::now() + TIMEOUT;
    while !cond()
    {
        assert!(Instant::now() < deadline, "quá {:?} mà `{}` vẫn chưa xong", TIMEOUT, what);
        std::thread::yield_now();
    }
}

#[test]
fn t0_pool_chay_het_task_da_day_vao()
{
    const TOTAL_TASK: usize = 2_000;

    let done = Arc::new(AtomicUsize::new(0));
    let mut pool = ThreadPool::new(cfg("t0", 4));

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Relaxed);
        }));
    }

    wait_until("chạy hết task", || done.load(Ordering::Relaxed) == TOTAL_TASK);
    assert_eq!(done.load(Ordering::Relaxed), TOTAL_TASK);
}

#[test]
fn t1_drop_pool_khong_treo()
{
    let done = Arc::new(AtomicUsize::new(0));

    {
        let mut pool = ThreadPool::new(cfg("t1", 4));
        for _ in 0..500
        {
            let done = done.clone();
            pool.push(Job::new(move || {
                done.fetch_add(1, Ordering::Relaxed);
            }));
        }
        wait_until("chạy được ít nhất một task", || done.load(Ordering::Relaxed) > 0);
    }

    // Tới đây `Drop` đã join xong mọi worker. Không worker nào còn chạy, nên con số đứng yên.
    let settled = done.load(Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(done.load(Ordering::Relaxed), settled, "còn worker chạy sau khi pool đã drop");
}

#[test]
fn t2_drop_pool_rong_ngay_sau_khi_dung()
{
    // Dựng rồi thả luôn, worker còn chưa kịp qua cổng khởi tạo. Vẫn phải dừng sạch.
    for _ in 0..20
    {
        let _pool = ThreadPool::new(cfg("t2", 4));
    }
}

#[test]
fn t3_pool_mot_worker_van_chay_duoc()
{
    const TOTAL_TASK: usize = 500;

    let done = Arc::new(AtomicUsize::new(0));
    let mut pool = ThreadPool::new(cfg("t3", 1));

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Relaxed);
        }));
    }

    wait_until("chạy hết task với đúng một worker", || done.load(Ordering::Relaxed) == TOTAL_TASK);
}

#[test]
fn t4_drop_giua_luc_worker_dang_tron_viec()
{
    // Cửa sổ nguy hiểm nhất của `Drop`: thả pool trong lúc worker vẫn đang ngó sang deque của
    // nhau. Nếu `shutdown` thu hồi worker nào đó trước khi join hết thì chỗ này đọc phải bộ nhớ đã
    // giải phóng. Trần cứng 200 vòng, đủ để lộ mà không chạy mãi.
    const ROUND: usize = 200;
    const TASK_PER_ROUND: usize = 300;

    let done = Arc::new(AtomicUsize::new(0));
    for round in 0..ROUND
    {
        let mut pool = ThreadPool::new(cfg("t4", 4));
        for _ in 0..TASK_PER_ROUND
        {
            let done = done.clone();
            pool.push(Job::new(move || {
                done.fetch_add(1, Ordering::Relaxed);
            }));
        }
        // Thả ngay, không chờ. Task chưa chạy kịp thì bị bỏ, đó là hành vi mong đợi.
        drop(pool);

        let settled = done.load(Ordering::Relaxed);
        assert!(
            settled <= (round + 1) * TASK_PER_ROUND,
            "đếm được {} task ở vòng {}, nhiều hơn số đã đẩy vào",
            settled,
            round
        );
    }
}

#[test]
fn t5_pool_ranh_thi_di_ngu_va_goi_day_duoc()
{
    const ROUND: usize = 20;

    let done = Arc::new(AtomicUsize::new(0));
    let mut pool = ThreadPool::new(cfg("t5", 4));

    // Không có việc thì backoff phải cạn và worker phải nằm xuống, chứ không quay vòng đốt core.
    wait_until("worker đi ngủ", || pool.inner.sleeping.load(Ordering::SeqCst) > 0);

    // Mỗi vòng: đẩy đúng một task vào lúc worker đang ngủ, rồi đợi nó chạy xong. Nếu `wake_one`
    // hỏng thì mỗi vòng phải chờ hết `SLEEP_SLICE` (100ms), tức là quá 2s cho cả 20 vòng.
    let started = Instant::now();
    for round in 0..ROUND
    {
        let counter = done.clone();
        pool.push(Job::new(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        }));
        wait_until("task chạy sau khi gọi dậy", || done.load(Ordering::Relaxed) == round + 1);
    }
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(1),
        "{} vòng đánh thức mất {:?}, nhiều khả năng đang chờ hết hạn giờ thay vì được gọi dậy",
        ROUND,
        elapsed
    );
}

#[test]
fn t6_capacity_le_duoc_lam_tron_len_power_of_two()
{
    const TOTAL_TASK: usize = 400;

    // 100 không phải power of two. Trước đây chỉ có `debug_assert` chặn, nên bản release lặng lẽ
    // tính sai mask. Giờ phải tự làm tròn lên 128 và chạy bình thường.
    let mut cfg = cfg("t6", 4);
    cfg.per_worker_task_capacity = 100;

    let done = Arc::new(AtomicUsize::new(0));
    let mut pool = ThreadPool::new(cfg);

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Relaxed);
        }));
    }

    wait_until("chạy hết task với capacity lẻ", || done.load(Ordering::Relaxed) == TOTAL_TASK);
}
