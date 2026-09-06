use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::apis::priority::Priority;
use crate::custom_type::Job;
use crate::sync::{AtomicUsize, Ordering};
use crate::thread_pool::{CfgThreadPool, ThreadPool};

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

fn wait_until(what: &str, cond: impl Fn() -> bool)
{
    let deadline = Instant::now() + TIMEOUT;
    while !cond()
    {
        assert!(
            Instant::now() < deadline,
            "Timeout of {:?} reached, but `{}` has not finished yet",
            TIMEOUT,
            what
        );
        std::thread::yield_now();
    }
}

#[test]
fn t0_pool_chay_het_task_da_day_vao()
{
    const TOTAL_TASK: usize = 2_000;

    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg("t0", 4));

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("all tasks have finished", || done.load(Ordering::Acquire) == TOTAL_TASK);
    assert_eq!(done.load(Ordering::Acquire), TOTAL_TASK);
}

#[test]
fn t1_drop_pool_khong_treo()
{
    let done = Arc::new(AtomicUsize::new(0));

    {
        let pool = ThreadPool::new(cfg("t1", 4));
        for _ in 0..500
        {
            let done = done.clone();
            pool.push(Job::new(move || {
                done.fetch_add(1, Ordering::Release);
            }));
        }
        wait_until("chạy được ít nhất một task", || done.load(Ordering::Acquire) > 0);
    }

    // Tới đây `Drop` đã join xong mọi worker. Không worker nào còn chạy, nên con số đứng yên.
    let settled = done.load(Ordering::Acquire);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(done.load(Ordering::Acquire), settled, "còn worker chạy sau khi pool đã drop");
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
    let pool = ThreadPool::new(cfg("t3", 1));

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("chạy hết task với đúng một worker", || done.load(Ordering::Acquire) == TOTAL_TASK);
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
        let pool = ThreadPool::new(cfg("t4", 4));
        for _ in 0..TASK_PER_ROUND
        {
            let done = done.clone();
            pool.push(Job::new(move || {
                done.fetch_add(1, Ordering::Release);
            }));
        }
        // Thả ngay, không chờ. Task chưa chạy kịp thì bị bỏ, đó là hành vi mong đợi.
        drop(pool);

        let settled = done.load(Ordering::Acquire);
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
    let pool = ThreadPool::new(cfg("t5", 4));

    // Không có việc thì backoff phải cạn và worker phải nằm xuống, chứ không quay vòng đốt core.
    wait_until("worker đi ngủ", || pool.inner.sleepings.len() == 4);

    // Mỗi vòng: đẩy đúng một task vào lúc worker đang ngủ, rồi đợi nó chạy xong. Nếu `wake_one`
    // hỏng thì mỗi vòng phải chờ hết `SLEEP_SLICE` (100ms), tức là quá 2s cho cả 20 vòng.
    let started = Instant::now();
    for round in 0..ROUND
    {
        let counter = done.clone();
        pool.push(Job::new(move || {
            counter.fetch_add(1, Ordering::Release);
        }));
        wait_until("task chạy sau khi gọi dậy", || done.load(Ordering::Acquire) == round + 1);
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
    let pool = ThreadPool::new(cfg);

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("chạy hết task với capacity lẻ", || done.load(Ordering::Acquire) == TOTAL_TASK);
}

/// Cho phép mang một tham chiếu tới pool vào trong [`Job`], vốn đòi `'static`.
///
/// Chỉ dùng trong file này, và mọi test dùng nó đều đợi job chạy xong trước khi thả pool.
#[derive(Clone, Copy)]
struct PoolRef(*const ThreadPool);
unsafe impl Send for PoolRef {}
unsafe impl Sync for PoolRef {}
impl PoolRef
{
    fn get(&self) -> &ThreadPool
    {
        unsafe { &*self.0 }
    }
}
#[test]
fn t10_scope_chay_het_job_va_cho_muon_stack()
{
    const TOTAL_JOB: usize = 2_000;

    let pool = ThreadPool::new(cfg("t10", 4));

    // `counter` nằm trên stack ngay đây, job mượn thẳng chứ không đi qua `Arc`. Đây là điểm khác
    // duy nhất giữa `scope` và `push`, và cũng là lý do scope phải chờ bằng được.
    let counter = AtomicUsize::new(0);
    pool.scope(|s| {
        for _ in 0..TOTAL_JOB
        {
            s.spawn(|| {
                counter.fetch_add(1, Ordering::Release);
            });
        }
    });

    assert!(counter.load(Ordering::Acquire) == TOTAL_JOB, "scope trả về khi job còn chưa chạy xong");
}

#[test]
fn t11_scope_deque_be_hon_so_job_thi_van_khong_treo()
{
    const TOTAL_JOB: usize = 5_000;

    // Deque riêng chỉ có 2 ô. Job không nhét hết vào đó được, phần dư phải ở lại trên stack của
    // người gọi rồi đẩy dần, chứ không được tuồn sang hàng đợi chung.
    let mut cfg = cfg("t11", 1);
    cfg.per_worker_task_capacity = 2;

    let pool = ThreadPool::new(cfg);
    let counter = AtomicUsize::new(0);

    pool.scope(|s| {
        for _ in 0..TOTAL_JOB
        {
            s.spawn(|| {
                counter.fetch_add(1, Ordering::Release);
            });
        }
    });

    assert_eq!(counter.load(Ordering::Acquire), TOTAL_JOB);
}

#[test]
fn t13_scope_long_nhau_tu_trong_mot_job()
{
    const OUTER: usize = 16;
    const INNER: usize = 32;

    let pool = ThreadPool::new(cfg("t13", 4));
    let pool_ref = PoolRef(&pool as *const ThreadPool);
    let counter = AtomicUsize::new(0);

    // Scope trong đợi ngay trên thread worker. Nếu điểm chờ đó đi ngủ thay vì chạy giúp thì cả
    // pool khoá cứng: mấy người ngủ chính là mấy người phải chạy nốt job mà họ đang đợi.
    pool.scope(|outer| {
        for _ in 0..OUTER
        {
            outer.spawn(|| {
                pool_ref.get().scope(|inner| {
                    for _ in 0..INNER
                    {
                        inner.spawn(|| {
                            counter.fetch_add(1, Ordering::Release);
                        });
                    }
                });
            });
        }
    });

    assert_eq!(counter.load(Ordering::Acquire), OUTER * INNER);
}

#[test]
fn t14_panic_trong_job_duoc_nem_lai_o_scope()
{
    let pool = ThreadPool::new(cfg("t14", 4));
    let done = AtomicUsize::new(0);

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.scope(|s| {
            s.spawn(|| panic!("job này chết giữa chừng"));
            for _ in 0..100
            {
                s.spawn(|| {
                    done.fetch_add(1, Ordering::Release);
                });
            }
        });
    }));

    std::panic::set_hook(previous);
    assert!(outcome.is_err(), "panic trong job bị nuốt mất, người mở scope không hề biết");
}

#[test]
fn t16_main_thread_chay_job_trong_luc_cho_o_scope()
{
    const TOTAL_JOB: usize = 2_000;

    // Một worker duy nhất, và job thì nhiều. Nếu main chỉ đứng chờ suông thì con số dưới đây bằng
    // 0. Main tham gia thật thì nó phải bốc được một phần, vì worker kia không thể nuốt hết 2000
    // job nhanh hơn main lấy một cái ra chạy.
    let pool = ThreadPool::new(cfg("t16", 1));
    let main_id = std::thread::current().id();

    let on_main = AtomicUsize::new(0);
    let total = AtomicUsize::new(0);

    pool.scope(|s| {
        for _ in 0..TOTAL_JOB
        {
            s.spawn(|| {
                if std::thread::current().id() == main_id
                {
                    on_main.fetch_add(1, Ordering::Release);
                }
                total.fetch_add(1, Ordering::Release);
            });
        }
    });

    assert_eq!(total.load(Ordering::Acquire), TOTAL_JOB, "scope trả về mà job chưa chạy hết");
    assert!(
        on_main.load(Ordering::Acquire) > 0,
        "main đứng chờ suông trong scope, không chạy giúp job nào trong {} job",
        TOTAL_JOB
    );
}

#[test]
fn t17_main_khong_bi_bien_thanh_worker_thuong_tru()
{
    const TOTAL_JOB: usize = 200;

    // Ra khỏi scope là main phải trả lại quyền cho chính nó. Job đẩy vào sau đó không được phép
    // chạy trên main, vì main có bao giờ quay lại vòng chạy job nữa đâu.
    let pool = ThreadPool::new(cfg("t17", 2));
    let main_id = std::thread::current().id();

    pool.scope(|s| {
        for _ in 0..8
        {
            s.spawn(|| {});
        }
    });

    let on_main = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..TOTAL_JOB
    {
        let on_main = on_main.clone();
        let done = done.clone();
        pool.push(Job::new(move || {
            if std::thread::current().id() == main_id
            {
                on_main.fetch_add(1, Ordering::Release);
            }
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("worker chạy hết việc ngoài scope", || done.load(Ordering::Acquire) == TOTAL_JOB);
    assert_eq!(
        on_main.load(Ordering::Acquire),
        0,
        "job chạy trên main trong lúc main không hề ở trong một điểm chờ nào"
    );
}
