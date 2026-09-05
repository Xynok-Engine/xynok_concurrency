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
    let pool = ThreadPool::new(cfg("t0", 4));

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("chạy hết task", || done.load(Ordering::Acquire) == TOTAL_TASK);
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
    wait_until("worker đi ngủ", || pool.inner.sleeping.load(Ordering::SeqCst) > 0);

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
fn t7_spawn_local_day_thi_tra_task_ve_cho_nguoi_goi()
{
    const TOTAL_CHILD: usize = 500;

    // Deque riêng chỉ có 2 ô, mà job cha đẻ ra 500 job con, tất cả đều nhắm vào deque của chính
    // worker đang chạy. Pool một người nên không ai trộm hộ. Hết chỗ thì `spawn_local` phải trả
    // nguyên task về ngay, chứ không được quay tại chỗ đợi có ô trống: người duy nhất lấy việc ra
    // khỏi deque đó đang kẹt ngay trong lúc đẩy vào, đợi là treo vĩnh viễn.
    let mut cfg = cfg("t7", 1);
    cfg.per_worker_task_capacity = 2;

    let done = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg);
    let pool_ref = PoolRef(&pool as *const ThreadPool);

    let counter = done.clone();
    let rejected_counter = rejected.clone();
    pool.push(Job::new(move || {
        for _ in 0..TOTAL_CHILD
        {
            let counter = counter.clone();
            let child = Job::new(move || {
                counter.fetch_add(1, Ordering::Release);
            });

            // Phần dư là chuyện của người gọi. Ở đây job cha chọn cách đơn giản nhất là chạy luôn
            // tại chỗ, chứ không tuồn sang hàng đợi chung.
            if let Err(task) = pool_ref.get().spawn_local(child)
            {
                rejected_counter.fetch_add(1, Ordering::Release);
                task.run_once();
            }
        }
    }));

    wait_until("chạy hết job con", || done.load(Ordering::Acquire) == TOTAL_CHILD);
    assert!(
        rejected.load(Ordering::Acquire) > 0,
        "deque chỉ có 2 ô mà 500 job con vào lọt hết, `spawn_local` đang không báo đầy"
    );
}

#[test]
fn t8_spawn_local_cheo_pool_bi_tu_choi()
{
    const TOTAL_TASK: usize = 400;

    // `b` khai báo trước để nó bị thả sau: worker của `a` còn cầm con trỏ tới nó.
    let pool_b = ThreadPool::new(cfg("t8b", 1));

    // `a` đông người hơn `b`. Nếu chỗ đứng thread local chỉ nhớ mỗi chỉ số thì worker số 3 của `a`
    // sẽ với vào ô số 3 trong dãy worker của `b`, mà dãy đó chỉ có một ô: vừa đọc ra ngoài vùng,
    // vừa đẩy vào deque của người khác từ một thread không phải chủ nó. Nhớ thêm số hiệu pool thì
    // lần này trượt, task được trả về nguyên vẹn và người gọi tự đưa nó vào hàng đợi chung của `b`.
    let pool_a = ThreadPool::new(cfg("t8a", 4));

    let done = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let b_ref = PoolRef(&pool_b as *const ThreadPool);

    for _ in 0..TOTAL_TASK
    {
        let counter = done.clone();
        let rejected_counter = rejected.clone();
        pool_a.push(Job::new(move || {
            let child = Job::new(move || {
                counter.fetch_add(1, Ordering::Release);
            });

            match b_ref.get().spawn_local(child)
            {
                Ok(()) =>
                {}
                Err(task) =>
                {
                    rejected_counter.fetch_add(1, Ordering::Release);
                    b_ref.get().push(task);
                }
            }
        }));
    }

    wait_until("pool b chạy hết việc do pool a giao", || done.load(Ordering::Acquire) == TOTAL_TASK);
    assert_eq!(
        rejected.load(Ordering::Acquire),
        TOTAL_TASK,
        "worker của pool a đẩy lọt việc vào deque riêng của pool b"
    );
}

#[test]
fn t9_spawn_local_o_lai_dung_worker_da_sinh_ra_no()
{
    const TOTAL_CHILD: usize = 100;

    // Một worker duy nhất, deque rộng rãi. Job con đẩy bằng `spawn_local` phải vào lọt hết, và
    // chạy trên đúng thread đã sinh ra chúng.
    let done = Arc::new(AtomicUsize::new(0));
    let same_thread = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg("t9", 1));
    let pool_ref = PoolRef(&pool as *const ThreadPool);

    let counter = done.clone();
    let same_thread_counter = same_thread.clone();
    pool.push(Job::new(move || {
        let parent = std::thread::current().id();
        for _ in 0..TOTAL_CHILD
        {
            let counter = counter.clone();
            let same_thread_counter = same_thread_counter.clone();
            let child = Job::new(move || {
                if std::thread::current().id() == parent
                {
                    same_thread_counter.fetch_add(1, Ordering::Release);
                }
                counter.fetch_add(1, Ordering::Release);
            });

            assert!(pool_ref.get().spawn_local(child).is_ok(), "deque còn chỗ mà `spawn_local` lại từ chối");
        }
    }));

    wait_until("chạy hết job con", || done.load(Ordering::Acquire) == TOTAL_CHILD);
    assert_eq!(
        same_thread.load(Ordering::Acquire),
        TOTAL_CHILD,
        "job con chạy ở thread khác thread đã sinh ra nó"
    );
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

    assert_eq!(counter.load(Ordering::Acquire), TOTAL_JOB, "scope trả về khi job còn chưa chạy xong");
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
fn t12_spawn_with_chia_doi_de_quy()
{
    const DEPTH: usize = 10;

    let pool = ThreadPool::new(cfg("t12", 4));
    let counter = AtomicUsize::new(0);

    fn split<'a>(s: &crate::thread_pool::scope::Scope<'a>, counter: &'a AtomicUsize, depth: usize)
    {
        counter.fetch_add(1, Ordering::Release);
        if depth == 0
        {
            return;
        }
        for _ in 0..2
        {
            s.spawn_with(move |s| split(s, counter, depth - 1));
        }
    }

    pool.scope(|s| split(s, &counter, DEPTH));

    // Cây nhị phân đủ tầng: 2^(DEPTH+1) - 1 nút.
    assert_eq!(counter.load(Ordering::Acquire), (1 << (DEPTH + 1)) - 1);
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
fn t15_huy_scope_thi_job_chua_chay_bo_qua_phan_than()
{
    const TOTAL_JOB: usize = 1_000;

    // Một worker duy nhất, nên job xếp hàng chờ tới lượt. Huỷ ngay từ đầu thì gần như cả đám bỏ
    // qua phần thân, nhưng scope vẫn phải đợi đủ vé mới được trả về.
    let pool = ThreadPool::new(cfg("t15", 1));
    let counter = AtomicUsize::new(0);

    pool.scope(|s| {
        s.cancel();
        for _ in 0..TOTAL_JOB
        {
            s.spawn(|| {
                counter.fetch_add(1, Ordering::Release);
            });
        }
    });

    assert_eq!(counter.load(Ordering::Acquire), 0, "job vẫn chạy phần thân dù scope đã bị huỷ");
    assert!(pool.inner.tasks.is_empty(), "còn job của scope đã huỷ nằm lại trong hàng đợi chung");
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

#[test]
fn t18_viec_sot_lai_trong_deque_cua_main_van_duoc_tron_di()
{
    const TOTAL_CHILD: usize = 100;

    // Main đẩy vào deque riêng của nó rồi bỏ đi làm việc khác, không quay lại chạy. Deque của host
    // phải nằm trong tầm trộm của worker, không thì đám job này không ai chạy và test treo tới khi
    // chạm trần.
    let pool = ThreadPool::new(cfg("t18", 2));

    let done = Arc::new(AtomicUsize::new(0));
    let mut pushed = 0usize;
    for _ in 0..TOTAL_CHILD
    {
        let done = done.clone();
        let child = Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        });

        // Host có ô riêng nên đường này phải mở, khác hẳn một thread lạ.
        match pool.spawn_local(child)
        {
            Ok(()) => pushed += 1,
            Err(task) => pool.push(task),
        }
    }

    assert!(pushed > 0, "host không đẩy được job nào vào deque riêng của nó");
    wait_until("worker trộm hết việc còn sót trong deque của host", || {
        done.load(Ordering::Acquire) == TOTAL_CHILD
    });
}

#[test]
fn t19_thread_la_van_bi_tu_choi_deque()
{
    // Host có ô, nhưng người lạ thì không. Một thread bất kỳ đẩy được vào deque nào đó nghĩa là hai
    // thread cùng làm chủ một ring, mà ring này chỉ chịu đúng một người đẩy.
    let pool = ThreadPool::new(cfg("t19", 2));
    let pool_ref = PoolRef(&pool as *const ThreadPool);

    let refused = std::thread::spawn(move || {
        let pool_ref = pool_ref;
        pool_ref.get().spawn_local(Job::new(|| {})).is_err()
    })
    .join()
    .expect("thread phụ panic");

    assert!(refused, "một thread không thuộc pool lại đẩy lọt vào deque riêng");
}
