//! Stress test cho pool: nhiều thread cùng đẩy việc, nhiều worker cùng giành việc.
//!
//! Mọi vòng gom kết quả ở đây đều có **trần cứng**, và đó không phải sự cẩn thận thừa. Một vòng
//! `while chưa đủ {}` chạy trên một pool đang hỏng thì không dừng, mà nếu nó vừa quay vừa cấp phát
//! thì nó ăn sạch RAM của máy trước khi ai kịp nhận ra. Ở đây mọi vòng chờ đều có deadline theo
//! đồng hồ, và chạm deadline là `panic` với thông tin đủ để lần ra chuyện gì đang kẹt.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::*;

/// Trần cứng cho mọi lần chờ trong file này.
const DEADLINE: Duration = Duration::from_secs(30);

/// Chờ `done` thành `true`, vừa chờ vừa chạy job giúp pool, và bỏ cuộc khi hết giờ.
fn run_until_or_panic(pool: &ThreadPool, what: &str, done: impl Fn() -> bool)
{
    let deadline = Instant::now() + DEADLINE;
    pool.run_until(|| done() || Instant::now() >= deadline);

    assert!(done(), "{what}: hết {DEADLINE:?} mà vẫn chưa xong, pool = {pool:?}");
}

/// xorshift64. Không cần thêm dependency, và cùng một seed thì lặp lại đúng cùng một kiểu ép tải.
struct Rng(u64);

impl Rng
{
    fn new(seed: u64) -> Self
    {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64
    {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64
    {
        self.next() % n.max(1)
    }
}

#[test]
fn nhieu_thread_ngoai_cung_day_viec_thi_khong_mat_job_nao()
{
    const FEEDERS: usize = 4;
    const PER_FEEDER: usize = 5_000;
    const TOTAL: usize = FEEDERS * PER_FEEDER;

    let pool = Arc::new(ThreadPool::new(Config {
        threads: 4,
        ring_capacity: 32,
        ..Config::default()
    }));
    let done = Arc::new(AtomicUsize::new(0));

    let feeders: Vec<_> = (0..FEEDERS)
        .map(|feeder| {
            let pool = Arc::clone(&pool);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                let mut rng = Rng::new(feeder as u64);
                for _ in 0..PER_FEEDER
                {
                    let done = Arc::clone(&done);
                    pool.spawn(move || {
                        done.fetch_add(1, Ordering::Relaxed);
                    });

                    // Thỉnh thoảng nghỉ một nhịp, để worker kịp cạn việc và đi ngủ thật. Đường ngủ
                    // rồi bị gọi dậy mới là đường đáng ép, chứ pool lúc nào cũng đầy việc thì không
                    // bao giờ chạm tới nó.
                    if rng.below(256) == 0
                    {
                        std::thread::yield_now();
                    }
                }
            })
        })
        .collect();

    for feeder in feeders
    {
        feeder.join().expect("một thread đẩy việc panic");
    }

    run_until_or_panic(&pool, "nhieu_thread_ngoai_cung_day_viec", || done.load(Ordering::Acquire) == TOTAL);
    assert_eq!(done.load(Ordering::Acquire), TOTAL);
}

#[test]
fn job_de_job_con_nhieu_tang_thi_khong_job_nao_bi_bo_lai()
{
    /// Mỗi job đẻ ra `FANOUT` job con cho tới khi hết tầng.
    const FANOUT: usize = 3;
    const DEPTH: usize = 8;

    fn total_nodes() -> usize
    {
        let mut nodes = 0;
        let mut level = 1;
        for _ in 0..DEPTH
        {
            nodes += level;
            level *= FANOUT;
        }
        nodes
    }

    let pool = Arc::new(ThreadPool::new(Config {
        threads: 4,
        ring_capacity: 16,
        ..Config::default()
    }));
    let done = Arc::new(AtomicUsize::new(0));

    fn grow(pool: Arc<ThreadPool>, done: Arc<AtomicUsize>, depth: usize)
    {
        done.fetch_add(1, Ordering::Relaxed);
        if depth == 0
        {
            return;
        }

        for _ in 0..FANOUT
        {
            let pool_child = Arc::clone(&pool);
            let done_child = Arc::clone(&done);
            pool.spawn(move || grow(pool_child, done_child, depth - 1));
        }
    }

    let root_pool = Arc::clone(&pool);
    let root_done = Arc::clone(&done);
    pool.spawn(move || grow(root_pool, root_done, DEPTH - 1));

    let total = total_nodes();
    run_until_or_panic(&pool, "job_de_job_con_nhieu_tang", || done.load(Ordering::Acquire) == total);
    assert_eq!(done.load(Ordering::Acquire), total);
}

#[test]
fn tung_dot_viec_xen_ke_luc_ngu_luc_thuc()
{
    const ROUNDS: usize = 200;
    const PER_ROUND: usize = 64;

    let pool = ThreadPool::new(Config {
        threads: 4,
        spin_rounds: 1,
        ..Config::default()
    });
    let mut rng = Rng::new(0xBEEF);

    for round in 0..ROUNDS
    {
        let done = Arc::new(AtomicUsize::new(0));

        for _ in 0..PER_ROUND
        {
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }

        run_until_or_panic(&pool, "tung_dot_viec_xen_ke", || done.load(Ordering::Acquire) == PER_ROUND);
        assert_eq!(done.load(Ordering::Acquire), PER_ROUND, "vòng {round}");

        // Nghỉ ngẫu nhiên: có vòng thì pool còn nóng, có vòng thì mọi worker đã park hẳn.
        if rng.below(3) == 0
        {
            std::thread::sleep(Duration::from_micros(rng.below(500)));
        }
    }
}

#[test]
fn moi_job_chay_dung_mot_lan_va_dung_mot_worker()
{
    const JOBS: usize = 20_000;

    let pool = Arc::new(ThreadPool::new(Config {
        threads: 4,
        ring_capacity: 32,
        ..Config::default()
    }));

    // Mỗi job có ô riêng: chạy hai lần thì ô đó thành 2, và tổng không nói ra được điều đó nếu có
    // một job khác bị mất.
    let slots: Arc<Vec<AtomicUsize>> = Arc::new((0..JOBS).map(|_| AtomicUsize::new(0)).collect());
    let done = Arc::new(AtomicUsize::new(0));

    for job in 0..JOBS
    {
        let slots = Arc::clone(&slots);
        let done = Arc::clone(&done);
        pool.spawn(move || {
            slots[job].fetch_add(1, Ordering::Relaxed);
            done.fetch_add(1, Ordering::Release);
        });
    }

    run_until_or_panic(&pool, "moi_job_chay_dung_mot_lan", || done.load(Ordering::Acquire) == JOBS);

    let wrong: Vec<usize> = (0..JOBS).filter(|&job| slots[job].load(Ordering::Acquire) != 1).collect();
    assert!(
        wrong.is_empty(),
        "{} job không chạy đúng một lần, ví dụ {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(8)]
    );
}

#[test]
fn dung_pool_giua_luc_dang_bon_viec_van_khong_bo_job_nao()
{
    const ROUNDS: usize = 20;
    const JOBS: usize = 2_000;

    for round in 0..ROUNDS
    {
        let done = Arc::new(AtomicUsize::new(0));
        {
            let pool = ThreadPool::new(Config {
                threads: 3,
                ring_capacity: 16,
                ..Config::default()
            });

            for _ in 0..JOBS
            {
                let done = Arc::clone(&done);
                pool.spawn(move || {
                    done.fetch_add(1, Ordering::Relaxed);
                });
            }
            // Không chờ gì cả: `shutdown` phải tự lo cho phần còn xếp hàng.
            pool.shutdown();
        }

        assert_eq!(done.load(Ordering::Acquire), JOBS, "vòng {round}: shutdown bỏ rơi job");
    }
}
