//! Đo pool: chi phí spawn một job, và `batch` bao nhiêu thì bõ công chia.
//!
//! Chạy: `cargo bench --bench pool`
//!
//! Mục 8.3 của tài liệu lane nói chi phí spawn cỡ 1 tới 5 micro giây và job nên dài chừng 20 micro
//! giây. Cả hai con số đó là phỏng đoán có lý do, và đây là chỗ biến chúng thành số đo trên chính
//! cái máy đang chạy.

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};

use divan::Bencher;
use xynok_concurrency::pool::{Config, ThreadPool};

fn main()
{
    divan::main();
}

fn pool_with(threads: usize) -> ThreadPool
{
    ThreadPool::new(Config {
        threads: threads,
        ..Config::default()
    })
}

/// Một job rỗng, đo qua `scope`. Đây là chi phí ghi sổ trần trụi của một lần fork-join.
#[divan::bench(args = [1, 2, 4, 8])]
fn spawn_mot_job_rong(bencher: Bencher, threads: usize)
{
    let pool = pool_with(threads);
    let counter = AtomicUsize::new(0);

    bencher.bench_local(|| {
        pool.scope(|s| {
            s.spawn(|| {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        });
    });

    black_box(counter.load(Ordering::Relaxed));
}

/// Một scope với nhiều job rỗng: chi phí mỗi job khi pool đang bận thật.
#[divan::bench(args = [8, 64, 512])]
fn spawn_nhieu_job_rong(bencher: Bencher, jobs: usize)
{
    let pool = pool_with(4);
    let counter = AtomicUsize::new(0);

    bencher.bench_local(|| {
        pool.scope(|s| {
            for _ in 0..jobs
            {
                s.spawn(|| {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    });

    black_box(counter.load(Ordering::Relaxed));
}

/// Cùng một khối lượng việc, chia theo các cỡ lô khác nhau.
///
/// `batch = N` nghĩa là một job duy nhất, tức là chạy tuần tự, nên nó vừa là số đối chứng vừa là
/// câu trả lời cho "chia có bõ không". Cỡ lô nào nhanh hơn cột đó thì cỡ ấy bõ.
#[divan::bench(args = [64, 256, 1024, 4096, 65_536])]
fn parallel_for_theo_co_lo(bencher: Bencher, batch: usize)
{
    const N: usize = 65_536;

    let pool = pool_with(4);
    let data: Vec<u64> = (0..N as u64).collect();

    bencher.bench_local(|| {
        pool.parallel_for(N, batch, |i| {
            // Việc thật, và quan trọng hơn: **không đụng vào thứ gì dùng chung**. Một `fetch_add`
            // vào một ô chung ở đây sẽ át hết mọi thứ khác, và phép đo sẽ nói về cache line ấy chứ
            // không nói gì về cỡ lô.
            let mut value = data[i];
            for _ in 0..16
            {
                value = value.wrapping_mul(2_654_435_761).rotate_left(7);
            }
            black_box(value);
        });
    });
}

/// Cùng khối lượng ấy nhưng mỗi phần tử chỉ tốn một phép nhân.
///
/// Đây là phía bên kia của ngưỡng chia: việc quá nhẹ thì chi phí spawn nuốt hết phần lãi, và cột
/// `65536`, tức là một job duy nhất, sẽ thắng.
#[divan::bench(args = [64, 1024, 65_536])]
fn parallel_for_viec_qua_nhe(bencher: Bencher, batch: usize)
{
    const N: usize = 65_536;

    let pool = pool_with(4);
    let data: Vec<u64> = (0..N as u64).collect();

    bencher.bench_local(|| {
        pool.parallel_for(N, batch, |i| {
            black_box(data[i].wrapping_mul(2_654_435_761));
        });
    });
}

/// `join` đệ quy, hình dạng mà mọi thuật toán chia đôi đi theo.
#[divan::bench(args = [1, 2, 4, 8])]
fn join_de_quy(bencher: Bencher, threads: usize)
{
    fn sum(pool: &ThreadPool, range: std::ops::Range<u64>) -> u64
    {
        let width = range.end - range.start;
        if width <= 1_024
        {
            return range.map(|value| value.wrapping_mul(3)).sum();
        }

        let middle = range.start + width / 2;
        let (left, right) = pool.join(|| sum(pool, range.start..middle), || sum(pool, middle..range.end));
        left + right
    }

    let pool = pool_with(threads);
    bencher.bench_local(|| black_box(sum(&pool, 0..131_072)));
}

/// Lane queue: chi phí khi job tới từ một thread không thuộc pool.
///
/// Pool được dựng trên một thread khác, nên thread chạy phép đo này là người ngoài thật sự: nó
/// không sở hữu ring nào và mọi job nó đẩy đều phải đi qua lane queue.
#[divan::bench]
fn day_tu_ngoai_pool(bencher: Bencher)
{
    let pool = std::thread::spawn(|| pool_with(4)).join().expect("không dựng được pool");
    let counter = std::sync::Arc::new(AtomicUsize::new(0));

    bencher.bench_local(|| {
        let counter = std::sync::Arc::clone(&counter);
        pool.spawn(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        });
    });

    // Chờ pool tiêu hoá hết chỗ vừa đẩy vào, để lần đo sau không bắt đầu từ một hàng đợi đầy.
    pool.shutdown();
    black_box(counter.load(Ordering::Relaxed));
}
