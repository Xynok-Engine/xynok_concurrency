use std::sync::Arc;
use std::time::Instant;
use xynok_concurrency::utils::spinlock::SpinLock;

const ITERS: usize = 1_000_000;

fn bench<F: Fn() + Send + Sync + 'static + Clone>(name: &str, threads: usize, f: F)
{
    let barrier = Arc::new(std::sync::Barrier::new(threads + 1));
    let hs: Vec<_> = (0..threads)
        .map(|_| {
            let f = f.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..ITERS
                {
                    f()
                }
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    for h in hs
    {
        h.join().unwrap();
    }
    let el = start.elapsed();
    println!("  {name:<30} {:>8.1} ns/op", el.as_nanos() as f64 / (ITERS * threads) as f64);
}
fn main()
{
    for threads in [1usize, 2, 4, 8]
    {
        println!("\n== {threads} thread(s), {ITERS} iters/thread ==");

        let l = Arc::new(SpinLock::new(0usize));
        {
            let l = l.clone();
            bench("SpinLock<usize> guard", threads, move || {
                *l.get() += 1;
            });
        }

        let m = Arc::new(std::sync::Mutex::new(0usize));
        {
            let m = m.clone();
            bench("std::sync::Mutex", threads, move || {
                *m.lock().unwrap() += 1;
            });
        }

        let m = Arc::new(MutexCondition::new(0usize));
        {
            let m = m.clone();
            bench("MutexCondition", threads, move || {
                *m.get() += 1;
            });
        }
    }
}
