use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::*;
use std::time::Instant;

const ITERS: usize = 2_000_000;

fn bench<F: Fn(&AtomicU64) + Send + Sync + Copy + 'static>(name: &str, threads: usize, f: F)
{
    let a = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let hs: Vec<_> = (0..threads)
        .map(|_| {
            let a = a.clone();
            std::thread::spawn(move || {
                for _ in 0..ITERS
                {
                    f(&a)
                }
            })
        })
        .collect();
    for h in hs
    {
        h.join().unwrap();
    }
    let el = start.elapsed();
    println!("  {name:<26} {:>7.2} ns/op", el.as_nanos() as f64 / (ITERS * threads) as f64);
}

fn main()
{
    for threads in [1usize, 4]
    {
        println!("\n===== {threads} thread(s) =====");
        println!(" -- load --");
        bench("load Relaxed", threads, |a| {
            black_box(a.load(Relaxed));
        });
        bench("load Acquire", threads, |a| {
            black_box(a.load(Acquire));
        });
        bench("load SeqCst", threads, |a| {
            black_box(a.load(SeqCst));
        });
        println!(" -- store --");
        bench("store Relaxed", threads, |a| a.store(black_box(1), Relaxed));
        bench("store Release", threads, |a| a.store(black_box(1), Release));
        bench("store SeqCst", threads, |a| a.store(black_box(1), SeqCst));
        println!(" -- fetch_add (RMW) --");
        bench("fetch_add Relaxed", threads, |a| {
            black_box(a.fetch_add(1, Relaxed));
        });
        bench("fetch_add AcqRel", threads, |a| {
            black_box(a.fetch_add(1, AcqRel));
        });
        bench("fetch_add SeqCst", threads, |a| {
            black_box(a.fetch_add(1, SeqCst));
        });
        println!(" -- compare_exchange --");
        bench("CAS Relaxed", threads, |a| {
            let v = a.load(Relaxed);
            let _ = a.compare_exchange(v, v + 1, Relaxed, Relaxed);
        });
        bench("CAS AcqRel", threads, |a| {
            let v = a.load(Relaxed);
            let _ = a.compare_exchange(v, v + 1, AcqRel, Acquire);
        });
        bench("CAS SeqCst", threads, |a| {
            let v = a.load(Relaxed);
            let _ = a.compare_exchange(v, v + 1, SeqCst, SeqCst);
        });
    }
}
