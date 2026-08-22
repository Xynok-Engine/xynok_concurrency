use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const ITER: usize = 1_000_000;
static VAL: AtomicUsize = AtomicUsize::new(0);

fn run<F>(amount: usize, body: F) -> (usize, Duration)
where F: Fn() + Send + Sync + 'static
{
    VAL.store(0, Ordering::SeqCst);

    let barrier = Arc::new(Barrier::new(amount + 1));
    let body = Arc::new(body);
    let handles: Vec<_> = (0..amount)
        .map(|_| {
            let barrier = barrier.clone();
            let body = body.clone();
            thread::spawn(move || {
                barrier.wait(); // start together
                body();
            })
        })
        .collect();

    barrier.wait();
    let started = Instant::now();
    for handle in handles
    {
        handle.join().unwrap();
    }
    let elapsed = started.elapsed();

    (VAL.load(Ordering::SeqCst), elapsed)
}

fn report(label: &str, amount: usize, got: usize, expected: usize, elapsed: Duration)
{
    let mark = if got == expected { "  ok" } else { "LOST" };
    let ns_per_op = elapsed.as_secs_f64() * 1e9 / expected as f64;
    let mops = expected as f64 / elapsed.as_secs_f64() / 1e6;
    println!(
        "{mark} | {amount}t | {label:<34} | {got:>8} / {expected:>8} | lost {:>8} | {:>8.2} ms | {ns_per_op:>7.2} ns/op | {mops:>7.1} Mop/s",
        expected - got,
        elapsed.as_secs_f64() * 1e3
    );
}

fn fetch_add(amount: usize, order: Ordering)
{
    let (got, elapsed) = run(amount, move || {
        for _ in 0..ITER
        {
            VAL.fetch_add(1, order);
        }
    });
    report(&format!("fetch_add({order:?})"), amount, got, amount * ITER, elapsed);
}

fn load_store(amount: usize, order_read: Ordering, order_store: Ordering)
{
    let (got, elapsed) = run(amount, move || {
        for _ in 0..ITER
        {
            let old = VAL.load(order_read);
            VAL.store(old + 1, order_store);
        }
    });
    report(&format!("load({order_read:?}) + store({order_store:?})"), amount, got, amount * ITER, elapsed);
}

fn cas_retry(amount: usize, success: Ordering, failure: Ordering)
{
    let (got, elapsed) = run(amount, move || {
        for _ in 0..ITER
        {
            let mut current = VAL.load(failure);
            loop
            {
                match VAL.compare_exchange_weak(current, current + 1, success, failure)
                {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }
    });
    report(&format!("cas_retry({success:?}, {failure:?})"), amount, got, amount * ITER, elapsed);
}

fn cas_no_retry(amount: usize, success: Ordering, failure: Ordering)
{
    let (got, elapsed) = run(amount, move || {
        for i in 0..ITER
        {
            let _ = VAL.compare_exchange(i, i + 1, success, failure);
        }
    });
    report(&format!("cas_no_retry({success:?}, {failure:?})"), amount, got, amount * ITER, elapsed);
}

fn main()
{
    for amount in [1usize, 2, 4, 8]
    {
        println!("--------------------------------------------------------------------------------------------------------------------");
        fetch_add(amount, Ordering::Relaxed);
        fetch_add(amount, Ordering::Release);
        fetch_add(amount, Ordering::Acquire);
        fetch_add(amount, Ordering::SeqCst);
        load_store(amount, Ordering::Acquire, Ordering::Release);
        load_store(amount, Ordering::SeqCst, Ordering::SeqCst);
        cas_retry(amount, Ordering::AcqRel, Ordering::Acquire);
        cas_no_retry(amount, Ordering::AcqRel, Ordering::Acquire);
    }
}
