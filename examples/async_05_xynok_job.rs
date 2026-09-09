// CASE: await a CPU job executed by the existing xynok thread pool.
// No second worker pool and no I/O reactor. Only main runs the async future.
mod async_support;
use xynok_concurrency::{thread_pool::{ThreadPool, cfg::CfgThreadPool}, utils::inline_fn::InlineFn};
fn main() {
    let pool = ThreadPool::new(CfgThreadPool::new("async-lesson", 2));
    let (tx, rx) = async_support::oneshot();
    pool.push(InlineFn::new(move || {
        let result: u64 = (1..=1000).sum();
        tx.send(result); // Publish the result, then wake main's root future.
    }));
    let result = async_support::block_on(async { rx.await.unwrap() });
    assert_eq!(result, 500500);
    println!("CPU result: {result}");
    // This bridges a job result; it does not yet make the pool an async executor.
}
