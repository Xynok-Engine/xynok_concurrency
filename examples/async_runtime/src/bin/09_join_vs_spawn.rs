// CASE: concurrency in one task vs independently scheduled tasks.
// join!/spawn are Tokio APIs, not Rust keywords or required standard interfaces.
use tokio::time::{sleep, Duration};
async fn work(name: &str) -> u32 {
    println!("{name}: start on {:?}", std::thread::current().id());
    sleep(Duration::from_millis(10)).await;
    println!("{name}: done");
    21
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let a = work("sequential A").await;
    let b = work("sequential B").await;
    assert_eq!(a + b, 42);
    // Two child futures inside this ONE task; concurrency, not parallel polling.
    let (a, b) = tokio::join!(work("joined A"), work("joined B"));
    assert_eq!(a + b, 42);
    // Separate tasks can run on different workers. Parallelism is possible,
    // not guaranteed. Awaiting the first handle doesn't stop the second task.
    let a = tokio::spawn(work("spawned A"));
    let b = tokio::spawn(work("spawned B"));
    assert_eq!(a.await.unwrap() + b.await.unwrap(), 42);
}
