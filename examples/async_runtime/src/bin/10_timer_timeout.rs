// CASE: waiting for time and limiting an operation's duration.
// Needs runtime timer driver, not necessarily a network I/O reactor.
use tokio::time::{sleep, timeout, Duration};
#[tokio::main(flavor = "current_thread")]
async fn main() {
    sleep(Duration::from_millis(5)).await;
    // A permanently pending future makes timeout deterministic.
    let result = timeout(Duration::from_millis(10), std::future::pending::<()>()).await;
    assert!(result.is_err());
    println!("timer woke task; timeout dropped its pending inner future");
    // timeout cannot preempt blocking code that monopolizes a poll.
}
