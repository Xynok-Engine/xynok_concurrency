// CASE: serialize access while an operation must hold ownership across await.
use std::sync::Arc;
use tokio::sync::Mutex;
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let value = Arc::new(Mutex::new(0));
    let update = |value: Arc<Mutex<u32>>| async move {
        let mut guard = value.lock().await; // Contended acquisition yields the task.
        tokio::task::yield_now().await; // Other task can run, but must await the lock.
        *guard += 1;
    };
    tokio::join!(update(value.clone()), update(value.clone()));
    assert_eq!(*value.lock().await, 2);
    println!("both updates completed");
    // Illustrates the mechanism, not a reason to hold locks longer than necessary.
    // For short updates with NO await while locked, std::sync::Mutex often suffices.
    // Blocking lock acquisition on this thread while another suspended task owns
    // the lock can deadlock. Async mutexes can also deadlock with cyclic lock order.
}
