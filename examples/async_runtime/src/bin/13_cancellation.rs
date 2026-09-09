// CASE: select drops losing futures; abort cancels a spawned async task.
// Cancellation drops state, but does not roll back external side effects.
use std::{future::pending, sync::{Arc, atomic::{AtomicBool, Ordering}}};
struct Guard(Arc<AtomicBool>);
impl Drop for Guard { fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); } }
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Guard(dropped.clone());
    let losing = async move { let _guard = guard; pending::<()>().await; };
    tokio::select! {
        biased;
        _ = losing => unreachable!(),
        _ = std::future::ready(()) => println!("ready branch wins"),
    }
    assert!(dropped.load(Ordering::SeqCst));

    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Guard(dropped.clone());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let _guard = guard;
        started_tx.send(()).unwrap();
        pending::<()>().await;
    });
    started_rx.await.unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(dropped.load(Ordering::SeqCst));
    println!("aborted task dropped its state");
    // Dropping a Tokio JoinHandle DETACHES its task; it does not abort it.
    // Abort is cooperative; it cannot interrupt blocking code inside poll.
    // Restarting a cancelled I/O operation may lose partial progress: examine
    // the operation's cancellation-safety contract before using select in loops.
}
