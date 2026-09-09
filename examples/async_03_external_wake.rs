// CASE: another thread completes work and wakes a waiting future.
// Needs: executor + shared result + Waker. No reactor required.
mod async_support;
fn main() {
    let (tx, rx) = async_support::oneshot();
    let worker = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        tx.send(42);
    });
    let value = async_support::block_on(async { rx.await.unwrap() });
    assert_eq!(value, 42);
    worker.join().unwrap();
    let (tx, rx) = async_support::oneshot::<u32>();
    drop(tx);
    assert!(async_support::block_on(rx).is_err());
    println!("received 42; sender-drop also completes instead of hanging");
}
