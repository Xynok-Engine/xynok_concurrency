// CASE: task execution errors and application errors are separate layers.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let handle = tokio::spawn(async { Err::<(), _>("asset missing") });
    let application_result = handle.await.expect("task itself failed");
    assert_eq!(application_result, Err("asset missing"));
    let handle = tokio::spawn(async { panic!("intentional lesson panic"); });
    assert!(handle.await.unwrap_err().is_panic());
    println!("panic was observed via JoinError; runtime remains usable");
    // The panic hook still prints the intentional panic. The process exits 0.
}
