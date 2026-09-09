// CASE: borrowed futures and !Send tasks. Send/'static are spawn API bounds,
// not universal requirements of Future or async/await.
use std::{cell::RefCell, rc::Rc};
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let text = String::from("borrowed");
    let borrowed = async { tokio::task::yield_now().await; text.len() };
    assert_eq!(borrowed.await, 8); // No 'static requirement for direct await.

    let local = tokio::task::LocalSet::new();
    let value = Rc::new(RefCell::new(0)); // Rc is !Send.
    let captured = value.clone();
    local.run_until(async move {
        tokio::task::spawn_local(async move {
            tokio::task::yield_now().await;
            *captured.borrow_mut() = 42;
        }).await.unwrap();
    }).await;
    assert_eq!(*value.borrow(), 42);
    // spawn_local still requires 'static ownership, but not Send.
    // tokio::spawn requires Send + 'static, even on a current-thread runtime.
    // 'static here means no short-lived borrowed references, not "lives forever".
    println!("borrowed future and local !Send task completed");
}
