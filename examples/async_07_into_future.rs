// CASE: make your own builder awaitable without implementing Future on it.
// .await uses standard IntoFuture; its result must implement standard Future.
mod async_support;
use std::future::{IntoFuture, Ready, ready};
struct Request { value: u32 }
impl IntoFuture for Request {
    type Output = u32;
    type IntoFuture = Ready<u32>;
    fn into_future(self) -> Self::IntoFuture { ready(self.value * 2) }
}
fn main() {
    let value = async_support::block_on(async { Request { value: 21 }.await });
    assert_eq!(value, 42);
    println!("custom awaitable builder: {value}");
}
