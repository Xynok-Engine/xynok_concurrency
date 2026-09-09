// CASE: borrow stack data; no heap storage needed for these futures or waker.
// This manual driver ONLY supports futures that complete on their first poll.
use std::{future::Future, pin::pin, task::{Context, Poll, Waker}};
fn main() {
    let mut value = 40;
    {
        let future = async { value += 2; value };
        let mut future = pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(42));
    }
    assert_eq!(value, 42);
    println!("stack future finished: {value}");
    // Printing itself may allocate. Claim is about future/waker storage only.
    // Noop would NOT provide progress for a future waiting on an external event.
}
