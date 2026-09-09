// CASE: custom RawWaker, with no Arc/Box allocation for the waker.
// Static data lives forever, so clone/drop need no reference counting.
// This waker is tied to ONE root future, driven only by this main thread.
use std::{future::{Future, poll_fn}, pin::pin, sync::atomic::{AtomicBool, Ordering}, task::{Context, Poll, RawWaker, RawWakerVTable, Waker}};
static NOTIFIED: AtomicBool = AtomicBool::new(true);
unsafe fn clone(_: *const ()) -> RawWaker { raw() }
unsafe fn wake(_: *const ()) { NOTIFIED.store(true, Ordering::Release); }
unsafe fn drop_waker(_: *const ()) {}
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake, drop_waker);
fn raw() -> RawWaker { RawWaker::new(std::ptr::null(), &VTABLE) }
fn main() {
    // SAFETY: callbacks never dereference data, use only a static atomic, are
    // thread-safe, and cloned wakers remain valid indefinitely. No owned data.
    let waker = unsafe { Waker::from_raw(raw()) };
    let mut yielded = false;
    let future = poll_fn(|cx| {
        if yielded { Poll::Ready(42) } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    });
    let mut future = pin!(future);
    for expected in [Poll::Pending, Poll::Ready(42)] {
        assert!(NOTIFIED.swap(false, Ordering::Acquire));
        assert_eq!(future.as_mut().poll(&mut Context::from_waker(&waker)), expected);
    }
    println!("static waker + stack future completed");
    // Controlled two-poll demonstration, not a general executor or busy-wait loop.
}
