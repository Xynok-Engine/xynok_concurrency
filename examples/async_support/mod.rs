//! Teaching helpers, not a production runtime.
#![allow(dead_code)]
use std::{future::Future, pin::{pin, Pin}, sync::{Arc, Mutex}, task::{Context, Poll, Wake, Waker}, thread};

struct ThreadWake(thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) { self.0.unpark(); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.unpark(); }
}
// One root future on the calling thread. No task queue or I/O/timer driver.
// unpark preserves a token if wake happens BEFORE park: no lost notification.
// Dedicated teaching loop: not intended for nested block_on calls.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}
pub async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded { Poll::Ready(()) } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }).await
}
struct State<T> { value: Option<T>, closed: bool, waker: Option<Waker> }
pub struct Sender<T>(Arc<Mutex<State<T>>>);
pub struct Receiver<T>(Arc<Mutex<State<T>>>);
pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(Mutex::new(State { value: None, closed: false, waker: None }));
    (Sender(state.clone()), Receiver(state))
}
impl<T> Sender<T> {
    pub fn send(self, value: T) {
        let waker = {
            let mut state = self.0.lock().unwrap();
            state.value = Some(value);
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker { waker.wake(); }
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.0.lock().unwrap();
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker { waker.wake(); }
    }
}
impl<T> Future for Receiver<T> {
    type Output = Result<T, &'static str>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Check readiness AND register the latest waker under the same lock.
        // Otherwise the sender could finish between those steps and lose a wake.
        let mut state = self.0.lock().unwrap();
        if let Some(value) = state.value.take() { Poll::Ready(Ok(value)) }
        else if state.closed { Poll::Ready(Err("sender dropped")) }
        else { state.waker = Some(cx.waker().clone()); Poll::Pending }
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) { self.0.lock().unwrap().waker.take(); }
}
