// CASE: custom single-thread executor with two independently scheduled tasks.
// Required boundary: Future, Pin, Context, Poll, Waker. Wake is optional convenience.
// Custom: task representation, IDs, queue, storage, spawn API, scheduling policy.
mod async_support;
use std::{future::Future, pin::Pin, sync::{mpsc, Arc}, task::{Context, Poll, Wake, Waker}};
struct TaskWake { id: usize, ready: mpsc::Sender<usize> }
impl Wake for TaskWake {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) { let _ = self.ready.send(self.id); }
}
struct Executor {
    tasks: Vec<Option<Pin<Box<dyn Future<Output = ()>>>>>,
    ready_tx: mpsc::Sender<usize>,
    ready_rx: mpsc::Receiver<usize>,
}
impl Executor {
    fn new() -> Self {
        let (ready_tx, ready_rx) = mpsc::channel();
        Self { tasks: Vec::new(), ready_tx, ready_rx }
    }
    fn spawn(&mut self, future: impl Future<Output = ()> + 'static) {
        let id = self.tasks.len();
        self.tasks.push(Some(Box::pin(future)));
        self.ready_tx.send(id).unwrap();
    }
    fn run(mut self) {
        let mut remaining = self.tasks.len();
        while remaining > 0 {
            let id = self.ready_rx.recv().unwrap();
            let Some(future) = self.tasks[id].as_mut() else { continue };
            let waker = Waker::from(Arc::new(TaskWake { id, ready: self.ready_tx.clone() }));
            if let Poll::Ready(()) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                self.tasks[id] = None;
                remaining -= 1;
            }
        }
    }
}
fn main() {
    let mut executor = Executor::new();
    for name in ["A", "B"] {
        executor.spawn(async move {
            println!("{name}: before yield");
            async_support::yield_once().await;
            println!("{name}: after yield");
        });
    }
    executor.run();
    // Expected: A before, B before, A after, B after.
    // Teaching limits: unbounded duplicate wake queue, no ID reuse, no panic
    // isolation/cancellation/shutdown API, allocation per task and per poll waker.
    // IDs are never reused, so stale notifications cannot refer to a new task.
}
