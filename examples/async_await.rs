#![allow(unused)]

async fn foo1() -> usize
{
    println!("foo1: this is a future");
    foo2().await
}

fn foo2() -> impl std::future::Future<Output = usize>
{
    async {
        println!("foo2: this is also a future");
        20
    }
}

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

struct ThreadWake
{
    thread: thread::Thread,
}

impl Wake for ThreadWake
{
    fn wake(self: Arc<Self>)
    {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>)
    {
        self.thread.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output
{
    let mut future = pin!(future);

    let waker = Waker::from(Arc::new(ThreadWake { thread: thread::current() }));

    let mut context = Context::from_waker(&waker);

    loop
    {
        match future.as_mut().poll(&mut context)
        {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

fn main()
{
    let result = block_on(foo1());
    println!("result = {result}");
}
