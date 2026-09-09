use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

// A future is a value that remembers how far its computation has progressed.
struct YieldOnce
{
    yielded: bool,
}

impl Future for YieldOnce
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>
    {
        if self.yielded
        {
            println!("  YieldOnce: Ready");
            Poll::Ready(())
        }
        else
        {
            self.yielded = true;
            println!("  YieldOnce: Pending; requesting another poll");
            // This future already knows it can finish on the next poll.
            // A future waiting for I/O would arrange a wake when I/O is ready.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

// Teaching aid: only logs the notification. A real executor would schedule
// the task to be polled again. Here main explicitly performs both polls.
struct LogWake;

impl Wake for LogWake
{
    fn wake(self: Arc<Self>)
    {
        println!("  Waker: please poll this task again");
    }

    fn wake_by_ref(self: &Arc<Self>)
    {
        println!("  Waker: please poll this task again");
    }
}

async fn lesson() -> u32
{
    println!("  lesson: before await");
    YieldOnce { yielded: false }.await;
    println!("  lesson: after await");
    42
}

fn main()
{
    println!("1. Create the future");
    let future = lesson();
    println!("2. Future created; lesson has not run yet");

    // Pin keeps the future at a stable location while we poll it.
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(LogWake));
    let mut cx = Context::from_waker(&waker);

    println!("3. First poll");
    let first = future.as_mut().poll(&mut cx);
    println!("   Result: {first:?}");
    assert_eq!(first, Poll::Pending);

    println!("4. Back in main; we could do other work here");

    println!("5. Second poll");
    let second = future.as_mut().poll(&mut cx);
    println!("   Result: {second:?}");
    assert_eq!(second, Poll::Ready(42));

    // Once Ready has been returned, do not poll this future again.
}
