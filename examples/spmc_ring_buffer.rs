use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use xynok_concurrency::collection::ring_buffer::spmc::consumer::Consumer;
use xynok_concurrency::collection::ring_buffer::spmc::*;
use xynok_concurrency::utils::backoff::Backoff;

type TaskType<'a> = &'a mut u32;

fn main()
{
    single_thread();
    multiple_thread();
}
fn multiple_thread()
{
    println!("--- multiple threads ---");

    let total_task = 1024;
    let ring_size = 16;
    let total_consumer = 8;

    let mut tasks = vec![0u32; total_task];

    {
        let ring = SpmcRingBuffer::<TaskType>::new(ring_size);
        let (producer, consumer) = ring.split();

        let tasks_as_mut = tasks.iter_mut();

        let done = AtomicBool::new(false);

        thread::scope(|scope| {
            for id in 0..total_consumer
            {
                // Since `Consumer` implements `Copy`, feel free to pass a copy to each thread.
                // Conversely, `Producer` is `!Sync`, so the compiler will prevent you from sharing it across multiple threads, which correctly enforces the single-producer constraint.
                let done = &done;
                scope.spawn(move || {
                    consumer_logic(id, consumer, done);
                });
            }

            let mut backoff = Backoff::new();
            for task in tasks_as_mut
            {
                // Our ring buffer only has 16 slots, but we have 64 values to process. We will use a while loop to push all these values into the buffer.
                let mut slot = task;
                while let Err(back) = producer.push(slot)
                {
                    slot = back;
                    backoff.snooze();
                }
                backoff.reset();
            }
            done.store(true, Ordering::Release);
        });
    }

    assert!(tasks.iter().all(|v| *v == 1));
    println!("all {} elements have been set to 1", total_task);
}
fn consumer_logic(id: usize, consumer: Consumer<TaskType>, done: &AtomicBool)
{
    let mut count = 0;
    // `Backoff` handles the waiting: it spins in place for the first few rounds so a
    // short wait stays cheap, then falls back to `yield_now` and hands the CPU back
    // to the OS instead of burning a whole core.
    let mut backoff = Backoff::new();
    loop
    {
        match consumer.pop()
        {
            Some(slot) =>
            {
                *slot = 1;
                count += 1;
                // Got work, so drop back to step 0: the next empty round reacts fast
                // again instead of starting from a long wait.
                backoff.reset();
            }
            None =>
            {
                if done.load(Ordering::Acquire)
                {
                    break;
                }
                backoff.snooze();
            }
        }
    }
    println!("consumer {} handled {} tasks", id, count);
}
fn single_thread()
{
    println!("--- single thread ---");

    let ring = SpmcRingBuffer::<u32>::new(4);
    let (producer, consumer) = ring.split();

    for i in 0..4u32
    {
        assert!(producer.push(i) == Ok(()));
    }

    assert!(producer.push(99) == Err(99));

    println!("pushed: ");
    while let Some(val) = consumer.pop()
    {
        print!("{} ", val);
    }
    println!();

    assert_eq!(consumer.pop(), None);
}
