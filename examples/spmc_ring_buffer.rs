use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use xynok_concurrency::collection::ring_buffer::spmc::*;
fn main()
{
    single_thread();
    multiple_thread();
}
fn multiple_thread()
{
    println!("--- multiple threads ---");

    let total_task = 64usize;
    let total_consumer = 4;

    let mut tasks = vec![0u32; total_task];

    {
        let ring = SpmcRingBuffer::<&mut u32>::new(16);
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
                    let mut count = 0;
                    loop
                    {
                        match consumer.pop()
                        {
                            Some(slot) =>
                            {
                                *slot = 1;
                                count += 1;
                            }
                            None =>
                            {
                                if done.load(Ordering::Acquire)
                                {
                                    break;
                                }
                                std::hint::spin_loop();
                            }
                        }
                    }
                    println!("consumer {} làm xong {} việc", id, count);
                });
            }

            for task in tasks_as_mut
            {
                // Our ring buffer only has 16 slots, but we have 64 values to process. We will use a while loop to push all these values into the buffer.
                let mut slot = task;
                while let Err(back) = producer.push(slot)
                {
                    slot = back;
                    std::hint::spin_loop();
                }
            }
            done.store(true, Ordering::Release);
        });
    }

    assert!(tasks.iter().all(|v| *v == 1));
    println!("all {} elements have been set to 1", total_task);
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
