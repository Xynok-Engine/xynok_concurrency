use std::sync::Arc;

use crate::sync::{AtomicUsize, Ordering};
use crate::utils::queue_batching::QueueBatching;

#[cfg(miri)]
const SCALE: usize = 100;
#[cfg(not(miri))]
const SCALE: usize = 1;

const fn scaled(n: usize) -> usize
{
    match n / SCALE
    {
        0 => 1,
        scaled => scaled,
    }
}

#[test]
fn t0_nhieu_thread_khong_mat_phan_tu()
{
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const BATCH: usize = 16;

    let per_producer = scaled(2_000);
    let total = PRODUCERS * per_producer;

    let queue = Arc::new(QueueBatching::<usize>::with_capacity(256));
    let consumed = Arc::new(AtomicUsize::new(0));
    let sum = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for producer in 0..PRODUCERS
    {
        let queue = Arc::clone(&queue);
        handles.push(std::thread::spawn(move || {
            let mut next = producer * per_producer;
            let end = next + per_producer;
            while next < end
            {
                let stop = end.min(next + BATCH);
                queue.push_batch(next..stop);
                next = stop;
            }
        }));
    }

    for _ in 0..CONSUMERS
    {
        let queue = Arc::clone(&queue);
        let consumed = Arc::clone(&consumed);
        let sum = Arc::clone(&sum);
        handles.push(std::thread::spawn(move || {
            let mut batch = Vec::with_capacity(BATCH);
            while consumed.load(Ordering::Relaxed) < total
            {
                match queue.pop_batch(&mut batch, BATCH)
                {
                    0 => std::thread::yield_now(),
                    taken =>
                    {
                        sum.fetch_add(batch.drain(..).sum::<usize>(), Ordering::Relaxed);
                        consumed.fetch_add(taken, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for handle in handles
    {
        handle.join().unwrap();
    }

    assert_eq!(consumed.load(Ordering::Relaxed), total);
    assert_eq!(sum.load(Ordering::Relaxed), (0..total).sum::<usize>(), "không phần tử nào bị mất hay nhân đôi");
    assert!(queue.is_empty());
}

#[test]
fn t1_guard_loai_tru_lan_nhau()
{
    const THREADS: usize = 8;
    let rounds = scaled(1_000);

    let queue = Arc::new(QueueBatching::new());
    queue.push(0usize);

    let mut handles = Vec::new();
    for _ in 0..THREADS
    {
        let queue = Arc::clone(&queue);
        handles.push(std::thread::spawn(move || {
            for _ in 0..rounds
            {
                let mut elements = queue.get();
                let value = elements.pop_front().expect("chỉ có đúng một phần tử, và ta đang giữ khoá");
                elements.push_back(value + 1);
            }
        }));
    }
    for handle in handles
    {
        handle.join().unwrap();
    }

    assert_eq!(queue.pop(), Some(THREADS * rounds));
}
