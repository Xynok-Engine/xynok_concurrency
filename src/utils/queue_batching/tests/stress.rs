use std::sync::Arc;

use crate::ring_buffer_fifo::RingBufferFifo;
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

/// Trần cứng ở đây là số phần tử cố định chia sẵn cho từng thread. Vòng gom không trần đã từng ăn
/// hết RAM khi cấu trúc bên dưới hỏng, nên mọi thứ ở đây đều đếm được từ trước.
#[test]
fn t2_nhieu_thread_day_vao_khong_mat_phan_tu()
{
    const THREADS: usize = 8;

    let per_thread = scaled(2_000);
    let total = THREADS * per_thread;

    let queue = Arc::new(QueueBatching::new());

    std::thread::scope(|scope| {
        for t in 0..THREADS
        {
            let queue = Arc::clone(&queue);
            scope.spawn(move || {
                for i in 0..per_thread
                {
                    match i % 3
                    {
                        0 => queue.push(t * per_thread + i),
                        _ => queue.push_batch(std::iter::once(t * per_thread + i)),
                    }
                }
            });
        }
    });

    assert_eq!(queue.len(), total, "bộ đếm không khoá lệch so với số phần tử đã đẩy vào");

    let mut seen = vec![false; total];
    let mut out = Vec::with_capacity(total);
    assert_eq!(queue.drain_into(&mut out), total);
    for value in out
    {
        assert!(!seen[value], "phần tử {value} ra khỏi hàng đợi hai lần");
        seen[value] = true;
    }
    assert!(seen.into_iter().all(|s| s), "có phần tử đẩy vào mà không bao giờ ra");
    assert!(queue.is_empty());
}

/// Nhiều worker cùng rút, mỗi người một ring riêng: không phần tử nào ra hai lần, không phần tử nào
/// biến mất.
#[test]
fn t3_nhieu_worker_cung_rut_khong_trung_khong_mat()
{
    const WORKERS: usize = 4;

    let total = scaled(20_000);

    let queue = Arc::new(QueueBatching::new());
    queue.push_batch(0..total);

    let taken: Vec<Vec<usize>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..WORKERS)
            .map(|_| {
                let queue = Arc::clone(&queue);
                scope.spawn(move || {
                    let mut ring: RingBufferFifo<usize> = RingBufferFifo::new(64);
                    let (mut owner, _) = ring.split();
                    let mut mine = Vec::new();

                    // Trần cứng: nhiều nhất `total` vòng, nên một hàng đợi hỏng làm test *fail* chứ
                    // không làm máy hết RAM.
                    for _ in 0..total
                    {
                        match queue.steal_batch_and_pop(&mut owner, WORKERS)
                        {
                            Some(job) => mine.push(job),
                            None => break,
                        }
                        while let Some(job) = owner.pop()
                        {
                            mine.push(job);
                        }
                    }
                    owner.drain(&mut mine);
                    mine
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().expect("worker panic")).collect()
    });

    assert!(queue.is_empty(), "còn {} phần tử kẹt lại trong hàng đợi", queue.len());

    let mut seen = vec![false; total];
    for job in taken.into_iter().flatten()
    {
        assert!(!seen[job], "phần tử {job} bị hai worker cùng nhận");
        seen[job] = true;
    }
    assert!(seen.into_iter().all(|s| s), "có phần tử không worker nào nhận");
}
