use loom::sync::Arc;

use crate::utils::queue_batching::QueueBatching;

#[test]
fn t0_hai_thread_day_khong_mat_phan_tu()
{
    loom::model(|| {
        let queue = Arc::new(QueueBatching::new());

        let other = Arc::clone(&queue);
        let handle = loom::thread::spawn(move || other.push(1usize));

        queue.push(2usize);
        handle.join().unwrap();

        let mut out = Vec::new();
        assert_eq!(queue.drain_into(&mut out), 2);
        out.sort_unstable();
        assert_eq!(out, vec![1, 2]);
    });
}

#[test]
fn t1_day_va_rut_song_song()
{
    loom::model(|| {
        let queue = Arc::new(QueueBatching::new());

        let other = Arc::clone(&queue);
        let handle = loom::thread::spawn(move || other.push(1usize));

        let popped = queue.pop();
        handle.join().unwrap();

        match popped
        {
            Some(value) => assert_eq!(value, 1),
            None => assert_eq!(queue.pop(), Some(1)),
        }
    });
}
