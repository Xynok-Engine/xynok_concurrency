use xynok_std::collection::Queue;

use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicBool, Ordering};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;

pub struct QueueBatching<T>
{
    elements: UnsafeCell<Queue<T>>,
    locked:   CachePadded<AtomicBool>,
}

pub struct QueueBatchingGuard<'a, T>
{
    queue_batching: &'a QueueBatching<T>,
}

unsafe impl<T: Send> Send for QueueBatching<T> {}
unsafe impl<T: Send> Sync for QueueBatching<T> {}
unsafe impl<T: Sync> Sync for QueueBatchingGuard<'_, T> {}

impl<T> QueueBatching<T>
{
    pub fn new() -> Self
    {
        Self::from_queue(Queue::new())
    }

    pub fn with_capacity(capacity: usize) -> Self
    {
        Self::from_queue(Queue::with_capacity(capacity))
    }

    fn from_queue(queue: Queue<T>) -> Self
    {
        Self {
            elements: UnsafeCell::new(queue),
            locked:   CachePadded::new(AtomicBool::new(false)),
        }
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn get(&self) -> QueueBatchingGuard<'_, T>
    {
        if self.try_lock_weak()
        {
            return QueueBatchingGuard { queue_batching: self };
        }
        self.get_contended()
    }

    #[inline]
    pub fn try_get(&self) -> Option<QueueBatchingGuard<'_, T>>
    {
        match self.try_lock()
        {
            true => Some(QueueBatchingGuard { queue_batching: self }),
            false => None,
        }
    }

    #[inline]
    pub fn get_mut(&mut self) -> &mut Queue<T>
    {
        self.elements.with_mut(|p| unsafe { &mut *p })
    }

    pub fn take(self) -> Queue<T>
    {
        let this = std::mem::ManuallyDrop::new(self);
        this.elements.with_mut(|p| unsafe { p.read() })
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn push(&self, val: T)
    {
        self.get().enqueue(val);
    }

    pub fn push_batch<I>(&self, values: I) -> usize
    where I: IntoIterator<Item = T>
    {
        let mut pushed = 0;
        let mut elements = self.get();
        for value in values
        {
            elements.enqueue(value);
            pushed += 1;
        }
        pushed
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn pop(&self) -> Option<T>
    {
        self.get().dequeue()
    }

    pub fn pop_batch(&self, out: &mut Vec<T>, limit: usize) -> usize
    {
        if limit == 0
        {
            return 0;
        }

        let mut elements = self.get();

        out.reserve(limit.min(elements.len()));

        let mut taken = 0;
        while taken < limit
        {
            let Some(value) = elements.dequeue()
            else
            {
                break;
            };
            out.push(value);
            taken += 1;
        }
        taken
    }

    pub fn pop_batch_with<F>(&self, limit: usize, mut f: F) -> usize
    where F: FnMut(T)
    {
        let mut batch = Vec::new();
        let taken = self.pop_batch(&mut batch, limit);
        for value in batch
        {
            f(value);
        }
        taken
    }

    #[inline]
    pub fn drain_into(&self, out: &mut Vec<T>) -> usize
    {
        self.pop_batch(out, usize::MAX)
    }

    pub fn clear(&self) -> usize
    {
        let mut cleared = 0;
        let mut elements = self.get();
        while elements.dequeue().is_some()
        {
            cleared += 1;
        }
        cleared
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn len(&self) -> usize
    {
        self.get().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.get().is_empty()
    }

    #[inline]
    pub fn is_locked(&self) -> bool
    {
        self.locked.load(Ordering::Relaxed)
    }
}

impl<T> QueueBatching<T>
{
    #[cold]
    fn get_contended(&self) -> QueueBatchingGuard<'_, T>
    {
        let mut backoff = Backoff::new();
        loop
        {
            if !self.is_locked() && self.try_lock_weak()
            {
                return QueueBatchingGuard { queue_batching: self };
            }
            backoff.snooze();
        }
    }

    #[inline]
    fn try_lock(&self) -> bool
    {
        self.locked.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }

    #[inline]
    fn try_lock_weak(&self) -> bool
    {
        self.locked.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }

    #[inline]
    fn unlock(&self)
    {
        self.locked.store(false, Ordering::Release);
    }
}

impl<T> Drop for QueueBatchingGuard<'_, T>
{
    fn drop(&mut self)
    {
        self.queue_batching.unlock();
    }
}

impl<T> std::ops::Deref for QueueBatchingGuard<'_, T>
{
    type Target = Queue<T>;

    #[inline]
    fn deref(&self) -> &Self::Target
    {
        self.queue_batching.elements.with(|p| unsafe { &*p })
    }
}

impl<T> std::ops::DerefMut for QueueBatchingGuard<'_, T>
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        self.queue_batching.elements.with_mut(|p| unsafe { &mut *p })
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for QueueBatchingGuard<'_, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        std::fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Default for QueueBatching<T>
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl<T> From<Queue<T>> for QueueBatching<T>
{
    fn from(value: Queue<T>) -> Self
    {
        Self::from_queue(value)
    }
}

impl<T> FromIterator<T> for QueueBatching<T>
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self
    {
        let mut queue = Queue::new();
        for value in iter
        {
            queue.enqueue(value);
        }
        Self::from_queue(queue)
    }
}

impl<T> Extend<T> for QueueBatching<T>
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I)
    {
        let elements = self.get_mut();
        for value in iter
        {
            elements.enqueue(value);
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for QueueBatching<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        let mut builder = f.debug_struct("QueueBatching");
        match self.try_get()
        {
            Some(elements) => builder.field("elements", &*elements),
            None => builder.field("elements", &format_args!("<locked>")),
        }
        .finish()
    }
}

#[cfg(all(test, not(loom)))]
mod test
{
    use super::QueueBatching;
    use crate::sync::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
    fn day_va_rut_giu_dung_thu_tu()
    {
        let queue = QueueBatching::new();
        assert!(queue.is_empty());
        assert_eq!(queue.pop(), None);

        queue.push(1);
        queue.push(2);
        queue.push(3);

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn push_batch_dem_dung_so_phan_tu()
    {
        let queue = QueueBatching::with_capacity(8);

        assert_eq!(queue.push_batch(0..5), 5);
        assert_eq!(queue.push_batch(std::iter::empty::<i32>()), 0);
        assert_eq!(queue.push_batch(vec![5, 6]), 2);

        let mut out = Vec::new();
        assert_eq!(queue.drain_into(&mut out), 7);
        assert_eq!(out, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn pop_batch_noi_them_va_ton_trong_limit()
    {
        let queue = QueueBatching::new();
        queue.push_batch(0..10);

        let mut out = vec![-1];
        assert_eq!(queue.pop_batch(&mut out, 4), 4);
        assert_eq!(out, vec![-1, 0, 1, 2, 3]);

        assert_eq!(queue.pop_batch(&mut out, 0), 0, "limit 0 không được chạm vào gì");
        assert_eq!(out.len(), 5);

        assert_eq!(queue.pop_batch(&mut out, usize::MAX), 6);
        assert_eq!(out, vec![-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        assert_eq!(queue.pop_batch(&mut out, 4), 0, "rỗng thì trả 0");
    }

    #[test]
    fn pop_batch_with_chay_ngoai_khoa()
    {
        let queue = QueueBatching::new();
        queue.push_batch(0..4);

        let mut seen = Vec::new();
        let taken = queue.pop_batch_with(2, |value| {
            assert!(!queue.is_locked(), "`f` không được chạy khi còn giữ khoá");
            seen.push(value);
            queue.push(value + 100);
        });

        assert_eq!(taken, 2);
        assert_eq!(seen, vec![0, 1]);

        let mut out = Vec::new();
        queue.drain_into(&mut out);
        assert_eq!(out, vec![2, 3, 100, 101]);
    }

    #[test]
    fn guard_giu_khoa_cho_toi_khi_bi_tha()
    {
        let queue = QueueBatching::new();
        {
            let mut elements = queue.get();
            assert!(queue.is_locked());
            assert!(queue.try_get().is_none(), "khoá không đệ quy");

            elements.enqueue(7);
            elements.enqueue(8);
            assert_eq!(elements.peek(), Some(&7));
            assert_eq!(elements.len(), 2);
        }
        assert!(!queue.is_locked(), "thả guard phải nhả khoá");
        assert!(queue.try_get().is_some());
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn panic_giua_push_batch_van_nha_khoa()
    {
        let queue = QueueBatching::new();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.push_batch((0..10).map(|i| match i
            {
                5 => panic!("iterator nổ giữa chừng"),
                i => i,
            }));
        }));

        assert!(result.is_err());
        assert!(!queue.is_locked(), "tháo stack vì panic vẫn phải nhả khoá");
        assert_eq!(queue.len(), 5, "những phần tử đã kịp vào thì vẫn còn nguyên");
    }

    #[test]
    fn get_mut_va_into_inner_khong_qua_khoa()
    {
        let mut queue = QueueBatching::new();
        queue.extend(0..3);
        queue.get_mut().enqueue(3);

        assert!(!queue.is_locked());

        let mut inner = queue.take();
        assert_eq!(inner.len(), 4);
        assert_eq!(inner.dequeue(), Some(0));
    }

    #[test]
    fn from_iter_va_debug()
    {
        let queue: QueueBatching<i32> = (0..3).collect();
        assert_eq!(queue.len(), 3);
        assert!(format!("{:?}", queue).contains("[0, 1, 2]"));

        let held = queue.get();
        assert!(format!("{:?}", queue).contains("<locked>"), "`Debug` không được đợi khoá");
        drop(held);
    }

    #[test]
    fn tha_hang_doi_keo_theo_phan_tu_chua_rut()
    {
        let alive = Arc::new(AtomicUsize::new(0));
        {
            let queue = QueueBatching::new();
            queue.push_batch((0..8).map(|_| Arc::clone(&alive)));
            assert_eq!(Arc::strong_count(&alive), 9);
        }
        assert_eq!(Arc::strong_count(&alive), 1, "phần tử chưa rút vẫn phải được thả sạch");
    }

    #[test]
    fn nhieu_thread_khong_mat_phan_tu()
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
    fn guard_loai_tru_lan_nhau()
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
                    let value = elements.dequeue().expect("chỉ có đúng một phần tử, và ta đang giữ khoá");
                    elements.enqueue(value + 1);
                }
            }));
        }
        for handle in handles
        {
            handle.join().unwrap();
        }

        assert_eq!(queue.pop(), Some(THREADS * rounds));
    }
}

#[cfg(all(test, loom))]
mod loom_test
{
    use super::QueueBatching;
    use loom::sync::Arc;

    #[test]
    fn hai_thread_day_khong_mat_phan_tu()
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
    fn day_va_rut_song_song()
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
}
