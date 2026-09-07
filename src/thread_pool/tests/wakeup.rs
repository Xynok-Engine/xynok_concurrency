//! Exercise the real sleep/wake protocol without the scheduler's unbounded stealing loop.
use crate::custom_type::Job;
use crate::sync::{thread, AtomicBool, Ordering};
use crate::thread_pool::shared::ThreadPoolInner;
use crate::thread_pool::worker::{Worker, WorkerSpec};
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;
use xynok_std::unsafe_ptr::HeapPtr;

fn model(f: impl Fn() + Send + Sync + 'static)
{
    #[cfg(loom)]
    loom::model(f);
    #[cfg(not(loom))]
    f();
}

struct Fixture(HeapPtr<ThreadPoolInner>);

impl Fixture
{
    fn new() -> Self
    {
        let inner = HeapPtr::new(ThreadPoolInner {
            id:             1,
            tasks:          QueueBatching::new(),
            workers:        FixedBuffer::new(2),
            host_index:     1,
            init_completed: CachePadded::new(AtomicBool::new(true)),
            is_running:     CachePadded::new(AtomicBool::new(true)),
            sleepers:       QueueBatching::new(),
            total_worker:   2,
        });
        // The model's calling thread is worker 0; queue 1 represents the host.
        for i in 0..2
        {
            unsafe {
                inner.workers.write(
                    i,
                    WorkerSpec {
                        thread: thread::current(),
                        worker: HeapPtr::new(Worker::new(2, inner.as_ref_mut(), i)),
                    },
                );
            }
        }
        Self(inner)
    }
}

impl Drop for Fixture
{
    fn drop(&mut self)
    {
        // Every model joins its publisher before destroying either queue.
        for i in 0..2
        {
            unsafe { self.0.workers.drop_at(i) };
        }
    }
}

#[test]
fn shared_publish_races_with_sleep_registration()
{
    model(|| {
        let fixture = Fixture::new();
        let root = fixture.0.as_ref_mut();
        let publisher = thread::spawn_named("publish".into(), move || {
            root.push(Job::new(|| {}));
        })
        .unwrap();
        fixture.0.sleep_if_idle(0);
        publisher.join().unwrap();
        assert!(fixture.0.tasks.pop().is_some());
        assert!(fixture.0.sleepers.get().is_empty());
    });
}

#[test]
fn host_publish_races_with_sleep_registration()
{
    model(|| {
        let fixture = Fixture::new();
        let root = fixture.0.as_ref_mut();
        let publisher = thread::spawn_named("publish".into(), move || {
            let host = unsafe { root.workers.get_at(1) };
            host.worker.tasks.push(Job::new(|| {})).unwrap();
            root.wake_one();
        })
        .unwrap();
        fixture.0.sleep_if_idle(0);
        publisher.join().unwrap();
        let host = unsafe { fixture.0.workers.get_at(1) };
        assert!(host.worker.tasks.pop().is_some());
        assert!(fixture.0.sleepers.get().is_empty());
    });
}

#[test]
fn existing_host_work_prevents_sleep()
{
    model(|| {
        let fixture = Fixture::new();
        let host = unsafe { fixture.0.workers.get_at(1) };
        host.worker.tasks.push(Job::new(|| {})).unwrap();
        // There is no notifier: parking here is a deadlock, even though our own queue is empty.
        fixture.0.sleep_if_idle(0);
        assert!(fixture.0.sleepers.get().is_empty());
    });
}

#[test]
fn unrelated_unpark_does_not_leave_a_sleep_registration()
{
    model(|| {
        let fixture = Fixture::new();
        for _ in 0..2
        {
            thread::current().unpark();
            fixture.0.sleep_if_idle(0);
            assert!(fixture.0.sleepers.get().is_empty());
        }
    });
}

#[test]
fn shutdown_races_with_sleep_registration()
{
    model(|| {
        let fixture = Fixture::new();
        let root = fixture.0.as_ref_mut();
        let sleeper = thread::current();
        let shutdown = thread::spawn_named("shutdown".into(), move || {
            // Same stop-store / unconditional unpark ordering as ThreadPoolInner::shutdown.
            root.is_running.store(false, Ordering::Release);
            sleeper.unpark();
        })
        .unwrap();
        fixture.0.sleep_if_idle(0);
        shutdown.join().unwrap();
        assert!(fixture.0.sleepers.get().is_empty());
    });
}
