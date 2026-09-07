//! Exercise the real sleep/wake protocol without the scheduler's unbounded stealing loop.
use crate::custom_type::Job;
use crate::sync::{AtomicBool, Ordering, thread};
use crate::thread_pool::shared::ThreadPoolInner;
use crate::thread_pool::worker::{Worker, WorkerSpec};
use crate::utils::cache_padded::CachePadded;
use crate::utils::fixed_buffer::FixedBuffer;
use crate::utils::queue_batching::QueueBatching;
use crate::utils::spinlock::SpinLock;
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
        Self::with_workers(1)
    }

    fn with_workers(background: usize) -> Self
    {
        let inner = HeapPtr::new(ThreadPoolInner {
            id:             1,
            tasks:          QueueBatching::new(),
            workers:        FixedBuffer::new(background + 1),
            host_index:     background,
            host_owner:     SpinLock::new(()),
            init_completed: CachePadded::new(AtomicBool::new(true)),
            is_running:     CachePadded::new(AtomicBool::new(true)),
            sleepers:       QueueBatching::new(),
            total_worker:   background,
        });
        // The model's calling thread is worker 0; queue 1 represents the host.
        for i in 0..=background
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
        for i in 0..self.0.workers.len()
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
            root.push_and_wake_one(Job::new(|| {}));
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

#[test]
fn batch_publish_races_with_sleep_registration()
{
    model(|| {
        let fixture = Fixture::new();
        let root = fixture.0.as_ref_mut();
        let publisher = thread::spawn_named("batch-publish".into(), move || {
            root.publish_batch(vec![Job::new(|| {}), Job::new(|| {})], 1);
        })
        .unwrap();
        fixture.0.sleep_if_idle(0);
        publisher.join().unwrap();
        for index in 0..2
        {
            let worker = unsafe { fixture.0.workers.get_at(index) };
            assert!(worker.worker.inbox.pop().is_some());
        }
        assert!(fixture.0.sleepers.get().is_empty());
    });
}

#[test]
fn batch_consumer_observes_the_complete_assignment()
{
    model(|| {
        let fixture = Fixture::new();
        let root = fixture.0.as_ref_mut();
        let publisher = thread::spawn_named("batch-publish".into(), move || {
            root.publish_batch(vec![Job::new(|| {}), Job::new(|| {})], 1);
        })
        .unwrap();
        let worker = unsafe { fixture.0.workers.get_at(0) };
        if worker.worker.pop_and_run_a_task()
        {
            // Acquiring the other inbox also observes its published length. The publisher
            // must have assigned this job before exposing the first one to a consumer.
            let host = unsafe { fixture.0.workers.get_at(1) };
            assert_eq!(host.worker.inbox.get().len(), 1);
        }
        publisher.join().unwrap();
    });
}

#[test]
fn existing_inbox_work_prevents_sleep()
{
    model(|| {
        let fixture = Fixture::new();
        let host = unsafe { fixture.0.workers.get_at(1) };
        host.worker.inbox.push(Job::new(|| {}));
        fixture.0.sleep_if_idle(0);
        assert!(fixture.0.sleepers.get().is_empty());
    });
}

#[cfg(not(loom))]
#[test]
fn batch_balances_assignments_and_excludes_an_inactive_host()
{
    // No worker loops run in the fixture, so inspect initial assignment before stealing
    // can legitimately change which thread executes a job.
    for caller in 0..=4
    {
        for jobs in [0, 1, 2, 4, 5, 7, 23]
        {
            let fixture = Fixture::with_workers(4);
            fixture.0.publish_batch((0..jobs).map(|_| Job::new(|| {})), caller);
            let participants = 4 + usize::from(caller == 4);
            let mut lengths = Vec::new();
            for index in 0..=4
            {
                let worker = unsafe { fixture.0.workers.get_at(index) };
                assert!(worker.worker.tasks.is_empty(), "publisher wrote to an owner's local deque");
                if index == 4 && caller != 4
                {
                    assert!(worker.worker.inbox.is_empty(), "inactive host received work");
                }
                else
                {
                    lengths.push(worker.worker.inbox.len());
                }
            }
            assert_eq!(lengths.iter().sum::<usize>(), jobs);
            assert!(lengths.iter().max().unwrap() - lengths.iter().min().unwrap() <= 1);
            assert_eq!(unsafe { fixture.0.workers.get_at(caller) }.worker.inbox.len(), jobs.div_ceil(participants));
        }
    }
}

#[test]
fn caller_drains_own_assignment_then_steals_worker_work()
{
    model(|| {
        let fixture = Fixture::new();
        let root = fixture.0.as_ref_mut();
        root.publish_batch(vec![Job::new(|| {}), Job::new(|| {})], 1);
        let caller = unsafe { root.workers.get_at(1) };
        let worker = unsafe { root.workers.get_at(0) };
        assert!(caller.worker.pop_and_run_a_task());
        assert!(caller.worker.inbox.is_empty());
        assert_eq!(worker.worker.inbox.len(), 1);
        assert!(caller.worker.steal_and_run_a_task(0));
        assert!(worker.worker.inbox.is_empty());
    });
}
