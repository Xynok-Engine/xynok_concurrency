use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::apis::priority::Priority;
use crate::custom_type::Job;
use crate::sync::{AtomicUsize, Ordering};
use crate::thread_pool::local::THREAD_LOCAL_CTX;
use crate::thread_pool::{CfgThreadPool, ThreadPool};
use crate::utils::available_cores;

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn idle_worker_runs_scoped_work_before_the_caller_starts_draining()
{
    let pool = ThreadPool::new(cfg("scope-wakeup", 1));
    for _ in 0..4
    {
        wait_until("worker registered for sleep", || !pool.inner.sleepers.get().is_empty());
        let (tx, rx) = std::sync::mpsc::channel();
        pool.scope(|scope| {
            scope.spawn(move || tx.send(std::thread::current().id()).unwrap());
            let worker_id = rx.recv_timeout(TIMEOUT).expect("sleeping worker missed scoped work");
            assert_ne!(worker_id, std::thread::current().id());
        });
    }
}

/// Shrinks the workload of a test when it is built for Miri.
///
/// Miri interprets the program instead of running it, so it is a couple of hundred times slower
/// than a native build. Left at their real sizes, the counts in this file turn a single
/// `cargo miri test` into a coffee break, and `-Zmiri-many-seeds` into an afternoon. What Miri is
/// looking for is the order the threads step on each other, not the sheer volume, so a few dozen
/// tasks buy the same coverage. Everything below 30 is already small enough to keep as it is.
#[cfg(miri)]
const fn scaled(n: usize) -> usize
{
    match n < 30
    {
        true => n,
        false => 30,
    }
}
#[cfg(not(miri))]
const fn scaled(n: usize) -> usize
{
    n
}

fn cfg(name: &str, worker_count: usize) -> CfgThreadPool
{
    CfgThreadPool {
        name:                     name.to_string(),
        priority:                 Priority::Frame,
        per_worker_task_capacity: 256,
        task_capacity:            1024,
        worker_count:             worker_count,
    }
}

fn wait_until(what: &str, cond: impl Fn() -> bool)
{
    let deadline = Instant::now() + TIMEOUT;
    while !cond()
    {
        assert!(
            Instant::now() < deadline,
            "Timeout of {:?} reached, but `{}` has not finished yet",
            TIMEOUT,
            what
        );
        std::thread::yield_now();
    }
}

/// Lets a reference to the pool travel inside a [`Job`], which demands `'static`.
///
/// Only used in this file, and every test that uses it waits for the job to finish before the
/// pool goes away.
#[derive(Clone, Copy)]
struct PoolRef(*const ThreadPool);
unsafe impl Send for PoolRef {}
unsafe impl Sync for PoolRef {}
impl PoolRef
{
    fn new(pool: &ThreadPool) -> Self
    {
        Self(pool as *const ThreadPool)
    }

    fn get(&self) -> &ThreadPool
    {
        unsafe { &*self.0 }
    }
}

/// Silences the panic printer for the duration of a test, then hands the old one back.
fn mute_panic_output() -> impl Drop
{
    struct Restore(Option<Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>>);
    impl Drop for Restore
    {
        fn drop(&mut self)
        {
            // `set_hook` panics when the current thread is already unwinding, and a panic inside a
            // `Drop` turns into an abort. If the test is failing, aborting here would hide the
            // assertion message behind "panic in a destructor during cleanup", so just leave the
            // hook alone in that case.
            if std::thread::panicking()
            {
                return;
            }
            if let Some(hook) = self.0.take()
            {
                std::panic::set_hook(hook);
            }
        }
    }

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    Restore(Some(previous))
}

// ---------------------------------------------------------------------------------------------
// Lifecycle and `push`
// ---------------------------------------------------------------------------------------------

#[test]
fn t0_pool_runs_every_pushed_task()
{
    const TOTAL_TASK: usize = scaled(2_000);

    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg("t0", 4));

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("all tasks have finished", || done.load(Ordering::Acquire) == TOTAL_TASK);
    assert_eq!(done.load(Ordering::Acquire), TOTAL_TASK);
}

#[test]
fn t1_dropping_the_pool_does_not_hang()
{
    let done = Arc::new(AtomicUsize::new(0));

    {
        let pool = ThreadPool::new(cfg("t1", 4));
        for _ in 0..500
        {
            let done = done.clone();
            pool.push(Job::new(move || {
                done.fetch_add(1, Ordering::Release);
            }));
        }
        wait_until("at least one task has run", || done.load(Ordering::Acquire) > 0);
    }

    // By now `Drop` has joined every worker. Nobody is left running, so the count stays put.
    let settled = done.load(Ordering::Acquire);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        done.load(Ordering::Acquire),
        settled,
        "a worker was still running after the pool had been dropped"
    );
}

#[test]
fn t2_dropping_a_fresh_empty_pool()
{
    // Build it and let it go straight away, before the workers even make it past the init gate.
    // It still has to shut down cleanly.
    for _ in 0..20
    {
        let _pool = ThreadPool::new(cfg("t2", 4));
    }
}

#[test]
fn t3_single_worker_pool_still_runs()
{
    const TOTAL_TASK: usize = scaled(500);

    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg("t3", 1));

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("every task ran on a single worker", || done.load(Ordering::Acquire) == TOTAL_TASK);
}

#[test]
fn t4_drop_while_the_workers_are_busy_stealing()
{
    // The nastiest window `Drop` has: releasing the pool while workers are still peeking into each
    // other's deques. If `shutdown` reclaims a worker before every thread is joined, this is where
    // we read freed memory. A hard cap of 200 rounds is enough to expose it without running forever.
    const ROUND: usize = scaled(200);
    const TASK_PER_ROUND: usize = scaled(300);

    let done = Arc::new(AtomicUsize::new(0));
    for round in 0..ROUND
    {
        let pool = ThreadPool::new(cfg("t4", 4));
        for _ in 0..TASK_PER_ROUND
        {
            let done = done.clone();
            pool.push(Job::new(move || {
                done.fetch_add(1, Ordering::Release);
            }));
        }
        // Drop right away, no waiting. Tasks that did not get their turn are dropped, and that is
        // the expected behaviour.
        drop(pool);

        let settled = done.load(Ordering::Acquire);
        assert!(
            settled <= (round + 1) * TASK_PER_ROUND,
            "counted {} tasks on round {}, more than were ever pushed",
            settled,
            round
        );
    }
}

#[test]
fn t5_idle_workers_go_to_sleep_and_can_be_woken_up()
{
    const ROUND: usize = 20;

    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg("t5", 4));

    // With nothing to do, the backoff has to run out and the workers have to lie down instead of
    // spinning and burning a core.
    wait_until("workers fall asleep", || pool.inner.sleepers.len() == pool.inner.total_worker());

    // Each round pushes exactly one task while the workers are asleep, then waits for it to finish.
    // If `wake_one` is broken, every round has to wait out a whole `SLEEP_SLICE` (100ms), which is
    // more than 2s across all 20 rounds.
    let started = Instant::now();
    for round in 0..ROUND
    {
        let counter = done.clone();
        pool.push(Job::new(move || {
            counter.fetch_add(1, Ordering::Release);
        }));
        wait_until("the task runs after the wake-up call", || done.load(Ordering::Acquire) == round + 1);
    }
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(1),
        "{} wake-ups took {:?}, which looks like waiting out the timer instead of being woken up",
        ROUND,
        elapsed
    );
}

#[test]
fn t6_odd_capacity_is_rounded_up_to_a_power_of_two()
{
    const TOTAL_TASK: usize = scaled(400);

    // 100 is not a power of two. It used to be guarded by a `debug_assert` only, so release builds
    // quietly computed the wrong mask. Now it has to round itself up to 128 and just work.
    let mut cfg = cfg("t6", 4);
    cfg.per_worker_task_capacity = 100;

    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg);

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("every task ran with an odd capacity", || done.load(Ordering::Acquire) == TOTAL_TASK);
}

#[test]
fn t7_worker_count_is_clamped_to_the_available_cores()
{
    // Asking for a thousand workers on a machine with eight cores is a config mistake, not a
    // request to melt the box. `workers` also holds the host slot, hence the `+ 1`.
    let pool = ThreadPool::new(cfg("t7", 1_000));
    assert_eq!(pool.inner.workers.len(), available_cores() + 1);
    assert_eq!(pool.inner.host_index, available_cores());
}

#[test]
fn t8_zero_workers_falls_back_to_one()
{
    const TOTAL_TASK: usize = scaled(200);

    // A pool with no worker would swallow every task without a word. One worker is the floor.
    let pool = ThreadPool::new(cfg("t8", 0));
    assert_eq!(pool.inner.workers.len(), 2, "expected one worker plus the host slot");

    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }
    wait_until("the fallback worker runs everything", || done.load(Ordering::Acquire) == TOTAL_TASK);
}

#[test]
fn t9_zero_capacity_falls_back_to_a_usable_deque()
{
    const TOTAL_TASK: usize = scaled(300);

    // A deque with zero slots cannot hold anything, so the config gets bumped up. Whatever the
    // floor ends up being, stealing splits the deque in half, so it has to stay big enough for
    // that half to be at least one slot.
    let mut cfg = cfg("t9", 4);
    cfg.per_worker_task_capacity = 0;

    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg);

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }
    wait_until("every task ran with the smallest deque", || done.load(Ordering::Acquire) == TOTAL_TASK);
}

#[test]
#[should_panic(expected = "exceeds the `u32` limit")]
fn t10_capacity_past_the_u32_limit_is_rejected_loudly()
{
    // The cursors are packed into a `u32`, so a deque this big would silently wrap around. Better
    // to refuse at construction time than to corrupt indices later.
    let mut cfg = cfg("t10", 2);
    cfg.per_worker_task_capacity = 1 << 31;
    let _pool = ThreadPool::new(cfg);
}

#[test]
fn t11_every_task_runs_exactly_once()
{
    const TOTAL_TASK: usize = scaled(4_000);

    // A plain counter only proves the total adds up. One slot per task also catches a task being
    // handed out twice, which is exactly what a botched steal looks like.
    let slots: Arc<Vec<AtomicUsize>> = Arc::new((0..TOTAL_TASK).map(|_| AtomicUsize::new(0)).collect());
    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg("t11", 4));

    for i in 0..TOTAL_TASK
    {
        let slots = slots.clone();
        let done = done.clone();
        pool.push(Job::new(move || {
            slots[i].fetch_add(1, Ordering::Release);
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("all tasks have finished", || done.load(Ordering::Acquire) == TOTAL_TASK);
    for (i, slot) in slots.iter().enumerate()
    {
        assert_eq!(slot.load(Ordering::Acquire), 1, "task {} ran {} times", i, slot.load(Ordering::Acquire));
    }
}

#[test]
fn t12_push_from_several_threads_at_once()
{
    const PRODUCER: usize = 4;
    const TASK_PER_PRODUCER: usize = scaled(500);

    // The shared queue takes work from any thread, not only from the one that built the pool.
    let pool = ThreadPool::new(cfg("t12", 4));
    let pool_ref = PoolRef::new(&pool);
    let done = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for _ in 0..PRODUCER
        {
            let done = done.clone();
            s.spawn(move || {
                for _ in 0..TASK_PER_PRODUCER
                {
                    let done = done.clone();
                    pool_ref.get().push(Job::new(move || {
                        done.fetch_add(1, Ordering::Release);
                    }));
                }
            });
        }
    });

    wait_until("every producer's task has run", || done.load(Ordering::Acquire) == PRODUCER * TASK_PER_PRODUCER);
}

#[test]
fn t13_push_from_inside_a_running_task()
{
    const PARENT: usize = scaled(200);
    const CHILD_PER_PARENT: usize = 5;

    // A task pushing more work is the normal shape of a recursive workload. The push comes from a
    // worker thread here, not from the outside, so it goes through the same queue in the other
    // direction.
    let pool = ThreadPool::new(cfg("t13", 4));
    let pool_ref = PoolRef::new(&pool);
    let done = Arc::new(AtomicUsize::new(0));

    for _ in 0..PARENT
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            for _ in 0..CHILD_PER_PARENT
            {
                let done = done.clone();
                pool_ref.get().push(Job::new(move || {
                    done.fetch_add(1, Ordering::Release);
                }));
            }
        }));
    }

    wait_until("every child task has run", || done.load(Ordering::Acquire) == PARENT * CHILD_PER_PARENT);
}

#[test]
fn t14_shared_queue_grows_past_its_configured_capacity()
{
    const TOTAL_TASK: usize = scaled(5_000);

    // `task_capacity` is a starting size, not a hard ceiling. Pushing far past it has to keep
    // working instead of dropping tasks or blocking the caller.
    let mut cfg = cfg("t14", 2);
    cfg.task_capacity = 8;

    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg);

    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("the queue grew and ran everything", || done.load(Ordering::Acquire) == TOTAL_TASK);
}

#[test]
fn t15_two_pools_stay_out_of_each_others_way()
{
    const TOTAL_TASK: usize = scaled(500);

    // Each pool has its own id, and the thread-local context is keyed by that id. If the ids ever
    // collided, a worker of one pool would look up a slot in the other pool's buffer.
    let a = ThreadPool::new(cfg("t15a", 2));
    let b = ThreadPool::new(cfg("t15b", 2));
    assert_ne!(a.inner.id, b.inner.id, "two live pools ended up with the same id");

    let done_a = Arc::new(AtomicUsize::new(0));
    let done_b = Arc::new(AtomicUsize::new(0));

    for _ in 0..TOTAL_TASK
    {
        let done_a = done_a.clone();
        a.push(Job::new(move || {
            done_a.fetch_add(1, Ordering::Release);
        }));

        let done_b = done_b.clone();
        b.push(Job::new(move || {
            done_b.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("pool a finished", || done_a.load(Ordering::Acquire) == TOTAL_TASK);
    wait_until("pool b finished", || done_b.load(Ordering::Acquire) == TOTAL_TASK);
}

#[test]
fn t16_a_panicking_pushed_task_does_not_wedge_the_pool()
{
    const TOTAL_TASK: usize = scaled(300);

    // `push` has no scope to carry a panic back to, so the worker catches it right where the task
    // runs and carries on. Letting it unwind instead would kill the worker thread along with every
    // task already sitting in its local deque, and on a single core box that is the only worker
    // there is, so the whole pool would be gone.
    let _muted = mute_panic_output();

    let done = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(cfg("t16", 4));

    pool.push(Job::new(|| panic!("this task dies halfway through")));
    for _ in 0..TOTAL_TASK
    {
        let done = done.clone();
        pool.push(Job::new(move || {
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("the pool drained the queue around the bad task", || done.load(Ordering::Acquire) == TOTAL_TASK);
    drop(pool);
}

// ---------------------------------------------------------------------------------------------
// `scope`
// ---------------------------------------------------------------------------------------------

#[test]
fn t17_scope_runs_every_job_and_lends_out_the_stack()
{
    const TOTAL_JOB: usize = scaled(2_000);

    let pool = ThreadPool::new(cfg("t17", 4));

    // `counter` lives on the stack right here, and the jobs borrow it directly instead of going
    // through an `Arc`. That is the one thing `scope` gives you over `push`, and the reason scope
    // has to wait no matter what.
    let counter = AtomicUsize::new(0);
    pool.scope(|s| {
        for _ in 0..TOTAL_JOB
        {
            s.spawn(|| {
                counter.fetch_add(1, Ordering::Release);
            });
        }
    });

    assert_eq!(counter.load(Ordering::Acquire), TOTAL_JOB, "scope returned while jobs were still running");
}

#[test]
fn t18_scope_does_not_hang_when_the_deque_is_smaller_than_the_job_count()
{
    const TOTAL_JOB: usize = scaled(5_000);

    // The private deque has only 2 slots. The jobs do not all fit, so the overflow has to stay on
    // the caller's stack and trickle in, rather than leaking into the shared queue.
    let mut cfg = cfg("t18", 1);
    cfg.per_worker_task_capacity = 2;

    let pool = ThreadPool::new(cfg);
    let counter = AtomicUsize::new(0);

    pool.scope(|s| {
        for _ in 0..TOTAL_JOB
        {
            s.spawn(|| {
                counter.fetch_add(1, Ordering::Release);
            });
        }
    });

    assert_eq!(counter.load(Ordering::Acquire), TOTAL_JOB);
}

#[test]
fn t19_scope_hands_back_the_value_of_its_closure()
{
    let pool = ThreadPool::new(cfg("t19", 2));
    let counter = AtomicUsize::new(0);

    let answer = pool.scope(|s| {
        for _ in 0..64
        {
            s.spawn(|| {
                counter.fetch_add(1, Ordering::Release);
            });
        }
        "done"
    });

    assert_eq!(answer, "done");
    assert_eq!(counter.load(Ordering::Acquire), 64);
}

#[test]
fn t20_an_empty_scope_returns_right_away()
{
    let pool = ThreadPool::new(cfg("t20", 4));

    // No jobs means no tickets, so the latch is already settled. If the wait loop only checked
    // after doing a round of work, this would sit through a whole backoff for nothing.
    let started = Instant::now();
    for _ in 0..100
    {
        pool.scope(|_s| {});
    }
    let elapsed = started.elapsed();

    assert!(elapsed < Duration::from_secs(1), "100 empty scopes took {:?}", elapsed);
}

#[test]
fn t21_nested_scopes_from_inside_a_job()
{
    const OUTER: usize = 16;
    const INNER: usize = scaled(32);

    let pool = ThreadPool::new(cfg("t21", 4));
    let pool_ref = PoolRef::new(&pool);
    let counter = AtomicUsize::new(0);

    // The inner scope waits while sitting on a worker thread. If that wait point went to sleep
    // instead of pitching in, the whole pool would deadlock: the sleepers are the very threads
    // that owe the jobs being waited on.
    pool.scope(|outer| {
        for _ in 0..OUTER
        {
            outer.spawn(|| {
                pool_ref.get().scope(|inner| {
                    for _ in 0..INNER
                    {
                        inner.spawn(|| {
                            counter.fetch_add(1, Ordering::Release);
                        });
                    }
                });
            });
        }
    });

    assert_eq!(counter.load(Ordering::Acquire), OUTER * INNER);
}

#[test]
fn t22_a_panicking_job_is_rethrown_at_the_scope()
{
    let pool = ThreadPool::new(cfg("t22", 4));
    let done = AtomicUsize::new(0);

    let _muted = mute_panic_output();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.scope(|s| {
            s.spawn(|| panic!("this job dies halfway through"));
            for _ in 0..100
            {
                s.spawn(|| {
                    done.fetch_add(1, Ordering::Release);
                });
            }
        });
    }));

    assert!(outcome.is_err(), "the job's panic was swallowed and the scope's owner never heard about it");
}

#[test]
fn t23_the_pool_still_works_after_a_job_panicked()
{
    const TOTAL_JOB: usize = scaled(500);

    // A panic inside a scope is caught and carried back by the scope, so no worker thread should
    // die over it. Whatever comes next has to run on a full pool.
    let pool = ThreadPool::new(cfg("t23", 4));

    {
        let _muted = mute_panic_output();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pool.scope(|s| {
                s.spawn(|| panic!("boom"));
            });
        }));
        assert!(outcome.is_err());
    }

    let counter = AtomicUsize::new(0);
    pool.scope(|s| {
        for _ in 0..TOTAL_JOB
        {
            s.spawn(|| {
                counter.fetch_add(1, Ordering::Release);
            });
        }
    });
    assert_eq!(counter.load(Ordering::Acquire), TOTAL_JOB, "the pool lost a worker to the earlier panic");
}

#[test]
fn t24_a_panic_in_the_scope_closure_still_waits_for_the_jobs()
{
    const TOTAL_JOB: usize = scaled(2_000);

    // The closure blows up after spawning. The jobs already handed out are borrowing this stack
    // frame, so scope has to wait for every one of them before letting the unwind continue,
    // otherwise a job writes into a frame that is being torn down.
    let pool = ThreadPool::new(cfg("t24", 4));
    let counter = AtomicUsize::new(0);

    let _muted = mute_panic_output();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.scope(|s| {
            for _ in 0..TOTAL_JOB
            {
                s.spawn(|| {
                    counter.fetch_add(1, Ordering::Release);
                });
            }
            panic!("the caller dies after spawning");
        });
    }));

    assert!(outcome.is_err(), "the closure's panic never made it out of the scope");

    let settled = counter.load(Ordering::Acquire);
    assert!(settled <= TOTAL_JOB, "counted {} runs out of {} jobs", settled, TOTAL_JOB);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        counter.load(Ordering::Acquire),
        settled,
        "a job was still touching the borrowed frame after the scope had unwound"
    );
}

#[test]
fn t25_the_calling_thread_runs_jobs_while_it_waits()
{
    const TOTAL_JOB: usize = scaled(2_000);
    const DEQUE_CAPACITY: usize = 2;

    // One worker, a tiny deque, and far more jobs than fit in it. Two separate things then push
    // work onto the caller, and the test only needs one of them to happen:
    //
    // - `spawn` runs the job on the spot when the deque is full, and with this many jobs against
    //   this few slots it fills almost immediately
    // - once the jobs are in, the caller drains alongside the worker instead of standing around
    //
    // The job count has to stay well above the capacity for the first of those to hold, which is
    // why the capacity is pinned here rather than left at whatever `cfg` hands out: shrinking the
    // job count for Miri would otherwise quietly turn this into a test that passes by luck.
    let mut cfg = cfg("t25", 1);
    cfg.per_worker_task_capacity = DEQUE_CAPACITY;
    assert!(
        TOTAL_JOB > DEQUE_CAPACITY * 4,
        "the job count has to stay well past the deque capacity or this test proves nothing"
    );

    let pool = ThreadPool::new(cfg);
    let main_id = std::thread::current().id();

    let on_main = AtomicUsize::new(0);
    let total = AtomicUsize::new(0);

    pool.scope(|s| {
        for _ in 0..TOTAL_JOB
        {
            s.spawn(|| {
                if std::thread::current().id() == main_id
                {
                    on_main.fetch_add(1, Ordering::Release);
                }
                total.fetch_add(1, Ordering::Release);
            });
        }
    });

    assert_eq!(total.load(Ordering::Acquire), TOTAL_JOB, "scope returned with jobs left to run");
    assert!(
        on_main.load(Ordering::Acquire) > 0,
        "the caller waited idly inside the scope and did not run a single one of the {} jobs",
        TOTAL_JOB
    );
}

#[test]
fn t26_the_calling_thread_does_not_become_a_permanent_worker()
{
    const TOTAL_JOB: usize = scaled(200);

    // Once the scope is over, the caller gets its own thread back. Work pushed afterwards must not
    // run there, because the caller never returns to the job loop.
    let pool = ThreadPool::new(cfg("t26", 2));
    let main_id = std::thread::current().id();

    pool.scope(|s| {
        for _ in 0..8
        {
            s.spawn(|| {});
        }
    });

    let on_main = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..TOTAL_JOB
    {
        let on_main = on_main.clone();
        let done = done.clone();
        pool.push(Job::new(move || {
            if std::thread::current().id() == main_id
            {
                on_main.fetch_add(1, Ordering::Release);
            }
            done.fetch_add(1, Ordering::Release);
        }));
    }

    wait_until("the workers cleared the queue outside the scope", || done.load(Ordering::Acquire) == TOTAL_JOB);
    assert_eq!(
        on_main.load(Ordering::Acquire),
        0,
        "a job ran on the calling thread while it was not parked at any wait point"
    );
}

#[test]
fn t27_scope_works_from_a_thread_that_did_not_build_the_pool()
{
    const TOTAL_JOB: usize = scaled(1_000);

    // Nothing ties `scope` to the thread that called `ThreadPool::new`. Any outsider borrows the
    // host slot for as long as it is waiting.
    let pool = ThreadPool::new(cfg("t27", 4));
    let pool_ref = PoolRef::new(&pool);
    let counter = AtomicUsize::new(0);

    std::thread::scope(|threads| {
        threads.spawn(|| {
            pool_ref.get().scope(|s| {
                for _ in 0..TOTAL_JOB
                {
                    s.spawn(|| {
                        counter.fetch_add(1, Ordering::Release);
                    });
                }
            });
        });
    });

    assert_eq!(counter.load(Ordering::Acquire), TOTAL_JOB);
}

#[test]
fn t28_a_scope_on_another_pool_puts_the_caller_context_back()
{
    const INNER_JOB: usize = scaled(64);

    // A worker of pool A opens a scope on pool B. To push into B's queues it has to pretend to be
    // B's host for a moment, then hand its own identity back. Forget the second half and that
    // worker keeps looking up slots in the wrong pool for the rest of its life.
    let a = ThreadPool::new(cfg("t28a", 2));
    let b = ThreadPool::new(cfg("t28b", 2));
    let b_ref = PoolRef::new(&b);

    let counter = AtomicUsize::new(0);
    let restored = AtomicUsize::new(0);

    a.scope(|outer| {
        outer.spawn(|| {
            let before = THREAD_LOCAL_CTX.get();

            b_ref.get().scope(|inner| {
                for _ in 0..INNER_JOB
                {
                    inner.spawn(|| {
                        counter.fetch_add(1, Ordering::Release);
                    });
                }
            });

            if THREAD_LOCAL_CTX.get() == before
            {
                restored.fetch_add(1, Ordering::Release);
            }
        });
    });

    assert_eq!(counter.load(Ordering::Acquire), INNER_JOB);
    assert_eq!(
        restored.load(Ordering::Acquire),
        1,
        "the thread kept the other pool's context after the nested scope returned"
    );
}

#[test]
fn t29_scopes_run_back_to_back_on_the_same_pool()
{
    const ROUND: usize = scaled(50);
    const JOB_PER_ROUND: usize = scaled(200);

    // Each scope brings its own latch, and nothing about the previous one may leak into the next.
    // A stale ticket count would show up here as a hang or as an early return.
    let pool = ThreadPool::new(cfg("t29", 4));

    for round in 0..ROUND
    {
        let counter = AtomicUsize::new(0);
        pool.scope(|s| {
            for _ in 0..JOB_PER_ROUND
            {
                s.spawn(|| {
                    counter.fetch_add(1, Ordering::Release);
                });
            }
        });
        assert_eq!(counter.load(Ordering::Acquire), JOB_PER_ROUND, "round {} came up short", round);
    }
}

#[test]
fn batch_borrows_mutable_data_and_runs_each_job_once()
{
    let pool = ThreadPool::new(cfg("batch-small-queues", 4).with_capacity(1, 1));
    for count in [0, 1, 3, 7, scaled(1025)]
    {
        let mut values = vec![0usize; count];
        for _ in 0..3
        {
            pool.run_batch(values.iter_mut().map(|value| move || *value += 1));
        }
        assert!(values.iter().all(|value| *value == 3));
    }
}

#[test]
fn batch_finishes_collecting_before_any_job_starts()
{
    let pool = ThreadPool::new(cfg("batch-collect", 4));
    let collected = AtomicUsize::new(0);
    let completed = AtomicUsize::new(0);
    let jobs = (0..17).map(|_| {
        collected.fetch_add(1, Ordering::Release);
        // Give a premature publication plenty of opportunity to run.
        std::thread::yield_now();
        || {
            assert_eq!(collected.load(Ordering::Acquire), 17);
            completed.fetch_add(1, Ordering::Release);
        }
    });
    pool.run_batch(jobs);
    assert_eq!(completed.load(Ordering::Acquire), 17);
}

#[test]
fn batch_iterator_panic_drops_collected_jobs_without_running_them()
{
    struct OnDrop<'a>(&'a AtomicUsize);
    impl Drop for OnDrop<'_>
    {
        fn drop(&mut self)
        {
            self.0.fetch_add(1, Ordering::Release);
        }
    }
    let pool = ThreadPool::new(cfg("batch-iterator-panic", 2));
    let dropped = AtomicUsize::new(0);
    let ran = AtomicUsize::new(0);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.run_batch((0..8).map(|index| {
            assert_ne!(index, 5, "iterator failed");
            let guard = OnDrop(&dropped);
            let ran = &ran;
            move || {
                ran.fetch_add(1, Ordering::Release);
                drop(guard);
            }
        }));
    }));
    assert!(outcome.is_err());
    assert_eq!(ran.load(Ordering::Acquire), 0);
    assert_eq!(dropped.load(Ordering::Acquire), 5);
    pool.run_batch([|| {}]);
}

#[test]
fn batch_job_panic_drains_borrowed_jobs_and_restores_context()
{
    let pool = ThreadPool::new(cfg("batch-job-panic", 2));
    let finished = AtomicUsize::new(0);
    let before = THREAD_LOCAL_CTX.get();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.run_batch((0..17).map(|index| {
            let finished = &finished;
            move || {
                assert_ne!(index, 3, "batch job failed");
                finished.fetch_add(1, Ordering::Release);
            }
        }));
    }));
    assert!(outcome.is_err());
    assert_eq!(finished.load(Ordering::Acquire), 16);
    assert_eq!(THREAD_LOCAL_CTX.get(), before);
    pool.run_batch([|| {
        finished.fetch_add(1, Ordering::Release);
    }]);
    assert_eq!(finished.load(Ordering::Acquire), 17);
}

#[test]
fn batch_nests_from_workers_and_inside_scopes()
{
    let pool = ThreadPool::new(cfg("batch-nested", 2).with_capacity(1, 1));
    let pool_ref = PoolRef::new(&pool);
    let completed = AtomicUsize::new(0);
    pool.scope(|scope| {
        for _ in 0..5
        {
            scope.spawn(|| {
                let before = THREAD_LOCAL_CTX.get();
                pool_ref.get().run_batch((0..7).map(|_| {
                    || {
                        pool_ref.get().run_batch((0..3).map(|_| {
                            || {
                                completed.fetch_add(1, Ordering::Release);
                            }
                        }));
                    }
                }));
                assert_eq!(THREAD_LOCAL_CTX.get(), before);
            });
        }
    });
    assert_eq!(completed.load(Ordering::Acquire), 5 * 7 * 3);
}

#[test]
fn batch_wakes_assigned_worker_while_caller_is_busy()
{
    let pool = ThreadPool::new(cfg("batch-wakeup", 1));
    for _ in 0..4
    {
        wait_until("batch worker parked", || !pool.inner.sleepers.get().is_empty());
        let completed = AtomicUsize::new(0);
        pool.run_batch((0..2).map(|_| {
            || {
                completed.fetch_add(1, Ordering::Release);
                wait_until("both assigned jobs started", || completed.load(Ordering::Acquire) == 2);
            }
        }));
        assert_eq!(completed.load(Ordering::Acquire), 2);
    }
}

#[test]
fn concurrent_external_batches_and_scopes_do_not_share_a_producer()
{
    let pool = ThreadPool::new(cfg("batch-callers", 2).with_capacity(2, 2));
    let pool_ref = PoolRef::new(&pool);
    let ready = std::sync::Barrier::new(4);
    let completed = AtomicUsize::new(0);
    std::thread::scope(|threads| {
        for _ in 0..4
        {
            let ready = &ready;
            let completed = &completed;
            threads.spawn(move || {
                let before = THREAD_LOCAL_CTX.get();
                pool_ref.get().scope(|scope| {
                    // Every caller must enter its callback even while another owns the host.
                    ready.wait();
                    for _ in 0..7
                    {
                        scope.spawn(|| {
                            completed.fetch_add(1, Ordering::Release);
                        });
                    }
                    pool_ref.get().run_batch((0..11).map(|_| {
                        || {
                            completed.fetch_add(1, Ordering::Release);
                        }
                    }));
                });
                assert_eq!(THREAD_LOCAL_CTX.get(), before);
            });
        }
    });
    assert_eq!(completed.load(Ordering::Acquire), 4 * (7 + 11));
}

#[test]
fn batch_can_reenter_a_pool_through_another_pool()
{
    let a = ThreadPool::new(cfg("batch-reenter-a", 1));
    let b = ThreadPool::new(cfg("batch-reenter-b", 1));
    let completed = AtomicUsize::new(0);
    // Both host slots are held on this thread when A is entered again. Blocking on
    // A's host lock would deadlock instead of using an external helper.
    a.scope(|_| {
        b.scope(|_| {
            let before = THREAD_LOCAL_CTX.get();
            a.run_batch([|| {
                completed.fetch_add(1, Ordering::Release);
            }]);
            assert_eq!(THREAD_LOCAL_CTX.get(), before);
        });
    });
    assert_eq!(completed.load(Ordering::Acquire), 1);
}
