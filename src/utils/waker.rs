use crate::sync::thread::{self, Thread};
use crate::sync::{AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;

/// ## The Concept
/// Imagine you are a manager. You assign three tasks to three people, then go take a nap. The only rule is:
/// There is a number 3 written on a whiteboard. Whoever finishes their task subtracts 1 from that number. The person who subtracts to reach 0 has the duty of waking the boss up.
/// That is it. That is the entire Latch. The counter is the whiteboard, and "waking the boss up" is the `unpark` operation.
pub struct Waker
{
    remaining: CachePadded<AtomicUsize>,
    waiter:    Thread,
}
unsafe impl Send for Waker {}
unsafe impl Sync for Waker {}

pub struct WakerSignal<'a>
{
    waker:   &'a Waker,
    sleeper: Thread,
}
impl Waker
{
    #[inline]
    pub fn new(worker_amount: usize) -> Self
    {
        Self {
            remaining: CachePadded::new(AtomicUsize::new(worker_amount)),
            waiter:    crate::sync::thread::current(),
        }
    }
    #[inline]
    pub fn ticket(&self) -> WakerSignal<'_>
    {
        WakerSignal {
            waker:   self,
            sleeper: self.waiter.clone(),
        }
    }
    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.remaining.load(Ordering::Acquire)
    }

    #[inline]
    pub fn wait(&self)
    {
        debug_assert!(
            self.waiter.id() == thread::current().id(),
            "this must be called from the same thread that created the `Waker`"
        );
        while self.remaining() > 0
        {
            thread::park();
        }
    }
}

impl Drop for WakerSignal<'_>
{
    fn drop(&mut self)
    {
        if self.waker.remaining.fetch_sub(1, Ordering::Release) == 1
        {
            self.sleeper.unpark();
        }
    }
}
/// Stress tests: they hammer the latch with real OS threads, so they are useless under `loom`
/// (which models a handful of threads exhaustively instead) and are compiled out there.
#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::mpsc::{RecvTimeoutError, channel};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use super::Waker;
    use crate::sync::{AtomicUsize, Ordering};
    use crate::utils::available_cores;
    use crate::utils::cache_padded::CachePadded;

    /// Miri interprets every instruction, so the iteration counts that take a couple of seconds on
    /// real hardware would run for hours there. Everything is divided by this.
    #[cfg(miri)]
    const SCALE: usize = 100;
    #[cfg(not(miri))]
    const SCALE: usize = 1;

    const fn scaled(n: usize) -> usize
    {
        if n / SCALE == 0 { 1 } else { n / SCALE }
    }

    /// xorshift64. No `rand` dependency, and the same seed replays the same pressure pattern, so a
    /// failure is at least as reproducible as a race can be.
    struct Rng(u64);
    impl Rng
    {
        fn new(seed: u64) -> Self
        {
            // The `| 1` keeps the state away from 0, which xorshift can never leave.
            Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }
        fn next(&mut self) -> u64
        {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64
        {
            self.next() % n.max(1)
        }
    }

    /// A random delay spread over "none at all", "a few cycles" and "long enough for the OS to
    /// reschedule". That spread is the whole point: it makes the last ticket land both *before*
    /// the waiter parks (so `unpark` only leaves a token) and *after* it (so `unpark` has to
    /// actually wake somebody).
    fn jitter(rng: &mut Rng)
    {
        match rng.below(4)
        {
            0 =>
            {}
            1 =>
            {
                for _ in 0..rng.below(128)
                {
                    std::hint::spin_loop();
                }
            }
            2 => std::thread::yield_now(),
            _ => std::thread::sleep(Duration::from_micros(rng.below(200))),
        }
    }

    fn join_or_repanic(handle: JoinHandle<()>)
    {
        if let Err(payload) = handle.join()
        {
            std::panic::resume_unwind(payload);
        }
    }

    /// Runs `body` on its own thread so a lost wake-up fails the test instead of hanging the run
    /// forever. The body has to own the whole scenario, because `Waker::new` latches onto the
    /// thread that builds it and only that thread is allowed to call `wait`.
    fn with_watchdog<F>(name: &'static str, timeout: Duration, body: F)
    where F: FnOnce() + Send + 'static
    {
        let (tx, rx) = channel::<()>();
        let scenario = std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                body();
                let _ = tx.send(());
            })
            .expect("could not spawn the scenario thread");

        match rx.recv_timeout(timeout)
        {
            // A panicking body drops the sender too, so a disconnect is not a deadlock: join and
            // re-raise whatever it panicked with, message intact.
            Ok(()) | Err(RecvTimeoutError::Disconnected) => join_or_repanic(scenario),
            Err(RecvTimeoutError::Timeout) =>
            {
                panic!("`{name}` deadlocked: `Waker::wait` did not return within {timeout:?} - a `LatchSignal` drop lost its `unpark`")
            }
        }
    }

    /// The base case, repeated until the scheduler has tried every ordering it feels like: a round
    /// of workers, a waiter racing them, and `wait` must always come back with the counter at 0.
    #[test]
    fn stress_wait_returns_after_the_last_ticket()
    {
        with_watchdog("stress_wait_returns_after_the_last_ticket", Duration::from_secs(120), || {
            let cores = available_cores().max(2);
            let mut rng = Rng::new(0xC0FF_EE00);

            for round in 0..scaled(400) as u64
            {
                let workers = 1 + rng.below((cores * 2) as u64) as usize;
                let waker = Waker::new(workers);

                std::thread::scope(|s| {
                    for i in 0..workers
                    {
                        let ticket = waker.ticket();
                        let seed = (round << 8) | i as u64;
                        s.spawn(move || {
                            let mut rng = Rng::new(seed);
                            jitter(&mut rng);
                            drop(ticket);
                        });
                    }
                    // Jitter here too, so the waiter sometimes arrives long after the counter has
                    // already hit 0.
                    jitter(&mut rng);
                    waker.wait();
                    assert_eq!(waker.remaining(), 0, "round {round}: `wait` returned with tickets still outstanding");
                });
            }
        });
    }

    /// The reason a latch exists at all: when `wait` returns, everything the workers produced has
    /// to be readable. The waiter reads the payload *inside* the scope, before the joins - a join
    /// would synchronise on its own and hide a missing release/acquire pair on `remaining`.
    #[test]
    fn stress_worker_writes_are_visible_after_wait()
    {
        with_watchdog("stress_worker_writes_are_visible_after_wait", Duration::from_secs(120), || {
            let workers = available_cores().max(2) * 2;
            let mut stale = 0usize;

            for round in 1..=scaled(400)
            {
                // One cache line per worker: no false sharing to accidentally push the writes out
                // early and paper over a stale read.
                let slots: Vec<CachePadded<AtomicUsize>> = (0..workers).map(|_| CachePadded::new(AtomicUsize::new(0))).collect();
                let waker = Waker::new(workers);

                std::thread::scope(|s| {
                    for (i, slot) in slots.iter().enumerate()
                    {
                        let ticket = waker.ticket();
                        let seed = ((round as u64) << 8) | i as u64;
                        s.spawn(move || {
                            let mut rng = Rng::new(seed);
                            jitter(&mut rng);
                            // The "result" this worker publishes through the latch.
                            slot.store(round, Ordering::Relaxed);
                            drop(ticket);
                        });
                    }
                    waker.wait();
                    stale += slots.iter().filter(|slot| slot.load(Ordering::Relaxed) != round).count();
                });
            }

            assert_eq!(
                stale, 0,
                "{stale} worker write(s) were still invisible after `wait` returned: the counter needs release/acquire, not `Relaxed`"
            );
        });
    }

    /// Every ticket is gone before `wait` is even called, so `wait` takes the fast path and the
    /// `unpark` it never consumed stays pending on the waiter thread. Rounds are back to back on
    /// purpose: a leftover token must not let the *next* round's `wait` return early, and the
    /// deliberately abandoned wakers in between pile up more of them.
    #[test]
    fn stress_signals_landing_before_wait()
    {
        with_watchdog("stress_signals_landing_before_wait", Duration::from_secs(120), || {
            let cores = available_cores().max(2);
            let mut rng = Rng::new(0x5EED_1234);

            for round in 0..scaled(600) as u64
            {
                if round % 4 == 0
                {
                    // Nobody ever waits on this one: its `unpark` becomes a stray token.
                    let orphan = Waker::new(1);
                    std::thread::scope(|s| {
                        let ticket = orphan.ticket();
                        s.spawn(move || drop(ticket));
                    });
                }

                let workers = 1 + rng.below(cores as u64) as usize;
                let waker = Waker::new(workers);
                std::thread::scope(|s| {
                    for _ in 0..workers
                    {
                        let ticket = waker.ticket();
                        s.spawn(move || drop(ticket));
                    }
                });
                // The scope joined, so every ticket is already dropped.
                assert_eq!(waker.remaining(), 0, "round {round}: a dropped ticket did not decrement the counter");
                waker.wait();
                // `wait` is idempotent once the latch is open.
                waker.wait();
                assert_eq!(waker.remaining(), 0, "round {round}");
            }
        });
    }

    /// Ticket churn rather than thread churn: a handful of threads each open and close hundreds of
    /// tickets, and the waiter holds a few of its own. Those last ones mean the counter often
    /// reaches 0 on the waiter thread itself, which makes it `unpark` a thread that is not parked.
    #[test]
    fn stress_many_tickets_per_thread()
    {
        with_watchdog("stress_many_tickets_per_thread", Duration::from_secs(120), || {
            let threads = available_cores().max(2);
            let per_thread = scaled(512);
            let self_held = 8;
            let mut rng = Rng::new(0xABCD_0F0F);

            for round in 0..scaled(20) as u64
            {
                let waker = Waker::new(threads * per_thread + self_held);

                std::thread::scope(|s| {
                    for t in 0..threads
                    {
                        let waker = &waker;
                        let seed = (round << 16) | t as u64;
                        s.spawn(move || {
                            let mut rng = Rng::new(seed);
                            for _ in 0..per_thread
                            {
                                let ticket = waker.ticket();
                                if rng.below(64) == 0
                                {
                                    jitter(&mut rng);
                                }
                                drop(ticket);
                            }
                        });
                    }

                    // Dropped after the workers are running, so the last decrement lands on
                    // whichever thread happens to get there first.
                    for _ in 0..self_held
                    {
                        let ticket = waker.ticket();
                        jitter(&mut rng);
                        drop(ticket);
                    }

                    waker.wait();
                    assert_eq!(waker.remaining(), 0, "round {round}: `wait` returned with tickets still outstanding");
                });
            }
        });
    }

    /// A `Waker` built for 0 workers is already open: `wait` must not park at all.
    #[test]
    fn wait_on_an_empty_latch_returns_immediately()
    {
        with_watchdog("wait_on_an_empty_latch_returns_immediately", Duration::from_secs(10), || {
            for _ in 0..scaled(1000)
            {
                let waker = Waker::new(0);
                waker.wait();
                assert_eq!(waker.remaining(), 0);
            }
        });
    }

    /// `Waker` is `Sync`, so nothing but the debug assert stops a foreign thread from parking on a
    /// latch that will never `unpark` it. Pin that guard down.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "same thread")]
    fn wait_from_a_foreign_thread_is_rejected()
    {
        with_watchdog("wait_from_a_foreign_thread_is_rejected", Duration::from_secs(10), || {
            let waker = Waker::new(1);
            std::thread::scope(|s| {
                // Kept alive so `wait` would really try to park if the assert ever went away.
                let _ticket = waker.ticket();
                let foreign = s.spawn(|| waker.wait());
                if let Err(payload) = foreign.join()
                {
                    std::panic::resume_unwind(payload);
                }
            });
        });
    }
}

/// `loom` models: instead of hammering the latch with real threads, these run tiny scenarios and
/// let `loom` enumerate *every* legal interleaving and every stale read the memory model permits.
/// Keep the thread counts at 2-3 - the state space grows explosively.
///
/// What they cover is the *protocol*: no interleaving may lose an `unpark`, and no leftover token
/// may open the latch early. What they cannot cover is publication - see
/// [`wait_publishes_the_work_it_waited_for`] for why - so the two suites are complementary rather
/// than redundant.
#[cfg(all(test, loom))]
mod loom_test
{
    use super::Waker;
    use crate::sync::Arc;
    use crate::sync::cell::UnsafeCell;

    /// Two workers plus the waiter is already three of `loom`'s four thread slots.
    const WORKERS: usize = 2;

    /// The work a latch exists to hand over. `loom`'s `UnsafeCell` records every access and fails
    /// the model with a causality violation when a read is not ordered after the write - which is
    /// exactly the question being asked here: does `wait` returning actually publish the work?
    struct Payload
    {
        slot: UnsafeCell<usize>,
    }
    unsafe impl Send for Payload {}
    unsafe impl Sync for Payload {}

    impl Payload
    {
        fn new() -> Self
        {
            Self { slot: UnsafeCell::new(0) }
        }
        fn publish(&self, value: usize)
        {
            self.slot.with_mut(|p| unsafe { *p = value });
        }
        fn read(&self) -> usize
        {
            self.slot.with(|p| unsafe { *p })
        }
    }

    /// `LatchSignal` borrows the `Waker`, but `loom::thread::spawn` demands `'static`, so the
    /// whole scenario lives behind an `Arc` and each worker takes its ticket after it starts.
    struct Shared
    {
        waker: Waker,
        slots: [Payload; WORKERS],
    }

    impl Shared
    {
        fn new(tickets: usize) -> Arc<Self>
        {
            Arc::new(Self {
                waker: Waker::new(tickets),
                slots: std::array::from_fn(|_| Payload::new()),
            })
        }
    }

    /// The liveness property: whatever order the tickets drop in, `wait` has to come back. A lost
    /// `unpark` shows up here as a `loom` deadlock, not as a hung test run.
    #[test]
    fn wait_returns_once_every_ticket_is_gone()
    {
        loom::model(|| {
            let waker = Arc::new(Waker::new(WORKERS));
            for _ in 0..WORKERS
            {
                let waker = Arc::clone(&waker);
                loom::thread::spawn(move || drop(waker.ticket()));
            }
            waker.wait();
            assert_eq!(waker.remaining(), 0, "`wait` returned with tickets still outstanding");
        });
    }

    /// When `wait` returns, the work the waiter waited for has to be readable.
    ///
    /// Be careful about what a green run here proves: `loom`'s `unpark` joins the unparker's
    /// causality into its target unconditionally (`loom-0.7.2/src/rt/thread.rs`), so the model
    /// hands out a happens-before edge even on the interleavings where the waiter never parks and
    /// real `park`/`unpark` would establish none. This model therefore passes on a counter that is
    /// purely `Relaxed`, while the same scenario under miri does not. Publication is pinned down by
    /// `test::stress_worker_writes_are_visible_after_wait`; this model only guards against a
    /// regression bad enough to break even the optimistic version.
    #[test]
    fn wait_publishes_the_work_it_waited_for()
    {
        loom::model(|| {
            let shared = Shared::new(WORKERS);
            for i in 0..WORKERS
            {
                let shared = Arc::clone(&shared);
                loom::thread::spawn(move || {
                    let ticket = shared.waker.ticket();
                    shared.slots[i].publish(i + 1);
                    drop(ticket);
                });
            }
            shared.waker.wait();
            for i in 0..WORKERS
            {
                assert_eq!(shared.slots[i].read(), i + 1, "slot {i} was not published by the time `wait` returned");
            }
        });
    }

    /// The waiter holds a ticket of its own, so on some interleavings the counter reaches 0 on the
    /// waiter's own thread and `unpark` targets a thread that is not parked.
    #[test]
    fn the_waiter_may_close_the_latch_itself()
    {
        loom::model(|| {
            let shared = Shared::new(2);
            {
                let shared = Arc::clone(&shared);
                loom::thread::spawn(move || {
                    let ticket = shared.waker.ticket();
                    shared.slots[0].publish(7);
                    drop(ticket);
                });
            }
            drop(shared.waker.ticket());
            shared.waker.wait();
            assert_eq!(shared.slots[0].read(), 7, "the worker's write was not published by `wait`");
        });
    }

    /// `unpark` on a thread that never parks leaves a token behind. The next `wait` must not spend
    /// it and walk out while a ticket is still alive - the re-check of the counter is what stops it.
    #[test]
    fn a_stale_unpark_does_not_open_the_next_latch()
    {
        loom::model(|| {
            // Nobody ever waits on this latch: its `unpark` is pure noise aimed at the main thread.
            let orphan = Arc::new(Waker::new(1));
            {
                let orphan = Arc::clone(&orphan);
                loom::thread::spawn(move || drop(orphan.ticket()));
            }

            let shared = Shared::new(1);
            {
                let shared = Arc::clone(&shared);
                loom::thread::spawn(move || {
                    let ticket = shared.waker.ticket();
                    shared.slots[0].publish(9);
                    drop(ticket);
                });
            }

            shared.waker.wait();
            assert_eq!(shared.waker.remaining(), 0, "`wait` returned on a stale `unpark` token");
            assert_eq!(shared.slots[0].read(), 9, "`wait` returned before the real ticket was dropped");
        });
    }

    /// A latch that is already open must not park at all - if it did, `loom` would report a
    /// deadlock, since there is no one left to wake it.
    #[test]
    fn an_empty_latch_never_parks()
    {
        loom::model(|| {
            let waker = Waker::new(0);
            waker.wait();
            assert_eq!(waker.remaining(), 0);
        });
    }
}
