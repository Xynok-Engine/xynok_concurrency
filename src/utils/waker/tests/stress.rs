//! Stress tests: they hammer the latch with real OS threads, so they are useless under `loom`
//! (which models a handful of threads exhaustively instead) and are compiled out there.
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::sync::{AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::cores::available_cores;
use crate::utils::waker::Waker;

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
fn t0_stress_wait_returns_after_the_last_ticket()
{
    with_watchdog("t0_stress_wait_returns_after_the_last_ticket", Duration::from_secs(120), || {
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
fn t1_stress_worker_writes_are_visible_after_wait()
{
    with_watchdog("t1_stress_worker_writes_are_visible_after_wait", Duration::from_secs(120), || {
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
fn t2_stress_signals_landing_before_wait()
{
    with_watchdog("t2_stress_signals_landing_before_wait", Duration::from_secs(120), || {
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
fn t3_stress_many_tickets_per_thread()
{
    with_watchdog("t3_stress_many_tickets_per_thread", Duration::from_secs(120), || {
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
fn t4_wait_on_an_empty_latch_returns_immediately()
{
    with_watchdog("t4_wait_on_an_empty_latch_returns_immediately", Duration::from_secs(10), || {
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
fn t5_wait_from_a_foreign_thread_is_rejected()
{
    with_watchdog("t5_wait_from_a_foreign_thread_is_rejected", Duration::from_secs(10), || {
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
