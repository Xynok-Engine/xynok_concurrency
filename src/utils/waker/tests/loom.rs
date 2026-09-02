//! `loom` models: instead of hammering the latch with real threads, these run tiny scenarios and
//! let `loom` enumerate *every* legal interleaving and every stale read the memory model permits.
//! Keep the thread counts at 2-3, the state space grows explosively.
//!
//! What they cover is the *protocol*: no interleaving may lose an `unpark`, and no leftover token
//! may open the latch early. What they cannot cover is publication, so the two suites are
//! complementary rather than redundant.
use crate::sync::Arc;
use crate::sync::cell::UnsafeCell;
use crate::utils::waker::Waker;

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
fn t0_wait_returns_once_every_ticket_is_gone()
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
fn t1_wait_publishes_the_work_it_waited_for()
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
fn t2_the_waiter_may_close_the_latch_itself()
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
fn t3_a_stale_unpark_does_not_open_the_next_latch()
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
fn t4_an_empty_latch_never_parks()
{
    loom::model(|| {
        let waker = Waker::new(0);
        waker.wait();
        assert_eq!(waker.remaining(), 0);
    });
}
