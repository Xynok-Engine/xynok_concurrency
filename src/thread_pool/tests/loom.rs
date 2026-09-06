//! `loom` models for [`WorkerQueue`].
//!
//! The unit and stress suites hammer the queue with real threads and hope to trip over a bad
//! interleaving. `loom` does the opposite: it runs tiny scenarios and enumerates *every* order the
//! memory model allows, so a hole either shows up on the first run or does not exist inside the
//! scenario. The catch is that the state space explodes, so keep the thread count at two or three
//! and the operation count in the single digits.
//!
//! What is under the microscope here is the ownership protocol between the single producer and the
//! thieves. The queue hands out three cursors:
//!
//! - `[stolen, blocked)` are slots a thief has claimed and is still reading
//! - `[blocked, tail)` hold items nobody has claimed yet
//! - `[tail, stolen + capacity)` are free for the owner to write into
//!
//! Those three ranges must never overlap. `loom`'s `UnsafeCell` records every access to every slot
//! and fails the model the moment two threads touch the same one without an ordering between them,
//! which is exactly the question being asked.

use loom::sync::Arc;

use crate::thread_pool::worker_queue::WorkerQueue;
use crate::utils::steal::Steal;

/// Small on purpose. A two slot ring wraps after two pushes, so the owner starts reusing indices
/// almost immediately and any confusion between "this slot is free" and "a thief is still reading
/// it" surfaces within a handful of operations instead of needing a long run.
const CAP: usize = 2;

/// The owner pops from the tail and pushes back, while a thief takes from the head.
///
/// This is the shape that shows up in the pool: the thread inside `scope` pushes jobs onto its own
/// deque and runs them from the tail, and the workers steal from the head at the same time. The
/// question is whether `empty_slots` can ever tell the owner that a slot is free while a thief is
/// still reading out of it.
#[test]
fn t0_owner_push_pop_against_one_thief()
{
    loom::model(|| {
        let victim = Arc::new(WorkerQueue::<usize>::new(CAP));
        let thief_queue = Arc::new(WorkerQueue::<usize>::new(CAP));

        // Fill the ring before the threads start, so the interesting part of the model is the
        // overlap between the two of them rather than the ramp up.
        victim.push(1).expect("a fresh queue has room");
        victim.push(2).expect("a fresh queue has room");

        let owner = {
            let victim = Arc::clone(&victim);
            loom::thread::spawn(move || {
                // Take one off the tail and put one back. The push reuses the slot the pop just
                // freed, which is where a wraparound mistake would land.
                let _ = victim.pop();
                let _ = victim.push(3);
            })
        };

        let thief = {
            let victim = Arc::clone(&victim);
            let thief_queue = Arc::clone(&thief_queue);
            loom::thread::spawn(move || {
                let _ = victim.try_steal_batch_to(1, &thief_queue);
            })
        };

        owner.join().expect("the owner thread panicked");
        thief.join().expect("the thief thread panicked");
    });
}

/// Same idea, but with two thieves racing each other.
///
/// One thief alone can never see a stale `stolen`, because it is the only one publishing. With two
/// of them the second has to wait its turn in `consumer_publish_stolen`, so `stolen` moves in steps
/// that neither thief controls on its own, and the owner reads it from an atomic separate from the
/// one holding `tail` and `blocked`.
#[test]
fn t1_owner_against_two_thieves()
{
    loom::model(|| {
        let victim = Arc::new(WorkerQueue::<usize>::new(CAP));

        victim.push(1).expect("a fresh queue has room");
        victim.push(2).expect("a fresh queue has room");

        let thieves: Vec<_> = (0..2)
            .map(|_| {
                let victim = Arc::clone(&victim);
                loom::thread::spawn(move || {
                    let queue = WorkerQueue::<usize>::new(CAP);
                    let _ = victim.try_steal_batch_to(1, &queue);
                })
            })
            .collect();

        let owner = {
            let victim = Arc::clone(&victim);
            loom::thread::spawn(move || {
                let _ = victim.push(3);
            })
        };

        owner.join().expect("the owner thread panicked");
        for thief in thieves
        {
            thief.join().expect("a thief thread panicked");
        }
    });
}

/// Nothing may be lost and nothing may come out twice.
///
/// The models above lean on `loom`'s `UnsafeCell` to catch two threads touching one slot. This one
/// asks the other half of the question: across every interleaving, does each item end up in exactly
/// one place, either still in the queue or in the hands of exactly one taker?
#[test]
fn t2_no_item_is_lost_or_duplicated()
{
    loom::model(|| {
        let victim = Arc::new(WorkerQueue::<usize>::new(CAP));

        victim.push(1).expect("a fresh queue has room");
        victim.push(2).expect("a fresh queue has room");

        let owner = {
            let victim = Arc::clone(&victim);
            loom::thread::spawn(move || victim.pop())
        };

        let thief = {
            let victim = Arc::clone(&victim);
            loom::thread::spawn(move || {
                let queue = WorkerQueue::<usize>::new(CAP);
                match victim.try_steal_batch_to(1, &queue)
                {
                    Steal::Success(_) => queue.pop(),
                    _ => None,
                }
            })
        };

        let from_owner = owner.join().expect("the owner thread panicked");
        let from_thief = thief.join().expect("the thief thread panicked");

        let mut seen: Vec<usize> = [from_owner, from_thief].into_iter().flatten().collect();

        // Both threads are done, so draining what is left is safe and accounts for the rest.
        while let Some(val) = victim.pop()
        {
            seen.push(val);
        }
        seen.sort_unstable();

        assert_eq!(seen, vec![1, 2], "the two items that went in did not come back out exactly once each");
    });
}

/// The wraparound case, which is the one the arithmetic in `empty_slots` actually exists for.
///
/// Two slots and three pushes, so the third one can only go through if the thief has already
/// finished with the slot it is about to reuse. That decision rests on `stolen`, which lives in a
/// different atomic from the `tail` and `blocked` pair, so the owner reads the two halves of the
/// picture at different moments. Only two threads here, to leave room for more operations.
#[test]
fn t3_owner_wraps_around_while_a_thief_reads()
{
    loom::model(|| {
        let victim = Arc::new(WorkerQueue::<usize>::new(CAP));

        let owner = {
            let victim = Arc::clone(&victim);
            loom::thread::spawn(move || {
                let _ = victim.push(1);
                let _ = victim.push(2);
                // Slot 0 again. It is only free if the thief is done reading it.
                let _ = victim.push(3);
            })
        };

        let thief = {
            let victim = Arc::clone(&victim);
            loom::thread::spawn(move || {
                let queue = WorkerQueue::<usize>::new(CAP);
                let _ = victim.try_steal_batch_to(1, &queue);
                let _ = victim.try_steal_batch_to(1, &queue);
            })
        };

        owner.join().expect("the owner thread panicked");
        thief.join().expect("the thief thread panicked");
    });
}

/// Wraparound with the owner popping in between, which is the exact shape `scope` produces.
///
/// The thread inside `scope` pushes its jobs, then turns around and runs them off the tail while
/// the workers steal from the head. `pop` moves `tail` backwards without touching `stolen`, so the
/// owner's idea of what is free changes from both ends at once, and the next push has to land
/// somewhere no thief is looking.
#[test]
fn t4_owner_pops_between_pushes_while_a_thief_reads()
{
    loom::model(|| {
        let victim = Arc::new(WorkerQueue::<usize>::new(CAP));

        let owner = {
            let victim = Arc::clone(&victim);
            loom::thread::spawn(move || {
                let _ = victim.push(1);
                let _ = victim.push(2);
                let _ = victim.pop();
                let _ = victim.push(3);
            })
        };

        let thief = {
            let victim = Arc::clone(&victim);
            loom::thread::spawn(move || {
                let queue = WorkerQueue::<usize>::new(CAP);
                let _ = victim.try_steal_batch_to(1, &queue);
                let _ = victim.try_steal_batch_to(1, &queue);
            })
        };

        owner.join().expect("the owner thread panicked");
        thief.join().expect("the thief thread panicked");
    });
}

// --- temporary bisection probes ---

#[test]
fn p0_push_push_pop_and_one_steal()
{
    loom::model(|| {
        let v = Arc::new(WorkerQueue::<usize>::new(CAP));
        let o = { let v = Arc::clone(&v); loom::thread::spawn(move || { let _ = v.push(1); let _ = v.push(2); let _ = v.pop(); }) };
        let t = { let v = Arc::clone(&v); loom::thread::spawn(move || { let q = WorkerQueue::<usize>::new(CAP); let _ = v.try_steal_batch_to(1, &q); }) };
        o.join().unwrap(); t.join().unwrap();
    });
}

#[test]
fn p1_push_pop_push_and_one_steal()
{
    loom::model(|| {
        let v = Arc::new(WorkerQueue::<usize>::new(CAP));
        let o = { let v = Arc::clone(&v); loom::thread::spawn(move || { let _ = v.push(1); let _ = v.pop(); let _ = v.push(2); }) };
        let t = { let v = Arc::clone(&v); loom::thread::spawn(move || { let q = WorkerQueue::<usize>::new(CAP); let _ = v.try_steal_batch_to(1, &q); }) };
        o.join().unwrap(); t.join().unwrap();
    });
}

#[test]
fn p2_pop_only_and_one_steal()
{
    loom::model(|| {
        let v = Arc::new(WorkerQueue::<usize>::new(CAP));
        v.push(1).unwrap();
        v.push(2).unwrap();
        let o = { let v = Arc::clone(&v); loom::thread::spawn(move || { let _ = v.pop(); }) };
        let t = { let v = Arc::clone(&v); loom::thread::spawn(move || { let q = WorkerQueue::<usize>::new(CAP); let _ = v.try_steal_batch_to(1, &q); }) };
        o.join().unwrap(); t.join().unwrap();
    });
}

#[test]
fn p3_push_push_pop_push_and_one_steal()
{
    loom::model(|| {
        let v = Arc::new(WorkerQueue::<usize>::new(CAP));
        let o = { let v = Arc::clone(&v); loom::thread::spawn(move || { let _ = v.push(1); let _ = v.push(2); let _ = v.pop(); let _ = v.push(3); }) };
        let t = { let v = Arc::clone(&v); loom::thread::spawn(move || { let q = WorkerQueue::<usize>::new(CAP); let _ = v.try_steal_batch_to(1, &q); }) };
        o.join().unwrap(); t.join().unwrap();
    });
}
