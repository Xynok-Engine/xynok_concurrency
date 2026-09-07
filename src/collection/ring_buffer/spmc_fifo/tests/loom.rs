//! `loom` models for [`SpmcRingBufferFifo`].
//!
//! The unit suite throws real threads at the ring and hopes to land on a bad interleaving. `loom`
//! turns that around: tiny scenarios, but it walks *every* order the memory model permits, so a
//! hole either shows up immediately or is not there. The price is that the state space blows up
//! fast, so keep it to two or three threads and a handful of operations.
//!
//! The question being asked is whether a consumer can ever read a slot the producer is still
//! writing, or is about to overwrite. The ring keeps three cursors:
//!
//! - `[stolen, blocked)` are slots a consumer has claimed and is still copying out
//! - `[blocked, tail)` hold items nobody has claimed
//! - `[tail, stolen + capacity)` are free for the producer
//!
//! `loom`'s `UnsafeCell` records every access to every slot and fails the model as soon as two
//! threads touch the same one with no ordering between them, which is exactly the property that
//! matters here.

use loom::sync::Arc;

use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;

/// Two slots, so the ring wraps after two pushes and the producer starts reusing indices right
/// away. A mix-up between "the consumer released this slot" and "the consumer is still in it"
/// shows up within a few operations instead of needing a long run.
const CAP: usize = 2;

/// The plain case: the producer fills the ring while one consumer drains it.
#[test]
fn t0_one_producer_one_consumer()
{
    loom::model(|| {
        let ring = Arc::new(SpmcRingBufferFifo::<usize>::new(CAP));

        let producer = {
            let ring = Arc::clone(&ring);
            loom::thread::spawn(move || {
                let _ = ring.push(1);
                let _ = ring.push(2);
            })
        };

        let consumer = {
            let ring = Arc::clone(&ring);
            loom::thread::spawn(move || {
                let _ = ring.pop();
                let _ = ring.pop();
            })
        };

        producer.join().expect("the producer thread panicked");
        consumer.join().expect("the consumer thread panicked");
    });
}

/// Wraparound, which is what the arithmetic in `empty_slots` exists for.
///
/// Three pushes into two slots, so the third can only land once the consumer has finished with the
/// slot it is about to reuse. That call rests on `stolen`, which the producer reads from a
/// different atomic than the one holding `blocked`, so it sees the two halves of the picture at
/// slightly different moments.
#[test]
fn t1_producer_wraps_around_while_a_consumer_reads()
{
    loom::model(|| {
        let ring = Arc::new(SpmcRingBufferFifo::<usize>::new(CAP));

        let producer = {
            let ring = Arc::clone(&ring);
            loom::thread::spawn(move || {
                let _ = ring.push(1);
                let _ = ring.push(2);
                // Slot 0 again, free only if the consumer is done with it.
                let _ = ring.push(3);
            })
        };

        let consumer = {
            let ring = Arc::clone(&ring);
            loom::thread::spawn(move || {
                let _ = ring.pop();
                let _ = ring.pop();
            })
        };

        producer.join().expect("the producer thread panicked");
        consumer.join().expect("the consumer thread panicked");
    });
}

/// Two consumers racing each other, which is the whole point of the SPMC design.
///
/// A single consumer can never see a stale `stolen`, since it is the only one publishing. With two
/// of them the second has to wait its turn in `publish_stolen`, so `stolen` advances in steps
/// neither of them controls alone, and the producer reads it while that is in flight.
#[test]
fn t2_one_producer_two_consumers()
{
    loom::model(|| {
        let ring = Arc::new(SpmcRingBufferFifo::<usize>::new(CAP));

        ring.push(1).expect("a fresh ring has room");
        ring.push(2).expect("a fresh ring has room");

        let consumers: Vec<_> = (0..2)
            .map(|_| {
                let ring = Arc::clone(&ring);
                loom::thread::spawn(move || ring.pop())
            })
            .collect();

        let producer = {
            let ring = Arc::clone(&ring);
            loom::thread::spawn(move || {
                let _ = ring.push(3);
            })
        };

        producer.join().expect("the producer thread panicked");
        for consumer in consumers
        {
            consumer.join().expect("a consumer thread panicked");
        }
    });
}

/// Nothing lost, nothing handed out twice.
///
/// The models above lean on `loom`'s `UnsafeCell` to catch two threads in one slot. This one asks
/// the other half: across every interleaving, does each item come out exactly once?
#[test]
fn t3_no_item_is_lost_or_duplicated()
{
    loom::model(|| {
        let ring = Arc::new(SpmcRingBufferFifo::<usize>::new(CAP));

        ring.push(1).expect("a fresh ring has room");
        ring.push(2).expect("a fresh ring has room");

        let consumers: Vec<_> = (0..2)
            .map(|_| {
                let ring = Arc::clone(&ring);
                loom::thread::spawn(move || ring.pop())
            })
            .collect();

        let mut seen: Vec<usize> = Vec::new();
        for consumer in consumers
        {
            if let Some(val) = consumer.join().expect("a consumer thread panicked")
            {
                seen.push(val);
            }
        }

        // Both consumers are finished, so whatever is left is ours to drain.
        while let Some(val) = ring.pop()
        {
            seen.push(val);
        }
        seen.sort_unstable();

        assert_eq!(seen, vec![1, 2], "the two items that went in did not come back out exactly once each");
    });
}
