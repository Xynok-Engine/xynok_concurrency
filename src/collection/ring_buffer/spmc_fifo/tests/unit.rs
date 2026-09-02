use crate::collection::ring_buffer::consts::MAX_CAPACITY;
use crate::collection::ring_buffer::spmc_fifo::SpmcRingBufferFifo;

#[test]
fn t0_test_drain()
{
    let mut vals = vec![0, 1, 2, 3, 4, 5];
    let take_amount = 2;
    let mut drained = Vec::new();
    for e in vals.drain(..take_amount)
    {
        drained.push(e);
    }
    println!("vals:    {:?}", vals);
    println!("drained: {:?}", drained);
}

// --- new() must reject invalid capacity ---

#[test]
#[should_panic(expected = "power of 2")]
fn t1_capacity_not_power_of_two_panics()
{
    let _ = SpmcRingBufferFifo::<u8>::new(3);
}

#[test]
#[should_panic]
fn t2_capacity_zero_panics()
{
    let _ = SpmcRingBufferFifo::<u8>::new(0);
}

#[test]
#[should_panic(expected = "exceeds")]
fn t3_capacity_exceeding_limit_panics()
{
    let _ = SpmcRingBufferFifo::<u8>::new(MAX_CAPACITY);
}

// --- push_batch / pop_batch reject nonsensical arguments ---

#[test]
#[should_panic(expected = "greater than zero")]
fn t4_push_batch_with_zero_max_panics()
{
    let ring = SpmcRingBufferFifo::new(4);
    let mut vals = vec![1u32];
    let _ = ring.push_batch(0, &mut vals);
}

#[test]
#[should_panic(expected = "empty source")]
fn t5_push_batch_with_empty_vals_panics()
{
    let ring = SpmcRingBufferFifo::new(4);
    let mut vals: Vec<u32> = Vec::new();
    let _ = ring.push_batch(4, &mut vals);
}

#[test]
#[should_panic(expected = "pop amount must > 0")]
fn t6_pop_batch_with_zero_max_panics()
{
    let ring = SpmcRingBufferFifo::<u32>::new(4);
    let mut out = Vec::new();
    let _ = ring.pop_batch(0, &mut out);
}

// --- basic push / pop ---

#[test]
fn t7_freshly_created_ring_pop_returns_none()
{
    let ring = SpmcRingBufferFifo::<u32>::new(4);
    assert_eq!(ring.pop(), None);
}

#[test]
fn t8_push_and_pop_keep_fifo_order()
{
    let ring = SpmcRingBufferFifo::new(4);
    for i in 0..4u32
    {
        assert_eq!(ring.push(i), Ok(()));
    }
    for i in 0..4u32
    {
        assert_eq!(ring.pop(), Some(i));
    }
    assert_eq!(ring.pop(), None);
}

#[test]
fn t9_push_when_full_returns_value_instead_of_dropping_it()
{
    let ring = SpmcRingBufferFifo::new(2);
    assert_eq!(ring.push(1), Ok(()));
    assert_eq!(ring.push(2), Ok(()));
    assert_eq!(ring.push(3), Err(3));

    assert_eq!(ring.pop(), Some(1));
    assert_eq!(ring.push(3), Ok(()));
}

#[test]
fn t10_index_wraparound_still_keeps_order()
{
    let ring = SpmcRingBufferFifo::new(4);
    for round in 0..10u32
    {
        for i in 0..4u32
        {
            assert_eq!(ring.push(round * 4 + i), Ok(()));
        }
        for i in 0..4u32
        {
            assert_eq!(ring.pop(), Some(round * 4 + i));
        }
    }
}

// --- push_batch ---

#[test]
fn t11_push_batch_limited_by_free_slots_and_keeps_the_remainder()
{
    let ring = SpmcRingBufferFifo::new(4);
    let mut vals: Vec<u32> = (0..10).collect();

    assert_eq!(ring.push_batch(10, &mut vals), 4);
    assert_eq!(vals, (4..10).collect::<Vec<_>>());

    assert_eq!(ring.push_batch(10, &mut vals), 0);
    assert_eq!(vals.len(), 6);
}

#[test]
fn t12_push_batch_limited_by_max_param()
{
    let ring = SpmcRingBufferFifo::new(8);
    let mut vals: Vec<u32> = (0..8).collect();

    assert_eq!(ring.push_batch(3, &mut vals), 3);
    assert_eq!(vals, (3..8).collect::<Vec<_>>());
}

#[test]
fn t13_push_batch_limited_by_vals_length()
{
    let ring = SpmcRingBufferFifo::new(8);
    let mut vals: Vec<u32> = (0..3).collect();

    assert_eq!(ring.push_batch(8, &mut vals), 3);
    assert!(vals.is_empty());
}

// --- pop_batch ---

#[test]
fn t14_pop_batch_returns_correct_order_and_is_limited_by_available_count()
{
    let ring = SpmcRingBufferFifo::new(8);
    let mut vals: Vec<u32> = (0..5).collect();
    assert_eq!(ring.push_batch(5, &mut vals), 5);

    let mut out = Vec::new();
    assert_eq!(ring.pop_batch(100, &mut out), 5);
    assert_eq!(out, vec![0, 1, 2, 3, 4]);
    assert_eq!(ring.pop_batch(100, &mut out), 0);
}

#[test]
fn t15_pop_batch_limited_by_max_param()
{
    let ring = SpmcRingBufferFifo::new(8);
    let mut vals: Vec<u32> = (0..8).collect();
    assert_eq!(ring.push_batch(8, &mut vals), 8);

    let mut out = Vec::new();
    assert_eq!(ring.pop_batch(3, &mut out), 3);
    assert_eq!(out, vec![0, 1, 2]);
    assert_eq!(ring.pop_batch(3, &mut out), 3);
    assert_eq!(out, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn t16_push_batch_and_pop_batch_still_correct_on_wraparound()
{
    let ring = SpmcRingBufferFifo::new(4);
    for round in 0..20u32
    {
        let mut vals: Vec<u32> = (0..3).map(|i| round * 3 + i).collect();
        assert_eq!(ring.push_batch(3, &mut vals), 3);

        let mut out = Vec::new();
        assert_eq!(ring.pop_batch(3, &mut out), 3);
        assert_eq!(out, (0..3).map(|i| round * 3 + i).collect::<Vec<_>>());
    }
}

// --- one producer, many consumers pushing/popping concurrently (the intended SPMC model) ---
//
// Only one thread ever calls `push`, but multiple threads call `pop`/`pop_batch` at the same
// time, matching how `worker_pool` is meant to access the ring through `HeapPtr::as_ref_mut`
// from several threads (bypassing the borrow checker with a raw pointer). This test recreates
// that same access pattern to check that the CAS on `head` never loses or duplicates elements
// when consumers race each other.
#[cfg(not(loom))]
#[test]
fn t17_one_producer_many_consumers_no_loss_no_duplication()
{
    use crate::sync::AtomicBool;
    use crate::sync::Ordering::{Acquire as SyncAcquire, Release as SyncRelease};
    use crate::utils::backoff::Backoff;

    const CAP: usize = 64;
    const CONSUMERS: u32 = 4;
    const TOTAL: u32 = 20_000;
    const CHUNK: usize = 16;

    #[derive(Clone, Copy)]
    struct RawPtr(*mut SpmcRingBufferFifo<u32>);
    unsafe impl Send for RawPtr {}
    unsafe impl Sync for RawPtr {}

    let mut boxed = Box::new(SpmcRingBufferFifo::<u32>::new(CAP));
    let raw = RawPtr(boxed.as_mut() as *mut _);
    let done = AtomicBool::new(false);

    let collected = std::thread::scope(|scope| {
        let consumers: Vec<_> = (0..CONSUMERS)
            .map(|_| {
                let done = &done;
                scope.spawn(move || {
                    let raw = raw; // force the closure to capture the whole `RawPtr`, not just the `.0` field
                    let ring: &mut SpmcRingBufferFifo<u32> = unsafe { &mut *raw.0 };
                    let mut out = Vec::new();
                    let mut backoff = Backoff::new();
                    loop
                    {
                        let mut chunk = Vec::new();
                        if ring.pop_batch(CHUNK, &mut chunk) == 0
                        {
                            if done.load(SyncAcquire)
                            {
                                break out;
                            }
                            backoff.snooze();
                        }
                        else
                        {
                            backoff.reset();
                            out.extend(chunk);
                        }
                    }
                })
            })
            .collect();

        let producer = scope.spawn(move || {
            let raw = raw; // same as above: force capturing the whole `RawPtr`
            let ring: &mut SpmcRingBufferFifo<u32> = unsafe { &mut *raw.0 };
            let mut backoff = Backoff::new();
            for val in 0..TOTAL
            {
                loop
                {
                    match ring.push(val)
                    {
                        Ok(()) =>
                        {
                            backoff.reset();
                            break;
                        }
                        Err(_) => backoff.snooze(),
                    }
                }
            }
        });
        producer.join().unwrap();
        // the producer has already pushed all TOTAL elements by this line, so it's safe to flag
        // done here: a consumer seeing done=true together with an empty pop means the ring is
        // truly drained, not just momentarily empty.
        done.store(true, SyncRelease);

        let mut all = Vec::new();
        for c in consumers
        {
            all.extend(c.join().unwrap());
        }
        all
    });

    let mut collected = collected;
    collected.sort_unstable();
    assert_eq!(
        collected.len(),
        TOTAL as usize,
        "total element count must match: lost or duplicated means it's broken"
    );
    assert!(
        collected.iter().copied().eq(0..TOTAL),
        "must be exactly the sequence 0..TOTAL, each number once"
    );
}
