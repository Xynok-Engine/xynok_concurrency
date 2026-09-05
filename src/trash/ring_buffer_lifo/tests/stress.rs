use crate::ring_buffer_lifo::{RingBufferLifo, Steal};
use crate::sync::{AtomicBool, Ordering};
use crate::utils::backoff::Backoff;

#[test]
fn t0_mot_nguoi_ghi_ba_ke_trom_khong_mat_khong_nhan_doi()
{
    #[cfg(not(miri))]
    const TOTAL: u32 = 20_000;
    #[cfg(miri)]
    const TOTAL: u32 = 240;

    #[cfg(not(miri))]
    const THIEVES: usize = 3;
    #[cfg(miri)]
    const THIEVES: usize = 2;

    #[cfg(not(miri))]
    const CAP: u32 = 64;
    #[cfg(miri)]
    const CAP: u32 = 8;

    const LOT: usize = 8;

    let mut ring = RingBufferLifo::<u32>::new(CAP);
    let (mut tx, rx) = ring.split();
    let done = AtomicBool::new(false);

    let mut all: Vec<u32> = std::thread::scope(|scope| {
        let thieves: Vec<_> = (0..THIEVES)
            .map(|_| {
                let done = &done;
                scope.spawn(move || {
                    let mut got = Vec::new();
                    let mut backoff = Backoff::new();
                    loop
                    {
                        match rx.try_steal()
                        {
                            Steal::Success(val) =>
                            {
                                got.push(val);
                                backoff = Backoff::new();
                            }
                            Steal::Busy => backoff.snooze(),
                            Steal::Empty =>
                            {
                                if done.load(Ordering::Acquire) && rx.is_empty()
                                {
                                    break got;
                                }
                                backoff.snooze();
                            }
                        }
                    }
                })
            })
            .collect();

        let mut mine = Vec::new();
        let mut pending = Vec::with_capacity(LOT);
        let mut backoff = Backoff::new();

        for i in 0..TOTAL
        {
            pending.push(i);
            if pending.len() < LOT
            {
                continue;
            }
            while !pending.is_empty()
            {
                if tx.push_batch(&mut pending) == 0 && tx.pop_batch(&mut mine, LOT) == 0
                {
                    backoff.snooze();
                    continue;
                }
                backoff = Backoff::new();
            }
        }
        while !pending.is_empty()
        {
            if tx.push_batch(&mut pending) == 0 && tx.pop_batch(&mut mine, LOT) == 0
            {
                backoff.snooze();
            }
        }
        done.store(true, Ordering::Release);

        while tx.pop_batch(&mut mine, LOT) > 0
        {}

        for thief in thieves
        {
            mine.extend(thief.join().unwrap());
        }
        mine
    });

    all.sort_unstable();
    assert_eq!(all.len(), TOTAL as usize, "tổng số job phải khớp: mất hoặc nhân đôi là hỏng");
    assert!(all.iter().copied().eq(0..TOTAL), "phải là đúng dãy 0..TOTAL, mỗi số một lần");
}

#[test]
fn t1_chu_va_trom_gianh_job_cuoi_cung()
{
    #[cfg(not(miri))]
    const ROUNDS: u32 = 20_000;
    #[cfg(miri)]
    const ROUNDS: u32 = 200;

    let mut ring = RingBufferLifo::<u32>::new(2);
    let (mut tx, rx) = ring.split();
    let done = AtomicBool::new(false);

    let total = std::thread::scope(|scope| {
        let done = &done;
        let thief = scope.spawn(move || {
            let mut taken = 0u32;
            let mut backoff = Backoff::new();
            loop
            {
                match rx.try_steal()
                {
                    Steal::Success(_) =>
                    {
                        taken += 1;
                        backoff = Backoff::new();
                    }
                    _ =>
                    {
                        if done.load(Ordering::Acquire) && rx.is_empty()
                        {
                            break taken;
                        }
                        backoff.snooze();
                    }
                }
            }
        });

        let mut mine = 0u32;
        for i in 0..ROUNDS
        {
            while tx.push(i).is_err()
            {
                if tx.pop().is_some()
                {
                    mine += 1;
                }
            }
            if tx.pop().is_some()
            {
                mine += 1;
            }
        }
        while tx.pop().is_some()
        {
            mine += 1;
        }
        done.store(true, Ordering::Release);

        mine + thief.join().unwrap()
    });

    assert_eq!(total, ROUNDS, "mỗi job phải được đúng một bên lấy");
}

#[test]
fn t2_ring_bon_o_hai_ke_trom_khong_dam_du_lieu()
{
    const TOTAL: u32 = 96;
    const THIEVES: usize = 2;
    const CAP: u32 = 4;
    const LOT: usize = 4;

    let mut ring = RingBufferLifo::<u32>::new(CAP);
    let (mut tx, rx) = ring.split();
    let done = AtomicBool::new(false);

    let mut all: Vec<u32> = std::thread::scope(|scope| {
        let thieves: Vec<_> = (0..THIEVES)
            .map(|_| {
                let done = &done;
                scope.spawn(move || {
                    let mut got = Vec::new();
                    loop
                    {
                        match rx.try_steal()
                        {
                            Steal::Success(val) => got.push(val),
                            Steal::Busy => std::thread::yield_now(),
                            Steal::Empty =>
                            {
                                if done.load(Ordering::Acquire) && rx.is_empty()
                                {
                                    break got;
                                }
                                std::thread::yield_now();
                            }
                        }
                    }
                })
            })
            .collect();

        let mut mine = Vec::new();
        let mut pending = Vec::with_capacity(LOT);
        for i in 0..TOTAL
        {
            pending.push(i);
            if pending.len() < LOT
            {
                continue;
            }
            while !pending.is_empty()
            {
                if tx.push_batch(&mut pending) == 0 && tx.pop_batch(&mut mine, LOT) == 0
                {
                    std::thread::yield_now();
                }
            }
        }
        while !pending.is_empty()
        {
            if tx.push_batch(&mut pending) == 0 && tx.pop_batch(&mut mine, LOT) == 0
            {
                std::thread::yield_now();
            }
        }
        done.store(true, Ordering::Release);
        while tx.pop_batch(&mut mine, LOT) > 0
        {}
        for thief in thieves
        {
            mine.extend(thief.join().unwrap());
        }
        mine
    });

    all.sort_unstable();
    assert!(all.iter().copied().eq(0..TOTAL), "mất hoặc nhân đôi job");
}
