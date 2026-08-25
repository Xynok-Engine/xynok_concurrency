
use super::*;
use crate::sync::AtomicBool;
use crate::utils::backoff::Backoff;

#[test]
fn mot_nguoi_ghi_ba_ke_trom_khong_mat_khong_nhan_doi()
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

    let mut ring = RingBufferFifo::<u32>::new(CAP);
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
                        match rx.steal_half(&mut got)
                        {
                            0 =>
                            {
                                if done.load(Ordering::Acquire) && rx.is_empty()
                                {
                                    break got;
                                }
                                backoff.snooze();
                            }
                            _ => backoff = Backoff::new(),
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
