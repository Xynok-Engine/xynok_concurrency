//! Stress test: thread thật, job thật. Bắt lỗi thống kê — mất job, nhân đôi job.

use super::*;
use crate::sync::AtomicBool;
use crate::utils::backoff::Backoff;

/// Một người ghi, ba kẻ trộm, hai vạn job. Mỗi job phải xuất hiện **đúng một lần** trong hợp
/// của tất cả các rổ — mất một cái là lỗi công bố, thừa một cái là lỗi phân xử.
#[test]
fn mot_nguoi_ghi_ba_ke_trom_khong_mat_khong_nhan_doi()
{
    // Dưới miri từng lệnh đều được diễn giải, nên mô hình phải bé lại vài trăm lần — vẫn đủ
    // để ring buffer quấn vòng nhiều lượt và ba đường (`push_batch` đầy, `pop_batch`, `steal_half`)
    // đều bị đạp qua, mà chạy xong trong vài giây thay vì vài giờ.
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

    let mut ring = RingBuffer::<u32>::new(CAP);
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
                                // Chỉ được nghỉ khi người ghi đã xong **và** không còn gì để bốc.
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
                // Ring buffer đầy: người ghi tự lấy bớt về cho mình thay vì đứng đợi. Đây đúng là
                // đường "tràn thì tự tiêu thụ" của một pool thật.
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

        // Vét nốt phần chưa ai kịp bốc.
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
