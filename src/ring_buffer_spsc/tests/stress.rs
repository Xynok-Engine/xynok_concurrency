use std::time::{Duration, Instant};

use crate::ring_buffer_spsc::RingBufferSpsc;
use crate::sync::{AtomicBool, Ordering as SyncOrdering};

/// Trần cứng cho mọi vòng chờ ở đây. Một ring hỏng thì vòng gom không tự dừng, và nếu nó vừa quay
/// vừa cấp phát thì nó ăn hết RAM trước khi ai kịp nhận ra.
const DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn t0_mot_nguoi_ghi_mot_nguoi_doc_khong_mat_khong_dao_thu_tu()
{
    #[cfg(not(miri))]
    const TOTAL: u32 = 200_000;
    #[cfg(miri)]
    const TOTAL: u32 = 500;

    let mut ring = RingBufferSpsc::<u32>::new(64);
    let (mut tx, mut rx) = ring.split();
    let done = AtomicBool::new(false);

    let received: Vec<u32> = std::thread::scope(|scope| {
        let writer = {
            let done = &done;
            scope.spawn(move || {
                let mut next = 0u32;
                let deadline = Instant::now() + DEADLINE;

                while next < TOTAL
                {
                    assert!(Instant::now() < deadline, "người ghi kẹt ở phần tử {next}");
                    match tx.push(next)
                    {
                        Ok(()) => next += 1,
                        // Ring đầy: nhường một nhịp rồi thử lại. Lane A thật thì bỏ lệnh, còn ở đây
                        // ta muốn đếm đủ để so.
                        Err(_) => std::thread::yield_now(),
                    }
                }
                done.store(true, SyncOrdering::Release);
            })
        };

        let mut got = Vec::with_capacity(TOTAL as usize);
        let deadline = Instant::now() + DEADLINE;

        loop
        {
            assert!(Instant::now() < deadline, "người đọc mới nhận {} phần tử thì kẹt", got.len());

            let taken = rx.drain_with(|val| got.push(val));
            if taken == 0
            {
                if done.load(SyncOrdering::Acquire) && rx.is_empty()
                {
                    break;
                }
                std::thread::yield_now();
            }
        }

        writer.join().expect("người ghi panic");
        got
    });

    assert_eq!(received.len(), TOTAL as usize, "số phần tử nhận được không khớp");
    assert!(
        received.iter().copied().eq(0..TOTAL),
        "SPSC phải giữ nguyên thứ tự: phần tử đầu tiên sai nằm ở {:?}",
        received.iter().copied().zip(0..TOTAL).position(|(got, want)| got != want)
    );
}

#[test]
fn t1_callback_audio_vet_tung_dot_ma_khong_bo_lenh_nao()
{
    #[cfg(not(miri))]
    const BURSTS: u32 = 2_000;
    #[cfg(miri)]
    const BURSTS: u32 = 20;
    const PER_BURST: u32 = 16;

    let mut ring = RingBufferSpsc::<u32>::new(128);
    let (mut tx, mut rx) = ring.split();
    let done = AtomicBool::new(false);

    let total_seen = std::thread::scope(|scope| {
        let writer = {
            let done = &done;
            scope.spawn(move || {
                let deadline = Instant::now() + DEADLINE;
                for burst in 0..BURSTS
                {
                    let mut sent = 0;
                    while sent < PER_BURST
                    {
                        assert!(Instant::now() < deadline, "người ghi kẹt ở đợt {burst}");
                        sent += tx.push_iter((sent..PER_BURST).map(|i| burst * PER_BURST + i)) as u32;
                        if sent < PER_BURST
                        {
                            std::thread::yield_now();
                        }
                    }
                }
                done.store(true, SyncOrdering::Release);
            })
        };

        // Vào vai callback: thức dậy, vét sạch, rồi ngủ lại. Không cấp phát trong lúc vét.
        let mut seen = 0u32;
        let mut expected = 0u32;
        let deadline = Instant::now() + DEADLINE;

        loop
        {
            assert!(Instant::now() < deadline, "callback mới thấy {seen} lệnh thì kẹt");

            let taken = rx.drain_with(|val| {
                assert_eq!(val, expected, "lệnh tới sai thứ tự");
                expected += 1;
            });
            seen += taken as u32;

            if taken == 0
            {
                if done.load(SyncOrdering::Acquire) && rx.is_empty()
                {
                    break;
                }
                std::thread::sleep(Duration::from_micros(50));
            }
        }

        writer.join().expect("người ghi panic");
        seen
    });

    assert_eq!(total_seen, BURSTS * PER_BURST);
}
