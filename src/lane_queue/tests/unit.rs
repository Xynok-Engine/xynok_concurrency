use crate::lane_queue::LaneQueue;

use std::sync::Arc;

use crate::ring_buffer_fifo::RingBufferFifo;
use crate::ring_buffer_lifo::RingBufferLifo;

#[test]
fn t0_push_pop_giu_dung_thu_tu_va_do_dai()
{
    let lane_queue = LaneQueue::new();
    assert!(lane_queue.is_empty());
    assert_eq!(lane_queue.pop(), None, "hàng đợi rỗng mà lấy ra được thì có gì đó rất sai");

    for i in 0..5
    {
        lane_queue.push(i);
    }
    assert_eq!(lane_queue.len(), 5);

    // FIFO: job vào trước ra trước, vì job trong hàng đợi thường già hơn và nên chạy trước.
    for i in 0..5
    {
        assert_eq!(lane_queue.pop(), Some(i));
    }
    assert!(lane_queue.is_empty());
}

#[test]
fn t1_steal_batch_ton_trong_max()
{
    let lane_queue = LaneQueue::new();
    lane_queue.push_batch(0..100);
    assert_eq!(lane_queue.len(), 100);

    let mut out = Vec::new();
    assert_eq!(lane_queue.steal_batch(&mut out, 30), 30);
    assert_eq!(lane_queue.len(), 70);
    assert_eq!(out, (0..30).collect::<Vec<_>>());

    assert_eq!(lane_queue.steal_batch(&mut out, 0), 0, "max = 0 thì không được chạm vào khoá");
    assert_eq!(lane_queue.drain_into(&mut out), 70);
    assert!(lane_queue.is_empty());
    assert_eq!(out.len(), 100);
}

/// Hình dạng chính của đường ra: một job về tay người gọi để chạy ngay, phần còn lại nằm sẵn
/// trong ring local và không phải đụng vào hàng đợi lần nữa.
#[test]
fn t2_steal_batch_and_pop_giu_mot_nap_phan_con_lai_vao_ring()
{
    let lane_queue = LaneQueue::new();
    lane_queue.push_batch(0..100);

    let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(64);
    let (mut owner, _) = ring.split();

    // Job chạy ngay được rút ra trước, còn lại 99. Phần nạp vào ring là 99/10 + 1 = 10, dưới
    // cả hai cái chặn kia.
    let first = lane_queue.steal_batch_and_pop(&mut owner, 10).expect("hàng đợi đang có 100 job");
    assert_eq!(first, 0, "job già nhất phải là job chạy ngay");

    let mut drained = Vec::new();
    assert_eq!(owner.drain(&mut drained), 10);
    assert_eq!(drained, (1..11).collect::<Vec<_>>());
    assert_eq!(lane_queue.len(), 100 - 11, "11 job rời hàng đợi: 1 chạy ngay, 10 vào ring");
}

#[test]
fn t3_khong_bao_gio_nap_qua_nua_ring()
{
    let lane_queue = LaneQueue::new();
    lane_queue.push_batch(0..1000);

    let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(8);
    let (mut owner, _) = ring.split();

    // Một worker, ngàn job: phần chia ra là 1001, nhưng nửa ring mới là cái chặn thật.
    lane_queue.steal_batch_and_pop(&mut owner, 1).expect("hàng đợi đang đầy");

    let mut drained = Vec::new();
    assert_eq!(owner.drain(&mut drained), 4, "phải chừa nửa ring cho job mà chính worker sắp spawn");
}

#[test]
fn t4_khong_nap_qua_cho_trong_con_lai()
{
    let lane_queue = LaneQueue::new();
    lane_queue.push_batch(0..1000);

    let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(8);
    let (mut owner, _) = ring.split();
    for i in 100..107
    {
        owner.push(i).expect("ring 8 ô, đẩy 7 cái phải lọt");
    }
    assert_eq!(owner.remaining(), 1);

    lane_queue.steal_batch_and_pop(&mut owner, 1).expect("hàng đợi đang đầy");

    let mut drained = Vec::new();
    assert_eq!(owner.drain(&mut drained), 8, "7 job cũ cộng đúng 1 job vừa nạp");
    assert_eq!(lane_queue.len(), 1000 - 2);
}

#[test]
fn t5_ring_day_thi_chi_lay_mot_job_chay_ngay()
{
    let lane_queue = LaneQueue::new();
    lane_queue.push_batch(0..50);

    let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(4);
    let (mut owner, _) = ring.split();
    for i in 0..4
    {
        owner.push(i).expect("ring 4 ô");
    }

    let first = lane_queue.steal_batch_and_pop(&mut owner, 1);
    assert_eq!(first, Some(0), "ring hết chỗ vẫn phải có job để chạy ngay");
    assert_eq!(lane_queue.len(), 49);
}

#[test]
fn t6_nap_duoc_vao_ca_ring_lifo()
{
    let lane_queue = LaneQueue::new();
    lane_queue.push_batch(0..100);

    let mut ring: RingBufferLifo<i32> = RingBufferLifo::new(64);
    let (mut owner, _) = ring.split();

    let first = lane_queue.steal_batch_and_pop(&mut owner, 10).expect("hàng đợi đang có 100 job");
    assert_eq!(first, 0);

    let mut drained = Vec::new();
    assert_eq!(owner.drain(&mut drained), 10);
    assert_eq!(lane_queue.len(), 89);
}

#[test]
fn t7_lane_queue_rong_thi_khong_lay_duoc_gi()
{
    let lane_queue: LaneQueue<i32> = LaneQueue::new();
    let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(16);
    let (mut owner, _) = ring.split();

    assert_eq!(lane_queue.steal_batch_and_pop(&mut owner, 4), None);
    assert_eq!(owner.remaining(), 16, "không có gì để lấy thì cũng không được chạm vào ring");
}

/// Trần cứng ở đây là số job cố định chia sẵn cho từng thread. Vòng gom không trần đã từng ăn
/// hết RAM khi cấu trúc bên dưới hỏng, nên mọi thứ ở đây đều đếm được từ trước.
#[test]
fn t8_nhieu_thread_day_vao_khong_mat_job()
{
    const THREADS: usize = 8;
    const PER_THREAD: usize = 2_000;
    const TOTAL: usize = THREADS * PER_THREAD;

    let lane_queue = Arc::new(LaneQueue::new());

    std::thread::scope(|scope| {
        for t in 0..THREADS
        {
            let lane_queue = Arc::clone(&lane_queue);
            scope.spawn(move || {
                for i in 0..PER_THREAD
                {
                    match i % 3
                    {
                        0 => lane_queue.push(t * PER_THREAD + i),
                        _ => lane_queue.push_batch(std::iter::once(t * PER_THREAD + i)),
                    }
                }
            });
        }
    });

    assert_eq!(lane_queue.len(), TOTAL, "bộ đếm không khoá lệch so với số job đã đẩy vào");

    let mut seen = vec![false; TOTAL];
    let mut out = Vec::with_capacity(TOTAL);
    assert_eq!(lane_queue.drain_into(&mut out), TOTAL);
    for value in out
    {
        assert!(!seen[value], "job {value} ra khỏi hàng đợi hai lần");
        seen[value] = true;
    }
    assert!(seen.into_iter().all(|s| s), "có job đẩy vào mà không bao giờ ra");
    assert!(lane_queue.is_empty());
}

/// Nhiều worker cùng rút, mỗi người một ring riêng: không job nào chạy hai lần, không job nào
/// biến mất.
#[test]
fn t9_nhieu_worker_cung_rut_khong_trung_khong_mat()
{
    const WORKERS: usize = 4;
    const TOTAL: usize = 20_000;

    let lane_queue = Arc::new(LaneQueue::new());
    lane_queue.push_batch(0..TOTAL);

    let taken: Vec<Vec<usize>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..WORKERS)
            .map(|_| {
                let lane_queue = Arc::clone(&lane_queue);
                scope.spawn(move || {
                    let mut ring: RingBufferFifo<usize> = RingBufferFifo::new(64);
                    let (mut owner, _) = ring.split();
                    let mut mine = Vec::new();

                    // Trần cứng: nhiều nhất TOTAL vòng, nên một hàng đợi hỏng làm test *fail*
                    // chứ không làm máy hết RAM.
                    for _ in 0..TOTAL
                    {
                        match lane_queue.steal_batch_and_pop(&mut owner, WORKERS)
                        {
                            Some(job) => mine.push(job),
                            None => break,
                        }
                        while let Some(job) = owner.pop()
                        {
                            mine.push(job);
                        }
                    }
                    owner.drain(&mut mine);
                    mine
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().expect("worker panic")).collect()
    });

    assert!(lane_queue.is_empty(), "còn {} job kẹt lại trong hàng đợi", lane_queue.len());

    let mut seen = vec![false; TOTAL];
    for job in taken.into_iter().flatten()
    {
        assert!(!seen[job], "job {job} bị hai worker cùng nhận");
        seen[job] = true;
    }
    assert!(seen.into_iter().all(|s| s), "có job không worker nào nhận");
}
