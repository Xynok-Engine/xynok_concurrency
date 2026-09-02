use crate::sync::{Arc, AtomicUsize, Ordering};
use crate::utils::inline_fn::InlineFn;
use crate::utils::inline_fn::consts::INLINE_BYTES;
use crate::utils::inline_fn::fn_buffer::FnBuffer;
use crate::utils::inline_fn::v_table::VTable;
use crate::utils::queue_batching::QueueBatching;

fn empty_fn() {}
fn params_fn(_a: u64) {}
fn params_fn2(_a: u64, _b: u64) {}

#[test]
fn t0_size_check()
{
    println!("size_of_val(&empty_fn):   {} bytes (fn item, ZST)", size_of_val(&empty_fn));
    println!("size_of_val(&params_fn):  {} bytes (fn item, ZST)", size_of_val(&params_fn));
    println!("size_of_val(&params_fn2): {} bytes (fn item, ZST)", size_of_val(&params_fn2));

    let p0: fn() = empty_fn;
    let p1: fn(u64) = params_fn;
    let p2: fn(u64, u64) = params_fn2;
    let p3 = || {
        let x: u64 = 10;
        let y: [u64; 20] = [1u64; 20];
        for i in y
        {
            println!("x {}: - i:{}", x, i);
        }
    };
    println!("size_of::<fn()>():        {} bytes (fn pointer)", size_of_val(&p0));
    println!("size_of::<fn(u64)>():     {} bytes (fn pointer)", size_of_val(&p1));
    println!("size_of::<fn(u64,u64)>(): {} bytes (fn pointer)", size_of_val(&p2));
    println!("size_of::<fn_closure>():  {} bytes (fn pointer)", size_of_val(&p3));

    println!("size of FnBuffer: {} bytes", size_of::<FnBuffer>());
    println!("size of VTable:   {} bytes", size_of::<VTable>());
}

#[test]
fn t1_mot_cap_cache_line()
{
    assert_eq!(size_of::<InlineFn>(), 64);
    assert_eq!(align_of::<InlineFn>(), 16);

    assert_eq!(align_of::<FnBuffer>() + INLINE_BYTES, 64, "vtable + đệm + buffer phải lấp kín 64 byte");
}

#[test]
fn t2_closure_nho_khong_vao_heap()
{
    assert!(InlineFn::is_fit::<[u8; INLINE_BYTES]>());
    assert!(!InlineFn::is_fit::<[u8; INLINE_BYTES + 1]>());

    let id = 7u32;
    let closure = move || assert_eq!(id, 7);
    assert!(
        size_of_val(&closure) <= INLINE_BYTES,
        "closure bắt một u32 mà không nằm tại chỗ thì hỏng hết ý nghĩa"
    );
    InlineFn::new(closure).run_once();
}

#[test]
fn t3_closure_to_van_chay_dung()
{
    let big = [9u64; 32];
    let job = InlineFn::new(move || assert_eq!(big[31], 9));
    job.run_once();
}

#[test]
fn t4_tha_ma_khong_chay()
{
    struct Bomb(Arc<AtomicUsize>);

    impl Drop for Bomb
    {
        fn drop(&mut self)
        {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let ran = Arc::new(AtomicUsize::new(0));

    for big in [false, true]
    {
        let bomb = Bomb(Arc::clone(&dropped));
        let ran = Arc::clone(&ran);
        let padding = [0u8; INLINE_BYTES];

        let job = match big
        {
            false => InlineFn::new(move || {
                let _ = &bomb;
                ran.fetch_add(1, Ordering::Relaxed);
            }),
            true => InlineFn::new(move || {
                let _ = (&bomb, &padding);
                ran.fetch_add(1, Ordering::Relaxed);
            }),
        };
        drop(job);
    }

    assert_eq!(dropped.load(Ordering::Relaxed), 2, "closure bị thả phải kéo theo mọi thứ nó bắt");
    assert_eq!(ran.load(Ordering::Relaxed), 0, "thả thì không được chạy");
}

#[test]
fn t5_chay_qua_queue_batching()
{
    let counter = Arc::new(AtomicUsize::new(0));
    let queue = QueueBatching::new();

    queue.push_batch((0..100).map(|i| {
        let counter = Arc::clone(&counter);
        InlineFn::new(move || {
            counter.fetch_add(i, Ordering::Relaxed);
        })
    }));

    let mut ran = 0;
    let mut batch = Vec::new();
    while queue.pop_batch(&mut batch, 16) > 0
    {
        for job in batch.drain(..)
        {
            job.run_once();
            ran += 1;
        }
    }

    assert_eq!(ran, 100, "mọi job đẩy vào đều phải được rút ra và chạy đúng một lần");
    assert_eq!(counter.load(Ordering::Relaxed), (0..100).sum::<usize>());
}

/// Ring là nơi job thật sự sống trên đường nóng, nên nó phải chạy được nguyên vẹn qua đó, kể
/// cả khi job đi vòng qua tay một kẻ trộm thay vì qua tay chủ ring.
#[test]
fn t6_chay_qua_ring_fifo()
{
    use crate::ring_buffer_fifo::RingBufferFifo;

    let counter = Arc::new(AtomicUsize::new(0));
    let mut ring: RingBufferFifo<InlineFn> = RingBufferFifo::new(128);
    let (mut owner, thief) = ring.split();

    for i in 0..100
    {
        let counter = Arc::clone(&counter);
        owner
            .push(InlineFn::new(move || {
                counter.fetch_add(i, Ordering::Relaxed);
            }))
            .expect("ring 128 ô mà 100 job đã đầy thì có gì đó sai");
    }

    // Nửa đầu do chủ tự lấy, nửa sau bị trộm: hai đường đọc khác nhau vào cùng một ô.
    let mut ran = 0;
    for _ in 0..50
    {
        owner.pop().expect("chủ không lấy được job đã đẩy vào").run_once();
        ran += 1;
    }
    let mut stolen = Vec::new();
    thief.steal_batch(&mut stolen, 100);
    for job in stolen.drain(..)
    {
        job.run_once();
        ran += 1;
    }

    assert_eq!(ran, 100, "job đi qua ring phải chạy đúng một lần");
    assert_eq!(counter.load(Ordering::Relaxed), (0..100).sum::<usize>());
}

/// Pool bị tắt khi ring còn job là chuyện thường. Job chưa chạy vẫn phải được thả sạch, không
/// thì mọi thứ nó bắt giữ (texture handle, `Arc` tới scene) rò ra ngoài.
#[test]
fn t7_ring_drop_keo_theo_job_chua_chay()
{
    use crate::ring_buffer_fifo::RingBufferFifo;

    let alive = Arc::new(AtomicUsize::new(0));
    {
        let mut ring: RingBufferFifo<InlineFn> = RingBufferFifo::new(64);
        let (mut owner, _) = ring.split();
        for _ in 0..50
        {
            let alive = Arc::clone(&alive);
            let _ = owner.push(InlineFn::new(move || {
                alive.fetch_sub(1, Ordering::Relaxed);
            }));
        }
    }

    assert_eq!(Arc::strong_count(&alive), 1, "ring bị thả mà job trong đó không được thả theo");
}

#[test]
fn t8_queue_drop_keo_theo_job_chua_chay()
{
    let alive = Arc::new(AtomicUsize::new(0));

    {
        let queue = QueueBatching::new();
        queue.push_batch((0..50).map(|_| {
            let alive = Arc::clone(&alive);
            alive.fetch_add(1, Ordering::Relaxed);
            InlineFn::new(move || {
                alive.fetch_sub(1, Ordering::Relaxed);
            })
        }));
    }

    // `Arc` trong closure bị thả theo job, nên strong count về 1 (chỉ còn `alive` ở đây).
    assert_eq!(Arc::strong_count(&alive), 1, "job chưa chạy vẫn phải được thả sạch");
}
