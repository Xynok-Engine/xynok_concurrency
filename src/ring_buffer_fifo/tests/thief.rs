use crate::ring_buffer_fifo::{RingBufferFifo, Steal};
use crate::sync::Ordering;
use crate::utils::bits::unpack;

#[test]
fn t0_go_bien_keo_steal_len_bang_real_hien_tai()
{
    let ring = RingBufferFifo::<u32>::new(8);
    let mut tx = unsafe { ring.producer() };
    let rx = ring.consumer();

    let mut vals: Vec<u32> = (0..4).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);

    let (start, n) = rx.claim(2).success().expect("ring rỗi thì phải nhận được");
    assert_eq!((start, n), (0, 2));

    let mut mine = Vec::new();
    assert_eq!(tx.pop_batch(&mut mine, 2), 2);
    assert_eq!(mine, vec![2, 3]);
    assert_eq!(unpack(ring.head.load(Ordering::Relaxed)), (0, 4));

    let mut got = Vec::new();
    unsafe { ring.drain_claimed_with(start, n, |val| got.push(val)) };
    rx.release();

    assert_eq!(got, vec![0, 1]);
    let (steal, real) = unpack(ring.head.load(Ordering::Relaxed));
    assert_eq!(steal, real);
    assert_eq!(tx.remaining(), 8);
    assert_eq!(rx.claim(1), Steal::Empty);
}

#[test]
fn t1_claim_khong_nhan_gi_khi_max_bang_khong()
{
    let ring = RingBufferFifo::<u32>::new(4);
    let mut tx = unsafe { ring.producer() };
    let rx = ring.consumer();

    assert_eq!(tx.push_iter(0..2u32), 2);
    assert_eq!(rx.claim(0), Steal::Busy);
    assert_eq!(rx.available(), 2);
}
