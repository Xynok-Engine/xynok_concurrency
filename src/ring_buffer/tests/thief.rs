//! Test riêng cho [`Consumer`]. Phải là module con của `thief` để gọi được `claim` và
//! `release` — hai nhịp mà API công khai không tách rời ra được.

use super::*;
use crate::ring_buffer::RingBuffer;

/// Bảo hiểm cho một cái bẫy mà không stress test nào chỉ ra được nguyên nhân: trong lúc kẻ trộm
/// đang chép, chủ ring buffer lấy tiếp và đẩy `real` đi xa hơn mốc cuối vùng đã nhận. Gỡ biển bằng mốc
/// cũ thì `steal != real` **vĩnh viễn** — ring buffer còn trống nhưng `push` tưởng đầy và mọi kẻ trộm
/// sau đó đều tưởng có người đang bê. Không panic, không lỗi, chỉ là một ring buffer chết lặng.
#[test]
fn go_bien_keo_steal_len_bang_real_hien_tai()
{
    let ring = RingBuffer::<u32>::new(8);
    // SAFETY: đúng một `Producer`, và test này lái tay từng nhịp nên cần cả hai quyền cùng lúc.
    let mut tx = unsafe { ring.producer() };
    let rx = ring.consumer();

    let mut vals: Vec<u32> = (0..4).collect();
    assert_eq!(tx.push_batch(&mut vals), 4);

    // Nhịp 1: kẻ trộm dán biển lên [0, 2).
    let (start, n) = rx.claim(2).expect("ring buffer đang rỗi thì phải nhận được");
    assert_eq!((start, n), (0, 2));

    // Chủ ring buffer lấy nốt [2, 4) trong lúc biển còn treo — `real` vượt qua mốc cuối vùng đã nhận.
    let mut mine = Vec::new();
    assert_eq!(tx.pop_batch(&mut mine, 2), 2);
    assert_eq!(mine, vec![2, 3]);
    assert_eq!(unpack(ring.head.load(Ordering::Relaxed)), (0, 4));

    // Nhịp 2: chép xong rồi mới gỡ biển.
    let mut got = Vec::new();
    unsafe { ring.drain_claimed(start, n, &mut got) };
    rx.release();

    assert_eq!(got, vec![0, 1]);
    let (steal, real) = unpack(ring.head.load(Ordering::Relaxed));
    assert_eq!(steal, real, "gỡ biển phải kéo `steal` lên bằng `real` hiện tại, không phải mốc cũ");
    assert_eq!(tx.remaining(), 8, "ring buffer phải trống hẳn trở lại");
    assert_eq!(rx.claim(1), None, "rỗng thì không nhận được gì, nhưng vì rỗng chứ không phải vì kẹt");
}
