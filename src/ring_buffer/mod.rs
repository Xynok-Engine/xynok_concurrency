//! Ring buffer không khoá: **một người ghi, nhiều người đọc**.
//!
//! ```text
//!            steal          real                      tail
//!              │              │                         │
//!              ▼              ▼                         ▼
//!   [ ][ ][ ][ X  X  X ][ A  B  C  D  E ][ ][ ][ ][ ][ ][ ]
//!             └đang bê ┘└ chưa ai nhận ┘
//! ```
//!
//! - [`Producer`] ghi vào `tail`. **Không CAS, không giành** — nó là thread duy nhất ghi `tail`.
//! - [`Consumer`] bốc ở `real`. Sao chép được, nhiều thread cùng bốc là chuyện bình thường.
//! - [`Producer`] **cũng** lấy ở `real` — cùng đầu với [`Consumer`], phân xử bằng một `compare_exchange`.
//!
//! # Vì sao chủ lấy ở đầu chứ không ở cuối
//!
//! Cách kia (chủ lấy ở `tail`, LIFO — giao thức Chase-Lev) cho chủ một đường `pop` gần như miễn
//! phí, nhưng chỉ đúng khi kẻ trộm bốc **đúng một** job mỗi lần. Ghép "chủ lấy ở đuôi" với "trộm
//! bốc cả lô" thì vùng chủ nhắm và vùng trộm nhắm có thể chồng lên nhau, và không có chỗ nào để
//! phân xử: `tail` chỉ mình chủ ghi, `head` chỉ mình trộm ghi, không lệnh nguyên tử nào nhìn cả
//! hai. Hàng rào `SeqCst` cứu được ca một-job, không cứu được ca một-vùng.
//!
//! Cho cả hai bên cùng lấy ở `real` thì mọi lần lấy — một job hay một lô — đều là **một lần CAS
//! trên `head`**, và ai thắng thì rõ ràng. Đổi lại chủ mất tính LIFO (job vừa đặt không còn nóng
//! trong cache) và mỗi lần `pop` phải trả một CAS.
//!
//! Món lời kèm theo mới là thứ đáng giá nhất: **cả ba chỉ số chỉ tăng, không bao giờ lùi.** Chase-
//! Lev phải hạ `tail` xuống một cách đầu cơ rồi trả về chỗ cũ, và mọi lập luận về `wrapping_sub`
//! đều phải mang theo cái ngoại lệ đó. Ở đây thì không: `steal ≤ real ≤ tail` là bất biến toàn cục,
//! đọc lệch nhịp cũng chỉ ra một con số **cũ hơn**, không bao giờ ra một con số vô nghĩa.
//!
//! # Ba chỉ số
//!
//! `steal` và `real` là hai nửa của **cùng một** `AtomicU64` (xem [`pack`]). Gộp lại vì kẻ trộm cần
//! vừa kiểm tra "có ai đang bê không" vừa nhận vùng trong đúng một thao tác nguyên tử — mà phần
//! cứng không có lệnh CAS hai ô.
//!
//! | chỉ số  | nghĩa                                   | ai đẩy lên                                    |
//! |---------|-----------------------------------------|-----------------------------------------------|
//! | `steal` | ô đầu của vùng **đang bị bê đi**        | kẻ trộm, lúc chép xong                        |
//! | `real`  | ô đầu **chưa ai nhận**                  | kẻ trộm lúc nhận vùng, và chủ lúc `pop`       |
//! | `tail`  | ô trống kế tiếp                          | chỉ mình chủ, lúc `push`                      |
//!
//! `steal == real` nghĩa là không ai đang bê. Khác nhau thì `[steal, real)` là tấm biển "đang thi
//! công": chủ **không được ghi đè** lên vùng đó, và kẻ trộm khác phải đi tìm chỗ khác.
//!
//! src: <https://docs.kernel.org/next/core-api/circular-buffers.html>
//! https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/scheduler/multi_thread/queue.rs

use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicU32, AtomicU64, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::{pack, unpack};
use std::mem::MaybeUninit;

pub mod owner;
pub mod thief;

pub use owner::Producer;
pub use thief::Consumer;

pub struct RingBuffer<T>
{
    /// Nửa cao là `steal`, nửa thấp là `real` — xem [`pack`].
    head:  CachePadded<AtomicU64>,
    /// Ô trống kế tiếp. Chỉ [`Producer`] ghi, mọi người khác chỉ đọc.
    tail:  CachePadded<AtomicU32>,
    /// Cấp phát đúng một lần lúc dựng, không bao giờ dời chỗ. `MaybeUninit` vì ô rỗng không chứa
    /// gì, và giá trị được **chuyển ra** khi lấy chứ không phải mượn.
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// `capacity - 1`. Sức chứa luôn là lũy thừa của 2 nên `idx & mask` thay được `idx % cap`.
    mask:  u32,
}

/// `Sync` chỉ cần `T: Send`: cấu trúc này **chuyển** `T` giữa các thread, không bao giờ cho hai
/// thread cùng nhìn một `&T`. Giống hệt một channel.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T>
{
    /// `total_slots` được làm tròn **lên** lũy thừa của 2 gần nhất, tối thiểu 2.
    ///
    /// Trần `1 << 31` không phải cho tròn số: mọi phép so sánh chỉ số đều là số học wrapping trên
    /// `u32`, và hiệu hai chỉ số chỉ còn diễn giải được một cách duy nhất khi khoảng cách giữa
    /// chúng không bao giờ chạm nửa vòng.
    #[track_caller]
    pub fn new(total_slots: u32) -> Self
    {
        // Kiểm tra **trước** khi làm tròn: `next_power_of_two` của một số lớn hơn `1 << 31` tự nó
        // đã tràn, và lúc đó không còn gì để mà kiểm tra nữa.
        assert!(
            total_slots <= 1 << 31,
            "total_slots {total_slots} exceeds the 2^31 limit for wrapping u32 indices"
        );
        let total_slots = total_slots.next_power_of_two().max(2);

        let slots = (0..total_slots)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            head: CachePadded::new(AtomicU64::new(pack(0, 0))),
            tail: CachePadded::new(AtomicU32::new(0)),
            slots,
            mask: total_slots - 1,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.mask as usize + 1
    }

    /// Tách thành hai quyền.
    ///
    /// Nhận `&mut self` chứ không phải `&self`, và đó là một ràng buộc chứ không phải cho đẹp: cả
    /// [`Producer`] lẫn [`Consumer`] trả về đều mượn `self` suốt vòng đời của chúng, nên trình biên
    /// dịch **không cho** gọi hàm này lần thứ hai khi chúng còn sống. Đúng một `Producer` tại một
    /// thời điểm chính là toàn bộ nền móng để `push` không cần CAS.
    ///
    /// Chia cho nhiều thread bằng [`std::thread::scope`]; cần `Arc` thì xem
    /// [`consumer`](Self::consumer) và [`producer`](Self::producer).
    #[inline]
    pub fn split(&mut self) -> (Producer<'_, T>, Consumer<'_, T>)
    {
        let this = &*self;
        (Producer::new(this), Consumer::new(this))
    }

    /// Một quyền đọc, chỉ cần `&self` — cho những chỗ ring buffer nằm sau một `Arc`.
    ///
    /// An toàn, không kèm điều kiện gì: `head` lo hết phần giành giật, nên bao nhiêu [`Consumer`]
    /// cùng sống trên một ring buffer cũng được. Đó là đường chạy bình thường của work-stealing.
    #[inline]
    pub fn consumer(&self) -> Consumer<'_, T>
    {
        Consumer::new(self)
    }

    /// Quyền ghi, chỉ cần `&self`.
    ///
    /// Tách khỏi [`consumer`](Self::consumer) chứ không trả về cả cặp, vì `unsafe` chỉ dính vào
    /// nửa này: hầu hết chỗ gọi chỉ cần một trong hai, và không nên phải viết `unsafe` để lấy nửa
    /// vốn an toàn.
    ///
    /// # Safety
    ///
    /// Chỗ gọi phải tự bảo đảm **không bao giờ có hai [`Producer`] cùng sống** trên một ring buffer. Hai
    /// `Producer` là hai thread cùng ghi `tail`, mà `tail` được ghi bằng một `store` trần không
    /// CAS — mất job và ghi đè lên nhau, im lặng.
    #[inline]
    pub unsafe fn producer(&self) -> Producer<'_, T>
    {
        Producer::new(self)
    }

    /// Số ô đang **bị chiếm**, kể cả vùng một kẻ trộm đang bê dở. Đây là con số mà phép kiểm tra
    /// đầy của [`Producer::push`] dùng.
    ///
    /// Tên là `occupied` chứ không phải `len`, và đó là chủ ý: `len` sẽ ngụ ý
    /// `is_empty() == (len() == 0)` theo lệ của Rust, mà ở đây điều đó **sai** — một kẻ trộm đang
    /// bê cả ring buffer thì `occupied()` bằng sức chứa trong khi [`is_empty`](Self::is_empty) vẫn `true`.
    /// Hai con số trả lời hai câu hỏi khác nhau: `occupied` là "còn chỗ trống không" (cho `push`),
    /// [`available`](Self::available) là "còn gì để lấy không" (cho `pop` và `steal`).
    ///
    /// Xấp xỉ — đọc xong là số đã cũ. Thứ tự hai lần đọc là cố ý: `head` **trước**, `tail` sau. Cả
    /// hai chỉ số chỉ tiến, nên `tail` đọc sau luôn ≥ `steal` đọc trước và hiệu không bao giờ âm.
    /// Đọc ngược lại thì `steal` có thể đã vượt qua `tail` cũ, và `wrapping_sub` trả về một con số
    /// khổng lồ.
    #[inline]
    pub fn occupied(&self) -> usize
    {
        let (steal, _) = unpack(self.head.load(Ordering::Acquire));
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(steal) as usize
    }

    /// Số job **còn lấy được** — không tính vùng đang bị bê. Đây là con số để quyết định "ring buffer này
    /// có đáng trộm không".
    #[inline]
    pub fn available(&self) -> usize
    {
        let (_, real) = unpack(self.head.load(Ordering::Acquire));
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(real) as usize
    }

    /// Không còn gì để lấy. Cặp với [`available`](Self::available), **không** với
    /// [`occupied`](Self::occupied) — xem giải thích ở đó.
    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.available() == 0
    }

    /// Ô ứng với một chỉ số. Chỉ số chạy vô hạn, ô thì quay vòng — `tail` và `real` **không bao
    /// giờ** bị mask, chỉ chỗ này mới mask.
    #[inline]
    fn slot(&self, idx: u32) -> &UnsafeCell<MaybeUninit<T>>
    {
        &self.slots[(idx & self.mask) as usize]
    }

    /// Chuyển `n` giá trị từ `[start, start + n)` ra `out`.
    ///
    /// # Safety
    ///
    /// Chỗ gọi phải đã nhận vùng `[start, start + n)` bằng một lần CAS thành công trên `head`, và
    /// vùng đó chưa được đọc ra lần nào. Đọc hai lần là double-free.
    #[inline]
    unsafe fn drain_claimed(&self, start: u32, n: u32, out: &mut Vec<T>)
    {
        // Xin chỗ **trước** vòng chép: sau khi đã nhận vùng thì mọi giá trị trong đó chỉ còn mình
        // ta giữ, và một cú panic giữa chừng là mất trắng chúng. `push` vào một `Vec` đã đủ chỗ
        // thì không cấp phát, nên không panic.
        out.reserve(n as usize);
        for offset in 0..n
        {
            let val = self.slot(start.wrapping_add(offset)).with(|p| unsafe { (*p).assume_init_read() });
            out.push(val);
        }
    }
}

impl<T> Drop for RingBuffer<T>
{
    /// `MaybeUninit` không tự biết ô nào có gì, nên phải đi từ `real` tới `tail` và
    /// `drop_in_place` từng ô. Bỏ bước này thì mọi job chưa chạy đều rò rỉ.
    ///
    /// Mốc dưới là `real`, **không** phải `steal`. `[steal, real)` là vùng đã có chủ: hoặc một kẻ
    /// trộm đang chép nó ra, hoặc [`Producer::pop`] đã bê nó đi rồi mà `steal` chưa kịp đuổi theo.
    /// Cả hai trường hợp, giá trị **đã được chuyển ra khỏi ô** — thả thêm lần nữa là double-free.
    ///
    /// `&mut self` nghĩa là mọi [`Producer`] và [`Consumer`] đều đã hết vòng đời, nên không thể còn
    /// ai đang bê dở. Đường duy nhất tới đây với `steal != real` là một cú panic giữa lúc chép, và
    /// khi đó rò rỉ vài job là cái giá đúng để trả.
    fn drop(&mut self)
    {
        // Có `&mut self` nên không còn thread nào chạm vào nữa; `Relaxed` là đủ.
        let (_, real) = unpack(self.head.load(Ordering::Relaxed));
        let tail = self.tail.load(Ordering::Relaxed);

        let mut idx = real;
        while idx != tail
        {
            self.slot(idx).with_mut(|p| unsafe { (*p).assume_init_drop() });
            idx = idx.wrapping_add(1);
        }
    }
}

impl<T> std::fmt::Debug for RingBuffer<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        let (steal, real) = unpack(self.head.load(Ordering::Relaxed));
        f.debug_struct("RingBuffer")
            .field("capacity", &self.capacity())
            .field("steal", &steal)
            .field("real", &real)
            .field("tail", &self.tail.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
