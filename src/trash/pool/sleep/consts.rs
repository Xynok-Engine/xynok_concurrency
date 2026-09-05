/// Bit dành cho `unparked`, và cũng là chỗ `searching` bắt đầu. 16 bit đủ cho 65 535 worker, thừa
/// bốn bậc so với mọi pool có thật, và để lại 32 bit cho bộ đếm sự kiện.
pub(crate) const SLEEPER_BITS: u32 = 16;
/// Một worker thức, ở dạng đã đóng gói.
pub(crate) const ONE_UNPARKED: u64 = 1;
/// Một người đang lùng việc, ở dạng đã đóng gói.
pub(crate) const ONE_SEARCHING: u64 = 1 << SLEEPER_BITS;
/// Một sự kiện "có job mới ở đâu đó", ở dạng đã đóng gói.
pub(crate) const ONE_EVENT: u64 = 1 << (2 * SLEEPER_BITS);
pub(crate) const COUNT_MASK: u64 = (1 << SLEEPER_BITS) - 1;

/// Worker đang chạy hoặc đang lùng việc, không nằm trong danh sách ngủ.
pub(crate) const AWAKE: u32 = 0;
/// Worker đã ghi tên vào danh sách ngủ và sắp `park`, hoặc đang park.
pub(crate) const PARKED: u32 = 1;
/// Có người đã bốc worker này ra khỏi danh sách và gửi `unpark`. Nó phải thức.
pub(crate) const NOTIFIED: u32 = 2;
