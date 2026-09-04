use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::sync::thread::{self, ThreadId};

/// "Thread này chưa thuộc pool nào". Pool thật đánh số từ 1 trở đi.
const NO_POOL: u64 = 0;

/// Cấp một số hiệu chưa ai dùng cho pool mới dựng.
pub fn next_pool_id() -> u64
{
    static NEXT: AtomicU64 = AtomicU64::new(NO_POOL + 1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Chỗ đứng của thread hiện tại: nó là người thứ mấy, của pool nào.
///
/// Số hiệu pool là phần không được bỏ. Chỉ nhớ mỗi `index` thì một thread từng làm worker cho pool
/// khác vẫn mang theo một con số, và con số đó lại rất dễ là chỉ số hợp lệ ở pool đang được hỏi.
/// Đi tiếp với nó là đẩy việc vào deque của người khác từ một thread không phải chủ, hoặc tệ hơn là
/// đọc ra ngoài vùng `workers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Context
{
    pub pool:  u64,
    pub index: usize,
}

impl Context
{
    pub const NONE: Self = Self {
        pool:  NO_POOL,
        index: usize::MAX,
    };
}

thread_local! {

    pub static THREAD_LOCAL_CTX: Cell<Context> = const { Cell::new(Context::NONE) };
    pub static THREAD_LOCAL_SELF_ID: ThreadId = thread::current().id();
}
