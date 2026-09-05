//! Chỗ đứng của thread hiện tại trong pool nào, giữ trong thread local.

use std::cell::Cell;

use crate::sync::thread::{self, ThreadId};

use super::shared::Shared;

/// "Thread này chưa từng thuộc pool nào". Pool thật đánh số từ 1.
const NO_POOL: u64 = 0;

pub(super) fn next_pool_id() -> u64
{
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(NO_POOL + 1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Chỗ đứng của thread hiện tại: nó là người thứ mấy của pool nào, và nó có đang ở trong vòng chạy
/// job hay không.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Context
{
    pub(super) pool:    u64,
    pub(super) index:   usize,
    /// Đang ở trong vòng chạy job của pool đó.
    ///
    /// Phân biệt này quyết định job mới đi đâu. Ở trong vòng thì ô LIFO là chỗ tốt nhất, vì chính
    /// thread này sẽ quay lại lấy nó ngay sau job hiện tại. Ở ngoài vòng (host vừa gọi `spawn` rồi
    /// đi làm việc khác) mà nhét vào ô LIFO thì job nằm im: không ai trộm được ô LIFO, và chủ của
    /// nó thì đang không nhìn tới.
    pub(super) in_loop: bool,
}

impl Context
{
    pub(super) const NONE: Self = Self {
        pool:    NO_POOL,
        index:   usize::MAX,
        in_loop: false,
    };
}

thread_local! {
    pub(super) static CONTEXT: Cell<Context> = const { Cell::new(Context::NONE) };

    /// Id của chính thread này, tính một lần.
    ///
    /// `thread::current()` phải clone một `Arc`, quá đắt cho một thứ nằm trên đường push. Cái id
    /// thì chỉ là mấy byte, và nó không đổi suốt đời thread.
    pub(super) static SELF_ID: ThreadId = thread::current().id();
}

/// Chỉ số của thread hiện tại trong `0..pool.worker_count()`.
///
/// Worker nhận `0..worker_threads()`, host nhận ô ngay sau chúng, vì host cũng chạy job. Dùng nó để
/// đánh chỉ số cho dữ liệu "mỗi worker một ô": một command buffer, một bộ đếm profiler, một arena.
/// Nhờ nó mà không bao giờ có hai thread ghi vào cùng một cache line.
///
/// # Vì sao phải so id chứ không chỉ so khoảng
///
/// Chỉ kiểm "chỉ số có nằm trong `0..=workers` không" thì chấp nhận luôn cái chỉ số mà một pool
/// **khác** đã phát cho thread này. Dựng hai pool trên cùng một thread là đủ để dính: lần đăng ký
/// sau ghi đè lần trước, và host đọc lại ra một con số vốn cũng là chỉ số hợp lệ của pool thứ nhất.
/// Hai thread cùng qua được bài kiểm tra, cùng được trao một ô, và thế là hai thread ghi vào một
/// chỗ. So id biến chuyện đó thành một lần trượt, và trượt thì rơi xuống nhánh kiểm host bên dưới.
pub(crate) fn worker_index_in(shared: &Shared) -> usize
{
    let context = CONTEXT.get();

    if context.pool == shared.id
    {
        return context.index;
    }

    assert_eq!(
        thread::current().id(),
        shared.host.id(),
        "worker_index() gọi từ một thread không phải worker của pool này, cũng không phải thread đã dựng nó"
    );
    shared.workers
}
