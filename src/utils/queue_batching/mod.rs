//! ## Hàng đợi gom theo lô
//!
//! Một hàng đợi vào trước ra trước, nhiều thread đẩy vào và nhiều thread rút ra được, sức chứa tự
//! nới theo nhu cầu.
//!
//! ### Vì sao lại gom theo lô
//!
//! Hàng đợi nào cũng có một điểm nóng ở chỗ đồng bộ. Đẩy từng phần tử một thì mỗi phần tử phải trả
//! trọn chi phí ở điểm nóng đó, và khi nhiều thread cùng làm vậy thì phần lớn thời gian là giành
//! nhau chứ không phải làm việc.
//!
//! Nên ở đây thao tác nào cũng có bản gom lô đi kèm: chiếm quyền một lần rồi chuyển cả cụm phần
//! tử. Chi phí đồng bộ được chia đều cho cả lô thay vì cho từng phần tử.
//!
//! ### Cách hoạt động
//!
//! Bên trong là một hàng đợi hai đầu thường, được bọc bởi một cờ nguyên tử. Ai chiếm được cờ thì
//! nhận về một tấm vé cho mượn thẳng hàng đợi bên trong, làm bao nhiêu việc cũng được trong một
//! lần chiếm, và vé rơi khỏi tầm nhìn thì quyền tự trả lại.
//!
//! Người chờ thì lùi lại theo nhịp tăng dần chứ không quay tít, vì đoạn giữ quyền ở đây luôn ngắn.
//!
//! > [!NOTE]
//! > Vé cho mượn thẳng hàng đợi bên trong, nên còn giữ vé là còn chặn mọi thread khác. Lấy đủ việc
//! > cần rồi thả ra sớm.

pub mod queue_batching;
pub mod queue_batching_guard;

pub use queue_batching::QueueBatching;
pub use queue_batching_guard::QueueBatchingGuard;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
