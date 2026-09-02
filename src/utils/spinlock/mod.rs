//! ## Khoá xoay cho vùng tranh chấp rất ngắn
//!
//! Khoá của std sẽ nhờ hệ điều hành cho thread đi ngủ khi không giành được. Việc đó rất đáng cho
//! những đoạn giữ khoá lâu, nhưng lại quá đắt khi phần được bảo vệ chỉ là vài lệnh: chi phí gọi
//! xuống nhân và đánh thức lại còn lớn hơn cả công việc thật.
//!
//! Ở đây thread không đi ngủ mà chờ tại chỗ, vì nó biết người đang giữ khoá sắp nhả ra ngay.
//!
//! ### Cách hoạt động
//!
//! Một cờ nguyên tử đánh dấu khoá đang bận hay rảnh. Ai chiếm được cờ thì nhận về một tấm vé, cầm
//! vé là mượn được dữ liệu bên trong, và vé rơi khỏi tầm nhìn thì khoá tự nhả.
//!
//! Ai chưa chiếm được thì lùi lại theo nhịp tăng dần thay vì quay tít, để nhường băng thông bộ nhớ
//! cho chính người đang giữ khoá làm nốt việc.
//!
//! > [!IMPORTANT]
//! > Chỉ dùng cho đoạn găng ngắn và chắc chắn không chặn. Giữ khoá này rồi đi ngủ, chờ I/O, hay
//! > gọi một thứ có thể khoá tiếp sẽ đốt CPU của mọi người còn lại, và có thể treo hẳn.

pub mod spin_guard;
pub mod spin_lock;

pub use spin_guard::SpinGuard;
pub use spin_lock::SpinLock;
