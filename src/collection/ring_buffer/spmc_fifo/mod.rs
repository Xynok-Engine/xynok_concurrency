//! ## Ring vào trước ra trước, một người ghi nhiều người lấy
//!
//! Một chỗ chứa cỡ cố định, đúng một bên đẩy vào và nhiều bên cùng lấy ra, tất cả theo đúng thứ tự
//! đã vào.
//!
//! ### Giải quyết chuyện gì
//!
//! Chia việc cho nhiều người tiêu thụ mà vẫn giữ nguyên thứ tự. Bên đẩy vào chỉ có một nên phần
//! ghi không phải chống tranh chấp gì cả, còn bên lấy ra thì nhiều, và đó là chỗ duy nhất phải
//! giành giật.
//!
//! ### Cách hoạt động
//!
//! Con trỏ đầu ring gói hai số vào chung một ô nhớ: chỗ đã thật sự nhả ra, và chỗ đang có người bê
//! dở. Gói chung để cả hai được đọc và sửa trong một nhịp, nhờ vậy người đẩy vào biết vùng nào còn
//! đang bị bê mà tránh ghi đè lên.
//!
//! Người lấy ra chiếm trước cả một khoảng, chép ra, rồi mới công bố là đã xong. Chi phí giành giật
//! nhờ đó chia cho cả lô thay vì trả lại từ đầu cho từng phần tử.
//!
//! Người thua trong một lần giành thì lùi lại theo nhịp tăng dần rồi thử lại, chứ không quay tít.
//!
//! > [!IMPORTANT]
//! > Đúng một thread được đẩy vào. Đường ghi không có chỗ nào chống hai người ghi cùng lúc, vì bỏ
//! > được phần đó chính là chỗ tiết kiệm lớn nhất.

pub mod consumer;
pub mod producer;
pub mod spmc_ring_buffer_fifo;

pub use consumer::Consumer;
pub use producer::Producer;
pub use spmc_ring_buffer_fifo::SpmcRingBufferFifo;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
