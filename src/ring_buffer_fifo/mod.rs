//! ## Ring vào trước ra trước
//!
//! Chỗ chứa việc riêng của một worker, cho phép người khác trộm bớt khi họ rảnh.
//!
//! ### Giải quyết chuyện gì
//!
//! Một hàng đợi dùng chung cho cả pool thì mỗi lần lấy việc là một lần tranh chấp. Cho mỗi worker
//! một ring riêng thì phần lớn thao tác không đụng tới ai, và chỉ khi có người hết việc mới cần
//! chạm vào ring của người khác.
//!
//! ### Cách hoạt động
//!
//! Chủ ring làm việc ở một đầu, kẻ trộm bốc từ đầu kia. Hai đầu nằm trên hai dòng cache khác nhau,
//! nên đường thường của chủ ring gần như không bao giờ giẫm lên kẻ trộm.
//!
//! Con trỏ đầu ring gói hai số vào chung một ô nhớ 64 bit: chỗ mà kẻ trộm đang bốc dở, và chỗ thật
//! sự còn hàng. Gói chung như vậy để cả hai được đọc và sửa trong một thao tác duy nhất, khỏi lo
//! hai lần đọc lệch nhau. Nhờ đó chủ ring biết được vùng nào đang có người bê để tránh ra.
//!
//! Kẻ trộm bốc cả lô một lần: chiếm trước một khoảng, chép ra, rồi mới nhả. Chi phí giành giật được
//! chia cho cả lô thay vì trả lại từ đầu cho từng việc.
//!
//! ### So với ring vào sau ra trước
//!
//! Bên đó chủ ring lấy việc mới nhất trước, nên cache còn nóng và hợp với việc đẻ ra việc con. Bên
//! này chủ ring lấy theo đúng thứ tự vào, nên hợp với những chỗ mà thứ tự là một phần của yêu cầu.
//!
//! > [!IMPORTANT]
//! > Chỉ đúng một thread được làm chủ ring. Đường đẩy việc vào không hề chống tranh chấp giữa nhiều
//! > người ghi, vì bỏ được phần đó chính là chỗ tiết kiệm lớn nhất.

pub mod consts;
pub mod owner;
pub mod ring_buffer_fifo;
pub mod thief;

pub use consts::MAX_SLOTS;
pub use owner::Producer;
pub use ring_buffer_fifo::RingBufferFifo;
pub use thief::Consumer;

pub use crate::utils::steal::Steal;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
