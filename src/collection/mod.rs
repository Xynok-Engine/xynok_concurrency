//! ## Chỗ chứa dùng chung giữa các thread
//!
//! Những cấu trúc chứa dữ liệu mà nhiều thread cùng ra vào được, đặt riêng với phần logic của pool
//! để dùng lại được ở chỗ khác.
//!
//! ### Có gì ở đây
//!
//! Hiện mới có nhóm ring buffer: vùng nhớ cỡ cố định, chạy vòng, được thiết kế sao cho hai phía ra
//! vào chạm vào hai đầu khác nhau và ít giẫm lên nhau nhất có thể.

pub mod ring_buffer;
