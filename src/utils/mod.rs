//! ## Đồ nghề dùng chung
//!
//! Những mảnh nhỏ mà gần như mọi cấu trúc đồng thời trong crate này đều cần tới, gom về một chỗ để
//! khỏi mỗi nơi viết lại một kiểu. Phần lớn chúng sinh ra từ cùng một nỗi đau: viết code chạy nhiều
//! thread thì lúc nào cũng vướng mấy chi tiết lặt vặt về bộ nhớ và tranh chấp, và nếu để lẫn vào
//! logic chính thì đọc lại rất mệt.
//!
//! ### Có những nhóm gì
//!
//! - **Chờ và khoá:** cách lùi lại khi đang tranh nhau một ô nhớ, khoá xoay cho vùng tranh chấp
//!   ngắn, và cơ chế cho thread đi ngủ rồi được gọi dậy.
//! - **Chỗ chứa:** vùng ô nhớ cỡ cố định, con trỏ chạy vòng, và hàng đợi gom theo lô để bớt số lần
//!   phải đụng vào khoá.
//! - **Xếp đặt bộ nhớ:** đệm cho mỗi biến nằm riêng một dòng cache, và mẹo nhét hai con trỏ 32 bit
//!   vào chung một từ nhớ để đọc ghi cả cặp trong một nhịp.
//! - **Gói việc lại:** chỗ cất một closure ngay trong ngăn xếp thay vì cấp phát trên heap.
//!
//! > [!NOTE]
//! > Đây là đồ dùng nội bộ được mở ra ngoài, không phải mặt tiền của thư viện. Muốn chạy việc song
//! > song thì nên dùng pool và scope, đừng ghép tay từ mấy mảnh này.

pub mod backoff;
pub mod backoff_manual;
pub mod bits;
pub mod cache_padded;
pub mod cores;
pub mod cursors;
pub mod fixed_buffer;
pub mod inline_fn;
pub mod lock_free;
pub mod packed;
pub mod poison;
pub mod queue_batching;
pub mod slots;
pub mod spinlock;
pub mod steal;
pub mod waker;
