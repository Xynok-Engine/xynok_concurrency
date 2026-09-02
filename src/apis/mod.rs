//! ## Những nút vặn mà người dùng thư viện chạm tới
//!
//! Gom các lựa chọn có ảnh hưởng tới hành vi lúc chạy về một chỗ, để đọc code gọi là biết ngay
//! thứ gì đang được điều chỉnh mà không phải lần vào tận trong ruột.
//!
//! ### Có gì ở đây
//!
//! - **Mức ưu tiên của thread:** nói với hệ điều hành rằng thread này cần chạy trước, hay nên
//!   nhường đường cho người khác. Mỗi nền tảng có một cách riêng, ở đây chỉ còn một cái tên chung.
//! - **Ngưỡng chờ:** bao nhiêu vòng quay tại chỗ thì chuyển sang nhường lượt. Mấy con số này đổi
//!   theo môi trường chạy, nên để tách ra thay vì rải khắp nơi.

pub mod consts;
pub mod priority;
