//! ## Các lane của engine
//!
//! Ai chạy ở đâu, và việc đi từ lane này sang lane kia bằng đường nào.
//!
//! ### Sai lầm mà module này tránh
//!
//! Nghe "engine có lane physics, lane render, lane audio, lane IO" thì phản xạ đầu tiên là cho mỗi
//! lane một pool riêng. Sáu lane nhân tám thread là 48 thread trên 8 core, mà việc trộm việc chỉ có
//! nghĩa khi số worker xấp xỉ số core. Vượt qua đó thì thứ mua được là các worker giành CPU của
//! nhau và cache bị đá qua đá lại.
//!
//! Nên lane ở đây chia theo **tính chất chạy**, không theo tên miền.
//!
//! ### Ba lane, ba tính chất
//!
//! - **Lane tính toán:** phần lớn công việc của một khung hình sống ở đây. Một pool trộm việc ăn
//!   gần hết số core, và thread gọi cũng tham gia chạy. Việc ở đây không bao giờ chờ đợi, chỉ tính.
//! - **Lane bất đồng bộ:** nạp dữ liệu, giải nén, biên dịch. Một pool nhỏ riêng, mức ưu tiên thấp,
//!   và là lane duy nhất chạy tác vụ có thể dừng giữa chừng. Việc ở đây là chuỗi chờ nối nhau, và
//!   một cái chờ không được phép chiếm core của lane tính toán.
//! - **Lane chính:** không có thread nào cả, chỉ là một hàng đợi mà chính thread của người dùng vét
//!   trong vòng lặp khung hình. Dành cho những lời gọi bắt buộc phải đúng thread đó.
//!
//! Physics, dựng hình và tính toán **không** phải ba lane riêng. Chúng là ba nhóm việc trong cùng
//! lane tính toán, phân biệt nhau bằng vị trí trong đồ thị phụ thuộc chứ không bằng thread riêng.
//! Cho physics một pool riêng nghĩa là trong lúc physics chạy thì các core dành cho dựng hình ngồi
//! không, và ngược lại.
//!
//! ### Chờ đợi thì đi đường nào
//!
//! Một tác vụ ở lane bất đồng bộ dừng giữa chừng thì trả thread lại cho lane ngay tại đó, nên "đợi"
//! ở lane này không tốn thread nào. Cả chuỗi nạp một tài nguyên vì vậy viết được thành một mạch
//! liền, thay vì cắt vụn thành các bước gọi lại lẫn nhau.
//!
//! Phần chặn thật, tức là lời gọi xuống hệ điều hành ở đáy chuỗi, có cửa riêng: nó chiếm một thread
//! của lane trong lúc chạy, còn nơi gọi nó thì chỉ đợi chứ không chiếm gì.
//!
//! Còn một cửa thoát hẹp cho việc ở lane tính toán lỡ phải chờ một thứ ngoài tầm với. Nó gọi thêm
//! một worker dậy để bù vào chỗ trống rồi chạy tại chỗ, đủ cho một lần chờ ngắn và không thay được
//! cho việc gửi hẳn công việc sang lane bất đồng bộ.
//!
//! > [!NOTE]
//! > Thread âm thanh cố ý không nằm trong bảng này: nó không bao giờ là worker của pool nào. Nó
//! > nhận lệnh qua một kênh một chiều và chỉ đọc, vì nó có hạn chót cứng mà không lịch trình nào
//! > của pool dám hứa.

pub mod block_in_place;
pub mod lane_id;
pub mod lanes;
pub mod lanes_config;
pub mod main_queue;

pub use block_in_place::block_in_place;
pub use lane_id::LaneId;
pub use lanes::Lanes;
pub use lanes_config::LanesConfig;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
