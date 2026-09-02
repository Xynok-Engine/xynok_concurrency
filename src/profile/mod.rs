//! ## Chỗ cho profiler cắm vào
//!
//! Để hành vi của chính pool nhìn thấy được, thay vì phải đoán.
//!
//! ### Giải quyết chuyện gì
//!
//! Profiler vốn đã đo được **bên trong** một job, vì đó là code ứng dụng bình thường. Thứ nó không
//! nhìn thấy từ ngoài là mọi chuyện xảy ra **giữa** các job: worker nằm ngủ bao lâu, bao nhiêu
//! phần của một khung hình đi vào việc lập lịch thay vì việc thật, và pool có ngủ giữa chừng vì
//! một anh đi sau đang giữ điểm hẹn hay không.
//!
//! Đó mới là những câu quyết định một khung hình đang được tiêu hay đang bị phí, và chúng chỉ trả
//! lời được từ bên trong pool.
//!
//! ### Cách hoạt động
//!
//! Cắm vào một bộ nhận thông báo cho cả tiến trình, rồi pool tự gọi tới nó ở các mốc: job bắt đầu,
//! job kết thúc, worker đi ngủ, worker dậy, hết một khung hình.
//!
//! Mốc kết thúc job được phát cả khi job đang panic, vì một khoảng đo mở ra mà không đóng lại sẽ
//! làm hỏng mọi phép đo phía sau nó chứ không riêng phép đo của chính nó.
//!
//! Mọi mốc đều có bản mặc định không làm gì, nên ai cắm vào chỉ cần nhận đúng thứ mình quan tâm.
//!
//! ### Giá phải trả khi không cắm gì
//!
//! Một lần đọc con trỏ và một nhánh rẽ mà bộ dự đoán đoán đúng mọi lần, tính trên mỗi job. So với
//! chi phí mỗi job của chính pool thì nó không hiện lên.
//!
//! > [!IMPORTANT]
//! > Chỉ cắm được đúng một lần cho cả tiến trình, và phải cắm **trước** khi dựng pool. Cắm sau thì
//! > đám worker đang chạy sẽ báo cáo vào hư không cho tới lần chúng nhìn lại.

pub mod end_on_drop;
pub mod registry;
pub mod sink;

pub use registry::install;
pub use sink::Sink;

pub(crate) use registry::{current_sink, job};

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
