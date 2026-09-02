//! ## Chia việc ra rồi đợi nó xong
//!
//! Đây là tầng mà mọi thứ phía trên gọi tới: một bộ lập lịch chia công việc theo lô, một đồ thị
//! khung hình chạy nhiều chặng song song, hay chỉ là một vòng lặp đủ nặng để đáng chia.
//!
//! ### Giải quyết chuyện gì
//!
//! Việc giao ra pool bình thường không được mượn gì trên ngăn xếp của người giao, vì pool không có
//! cách nào biết ngăn xếp đó còn sống hay không.
//!
//! Một vùng chia việc thì biết: nó không trả về cho tới khi mọi việc nó đẻ ra đã xong, **kể cả khi
//! đang tháo ngăn xếp vì panic**. Nhờ vậy thứ mà việc con mượn chắc chắn còn sống suốt thời gian nó
//! có thể chạy, và người dùng khỏi phải sao chép dữ liệu ra chỉ để làm vừa lòng bộ kiểm tra.
//!
//! ### Có gì trong đây
//!
//! - **Giao một việc rồi đi tiếp,** không chờ gì cả.
//! - **Chạy hai việc song song** và nhận lại cả hai kết quả.
//! - **Chia một khoảng chỉ số** thành từng lô và rải ra cho cả pool.
//! - **Gộp một khoảng chỉ số** thành một giá trị, với thứ tự nối cố định nên kết quả không đổi giữa
//!   các lần chạy. Chuyện này vô hại với dựng hình nhưng sống còn với phát lại và đồng bộ mạng.
//! - **Nói ra quan hệ trước sau** giữa các việc, để một khung hình là một đồ thị chứ không phải một
//!   chuỗi điểm dừng nối nhau.
//!
//! ### Chuyện panic
//!
//! Một cú panic thoát thẳng ra khỏi worker sẽ làm sổ đếm thiếu một, và điểm chờ treo mãi mãi. Nên
//! panic của việc con được bắt ngay tại chỗ, giữ lại, và ném lại ở chỗ mở vùng sau khi mọi việc anh
//! em đã xong. Nhiều việc cùng panic thì cái được ghi nhận đầu tiên thắng.
//!
//! > [!IMPORTANT]
//! > Thread đang chờ ở đây **không nằm không**: nó lấy việc khác của pool về chạy. Đó là cách duy
//! > nhất để chia việc lồng nhau hoạt động được. Nghĩa là trong lúc một việc đang dừng ở điểm chờ,
//! > một việc **không liên quan** có thể chạy ngay bên dưới nó, trên cùng thread đó.
//! >
//! > Hệ quả thực dụng: **đừng giữ một cái khoá khi gọi vào crate này**. Nếu việc chạy bên dưới lại
//! > đi xin đúng cái khoá ấy, thread tự khoá chính mình, mà không dòng nào của cả hai việc sai cả.

pub mod chunk_slots;
pub mod panic_sink;
pub mod params;
pub mod scope;
pub mod scope_in;
pub mod scope_ptr;

pub use params::ParamsParReduce;
pub use scope::Scope;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
