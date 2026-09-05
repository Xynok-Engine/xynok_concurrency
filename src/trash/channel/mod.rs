//! ## Kênh một lần
//!
//! Một việc gửi đúng một giá trị về cho một người đang chờ.
//!
//! ### Giải quyết chuyện gì
//!
//! Đây là cách một việc trả kết quả **ra khỏi** pool, và cố ý là một cái kênh chứ không phải giá
//! trị trả về của một lời gọi chờ sẵn.
//!
//! Lý do là chỗ đứng của người gọi: đã không giả định "gọi xong là có kết quả ngay" thì sau này đổi
//! nền phía dưới cũng không đụng gì tới họ.
//!
//! ### Ba kiểu chờ, một cái kênh
//!
//! Đầu nhận vừa chờ được kiểu thường vừa `await` được, nên cùng một cái kênh phục vụ cả ba tình
//! huống:
//!
//! - **Thread ngoài pool** thì ngủ hẳn cho tới khi có kết quả.
//! - **Thread đang đứng trong pool** thì chạy việc giúp trong lúc chờ, khỏi bỏ phí một core.
//! - **Một tác vụ async** thì `await`, và không giữ thread nào cả trong lúc đợi.
//!
//! ### Cách hoạt động
//!
//! Hai đầu dùng chung một ô nhớ cộng một cái cờ báo đã có hàng. Người chờ ghi tên mình vào trước
//! khi đi ngủ, người gửi đọc tên đó ra để gọi dậy. Tên được ghi lúc chờ chứ không phải lúc tạo
//! kênh, vì người tạo kênh và người chờ kết quả không nhất thiết là một.
//!
//! ### Khi một đầu biến mất
//!
//! Đầu gửi biến mất mà chưa gửi gì thì người chờ nhận về tin "không có kết quả" chứ không treo. Đó
//! cũng là đường mà một việc panic đi ra: đầu gửi bị thả trong lúc tháo ngăn xếp, và người chờ biết
//! ngay.
//!
//! Đầu nhận biến mất trước thì giá trị nằm lại trong kênh cho tới lúc kênh được dọn. Người gửi
//! không phải bận tâm chuyện còn ai nhận hay không.

pub mod inner;
pub mod oneshot;
pub mod receiver;
pub mod sender;
pub mod waiter;

pub use oneshot::oneshot;
pub use receiver::Receiver;
pub use sender::Sender;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
