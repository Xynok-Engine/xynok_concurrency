//! ## Hàng đợi chung của một lane
//!
//! Chỗ việc từ ngoài pool rơi vào, và chỗ worker xả bớt khi ring riêng của nó đã đầy.
//!
//! ### Giải quyết chuyện gì
//!
//! Ring riêng của mỗi worker che được gần hết nhu cầu, nhưng để hở hai lỗ mà nó không tự bịt được:
//!
//! - Thread không phải worker thì không sở hữu ring nào, nên không có chỗ nào để đẩy việc vào.
//! - Ring có biên, còn lượng việc thì không. Đầy rồi thì phần tràn phải đi đâu đó.
//!
//! Cả hai đều đổ về đây.
//!
//! ### Lấy ra theo cụm mới là phần quan trọng
//!
//! Đây là điểm dùng chung của mọi thread trong lane, nên lấy một việc mỗi lượt nghĩa là mỗi việc
//! phải trả một lần tranh chấp trên cùng một dòng cache.
//!
//! Lấy cả cụm thì trả giá đúng một lần, rồi những việc còn lại chạy từ ring riêng và không đụng vào
//! ai. Cụm lấy về cũng không lấy tham lam: chia đều theo số worker của lane, và luôn chừa lại nửa
//! ring trống cho việc con mà chính worker đó sắp đẻ ra.
//!
//! ### Vì sao không lock-free
//!
//! Bên dưới là một hàng đợi thường nằm dưới khoá xoay, và đó là lựa chọn có chủ ý: mỗi lane giữ
//! một hàng đợi riêng nên tranh chấp vốn đã thấp, còn một cấu trúc mình hiểu rõ thì sửa được lúc
//! 2 giờ sáng.
//!
//! > [!NOTE]
//! > Chỗ này không cần biết ring riêng của worker thuộc loại nào. Hai loại ring của crate cùng có
//! > một bộ thao tác với cùng ý nghĩa, nên pool chọn loại nào cũng được, hàng đợi cứ thế đổ việc
//! > vào.

pub mod batch_size;
pub mod lane_queue;
pub mod local_queue;

pub use lane_queue::LaneQueue;
pub use local_queue::LocalQueue;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
