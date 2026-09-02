//! ## Đợi một nhóm việc cùng báo xong
//!
//! "Chạy n job xong rồi gọi tôi dậy." Đây là nguyên liệu mà mọi điểm hẹn trong crate này dựng lên
//! từ đó.
//!
//! Ý tưởng đúng một câu: có một con số trên bảng, ai làm xong phần mình thì trừ đi một, người trừ
//! tới 0 có nhiệm vụ gọi người đang chờ dậy.
//!
//! ### Chờ kiểu gì
//!
//! Có hai đường chờ, và chọn đúng đường mới là phần quan trọng.
//!
//! - **Thread thuộc pool** thì không nằm không, mà đi chạy job giúp trong lúc đợi. Ngủ ở đây là
//!   pool mất một core đúng lúc đang cần nhất, và với các điểm hẹn lồng nhau thì thread ngủ có thể
//!   chính là thread lẽ ra phải chạy cái việc mà nó đang chờ.
//! - **Thread ngoài pool** thì ngủ hẳn, vì nó không có việc gì để chạy giúp, quay tại chỗ chỉ đốt
//!   một core vô ích.
//!
//! ### Vì sao vé lại đi được vào trong một job
//!
//! Cơ chế đợi bên [`utils`](crate::utils::waker) cũng đếm ngược như vậy, nhưng vé của nó mượn
//! tham chiếu nên không nhét vào một job được, mà job thì bắt buộc không được mượn gì. Vé ở đây
//! mang một con trỏ thô thay cho tham chiếu, nên nó đi theo job sang thread khác được.
//!
//! Vé tự báo về khi rơi khỏi tầm nhìn, kể cả lúc job đang panic, nên không có đường nào để một job
//! biến mất mà không báo.
//!
//! > [!IMPORTANT]
//! > Con trỏ thô ấy chỉ an toàn nhờ đúng một điều, và điều đó phải luôn đúng: **lệnh chờ không
//! > được trả về cho tới khi mọi vé đã bị thả**. Bảng đếm nằm trên ngăn xếp của người chờ, và
//! > người chờ không rời khung ngăn xếp đó chừng nào còn vé sống. Dựng bảng mà không chờ hết vé là
//! > tạo ra một use-after-free, không phải một cái lỗi nhỏ.

pub mod latch;
pub mod latch_ticket;

pub use latch::Latch;
pub use latch_ticket::LatchTicket;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
