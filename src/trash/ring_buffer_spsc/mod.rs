//! ## Ring một người ghi một người đọc
//!
//! Một kênh có trần, đúng một bên đẩy vào và đúng một bên lấy ra.
//!
//! ### Ai đặt hàng cái này
//!
//! Hai ring kia trong crate đều là một chủ nhiều kẻ trộm. Chỗ này cần thứ ngược lại, và người đặt
//! hàng là **thread âm thanh**.
//!
//! Thread âm thanh bị trình điều khiển gọi lại mỗi vài mili giây và phải trả xong bộ đệm trước hạn,
//! lần nào cũng vậy. Trong lượt gọi đó nó **không được** xin bộ nhớ, không được lấy khoá, không
//! được đi ngủ. Một lần trễ hạn không phải là một khung hình tụt xuống 58 hình mỗi giây, nó là một
//! tiếng "pop" nghe rõ mồn một.
//!
//! Nên nó không bao giờ là worker của pool. Lane khác gửi lệnh cho nó qua đúng cấu trúc này, và
//! trong lượt gọi nó chỉ đọc:
//!
//! ```text
//!   một worker                            thread âm thanh
//!        │ đẩy lệnh vào                         │ lấy ra trong lượt gọi
//!        ▼                                      ▼
//!   ┌──────────────────────────────────────────────┐
//!   │ [x][x][x][ ][ ][ ][ ][ ]                     │
//!   └──────────────────────────────────────────────┘
//!        đuôi (chỉ người ghi đụng)   đầu (chỉ người đọc đụng)
//! ```
//!
//! ### Vì sao mỗi bên một con trỏ
//!
//! Người ghi chỉ ghi con trỏ đuôi, người đọc chỉ ghi con trỏ đầu. Không có chỗ nào phải giành giật
//! cả, nên cả đẩy vào lẫn lấy ra đều chạy trong một số bước cố định bất kể phía bên kia đang làm gì.
//!
//! Đó mới là điều kiện thật sự của một lượt gọi có hạn chót cứng. Chỉ "không dùng khoá" thì chưa
//! đủ: một vòng giành giật vẫn có thể quay lại nhiều lần.
//!
//! ### Bản chụp con trỏ của bên kia
//!
//! Mỗi bên còn giữ thêm một bản chụp con trỏ của phía đối diện. Nhờ nó, đường thường chỉ đọc dòng
//! cache của chính mình: người ghi chỉ đi hỏi con trỏ thật khi bản chụp nói là ring đã đầy, và phần
//! lớn thời gian thì nó không đầy.
//!
//! Không có bản chụp này thì mỗi lần đẩy vào là một lần kéo dòng cache của người đọc về, và hai core
//! đá qua đá lại một dòng cache suốt cả khung hình.
//!
//! > [!IMPORTANT]
//! > Đúng một thread ghi và đúng một thread đọc. Hai thread cùng ghi, hoặc hai thread cùng đọc, là
//! > phá vỡ toàn bộ lập luận ở trên. Hai đầu gửi được sang thread khác nhưng không dùng chung được.

pub mod consts;
pub mod receiver;
pub mod ring_buffer_spsc;
pub mod sender;

pub use consts::MAX_SLOTS;
pub use receiver::Receiver;
pub use ring_buffer_spsc::RingBufferSpsc;
pub use sender::Sender;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
