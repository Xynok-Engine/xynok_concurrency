//! ## Ring vào sau ra trước
//!
//! Chỗ chứa việc riêng của một worker, ưu tiên chạy thứ vừa đẻ ra.
//!
//! ### Giải quyết chuyện gì
//!
//! Một việc vừa được đẻ ra thường là việc mà cache của chính thread này còn nóng nhất: dữ liệu nó
//! cần vừa được chạm tới xong. Chạy nó ngay là rẻ nhất.
//!
//! Đồng thời, những việc cũ nhất trong ring thường là những việc to nhất, vì thuật toán chia đôi
//! đẻ ra việc theo kiểu to trước nhỏ sau. Đó mới là thứ đáng để người khác sang trộm.
//!
//! Nên hai bên lấy từ hai đầu khác nhau, và ai cũng được phần hợp với mình.
//!
//! ### Cách hoạt động
//!
//! Chủ ring đẩy vào và lấy ra ở cùng một đầu, giống một chồng đĩa. Kẻ trộm bốc từ đầu kia. Hai đầu
//! nằm trên hai dòng cache riêng, nên đường thường của chủ ring không giẫm lên kẻ trộm.
//!
//! Con trỏ đầu ring gói hai số vào chung một ô nhớ 64 bit, để chỗ kẻ trộm đang bốc dở và chỗ thật
//! sự còn hàng luôn được đọc trong cùng một nhịp.
//!
//! ### Chỗ khó nhất: giành việc cuối cùng
//!
//! Khi ring chỉ còn đúng một việc, chủ và kẻ trộm có thể cùng nhắm vào nó. Cả hai phải cùng đi qua
//! một lần giành nguyên tử trên con trỏ đầu ring, nên đúng một bên thắng và bên kia ra về tay
//! không. Đó là chỗ duy nhất trên đường thường mà chủ ring phải trả giá cho việc đồng bộ.
//!
//! > [!IMPORTANT]
//! > Chỉ đúng một thread được làm chủ ring. Đường đẩy việc vào không chống tranh chấp giữa nhiều
//! > người ghi, vì bỏ được phần đó chính là chỗ tiết kiệm lớn nhất.

pub mod consts;
pub mod owner;
pub mod ring_buffer_lifo;
pub mod thief;

pub use consts::MAX_SLOTS;
pub use owner::Producer;
pub use ring_buffer_lifo::RingBufferLifo;
pub use thief::Consumer;

pub use crate::utils::steal::Steal;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
