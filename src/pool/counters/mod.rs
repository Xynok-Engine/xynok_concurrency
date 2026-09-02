//! ## Bộ đếm của pool
//!
//! Không có số thì không tinh chỉnh được.
//!
//! ### Giải quyết chuyện gì
//!
//! Mọi hằng số trong crate này đều là một phỏng đoán có lý do: cỡ ring, số worker, nhịp ngó hàng
//! đợi chung, trần số người cùng đi lùng việc, ngưỡng chia việc.
//!
//! Phỏng đoán có lý do vẫn là phỏng đoán, và cách duy nhất để biến nó thành một con số đúng cho
//! **máy này, tải này** là đo.
//!
//! ### Vì sao mỗi worker một bộ đếm riêng
//!
//! Một bộ đếm dùng chung cho cả pool thì mỗi lần cộng là một lần tranh chấp trên đúng cái dòng
//! cache mà mọi worker đều chạm. Đo mà làm chậm thứ đang đo thì con số đọc ra không nói về hệ thống
//! thật nữa.
//!
//! Nên mỗi worker cộng vào ô của riêng nó, mỗi ô nằm trên một dòng cache riêng, và phép cộng lại
//! chỉ xảy ra khi có ai đó hỏi.
//!
//! ### Đọc mấy con số này thế nào
//!
//! - **Tỷ lệ trộm trượt cao** nghĩa là worker tốn thời gian sờ vào ring của người khác mà không
//!   được gì. Thường là việc bị chia quá nhỏ, hoặc số worker nhiều hơn lượng việc thật sự có.
//! - **Số lần xả xuống hàng đợi chung lớn** nghĩa là ring quá nhỏ so với nhịp đẻ việc. Không sai,
//!   chỉ là những việc đó lẽ ra được ở lại chỗ nóng.
//! - **Nhiều lần đi ngủ giữa một khung hình** nghĩa là khung hình chưa được chia đủ nhỏ để nuôi hết
//!   pool. Đánh thức một thread tốn một cặp lời gọi xuống hệ điều hành.
//! - **Số việc đang chờ luôn dương và không tụt** nghĩa là worker đang bị bỏ đói, hoặc hàng đợi
//!   chung đang nghẽn.

pub mod counters;
pub mod pool_counters;
pub mod worker_counters;

pub use counters::Counters;

pub(crate) use pool_counters::PoolCounters;
