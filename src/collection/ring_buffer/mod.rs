//! ## Ring buffer
//!
//! Một vùng nhớ cỡ cố định, chạy vòng, để nhiều thread trao đổi dữ liệu mà ít phải giành nhau.
//!
//! ### Giải quyết chuyện gì
//!
//! Chỗ chứa dùng chung giữa các thread thường có một điểm nóng: ai ra vào cũng phải đụng vào cùng
//! một ô nhớ, và khi nhiều core cùng chạm thì dòng cache bị đá qua đá lại.
//!
//! Cách bịt ở đây là cho hai phía làm việc ở hai đầu khác nhau của vùng nhớ. Đầu vào và đầu ra nằm
//! trên hai dòng cache riêng, nên phần lớn thời gian hai bên không hề biết tới nhau.
//!
//! ### Hai biến thể
//!
//! Cả hai đều là một người đẩy vào, nhiều người lấy ra. Khác nhau ở phía người đẩy:
//!
//! - **Vào trước ra trước:** người đẩy cũng lấy theo đúng thứ tự đã vào. Hợp với chỗ mà thứ tự là
//!   một phần của yêu cầu.
//! - **Vào sau ra trước ở phía chủ:** người đẩy lấy thứ mới nhất trước để tận dụng cache còn nóng,
//!   còn người lấy từ xa vẫn nhận theo thứ tự cũ nhất trước. Đây là hình dạng mà một pool trộm việc
//!   cần.
//!
//! > [!IMPORTANT]
//! > Sức chứa luôn là luỹ thừa của hai, và cố định từ lúc dựng. Nhờ vậy phép quấn vòng chỉ là một
//! > phép và bit, và không có lần nới nào làm dữ liệu dời chỗ dưới chân người đang cầm nó.

pub mod spmc_fifo;
pub mod spmc_lifo_produce_fifo_consume;

pub(crate) mod consts;
pub(crate) mod params;
