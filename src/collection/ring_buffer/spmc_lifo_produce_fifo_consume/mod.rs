//! ## Ring mà chủ lấy ngược, kẻ trộm lấy xuôi
//!
//! Một chỗ chứa cỡ cố định. Chủ đẩy vào và lấy ra ở cùng một đầu, những người khác vét từ đầu kia.
//!
//! ### Giải quyết chuyện gì
//!
//! Đây là hình dạng mà một pool trộm việc cần.
//!
//! Chủ lấy thứ mới nhất trước, vì đó là thứ mà cache của chính thread này còn nóng nhất. Người đi
//! vét lấy thứ cũ nhất trước, vì đó thường là phần việc to nhất, và cũng là đầu xa nhất nên hai bên
//! ít giẫm lên nhau.
//!
//! ### Vét sang chỗ mình chứ không vét ra tay
//!
//! Điểm khác biệt so với ring anh em: người đi vét chuyển thẳng cả lô từ chỗ của nạn nhân sang chỗ
//! của chính mình, không đi vòng qua một chỗ chứa trung gian. Vét một lần rồi tự xử dần, nên chi
//! phí giành giật được chia cho cả lô.
//!
//! Khi vét hụt thì kết quả nói rõ lý do: hết hàng thật, hay chỉ là đang có người khác giành. Hai
//! chuyện đó dẫn tới hai cách xử khác nhau, một bên là đi tìm nạn nhân khác, một bên là thử lại.
//!
//! ### Cách hoạt động
//!
//! Con trỏ đầu ring gói hai số vào chung một ô nhớ: chỗ đã thật sự nhả ra, và chỗ đang có người bê
//! dở. Người đẩy vào nhờ đó biết vùng nào còn đang bị bê mà tránh ghi đè lên.
//!
//! Người đi vét chiếm trước một khoảng, chép sang chỗ mình, rồi mới công bố là đã xong. Người thua
//! trong một lần giành thì lùi lại theo nhịp tăng dần rồi thử lại.
//!
//! > [!IMPORTANT]
//! > Đúng một thread được làm chủ, và ring không tự kiểm chuyện đó. Gọi `push`, `pop_lifo` hay
//! > `push_batch_by_taking_from` từ một thread không phải chủ là hỏng con trỏ, im lặng, không có
//! > lỗi nào báo ra. Người dùng ring phải tự bảo đảm điều này ở tầng trên.

pub mod consumer;
pub mod spmc_ring_buffer_lifo_produce_fifo_consume;

pub use consumer::Consumer;
pub use spmc_ring_buffer_lifo_produce_fifo_consume::SpmcRingBufferLifoProduceFifoConsume;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;
