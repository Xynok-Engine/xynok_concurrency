//! ## Xương sống chạy song song của Xynok
//!
//! Bộ đồ nghề để chia việc ra nhiều thread và ghép kết quả lại, viết cho nhịp làm việc của một
//! engine: mỗi khung hình có hạn chót cứng, và mọi thứ phải xong trước khi hạn đó tới.
//!
//! ### Giải quyết chuyện gì
//!
//! Chia việc thì dễ, nhưng ba chỗ sau mới là chỗ đau, và cả ba đều được nhắm thẳng ở đây.
//!
//! - **Chi phí giao việc.** Một khung hình đẻ ra hàng nghìn việc nhỏ. Nếu mỗi lần giao việc phải
//!   xin bộ nhớ và giành một cái khoá thì riêng phần chuẩn bị đã ăn hết ngân sách. Nên việc ở đây
//!   được gói vào một ô cỡ cố định, và chỗ chứa việc thì mỗi thread một cái riêng.
//! - **Chờ đợi mà không bỏ phí core.** Thread đang đợi ở một điểm hẹn sẽ đi chạy việc khác giúp
//!   pool, chứ không nằm không. Nhờ vậy chia việc lồng nhau mới hoạt động được.
//! - **Mượn dữ liệu trên ngăn xếp.** Việc giao ra pool bình thường không mượn được gì của người
//!   giao. Vùng chia việc gỡ chỗ đó bằng cách không trả về cho tới khi mọi việc con đã xong, kể cả
//!   khi đang panic.
//!
//! ### Đi vào từ đâu
//!
//! Phần lớn nhu cầu chỉ cần tới ba chỗ:
//!
//! - **[`lanes`]** chia công việc theo tính chất chạy: việc nặng CPU, việc chờ đợi, và việc buộc
//!   phải nằm trên thread chính. Đây là chỗ nên đọc đầu tiên.
//! - **[`scope`]** để chia một việc ra chạy song song rồi ghép lại, kể cả khi việc con cần mượn dữ
//!   liệu của người gọi.
//! - **[`pool`]** nếu chỉ cần đúng một pool trộm việc, không cần tới cách chia lane ở trên.
//!
//! Phần còn lại là ruột: các loại ring buffer, hàng đợi, điểm hẹn, kênh trả kết quả, và đồ nghề
//! dùng chung. Chúng được mở ra để dùng lại và để đọc hiểu, không phải mặt tiền của thư viện.
//!
//! ### Vài điều nên biết trước
//!
//! Cả engine dùng một pool cho toàn bộ việc nặng CPU, chứ không phải mỗi hệ thống một pool. Việc
//! trộm việc chỉ có nghĩa khi số worker xấp xỉ số core, chia nhỏ ra thì trong lúc pool này chạy,
//! các core của pool kia ngồi không.
//!
//! Việc chạy song song ở đây do người dùng khai báo, không có ai tự động phân tích rồi song song
//! hoá giúp. Cái gì chạy cùng cái gì là quyết định của người viết.
//!
//! > [!IMPORTANT]
//! > **Đừng giữ một cái khoá khi gọi vào crate này.** Thread đang đợi sẽ chạy việc khác ngay bên
//! > dưới điểm đợi, và nếu việc đó lại đi xin đúng cái khoá ấy thì thread tự khoá chính mình, mà
//! > không dòng nào của cả hai việc sai cả.

pub mod apis;
pub mod bump;
pub mod channel;
pub mod collection;
pub mod custom_type;
pub mod job_graph;
pub mod lanes;
pub mod latch;
pub mod mutex_condition;
pub mod per_worker;
pub mod pool;
pub mod profile;
pub mod ring_buffer_fifo;
pub mod ring_buffer_lifo;
pub mod ring_buffer_spsc;
pub mod scope;
pub mod task;
pub mod utils;
pub mod thread_pool;
pub(crate) mod sync;
