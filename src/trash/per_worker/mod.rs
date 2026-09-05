//! ## Mỗi thread một ô riêng
//!
//! Chỗ để mỗi thread tham gia pool ghi kết quả của mình mà không phải chia sẻ gì với ai.
//!
//! ### Giải quyết chuyện gì
//!
//! Chạy song song mà chỉ đọc thì dễ. Mọi chỗ dùng thật đều có phần **ghi**, và cái đích ghi thì
//! không chia sẻ được: một chỗ chứa lệnh dùng chung là điểm tranh chấp giữa mọi worker, còn vài
//! API đồ hoạ thì thẳng thừng đòi mỗi thread một chỗ chứa lệnh riêng.
//!
//! Câu trả lời không phải là thêm một cái khoá, mà là không chia sẻ ngay từ đầu. Mỗi thread ghi vào
//! ô của mình, mỗi ô nằm trên một dòng cache riêng để không ai giẫm lên ai, và kết quả được gộp lại
//! sau khi mọi người đã xong.
//!
//! ### Gộp kết quả là chỗ quyết định tính lặp lại được
//!
//! Việc trộm việc giữa các worker làm **thứ tự** kết thúc trở nên không đoán trước. Chuyện đó vô
//! hại với dựng hình, nhưng chí mạng với phát lại, với đồng bộ mạng theo bước khoá, hay với việc
//! dựng lại một con lỗi.
//!
//! Nên vòng gộp ở đây đi theo chỉ số ô chứ không theo thứ tự ai xong trước, và một phép gộp dựng
//! trên nó cho ra cùng một đáp án ở mọi lần chạy. Gộp theo thứ tự kết quả bay về là đúng cái sai mà
//! module này sinh ra để chặn.
//!
//! > [!NOTE]
//! > Mỗi thread chỉ với tới ô của chính nó, nghĩa là dữ liệu **di chuyển** giữa các thread chứ
//! > không bị dùng chung. Đó là lý do kiểu dữ liệu đặt vào đây không cần chia sẻ được giữa các
//! > thread, chỉ cần gửi đi được.

pub mod borrow_guard;
pub mod per_worker;
pub mod slot;

pub use per_worker::PerWorker;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
