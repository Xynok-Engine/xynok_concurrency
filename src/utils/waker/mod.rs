//! ## Đợi một nhóm việc cùng xong
//!
//! Giao vài việc ra cho nhiều thread rồi muốn biết lúc nào cả nhóm xong hết. Chờ bằng cách hỏi đi
//! hỏi lại thì đốt CPU vô ích, còn ngủ mà không ai gọi dậy đúng lúc thì mất luôn thời gian phản
//! hồi.
//!
//! ### Cách hoạt động
//!
//! Hình dung có một tấm bảng ghi số việc còn lại. Mỗi người nhận việc cầm theo một tấm vé, làm
//! xong thì bỏ vé đi, và mỗi lần bỏ vé là con số trên bảng giảm một. Ai làm số đó về không thì có
//! trách nhiệm gọi người đang chờ dậy.
//!
//! Người chờ thì ngủ hẳn chứ không hỏi vòng, nên không tốn gì trong lúc đợi.
//!
//! ### Vài chỗ dễ vấp
//!
//! Vé tự bỏ khi rơi khỏi tầm nhìn, nên việc có panic giữa chừng thì con số vẫn giảm và người chờ
//! vẫn dậy được, không ai bị treo vì một việc chết nửa đường.
//!
//! Cú gọi dậy có thể tới trước lúc người chờ kịp ngủ. Khi đó nó để lại một dấu, người chờ thấy dấu
//! là biết khỏi ngủ nữa.
//!
//! > [!IMPORTANT]
//! > Chỉ đúng thread đã dựng ra tấm bảng mới được chờ trên nó, vì cú gọi dậy nhắm thẳng vào thread
//! > đó. Thread khác gọi chờ là ngủ mãi không ai đánh thức.

pub mod waker;
pub mod waker_signal;

pub use waker::Waker;
pub use waker_signal::WakerSignal;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
