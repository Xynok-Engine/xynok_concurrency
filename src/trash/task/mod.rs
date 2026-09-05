//! ## Bộ chạy tác vụ của lane async
//!
//! Một future thành một tác vụ, và tác vụ tự xếp mình vào pool mỗi lần được gọi dậy.
//!
//! ### Giải quyết chuyện gì
//!
//! Lane async không còn là mấy thread ngồi chặn trong một lời gọi đọc file nữa. Nó chạy tác vụ, và
//! một tác vụ có thể dừng giữa chừng: chưa có gì để làm thì nó trả thread lại cho lane ngay tại đó,
//! rồi để lại một cái chuông. Ai đó rung chuông là nó được xếp lại vào hàng.
//!
//! Nhờ vậy một tác vụ đang đợi kết quả không ngồi trên thread nào cả, và số tác vụ chạy cùng lúc
//! không còn bị chặn bởi số thread của lane.
//!
//! ### Một tác vụ đi đường nào
//!
//! ```text
//!   giao future vào
//!        │
//!        ▼
//!   [đã xếp hàng] ─▶ worker lấy ra ─▶ [đang chạy] ──┬── xong    ─▶ [kết thúc]
//!        ▲                                 │        │
//!        │                                 │        └── chưa xong ─▶ [nằm chờ]
//!        │                                 │                             │
//!        └──────── có người rung chuông ◀──┴── chuông rung giữa lúc chạy ┘
//! ```
//!
//! Cái ô trạng thái ấy tồn tại vì hai chuyện có thật:
//!
//! - **Chuông rung nhiều lần liền nhau.** Tác vụ chỉ được nằm trong hàng đợi đúng một bản, không
//!   thì hai worker cùng chạy một future.
//! - **Chuông rung đúng lúc tác vụ đang chạy.** Lần rung ấy không được rơi mất, nếu không tác vụ
//!   ngủ luôn. Có một trạng thái riêng để ghi lại nó cho tới khi lượt chạy kết thúc.
//!
//! ### Ba kiểu chờ một tác vụ
//!
//! Thread ngoài pool thì ngủ giữa các lượt chạy. Thread đang đứng trong một lane thì chạy việc giúp
//! lane trong lúc chờ. Còn bên trong một tác vụ khác thì `await`, và không giữ thread nào cả.
//!
//! > [!IMPORTANT]
//! > Bên trong một tác vụ, `await` là đường duy nhất. Chặn một thread của lane để đợi một tác vụ
//! > khác của chính lane đó là cách treo nhanh nhất khi lane chỉ có vài thread.
//!
//! > [!NOTE]
//! > Bên dưới vẫn chưa có bộ theo dõi sự kiện của hệ điều hành. Một future tự gọi thẳng lời đọc file
//! > trong lượt chạy của nó thì vẫn giữ nguyên một thread của lane cho tới lúc xong. Cái đã đổi là
//! > **hình dạng**: phần chặn giờ nằm gọn một chỗ và trả về thứ mà tác vụ `await` được. Ngày cắm bộ
//! > theo dõi vào, chỗ phải sửa là chỗ đó, không phải các nơi gọi.

pub mod block_on;
pub mod consts;
pub mod park_waker;
pub mod spawn;
pub mod task;
pub mod yield_now;

pub use block_on::{block_on, block_on_in};
pub use spawn::spawn;
pub use yield_now::yield_now;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
