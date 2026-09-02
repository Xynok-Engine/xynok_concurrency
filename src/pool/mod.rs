//! ## Pool trộm việc của một lane
//!
//! N thread worker, mỗi thread một ring riêng, cộng một hàng đợi dùng chung.
//!
//! ### Giải quyết chuyện gì
//!
//! Cả engine chỉ có một pool cho toàn bộ việc nặng CPU, thay vì mỗi hệ thống một pool. Lý do là
//! việc trộm việc chỉ có nghĩa khi số worker xấp xỉ số core: chia nhỏ thành nhiều pool thì trong
//! lúc pool này chạy, các core của pool kia ngồi không.
//!
//! ### Một việc đi đường nào
//!
//! ```text
//!   giao từ trong một việc         giao từ ngoài pool
//!            │                              │
//!            ▼                              ▼
//!      ô nóng của worker              hàng đợi chung
//!            │  (đầy thì đẩy xuống)         │
//!            ▼                              │
//!      ring riêng của worker  ◀─────────────┘  nạp cả cụm khi worker ngó tới
//!            │  (đầy thì xả nửa cũ xuống hàng đợi chung)
//!            ▼
//!      kẻ trộm bốc lô từ đầu ring
//! ```
//!
//! **Ô nóng** giữ đúng một việc: việc vừa được đẻ ra thường là việc mà cache của chính thread này
//! còn nóng nhất, nên chạy nó ngay là rẻ nhất.
//!
//! **Ring riêng** giữ phần còn lại và cho người khác trộm. Chủ ring lấy từ một đầu, kẻ trộm bốc từ
//! đầu kia, nên hai bên gần như không giẫm lên nhau.
//!
//! **Hàng đợi chung** hứng mọi thứ tràn ra, và là chỗ duy nhất thread ngoài pool đẩy việc vào được.
//!
//! ### Vòng tìm việc của một worker
//!
//! ```text
//!  1. ô nóng           việc vừa đẻ, nóng nhất
//!  2. ring riêng       việc của chính mình
//!  3. hàng đợi chung   việc từ ngoài, và việc bị xả ra
//!  4. trộm             đắt: phải giành với chủ ring người khác
//!  5. hàng đợi chung   ngó lần cuối trước khi ngủ
//!  6. ngủ
//! ```
//!
//! ### Vì sao thỉnh thoảng phải ngó hàng đợi chung trước
//!
//! Cứ vài chục vòng thì bước ngó hàng đợi chung được kéo lên trước cả ô nóng. Không có luật đó thì
//! một worker mà việc của nó cứ đẻ việc con sẽ tự nuôi mình mãi mãi, và việc của thread ngoài nằm
//! trong hàng đợi chung có thể chờ rất lâu dù pool nhìn từ ngoài vẫn "đang chạy".
//!
//! > [!NOTE]
//! > Thread nào gọi vào pool cũng trở thành một người tham gia, chứ không đứng ngoài nhìn. Chờ ở
//! > đây nghĩa là chạy việc giúp, nên một điểm hẹn không bao giờ bỏ phí một core.

pub mod config;
pub mod consts;
pub mod context;
pub mod counters;
pub mod local;
pub mod owner;
pub mod shared;
pub mod sleep;
pub mod thread_pool;
pub mod worker;

pub use config::Config;
pub use consts::LANE_QUEUE_TICK;
pub use thread_pool::ThreadPool;

pub(crate) use shared::Shared;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
