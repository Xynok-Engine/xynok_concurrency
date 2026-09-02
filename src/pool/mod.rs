//! Pool work-stealing của một lane: N thread worker, mỗi thread một ring, một hàng đợi chung.
//!
//! Đọc [`docs_internal/lanes.md`](../../docs_internal/lanes.md) để biết vì sao engine chỉ có một
//! pool cho toàn bộ việc CPU-bound thay vì mỗi hệ thống một pool. Ở đây chỉ nói cách pool chạy.
//!
//! # Một job đi đường nào
//!
//! ```text
//!   spawn từ trong một job         spawn từ ngoài pool
//!            │                              │
//!            ▼                              ▼
//!      ô LIFO của worker              lane queue (dùng chung)
//!            │  (đầy thì đẩy xuống)         │
//!            ▼                              │
//!      ring local của worker  ◀─────────────┘  nạp cả cụm khi worker ngó tới
//!            │  (đầy thì spill nửa cũ xuống lane queue)
//!            ▼
//!      kẻ trộm bốc lô từ đầu ring
//! ```
//!
//! Ô LIFO giữ đúng một job: job vừa spawn ra thường là job mà cache của chính thread này còn nóng
//! nhất, nên chạy nó ngay là rẻ nhất. Ring local giữ phần còn lại và cho người khác trộm. Lane
//! queue hứng mọi thứ tràn ra, và là chỗ duy nhất thread ngoài pool đẩy job vào được.
//!
//! # Vòng tìm việc của một worker
//!
//! ```text
//!  1. ô LIFO         job vừa spawn, nóng nhất
//!  2. ring local     việc của chính mình
//!  3. lane queue     việc từ ngoài, và việc bị xả ra
//!  4. trộm           đắt: phải CAS vào ring người khác
//!  5. lane queue     ngó lần cuối trước khi ngủ
//!  6. ngủ
//! ```
//!
//! Cứ [`LANE_QUEUE_TICK`] vòng thì bước 3 được kéo lên trước bước 1. Không có luật đó thì một
//! worker mà job của nó cứ đẻ job con sẽ tự nuôi mình mãi mãi, và job của thread ngoài nằm trong
//! lane queue có thể chờ rất lâu dù pool nhìn từ ngoài vẫn "đang chạy".

use std::time::Duration;

pub mod counters;
pub mod sleep;

mod config;
mod context;
mod local;
mod owner;
mod shared;
mod thread_pool;
mod worker;

pub use config::Config;
pub use thread_pool::ThreadPool;

pub(crate) use shared::Shared;

#[cfg(doc)] use crate::utils::backoff::Backoff;

/// Cứ bấy nhiêu vòng thì worker ngó lane queue trước cả ring của mình.
///
/// Số nguyên tố, và không phải để cho đẹp: một hằng chia hết cho số worker, hoặc chia hết cho nhịp
/// đẻ job của một thuật toán chia đôi, sẽ khiến nhiều worker cùng ngó lane queue đúng một lúc rồi
/// cùng giành một cái khoá. Số nguyên tố đủ lớn thì các worker rải đều ra.
pub const LANE_QUEUE_TICK: u32 = 61;

/// Một giấc ngủ ngắn của thread đang chờ ở [`ThreadPool::run_until`] mà pool thì hết việc.
///
/// Ngắn có chủ ý: nó là hạn chót cho trường hợp xấu nhất, tức là khi thứ đang được chờ hoàn thành
/// mà không gọi ai dậy. Đường bình thường thì [`Latch`](crate::latch::Latch) gọi dậy ngay.
const IDLE_NAP: Duration = Duration::from_micros(50);

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
