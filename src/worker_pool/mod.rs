//! ## Pool worker
//!
//! Bản dựng lại của pool, đứng trên nhóm ring buffer trong [`collection`](crate::collection).
//!
//! ### Giải quyết chuyện gì
//!
//! Mỗi worker giữ một chỗ chứa việc riêng, và khi hết việc thì sang vét bớt của người khác. Nhờ vậy
//! phần lớn thao tác lấy việc không đụng tới ai, mà tải vẫn tự san đều giữa các thread.
//!
//! Ngoài ra còn một hàng đợi chung để thread bên ngoài đẩy việc vào, vì thread đó không sở hữu chỗ
//! chứa riêng nào.
//!
//! ### Cấu hình được tự nắn lại
//!
//! Vài con số truyền vào sẽ được sửa cho hợp lệ thay vì báo lỗi: số worker bị kẹp xuống theo số
//! core thật của máy, còn sức chứa mỗi worker được nâng lên luỹ thừa hai gần nhất để phép quấn vòng
//! chỉ còn là một phép và bit.
//!
//! > [!NOTE]
//! > Phần này đang dựng dở, vòng chạy của worker mới chỉ là khung. Đường đang chạy thật là
//! > [`pool`](crate::pool).

pub mod cfg;
pub mod identifies;
pub mod worker_pool;

pub(crate) mod local;
pub(crate) mod thread_meta;
pub(crate) mod worker;

pub use worker_pool::WorkerPool;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
