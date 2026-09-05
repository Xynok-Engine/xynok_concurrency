//! ## Nói ra quan hệ trước sau giữa các việc
//!
//! Để một khung hình là một đồ thị, chứ không phải một chuỗi điểm dừng nối nhau.
//!
//! ### Giải quyết chuyện gì
//!
//! Một vùng chia việc kết thúc bằng "chờ tất cả", tức là khung hình bị cắt thành từng chặng ngăn
//! bởi các điểm dừng. Mà một điểm dừng thì tốn đúng bằng việc **chậm nhất** trong chặng đó, còn mọi
//! thread khác ngồi không cho hết phần chênh lệch.
//!
//! ```text
//! chỉ có vùng chia việc  [== A ==][=====]│[== B ==][==========]│[== C ==]
//!                                        ▲ cả pool chờ một anh đi sau
//!
//! có quan hệ phụ thuộc   [== A ==][== B ==][== C ==]
//!                        [=====][==========][··· bốc luôn việc của chặng sau ···]
//! ```
//!
//! Bộ lập lịch vốn đã biết việc nào đọc gì ghi gì, tức là nó đã cầm sẵn đồ thị phụ thuộc trong tay.
//! Thứ nó thiếu chỉ là một cách **nói ra** điều đó.
//!
//! ### Cách hoạt động
//!
//! Giao việc thì nhận lại một tay cầm, và việc sau treo lên tay cầm đó. Mỗi việc giữ một sổ đếm số
//! thứ nó còn đang chờ, ai kéo sổ về không thì người đó đẩy việc đi. Phụ thuộc nào đã xong sẵn thì
//! không phải chờ chút nào.
//!
//! Mọi thao tác đều trả về ngay, không có chỗ nào chặn: thread đang mô tả đồ thị thì đi mô tả tiếp,
//! hoặc đi chạy việc, chứ không ngồi lên một điểm dừng.
//!
//! ### Không dựng được vòng lặp phụ thuộc
//!
//! Tay cầm chỉ tồn tại sau khi việc của nó đã được tạo, nên mọi cạnh đều chỉ ngược về quá khứ.
//! Không có cách nào gọi tên một việc chưa tồn tại, nên cũng không có cách nào viết ra một vòng.
//!
//! Điều đó quan trọng, vì một vòng ở đây là một vùng chia việc không bao giờ đếm về không, tức là
//! treo hẳn chứ không phải một thông báo lỗi.
//!
//! > [!NOTE]
//! > Tay cầm cố ý **không** có lệnh chờ. Chờ một việc thì cách giao việc thường cộng điểm chờ sẵn
//! > có đã làm rồi, và bày thêm lệnh chờ ở đây là mời gọi đúng cái lối mà module này sinh ra để
//! > thay: giao, chờ, giao, chờ.

pub mod job_handle;
pub mod node;

pub use job_handle::JobHandle;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
