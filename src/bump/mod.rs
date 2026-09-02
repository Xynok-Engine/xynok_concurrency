//! ## Bộ nhớ nháp cho một khung hình
//!
//! Cấp phát ở đây là dịch một con trỏ về phía trước, còn giải phóng là quên nó đi.
//!
//! ### Giải quyết chuyện gì
//!
//! Pool hứa là không bao giờ chặn thread, và một lần xin bộ nhớ từ bộ cấp phát toàn cục lặng lẽ phá
//! lời hứa đó, vì bên trong nó có khoá. Một job đang xin bộ nhớ có thể làm nghẽn mọi thread khác
//! cũng đang xin.
//!
//! Cho mỗi worker một vùng nháp riêng là bỏ hẳn cái khoá đi: xin bộ nhớ chỉ còn một phép cộng cộng
//! một lần kiểm biên, và không có tranh chấp vì không ai khác với tới vùng của worker này.
//!
//! Hợp cho những chỗ chứa tạm mà một khung hình sinh ra rồi bỏ: danh sách thứ đang nhìn thấy, đám
//! đối tượng sắp tạo, đống lệnh vẽ chờ sắp xếp. Tất cả chết cùng một lúc khi vùng nháp được kéo về
//! đầu.
//!
//! ### Vì sao sức chứa cố định
//!
//! Nới rộng thì vùng nhớ phải dời chỗ, và mọi tham chiếu đã phát ra thành treo lơ lửng. Giữ đúng
//! một vùng từ lúc khởi động chính là thứ cho phép phát ra tham chiếu ghi được từ một tham chiếu
//! chia sẻ: các vùng phát ra không bao giờ chồng nhau, và bộ nhớ đằng sau chúng không bao giờ dời.
//!
//! Hết chỗ thì báo hết chỗ chứ không tự nới, nên đó là một quyết định của người gọi chứ không phải
//! một lần khựng lặng lẽ.
//!
//! > [!IMPORTANT]
//! > Kéo con trỏ về đầu là xong tất cả, nghĩa là không có destructor nào được chạy. Chỉ đặt vào
//! > đây những kiểu không cần dọn dẹp, còn lại sẽ rò bộ nhớ đều đặn mỗi khung hình.

pub mod bump;
pub mod chunk;
pub mod consts;

pub use bump::Bump;
pub use consts::MAX_ALIGN;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;
