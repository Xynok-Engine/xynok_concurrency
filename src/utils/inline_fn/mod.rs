//! ## Gói một việc lại mà không đụng tới heap
//!
//! Một job trong pool này chỉ là một closure. Cách thông thường để cất closure đi rồi chạy sau là
//! bọc nó vào một con trỏ heap, nhưng như vậy mỗi lần giao việc lại là một lần cấp phát, và lúc
//! chạy thì phải nhảy qua một tầng con trỏ nữa mới tới được dữ liệu.
//!
//! Với những việc nhỏ và nhiều, riêng khoản chuẩn bị đó đã đắt hơn cả bản thân công việc.
//!
//! ### Cách hoạt động
//!
//! Ở đây job có kích thước cố định, vừa đúng một dòng cache. Closure nào đủ nhỏ thì nằm thẳng
//! trong đó, khỏi cấp phát và khỏi con trỏ trung gian. Closure to quá thì mới lùi về cách cũ, và
//! chỗ gọi không cần biết nó rơi vào đường nào.
//!
//! Vì kích thước cố định nên job xếp khít nhau trong hàng đợi và ring buffer, đọc tuần tự là đọc
//! liền mạch chứ không phải đi lượm từng mảnh rải rác trên heap.
//!
//! ### Việc mượn dữ liệu bên ngoài
//!
//! Ngoài đường thường dành cho closure sống lâu, còn một đường cho closure mượn dữ liệu trên ngăn
//! xếp của người giao việc. Đó là thứ mà scope cần, và nó cũng tránh được lần cấp phát mà cách bọc
//! con trỏ bắt phải trả.
//!
//! > [!IMPORTANT]
//! > Đường mượn dữ liệu là `unsafe`, và người gọi phải tự bảo đảm job chạy xong trước khi thứ nó
//! > mượn biến mất. Scope lo việc đó giúp, còn dùng tay trực tiếp thì phải tự lo.

pub mod fn_buffer;
pub mod inline_fn;
pub mod runnable;
pub mod unbound_v_table;
pub mod v_table;
pub mod v_table_alias;

pub use inline_fn::InlineFn;
pub use runnable::Runnable;
