/// Điều kiện để một closure được nhận vào đường thường: chạy đúng một lần, gửi được sang thread
/// khác, và không mượn gì có thời hạn.
///
/// Không cần cài đặt bằng tay, closure nào hợp lệ thì tự có.
pub trait Runnable: FnOnce() + Send + 'static {}

impl<F> Runnable for F where F: FnOnce() + Send + 'static {}
