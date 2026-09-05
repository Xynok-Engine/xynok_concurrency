use crate::pool::ThreadPool;

/// Cửa thoát cho một job lane compute lỡ phải chờ một thứ ngoài tầm với: một fence GPU, một cái
/// khoá của thư viện bên thứ ba, một lời gọi driver đồng bộ.
///
/// Nó gọi thêm một worker dậy trước khi chạy `f`, để chỗ trống mà thread này để lại có người bù
/// vào, rồi chạy `f` ngay tại đây.
///
/// # Giới hạn, nói thẳng ra
///
/// Đây **không** phải `block_in_place` của tokio: thread này vẫn giữ chỗ của nó trong pool, không
/// có worker thay thế nào được spawn ra. Nếu mọi worker cùng gọi hàm này một lúc thì pool đứng
/// im cho tới khi có ai đó xong. Nó đủ cho trường hợp thật hay gặp, tức là một job lẻ phải chờ một
/// thứ ngắn, và không đủ để thay cho việc gửi hẳn công việc sang [`LaneId::Async`]. Chờ lâu thì
/// dùng [`Lanes::run_blocking`], còn nếu chỗ gọi viết được thành async thì [`Lanes::spawn_async`]
/// mới là đường đúng: ở đó chờ không tốn thread nào.
pub fn block_in_place<F, R>(pool: &ThreadPool, f: F) -> R
where F: FnOnce() -> R
{
    pool.wake_one();
    f()
}
