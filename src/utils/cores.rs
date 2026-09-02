/// Số luồng phần cứng mà máy đang cho phép chạy song song.
///
/// Hỏi được thì trả về con số thật, hỏi không được thì trả về 1 để nơi gọi cứ thế chạy tiếp thay
/// vì phải xử lỗi.
#[inline]
pub fn available_cores() -> usize
{
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}
