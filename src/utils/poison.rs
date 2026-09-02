/// Lấy dữ liệu trong khoá std kể cả khi khoá đã bị nhiễm độc vì có thread panic lúc đang giữ nó.
///
/// Ở đây mọi chỗ dùng khoá đều tự giữ dữ liệu ở trạng thái hợp lệ trước khi nhả, nên một cú panic
/// của thread khác không có lý do gì để làm hỏng phần còn lại của pool.
#[inline]
pub fn ignore_poison<G>(result: std::sync::LockResult<G>) -> G
{
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}
