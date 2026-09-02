use crate::lane_queue::local_queue::LocalQueue;

/// Nạp thêm bao nhiêu vào ring là vừa.
///
/// Con số này **không tính job chạy ngay**: nó được rút ra trước, và nó không chiếm ô nào của ring.
/// Nên tổng số job rời hàng đợi trong một lượt là `1 + batch_size(...)`.
///
/// Ba cái chặn, lấy cái nhỏ nhất:
///
/// - `len / workers + 1`: phần chia đều cho mọi worker của lane, cộng một để không bao giờ ra 0 khi
///   còn việc.
/// - `capacity / 2`: chừa nửa ring trống cho job mà chính bạn sắp spawn ra. Nạp đầy ring rồi thì
///   job con đầu tiên đã phải spill ngược xuống hàng đợi lane.
/// - `remaining`: chỗ trống thật sự còn lại. Chủ ring là người duy nhất được push và đang là bạn,
///   nên con số này chỉ có thể tăng, không thể tụt xuống dưới lưng bạn.
#[inline]
pub(crate) fn batch_size<T, Q: LocalQueue<T>>(len: usize, workers: usize, dst: &Q) -> usize
{
    let share = len / workers.max(1) + 1;
    share.min(dst.capacity() / 2).min(dst.remaining())
}
