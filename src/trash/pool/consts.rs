use std::time::Duration;

/// Cứ bấy nhiêu vòng thì worker ngó hàng đợi chung trước cả ring của mình.
///
/// Số nguyên tố, và không phải để cho đẹp: một hằng chia hết cho số worker, hoặc chia hết cho nhịp
/// đẻ việc của một thuật toán chia đôi, sẽ khiến nhiều worker cùng ngó hàng đợi chung đúng một lúc
/// rồi cùng giành một cái khoá. Số nguyên tố đủ lớn thì các worker rải đều ra.
pub const LANE_QUEUE_TICK: u32 = 61;

/// Một giấc ngủ ngắn của thread đang chờ mà pool thì hết việc.
///
/// Ngắn có chủ ý: nó là hạn chót cho trường hợp xấu nhất, tức là khi thứ đang được chờ hoàn thành
/// mà không gọi ai dậy. Đường bình thường thì điểm hẹn gọi dậy ngay.
pub(crate) const IDLE_NAP: Duration = Duration::from_micros(50);
