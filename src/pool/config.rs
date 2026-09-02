//! Cấu hình dựng pool.

use crate::apis::priority::Priority;
use crate::utils::available_cores;

#[cfg(doc)] use super::ThreadPool;
#[cfg(doc)] use crate::utils::backoff::Backoff;

/// Cách dựng một pool. Xem [`ThreadPool::new`].
#[derive(Debug, Clone)]
pub struct Config
{
    /// Số thread worker sẽ spawn.
    ///
    /// Đây **không** phải tổng số thread chạy job: thread gọi vào pool cũng chạy job cùng chúng,
    /// nên tổng là `threads + 1`. Mặc định là `cores - 1` đúng vì lý do đó.
    ///
    /// `0` là hợp lệ và có nghĩa mọi job chạy ngay trên thread gọi. Xem [`ThreadPool::new`].
    pub threads:       usize,
    /// Số ô của **mỗi** ring local. Được làm tròn lên luỹ thừa hai.
    ///
    /// Ring đầy không phải lỗi: chủ xả nửa cũ xuống lane queue rồi push tiếp. Nên con số này đổi
    /// chác giữa bộ nhớ và số lần phải xả.
    pub ring_capacity: usize,
    /// Số byte arena nháp cho **mỗi** người tham gia. Xem [`ThreadPool::scratch`].
    pub scratch_bytes: usize,
    /// Bao nhiêu vòng tìm việc hụt trước khi một worker chịu đi ngủ.
    ///
    /// Đánh thức một thread đang park tốn một cặp syscall cộng một lần chuyển ngữ cảnh, và trên
    /// máy có core P và core E thì còn tốn hơn thế: thread vừa dậy không chắc quay lại đúng loại
    /// core nó vừa rời. Pool nào ngủ quá nhanh thì trả cái giá đó ở mỗi đợt việc mới. Ngược lại,
    /// giữ nó cao thì một máy đang rảnh vẫn có core quay tại chỗ.
    ///
    /// Đơn vị là **vòng của vòng lặp worker**, không phải bậc backoff. Hai thứ đó từng bị lẫn với
    /// nhau ở đây: [`Backoff`] chặn bậc của nó ở [`YIELD_LIMIT`](crate::apis::consts::YIELD_LIMIT)
    /// cộng một, nên hồi so với bậc thì mọi giá trị lớn hơn 11 đều có đúng một nghĩa, là "không bao
    /// giờ ngủ". `lanes` và `task` đặt `1` nên chúng vẫn chạy đúng như trước; con số mặc định thì
    /// không, và [`Config::default`] nói nó được chọn lại thế nào.
    pub spin_rounds:   u32,
    /// Tiền tố tên thread, để nhìn ra chúng trong debugger hoặc profiler.
    pub thread_name:   String,
    /// Standing mà pool xin OS cho thread của mình. Xem [`Priority`].
    pub priority:      Priority,
}

impl Default for Config
{
    /// Một worker cho mỗi core, trừ một core để lại cho thread gọi.
    ///
    /// `available_parallelism` đếm core **logic**, nên trên x86 có siêu phân luồng thì đây là đếm
    /// dư. Với việc nặng bộ nhớ, thread thứ hai trên cùng một core gần như không mua được gì. Chỗ
    /// này đáng chỉnh lại theo từng nền tảng khi đã có số đo, chứ đoán ở đây thì tệ hơn.
    ///
    /// # Vì sao `spin_rounds` lớn đến vậy
    ///
    /// Vì đo ra thế, và vì con số cũ chưa bao giờ là một lựa chọn. Nó là `40` khi điều kiện đi ngủ
    /// còn so với bậc backoff, mà bậc đó bão hoà ở 11, nên `40` có nghĩa là worker không bao giờ
    /// ngủ. Sửa lại cách đếm mà giữ nguyên `40` thì hoá ra là đổi mặc định từ "không bao giờ ngủ"
    /// sang "ngủ sau ~40 vòng", và đó là một thay đổi hiệu năng chứ không phải một bản vá: một pass
    /// 1M entity trên 3 worker chậm đi 21%, từ 188 xuống 227 micro giây, đo xen kẽ với bevy để
    /// chắc không phải máy đang trôi.
    ///
    /// `2048` giữ lại cái mà con số cũ vô tình mua được, mà không phải trả bằng "vĩnh viễn": mỗi
    /// vòng hụt tốn cỡ 150 tới 200 nano giây, nên nó là khoảng ba tới bốn trăm micro giây quay tại
    /// chỗ. Đủ dài để một worker còn nóng khi hệ thống kế tiếp trong cùng một frame đẩy việc ra,
    /// đủ ngắn để giữa hai frame, hoặc lúc app dừng, core được trả lại thay vì quay không tới hết
    /// đời tiến trình.
    fn default() -> Self
    {
        Self {
            threads:       available_cores().saturating_sub(1),
            ring_capacity: 256,
            scratch_bytes: 1 << 20,
            spin_rounds:   2048,
            thread_name:   "xynok-worker".to_string(),
            priority:      Priority::Frame,
        }
    }
}

impl Config
{
    /// [`Self::default`], với `XYNOK_LANE_THREADS` đè lên số thread.
    ///
    /// Biến này là **tổng** số thread chạy job, tính cả thread gọi, nên `XYNOK_LANE_THREADS=1`
    /// nghĩa là không spawn worker nào và mọi job chạy inline. Đó là thứ đầu tiên nên thử khi một
    /// con bug xuất hiện trong game: nó trả lời câu "cái này có phải do đa luồng không" trong một
    /// lần chạy, không phải sửa dòng code nào.
    pub fn from_env() -> Self
    {
        let mut config = Self::default();

        if let Ok(raw) = std::env::var("XYNOK_LANE_THREADS")
            && let Ok(total) = raw.trim().parse::<usize>()
        {
            config.threads = total.saturating_sub(1);
        }

        config
    }

    /// Pool chạy inline: không thread nào được spawn, job chạy ngay trên thread gọi.
    pub fn inline() -> Self
    {
        Self { threads: 0, ..Self::default() }
    }
}
