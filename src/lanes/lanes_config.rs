use crate::apis::priority::Priority;
use crate::pool::Config;
use crate::utils::cores::available_cores;

/// Cách dựng cả bộ lane.
#[derive(Debug, Clone)]
pub struct LanesConfig
{
    pub compute:    Config,
    pub async_lane: Config,
}

impl Default for LanesConfig
{
    /// Lane compute ăn gần hết core, lane async chỉ vài thread và priority thấp.
    ///
    /// Lane async cố ý **không** co giãn theo số core: nó không tồn tại để chạy nhanh mà để giữ cho
    /// việc chờ khỏi chiếm core của frame. Số thread ở đây là số syscall chặn chạy song song được,
    /// chứ không phải số task: task đang `.await` thì không nằm trên thread nào. Hai tới bốn là đủ
    /// cho vài file lớn đọc cùng lúc, và nhiều hơn thế chỉ tổ làm ổ đĩa phải nhảy đầu đọc.
    fn default() -> Self
    {
        Self {
            compute:    Config {
                thread_name: "xynok-compute".to_string(),
                priority: Priority::Frame,
                ..Config::default()
            },
            async_lane: Config {
                threads:       available_cores().clamp(2, 4),
                ring_capacity: 64,
                scratch_bytes: 0,
                thread_name:   "xynok-async".to_string(),
                priority:      Priority::Io,
                // Thread ở đây phần lớn thời gian là chờ, nên quay tại chỗ chờ việc là đốt core của
                // lane compute. Ngủ sớm.
                spin_rounds:   1,
            },
        }
    }
}

impl LanesConfig
{
    /// [`Self::default`], với `XYNOK_LANE_THREADS` và `XYNOK_ASYNC_THREADS` đè lên số thread.
    ///
    /// Đọc từ env để tune được mà không phải build lại: đổi số thread rồi chạy lại game là một vòng
    /// lặp vài giây, còn build lại engine thì không.
    pub fn from_env() -> Self
    {
        let mut config = Self::default();
        config.compute.threads = Config::from_env().threads;

        if let Ok(raw) = std::env::var("XYNOK_ASYNC_THREADS")
            && let Ok(threads) = raw.trim().parse::<usize>()
        {
            config.async_lane.threads = threads;
        }

        config
    }
}
