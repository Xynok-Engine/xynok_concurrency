/// Ảnh chụp các bộ đếm, và cách đọc từng con số.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters
{
    /// Số job đã chạy.
    pub jobs_run:     u64,
    /// Số lần đi trộm và mang được việc về.
    pub steal_hits:   u64,
    /// Số lần đi trộm về tay không.
    ///
    /// Tỷ lệ trượt cao nghĩa là worker tốn thời gian sờ vào ring của người khác mà không được gì.
    /// Thường là dấu hiệu việc bị chia quá nhỏ, hoặc số worker nhiều hơn lượng việc thật sự có.
    pub steal_misses: u64,
    /// Số lần lấy được việc từ lane queue.
    pub lane_pops:    u64,
    /// Số lần ring local đầy và phải xả nửa cũ xuống lane queue.
    ///
    /// Con số này lớn nghĩa là ring quá nhỏ so với nhịp đẻ job. Nó không sai, chỉ là mỗi lần xả là
    /// một lần chạm khoá của lane queue, mà lẽ ra job đó có thể ở lại chỗ nóng.
    pub spills:       u64,
    /// Số lần worker đi ngủ.
    ///
    /// Nhiều lần park **giữa** frame là dấu hiệu frame chưa được chia đủ nhỏ để nuôi hết pool: đánh
    /// thức một thread tốn một cặp syscall, và trả cái giá đó nhiều lần trong một frame thì đáng để
    /// xem lại cách chia việc.
    pub parks:        u64,
    /// Số job đang nằm chờ trong lane queue tại thời điểm chụp.
    ///
    /// Luôn dương và không tụt xuống nghĩa là worker đang bị bỏ đói hoặc lane queue đang bị nghẽn.
    pub queued:       usize,
}

impl Counters
{
    /// Tỷ lệ trộm trúng trong `0.0..=1.0`. Trả `None` khi chưa có lần trộm nào.
    pub fn steal_hit_rate(&self) -> Option<f64>
    {
        let attempts = self.steal_hits + self.steal_misses;
        match attempts
        {
            0 => None,
            _ => Some(self.steal_hits as f64 / attempts as f64),
        }
    }

    /// Cộng ảnh chụp của một worker khác vào đây.
    pub(crate) fn merge(&mut self, other: Counters)
    {
        self.jobs_run += other.jobs_run;
        self.steal_hits += other.steal_hits;
        self.steal_misses += other.steal_misses;
        self.lane_pops += other.lane_pops;
        self.spills += other.spills;
        self.parks += other.parks;
    }
}
