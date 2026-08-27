//! Bộ đếm của pool: không có số thì không tune được.
//!
//! Mọi hằng số trong crate này đều là một phỏng đoán có lý do: cỡ ring, số worker, con số 61 của
//! luật fairness, trần 50% người lùng việc, ngưỡng chia job 20 micro giây. Phỏng đoán có lý do vẫn
//! là phỏng đoán, và cách duy nhất để biến nó thành một con số đúng cho **máy này, tải này** là đo.
//!
//! # Vì sao mỗi worker một bộ đếm riêng
//!
//! Một bộ đếm dùng chung cho cả pool thì mỗi lần cộng là một lần tranh chấp trên đúng cái cache line
//! mà mọi worker đều chạm. Đo mà làm chậm thứ đang đo thì con số đọc ra không nói về hệ thống thật
//! nữa. Nên mỗi worker cộng vào ô của riêng nó, mỗi ô một cache line, và phép cộng lại chỉ xảy ra
//! khi có ai đó hỏi.

use crate::sync::{AtomicU64, Ordering};
use crate::utils::cache_padded::CachePadded;

/// Bộ đếm của một worker. Chỉ chủ của nó ghi, ai cũng đọc được.
#[derive(Debug, Default)]
pub(crate) struct WorkerCounters
{
    jobs_run:     AtomicU64,
    steal_hits:   AtomicU64,
    steal_misses: AtomicU64,
    lane_pops:    AtomicU64,
    spills:       AtomicU64,
    parks:        AtomicU64,
}

impl WorkerCounters
{
    /// Cộng một vào một ô.
    ///
    /// `Relaxed` là đủ và là cố ý: không có gì được công bố qua những con số này, chúng chỉ để đọc
    /// ra và nhìn. Trả giá một `Release` ở đây là trả cho một bảo đảm không ai dùng tới.
    #[inline]
    fn bump(slot: &AtomicU64)
    {
        slot.store(slot.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn job_run(&self)
    {
        Self::bump(&self.jobs_run);
    }

    #[inline]
    pub(crate) fn steal_hit(&self)
    {
        Self::bump(&self.steal_hits);
    }

    #[inline]
    pub(crate) fn steal_miss(&self)
    {
        Self::bump(&self.steal_misses);
    }

    #[inline]
    pub(crate) fn lane_pop(&self)
    {
        Self::bump(&self.lane_pops);
    }

    #[inline]
    pub(crate) fn spill(&self)
    {
        Self::bump(&self.spills);
    }

    #[inline]
    pub(crate) fn park(&self)
    {
        Self::bump(&self.parks);
    }

    fn snapshot(&self) -> Counters
    {
        Counters {
            jobs_run:     self.jobs_run.load(Ordering::Relaxed),
            steal_hits:   self.steal_hits.load(Ordering::Relaxed),
            steal_misses: self.steal_misses.load(Ordering::Relaxed),
            lane_pops:    self.lane_pops.load(Ordering::Relaxed),
            spills:       self.spills.load(Ordering::Relaxed),
            parks:        self.parks.load(Ordering::Relaxed),
            queued:       0,
        }
    }
}

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

/// Bộ đếm của cả pool, một ô cho mỗi người tham gia.
#[derive(Debug)]
pub(crate) struct PoolCounters
{
    workers: Box<[CachePadded<WorkerCounters>]>,
}

impl PoolCounters
{
    pub(crate) fn new(participants: usize) -> Self
    {
        Self {
            workers: (0..participants).map(|_| CachePadded::new(WorkerCounters::default())).collect(),
        }
    }

    #[inline]
    pub(crate) fn of(&self, index: usize) -> &WorkerCounters
    {
        &self.workers[index]
    }

    /// Ảnh chụp của một người tham gia.
    pub(crate) fn snapshot_of(&self, index: usize) -> Counters
    {
        self.workers[index].snapshot()
    }

    /// Tổng của cả pool. Không phải một ảnh chụp nguyên tử: các ô được đọc lần lượt, nên tổng có thể
    /// gồm một worker đọc sớm và một worker đọc muộn. Với việc tune thì sai lệch đó không đáng kể,
    /// còn muốn con số khít thì đọc lúc pool đang rảnh.
    pub(crate) fn total(&self) -> Counters
    {
        let mut total = Counters::default();
        for worker in &self.workers
        {
            total.merge(worker.snapshot());
        }
        total
    }
}
