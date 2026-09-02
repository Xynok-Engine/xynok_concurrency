use crate::pool::counters::counters::Counters;
use crate::pool::counters::worker_counters::WorkerCounters;
use crate::utils::cache_padded::CachePadded;

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
