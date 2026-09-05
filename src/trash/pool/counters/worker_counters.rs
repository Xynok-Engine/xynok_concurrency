use crate::pool::counters::counters::Counters;
use crate::sync::{AtomicU64, Ordering};

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

    pub(crate) fn snapshot(&self) -> Counters
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
