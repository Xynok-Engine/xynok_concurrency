use crate::lane_queue::batch_size::batch_size;
use crate::lane_queue::local_queue::LocalQueue;
use crate::sync::{AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::queue_batching::QueueBatching;

/// Hàng đợi không giới hạn, nhiều người ghi nhiều người đọc, dùng chung cho cả một lane.
pub struct LaneQueue<T>
{
    queue:  QueueBatching<T>,
    /// Bản sao độ dài đọc được mà không cần giành khoá.
    ///
    /// Mọi thao tác đổi hàng đợi đều cập nhật nó *trong lúc còn giữ khoá*, nên tại thời điểm khoá
    /// được nhả nó luôn đúng. Người đọc thấy giá trị cũ vài nhịp là chuyện bình thường: họ chỉ dùng
    /// nó để quyết định có bõ công giành khoá hay không, và nếu đoán sai thì lần giành khoá tiếp
    /// theo nói sự thật.
    ///
    /// Một chỗ *không* được phép dựa vào nó: quyết định cho worker đi ngủ. Đọc ra `0` rồi park có
    /// thể bỏ lỡ một job vừa được đẩy vào. Chống lost wakeup là việc của giao thức ngủ trong pool,
    /// không phải của con số này.
    length: CachePadded<AtomicUsize>,
}

impl<T> LaneQueue<T>
{
    pub fn new() -> Self
    {
        Self {
            queue:  QueueBatching::new(),
            length: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Cấp sẵn chỗ cho `capacity` job. `LaneQueue` vẫn không giới hạn, đây chỉ là tránh vài lần
    /// realloc đầu tiên.
    pub fn with_capacity(capacity: usize) -> Self
    {
        Self {
            queue:  QueueBatching::with_capacity(capacity),
            length: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Số job đang chờ, đọc không cần khoá. Xem ghi chú ở [`Self::length`].
    #[inline]
    pub fn len(&self) -> usize
    {
        self.length.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.len() == 0
    }
}

impl<T> LaneQueue<T>
{
    /// Đẩy một job vào. Không bao giờ từ chối, đó là toàn bộ lý do `LaneQueue` tồn tại.
    pub fn push(&self, val: T)
    {
        let mut queue = self.queue.get();
        queue.push_back(val);
        self.length.store(queue.len(), Ordering::Relaxed);
    }

    /// Đẩy cả cụm bằng một lần giành khoá. Đây là đường mà `spill_half` đi khi ring local đầy.
    pub fn push_batch<I>(&self, vals: I)
    where I: IntoIterator<Item = T>
    {
        let mut queue = self.queue.get();
        queue.extend(vals);
        self.length.store(queue.len(), Ordering::Relaxed);
    }

    /// Lấy đúng một job. Có chỗ cần nó thật (worker kiểm lại lần cuối trước khi ngủ), nhưng nếu bạn
    /// đang gọi nó trong vòng lặp thì thứ bạn muốn là [`Self::steal_batch`].
    pub fn pop(&self) -> Option<T>
    {
        if self.is_empty()
        {
            return None;
        }

        let mut queue = self.queue.get();
        let val = queue.pop_front();
        self.length.store(queue.len(), Ordering::Relaxed);
        val
    }

    /// Rút tối đa `max` job ra một `Vec`.
    ///
    /// Dành cho người không sở hữu ring nào: thread ngoài đang chạy giúp pool, hoặc test. Worker
    /// thật thì dùng [`Self::steal_batch_and_pop`] để job đi thẳng vào ring, khỏi qua `Vec` trung
    /// gian.
    pub fn steal_batch(&self, out: &mut Vec<T>, max: usize) -> usize
    {
        if max == 0 || self.is_empty()
        {
            return 0;
        }

        let mut queue = self.queue.get();
        let taken = max.min(queue.len());
        out.extend(queue.drain(..taken));
        self.length.store(queue.len(), Ordering::Relaxed);
        taken
    }

    /// Đường ra chính: giữ một job để chạy ngay, đổ phần còn lại thẳng vào ring local.
    ///
    /// `workers` là số worker của lane, dùng để chia phần. Không có nó thì worker đầu tiên tới hốt
    /// sạch hàng đợi và những worker sau vẫn đói, dù nhìn vào tổng thì có thừa việc cho tất cả.
    ///
    /// Trả `None` khi hàng đợi rỗng. Trả `Some(job)` thì job đó là của bạn, chạy nó ngay, phần đã
    /// nạp vào ring sẽ được chính bạn hoặc kẻ trộm lấy sau.
    pub fn steal_batch_and_pop<Q>(&self, dst: &mut Q, workers: usize) -> Option<T>
    where Q: LocalQueue<T>
    {
        if self.is_empty()
        {
            return None;
        }

        let mut queue = self.queue.get();
        // Rỗng thật (ai đó vừa vét sạch giữa lúc mình đọc `len` và lúc giành được khoá): nhả khoá
        // và về tay không, đúng như khi đọc `len` thấy 0.
        let first = queue.pop_front()?;

        let want = batch_size(queue.len(), workers, dst);
        if want > 0
        {
            // `from_fn` giữ cho việc rút ra lười: `push_iter` chỉ gọi `pop_front` đúng số lần nó thực
            // sự ghi được, nên không có job nào bị rút ra rồi phải nhét ngược lại.
            dst.push_iter(std::iter::from_fn(|| queue.pop_front()).take(want));
        }

        self.length.store(queue.len(), Ordering::Relaxed);
        Some(first)
    }

    /// Vét sạch. Dùng lúc shutdown, để job đang xếp hàng vẫn được chạy chứ không bị bỏ.
    pub fn drain_into(&self, out: &mut Vec<T>) -> usize
    {
        self.steal_batch(out, usize::MAX)
    }
}

impl<T> Default for LaneQueue<T>
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl<T> std::fmt::Debug for LaneQueue<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("LaneQueue").field("len", &self.len()).finish_non_exhaustive()
    }
}
