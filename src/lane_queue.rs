//! Hàng đợi chung của một lane: chỗ job từ ngoài pool rơi vào, và chỗ worker xả bớt khi ring local
//! đầy.
//!
//! [`docs/lane_queue.md`](../docs/lane_queue.md) nói vì sao nó phải tồn tại. Tóm lại là hai lỗ hổng
//! mà ring local không tự bịt được: thread không phải worker thì không sở hữu ring nào để push, và
//! ring thì có biên còn công việc thì không.
//!
//! Bản này dựng trên [`QueueBatching`], tức là một `Queue` dưới spinlock. Nó **không** lock-free,
//! và đó là lựa chọn có chủ ý cho giai đoạn này: mỗi lane giữ một hàng đợi riêng nên tranh chấp vốn
//! đã thấp, còn một cấu trúc mình hiểu rõ thì sửa được lúc 2 giờ sáng. Mục 8.1 của
//! [`docs/lanes.md`](../docs/lanes.md) ghi lại bản block lock-free và khi nào nên quay lại nó.
//!
//! Thứ quan trọng nhất ở đây không phải hàng đợi mà là **cách lấy ra**: luôn theo cụm. `LaneQueue`
//! là điểm dùng chung của mọi thread trong lane, nên lấy một job mỗi lượt nghĩa là mỗi job phải trả
//! một lần tranh chấp trên cùng một cache line. Lấy 32 cái thì trả giá một lần, rồi 31 job sau chạy
//! từ ring local và không đụng vào ai.

use crate::sync::{AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::queue_batching::QueueBatching;

/// Ring local nhìn từ phía [`LaneQueue`].
///
/// Hai ring của crate ([`ring_buffer_fifo`](crate::ring_buffer_fifo) và
/// [`ring_buffer_lifo`](crate::ring_buffer_lifo)) có cùng bốn hàm này với cùng ý nghĩa, nên
/// `LaneQueue` không cần biết mình đang nạp vào loại nào. Pool chọn loại ring, `LaneQueue` chỉ đổ
/// job vào.
pub trait LocalQueue<T>
{
    /// Tổng số ô, cố định từ lúc khởi tạo.
    fn capacity(&self) -> usize;
    /// Số ô còn trống. Chỉ có thể tăng khi không ai khác đang push, và chủ ring là người duy nhất
    /// được push, nên con số này an toàn để chia cụm dựa trên nó.
    fn remaining(&self) -> usize;
    fn push(&mut self, val: T) -> Result<(), T>;
    /// Ghi cả cụm rồi publish **một lần**. Đây là lý do `LaneQueue` không cần `Vec` trung gian.
    fn push_iter<I: IntoIterator<Item = T>>(&mut self, vals: I) -> usize;
}

macro_rules! impl_local_queue {
    ($ring:path) => {
        impl<T> LocalQueue<T> for $ring
        {
            #[inline]
            fn capacity(&self) -> usize
            {
                Self::capacity(self)
            }
            #[inline]
            fn remaining(&self) -> usize
            {
                Self::remaining(self)
            }
            #[inline]
            fn push(&mut self, val: T) -> Result<(), T>
            {
                Self::push(self, val)
            }
            #[inline]
            fn push_iter<I: IntoIterator<Item = T>>(&mut self, vals: I) -> usize
            {
                Self::push_iter(self, vals)
            }
        }
    };
}

impl_local_queue!(crate::ring_buffer_fifo::Producer<'_, T>);
impl_local_queue!(crate::ring_buffer_lifo::Producer<'_, T>);

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
        queue.enqueue(val);
        self.length.store(queue.len(), Ordering::Relaxed);
    }

    /// Đẩy cả cụm bằng một lần giành khoá. Đây là đường mà `spill_half` đi khi ring local đầy.
    pub fn push_batch<I>(&self, vals: I)
    where I: IntoIterator<Item = T>
    {
        let mut queue = self.queue.get();
        queue.enqueue_batch(vals);
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
        let val = queue.dequeue();
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
        let taken = queue.dequeue_batch(max, out);
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
        let first = queue.dequeue()?;

        let want = batch_size(queue.len(), workers, dst);
        if want > 0
        {
            // `from_fn` giữ cho việc rút ra lười: `push_iter` chỉ gọi `dequeue` đúng số lần nó thực
            // sự ghi được, nên không có job nào bị rút ra rồi phải nhét ngược lại.
            dst.push_iter(std::iter::from_fn(|| queue.dequeue()).take(want));
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
fn batch_size<T, Q: LocalQueue<T>>(len: usize, workers: usize, dst: &Q) -> usize
{
    let share = len / workers.max(1) + 1;
    share.min(dst.capacity() / 2).min(dst.remaining())
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

#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::Arc;

    use super::*;
    use crate::ring_buffer_fifo::RingBufferFifo;
    use crate::ring_buffer_lifo::RingBufferLifo;

    #[test]
    fn push_pop_giu_dung_thu_tu_va_do_dai()
    {
        let lane_queue = LaneQueue::new();
        assert!(lane_queue.is_empty());
        assert_eq!(lane_queue.pop(), None, "hàng đợi rỗng mà lấy ra được thì có gì đó rất sai");

        for i in 0..5
        {
            lane_queue.push(i);
        }
        assert_eq!(lane_queue.len(), 5);

        // FIFO: job vào trước ra trước, vì job trong hàng đợi thường già hơn và nên chạy trước.
        for i in 0..5
        {
            assert_eq!(lane_queue.pop(), Some(i));
        }
        assert!(lane_queue.is_empty());
    }

    #[test]
    fn steal_batch_ton_trong_max()
    {
        let lane_queue = LaneQueue::new();
        lane_queue.push_batch(0..100);
        assert_eq!(lane_queue.len(), 100);

        let mut out = Vec::new();
        assert_eq!(lane_queue.steal_batch(&mut out, 30), 30);
        assert_eq!(lane_queue.len(), 70);
        assert_eq!(out, (0..30).collect::<Vec<_>>());

        assert_eq!(lane_queue.steal_batch(&mut out, 0), 0, "max = 0 thì không được chạm vào khoá");
        assert_eq!(lane_queue.drain_into(&mut out), 70);
        assert!(lane_queue.is_empty());
        assert_eq!(out.len(), 100);
    }

    /// Hình dạng chính của đường ra: một job về tay người gọi để chạy ngay, phần còn lại nằm sẵn
    /// trong ring local và không phải đụng vào hàng đợi lần nữa.
    #[test]
    fn steal_batch_and_pop_giu_mot_nap_phan_con_lai_vao_ring()
    {
        let lane_queue = LaneQueue::new();
        lane_queue.push_batch(0..100);

        let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(64);
        let (mut owner, _) = ring.split();

        // Job chạy ngay được rút ra trước, còn lại 99. Phần nạp vào ring là 99/10 + 1 = 10, dưới
        // cả hai cái chặn kia.
        let first = lane_queue.steal_batch_and_pop(&mut owner, 10).expect("hàng đợi đang có 100 job");
        assert_eq!(first, 0, "job già nhất phải là job chạy ngay");

        let mut drained = Vec::new();
        assert_eq!(owner.drain(&mut drained), 10);
        assert_eq!(drained, (1..11).collect::<Vec<_>>());
        assert_eq!(lane_queue.len(), 100 - 11, "11 job rời hàng đợi: 1 chạy ngay, 10 vào ring");
    }

    #[test]
    fn khong_bao_gio_nap_qua_nua_ring()
    {
        let lane_queue = LaneQueue::new();
        lane_queue.push_batch(0..1000);

        let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(8);
        let (mut owner, _) = ring.split();

        // Một worker, ngàn job: phần chia ra là 1001, nhưng nửa ring mới là cái chặn thật.
        lane_queue.steal_batch_and_pop(&mut owner, 1).expect("hàng đợi đang đầy");

        let mut drained = Vec::new();
        assert_eq!(owner.drain(&mut drained), 4, "phải chừa nửa ring cho job mà chính worker sắp spawn");
    }

    #[test]
    fn khong_nap_qua_cho_trong_con_lai()
    {
        let lane_queue = LaneQueue::new();
        lane_queue.push_batch(0..1000);

        let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(8);
        let (mut owner, _) = ring.split();
        for i in 100..107
        {
            owner.push(i).expect("ring 8 ô, đẩy 7 cái phải lọt");
        }
        assert_eq!(owner.remaining(), 1);

        lane_queue.steal_batch_and_pop(&mut owner, 1).expect("hàng đợi đang đầy");

        let mut drained = Vec::new();
        assert_eq!(owner.drain(&mut drained), 8, "7 job cũ cộng đúng 1 job vừa nạp");
        assert_eq!(lane_queue.len(), 1000 - 2);
    }

    #[test]
    fn ring_day_thi_chi_lay_mot_job_chay_ngay()
    {
        let lane_queue = LaneQueue::new();
        lane_queue.push_batch(0..50);

        let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(4);
        let (mut owner, _) = ring.split();
        for i in 0..4
        {
            owner.push(i).expect("ring 4 ô");
        }

        let first = lane_queue.steal_batch_and_pop(&mut owner, 1);
        assert_eq!(first, Some(0), "ring hết chỗ vẫn phải có job để chạy ngay");
        assert_eq!(lane_queue.len(), 49);
    }

    #[test]
    fn nap_duoc_vao_ca_ring_lifo()
    {
        let lane_queue = LaneQueue::new();
        lane_queue.push_batch(0..100);

        let mut ring: RingBufferLifo<i32> = RingBufferLifo::new(64);
        let (mut owner, _) = ring.split();

        let first = lane_queue.steal_batch_and_pop(&mut owner, 10).expect("hàng đợi đang có 100 job");
        assert_eq!(first, 0);

        let mut drained = Vec::new();
        assert_eq!(owner.drain(&mut drained), 10);
        assert_eq!(lane_queue.len(), 89);
    }

    #[test]
    fn lane_queue_rong_thi_khong_lay_duoc_gi()
    {
        let lane_queue: LaneQueue<i32> = LaneQueue::new();
        let mut ring: RingBufferFifo<i32> = RingBufferFifo::new(16);
        let (mut owner, _) = ring.split();

        assert_eq!(lane_queue.steal_batch_and_pop(&mut owner, 4), None);
        assert_eq!(owner.remaining(), 16, "không có gì để lấy thì cũng không được chạm vào ring");
    }

    /// Trần cứng ở đây là số job cố định chia sẵn cho từng thread. Vòng gom không trần đã từng ăn
    /// hết RAM khi cấu trúc bên dưới hỏng, nên mọi thứ ở đây đều đếm được từ trước.
    #[test]
    fn nhieu_thread_day_vao_khong_mat_job()
    {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 2_000;
        const TOTAL: usize = THREADS * PER_THREAD;

        let lane_queue = Arc::new(LaneQueue::new());

        std::thread::scope(|scope| {
            for t in 0..THREADS
            {
                let lane_queue = Arc::clone(&lane_queue);
                scope.spawn(move || {
                    for i in 0..PER_THREAD
                    {
                        match i % 3
                        {
                            0 => lane_queue.push(t * PER_THREAD + i),
                            _ => lane_queue.push_batch(std::iter::once(t * PER_THREAD + i)),
                        }
                    }
                });
            }
        });

        assert_eq!(lane_queue.len(), TOTAL, "bộ đếm không khoá lệch so với số job đã đẩy vào");

        let mut seen = vec![false; TOTAL];
        let mut out = Vec::with_capacity(TOTAL);
        assert_eq!(lane_queue.drain_into(&mut out), TOTAL);
        for value in out
        {
            assert!(!seen[value], "job {value} ra khỏi hàng đợi hai lần");
            seen[value] = true;
        }
        assert!(seen.into_iter().all(|s| s), "có job đẩy vào mà không bao giờ ra");
        assert!(lane_queue.is_empty());
    }

    /// Nhiều worker cùng rút, mỗi người một ring riêng: không job nào chạy hai lần, không job nào
    /// biến mất.
    #[test]
    fn nhieu_worker_cung_rut_khong_trung_khong_mat()
    {
        const WORKERS: usize = 4;
        const TOTAL: usize = 20_000;

        let lane_queue = Arc::new(LaneQueue::new());
        lane_queue.push_batch(0..TOTAL);

        let taken: Vec<Vec<usize>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..WORKERS)
                .map(|_| {
                    let lane_queue = Arc::clone(&lane_queue);
                    scope.spawn(move || {
                        let mut ring: RingBufferFifo<usize> = RingBufferFifo::new(64);
                        let (mut owner, _) = ring.split();
                        let mut mine = Vec::new();

                        // Trần cứng: nhiều nhất TOTAL vòng, nên một hàng đợi hỏng làm test *fail*
                        // chứ không làm máy hết RAM.
                        for _ in 0..TOTAL
                        {
                            match lane_queue.steal_batch_and_pop(&mut owner, WORKERS)
                            {
                                Some(job) => mine.push(job),
                                None => break,
                            }
                            while let Some(job) = owner.pop()
                            {
                                mine.push(job);
                            }
                        }
                        owner.drain(&mut mine);
                        mine
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().expect("worker panic")).collect()
        });

        assert!(lane_queue.is_empty(), "còn {} job kẹt lại trong hàng đợi", lane_queue.len());

        let mut seen = vec![false; TOTAL];
        for job in taken.into_iter().flatten()
        {
            assert!(!seen[job], "job {job} bị hai worker cùng nhận");
            seen[job] = true;
        }
        assert!(seen.into_iter().all(|s| s), "có job không worker nào nhận");
    }
}
