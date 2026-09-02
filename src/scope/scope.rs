use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::custom_type::Job;
use crate::job_graph::job_handle::JobHandle;
use crate::job_graph::node::Node;
use crate::latch::Latch;
use crate::pool::Shared;
use crate::scope::chunk_slots::ChunkSlots;
use crate::scope::panic_sink::{PanicSink, PanicSlot};
use crate::scope::params::ParamsParReduce;
use crate::scope::scope_in::scope_in;
use crate::scope::scope_ptr::ScopePtr;
use crate::sync::cell::UnsafeCell;
use crate::sync::{Arc, AtomicUsize, Mutex, Ordering};
use crate::utils::poison::ignore_poison;

/// Một vùng mà job spawn ra được phép mượn stack của người mở nó.
///
/// Job bình thường phải `'static`. Trong một scope thì không: scope không trả về cho tới khi mọi
/// job của nó đã xong, **kể cả khi đang unwind vì panic**, nên tham chiếu mà job mượn chắc chắn còn
/// sống suốt thời gian job có thể chạy.
pub struct Scope<'scope>
{
    /// Phần dùng chung của pool, **không** kèm đám join handle.
    ///
    /// Giữ nguyên một `ThreadPool` ở đây thì scope trở thành một tay cầm giữ cho pool sống, và một
    /// scope mở bên trong một job sẽ thả cái tay cầm ấy trên chính thread worker. Nếu nó là tay cầm
    /// cuối cùng thì `Drop` chạy `shutdown`, mà `shutdown` thì join mọi worker, kể cả worker đang
    /// chạy dòng lệnh đó. Miri gọi thẳng tên chuyện này ra: "trying to join itself".
    pub(crate) shared: Arc<Shared>,
    pub(crate) latch:  Latch,
    pub(crate) panic:  PanicSlot,
    /// Bất biến theo `'scope`, để tham chiếu mà job bắt được không bị co ngắn hay kéo dài ra khỏi
    /// lời hứa của scope.
    pub(crate) marker: PhantomData<&'scope mut &'scope ()>,
}

impl<'scope> Scope<'scope>
{
    /// Xếp `f` vào pool.
    ///
    /// Trả về ngay, job chạy ở đâu và lúc nào là việc của pool. Job được đếm **trước** khi nó có
    /// cơ hội chạy, nếu không thì điểm chờ có thể đọc thấy 0 đúng vào khoảnh khắc giữa lúc đẩy job
    /// và lúc cộng sổ.
    ///
    /// # Panics
    ///
    /// Không phải ở đây. Panic bên trong `f` được bắt lại và ném ra ở [`ThreadPool::scope`] bao
    /// ngoài, sau khi mọi job anh em đã xong.
    pub fn spawn<F>(&self, f: F)
    where F: FnOnce() + Send + 'scope
    {
        self.shared.inject(self.job(f));
    }

    /// Như [`Self::spawn`], nhưng job nhận lại chính scope để spawn tiếp vào đó.
    ///
    /// Đây là thứ mà mọi thuật toán chia đôi đệ quy cần. Không có nó thì mỗi tầng đệ quy phải mở một
    /// scope mới, mà mỗi scope mới là thêm một điểm join, tức là thêm một chỗ cả pool phải chờ anh
    /// đi sau cùng.
    ///
    /// ```
    /// use xynok_concurrency::pool::{Config, ThreadPool};
    /// use xynok_concurrency::scope::Scope;
    ///
    /// let pool = ThreadPool::new(Config {
    ///     threads: 3,
    ///     ..Default::default()
    /// });
    /// let counter = std::sync::atomic::AtomicUsize::new(0);
    ///
    /// fn split<'a>(s: &Scope<'a>, counter: &'a std::sync::atomic::AtomicUsize, depth: usize)
    /// {
    ///     counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    ///     if depth == 0
    ///     {
    ///         return;
    ///     }
    ///     for _ in 0..2
    ///     {
    ///         s.spawn_with(move |s| split(s, counter, depth - 1));
    ///     }
    /// }
    ///
    /// pool.scope(|s| split(s, &counter, 4));
    /// assert_eq!(counter.load(std::sync::atomic::Ordering::Acquire), 31);
    /// ```
    ///
    /// # Panics
    ///
    /// Không phải ở đây, giống [`Self::spawn`].
    pub fn spawn_with<F>(&self, f: F)
    where F: FnOnce(&Scope<'scope>) + Send + 'scope
    {
        let scope = ScopePtr(self as *const Scope<'scope>);
        self.spawn(move || {
            let scope = scope;
            // Safety: scope còn sống vì vé latch của chính job này chưa được thả.
            f(unsafe { &*scope.0 });
        });
    }

    /// Đóng gói `f` thành một job mà scope này đang đếm, và xoá lifetime của nó đi.
    pub(crate) fn job<F>(&self, f: F) -> Job
    where F: FnOnce() + Send + 'scope
    {
        let ticket = self.latch.ticket();
        let sink = PanicSink(&self.panic as *const PanicSlot);

        // Safety: job này chỉ mượn những thứ sống ít nhất bằng `'scope`, và
        // `ThreadPool::scope` không trả về khi bộ đếm latch chưa về 0, kể cả trên đường unwind.
        // Nên mọi tham chiếu `f` bắt được vẫn còn sống suốt thời gian job có thể chạy.
        unsafe {
            Job::new_unbound(move || {
                // Khai báo trước nên thả sau cùng, và thứ tự đó là bắt buộc: thả vé là báo "xong",
                // mà ngay sau tiếng "xong" cuối cùng thì người chờ được phép trả về và cái ô panic
                // trong scope biến mất. Ghi panic phải xảy ra trước.
                let _ticket = ticket;
                let sink = sink;

                if let Err(payload) = catch_unwind(AssertUnwindSafe(f))
                {
                    // Safety: scope còn sống vì vé chưa thả.
                    let slot = &*sink.0;
                    let mut first = ignore_poison(slot.lock());
                    // Cái đầu tiên thắng. Giữ cái sau nghĩa là chọn panic theo thứ tự job kết thúc,
                    // mà thứ tự đó thì lần chạy nào cũng khác.
                    if first.is_none()
                    {
                        *first = Some(payload);
                    }
                }
            })
        }
    }

    /// Số chỗ đứng của pool này, tức là số worker cộng thread gọi.
    #[inline]
    pub fn worker_count(&self) -> usize
    {
        self.shared.worker_count()
    }

    /// Số thread worker của pool này.
    #[inline]
    pub fn worker_threads(&self) -> usize
    {
        self.shared.worker_threads()
    }

    /// Phần dùng chung của pool, cho [`crate::job_graph`].
    #[inline]
    pub(crate) fn shared(&self) -> &Arc<Shared>
    {
        &self.shared
    }

    /// Số job của scope này còn chưa xong. Ảnh chụp, cho log và counter.
    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.latch.remaining()
    }

    /// Chạy `f` trên mọi chỉ số trong `0..n`, chia thành từng lô `batch` phần tử.
    ///
    /// # Vì sao phải tự nói `batch`
    ///
    /// Spawn một job không miễn phí: một lần đẩy vào ring, có thể một lần bị trộm (một CAS cộng một
    /// cache line bay từ core này sang core kia), một lần chạm latch lúc xong, có thể một lần gọi
    /// dậy. Tổng cỡ 1 tới 5 micro giây. Chia 1000 phần tử mà mỗi phần tử tốn 2 ns thì mỗi job chạy
    /// 250 ns cho một cái giá 2000 ns: chậm hơn hẳn so với cứ chạy tuần tự.
    ///
    /// Con số đáng nhắm là **20 micro giây một job**, đủ lớn để chi phí spawn chỉ chiếm chừng 10%,
    /// đủ nhỏ để mọi core vẫn chia đều được việc. Từ đó suy ngược ra `batch`: đo thấy mỗi phần tử
    /// tốn 200 ns thì `batch = 20µs / 200ns = 100`.
    ///
    /// `batch >= n` nghĩa là một job, tức là chạy tuần tự, và đó là một cách hợp lệ để tắt song
    /// song cho một chỗ cụ thể mà không phải sửa cấu trúc code.
    ///
    /// # Cách chia
    ///
    /// Không phải mỗi lô một job. Số job bằng số người tham gia, và các lô được phát ra qua một bộ
    /// đếm atomic: `fetch_add` một cái là xong cả thuật toán lập lịch. Với lô đều nhau, mà truy vấn
    /// ECS thì đúng là như vậy, cách này bám sát một deque work-stealing với một phần mười lượng
    /// code.
    pub fn parallel_for<F>(&self, n: usize, batch: usize, f: F)
    where F: Fn(usize) + Sync
    {
        let batch = batch.max(1);

        if n <= batch || self.worker_count() == 1
        {
            for i in 0..n
            {
                f(i);
            }
            return;
        }

        let chunks = n.div_ceil(batch);
        let cursor = AtomicUsize::new(0);

        let run = || {
            loop
            {
                let chunk = cursor.fetch_add(1, Ordering::Relaxed);
                if chunk >= chunks
                {
                    break;
                }

                let start = chunk * batch;
                for i in start..(start + batch).min(n)
                {
                    f(i);
                }
            }
        };

        // Nhiều nhất một người giúp cho mỗi worker, và không bao giờ nhiều hơn số lô có để phát.
        let helpers = self.worker_threads().min(chunks - 1);

        scope_in(&self.shared, |inner| {
            for _ in 0..helpers
            {
                inner.spawn(run);
            }
            // `spawn` đã gọi `notify` cho từng job rồi, nhưng `notify` im ngay khi thấy có người
            // đang lùng việc, nên cả đợt này chỉ đánh thức đúng một người và những người sau phải
            // chờ dây chuyền. Ở đây thì biết chắc có `helpers` phần việc rời nhau, nên gọi thẳng
            // đủ người. Xem `Sleep::notify_many`.
            inner.shared.wake_many(helpers);
            run();
        });
    }

    /// Gộp `0..n` thành một giá trị, song song, và kết quả không đổi giữa các lần chạy.
    ///
    /// Mỗi lô được gộp riêng, bắt đầu từ `identity()`, rồi các kết quả từng phần được nối lại bằng
    /// `join` **theo đúng thứ tự lô** sau khi mọi thứ đã xong.
    ///
    /// # Vì sao thứ tự nối là cố định
    ///
    /// Nối ngay khi kết quả về thì nhanh hơn một chút và bất định hơn rất nhiều: thread nào xong
    /// trước là chuyện của từng lần chạy, mà với số thực dấu phẩy động thì `(a + b) + c` không bằng
    /// `a + (b + c)`. Một pass culling trả về bounding box lệch nhau tí xíu mỗi frame là loại bug
    /// đi tìm cả tuần. `join` vẫn phải có tính kết hợp, vì nó được áp lên từng nhóm liền kề chứ
    /// không theo một cây cố định, nhưng nó không bao giờ gặp một **cách nhóm khác** giữa hai lần
    /// chạy.
    ///
    /// ```
    /// use xynok_concurrency::pool::{Config, ThreadPool};
    /// use xynok_concurrency::scope::ParamsParReduce;
    ///
    /// let pool = ThreadPool::new(Config {
    ///     threads: 3,
    ///     ..Default::default()
    /// });
    /// let total = pool.par_reduce(ParamsParReduce {
    ///     n:        1_000,
    ///     batch:    64,
    ///     identity: || 0u64,
    ///     fold:     |acc, i| acc + i as u64,
    ///     join:     |a, b| a + b,
    /// });
    /// assert_eq!(total, (0..1_000u64).sum::<u64>());
    /// ```
    pub fn par_reduce<T, I, F, J>(&self, params: ParamsParReduce<I, F, J>) -> T
    where
        T: Send,
        I: Fn() -> T + Sync,
        F: Fn(T, usize) -> T + Sync,
        J: Fn(T, T) -> T,
    {
        let ParamsParReduce {
            n,
            batch,
            identity,
            fold,
            join,
        } = params;

        let batch = batch.max(1);

        if n <= batch || self.worker_count() == 1
        {
            let mut accumulator = identity();
            for i in 0..n
            {
                accumulator = fold(accumulator, i);
            }
            return accumulator;
        }

        let chunks = n.div_ceil(batch);

        // Một ô kết quả cho mỗi **lô**, không phải cho mỗi thread. Mỗi thread một ô thì rẻ hơn,
        // nhưng thread nào gộp những chỉ số nào lại phụ thuộc vào lúc chạy, nên kết quả từng phần
        // sẽ khác nhau giữa các lần chạy dù thứ tự nối cuối cùng vẫn thế.
        let slots: Vec<UnsafeCell<Option<T>>> = (0..chunks).map(|_| UnsafeCell::new(None)).collect();
        let slots = ChunkSlots(&slots);
        let cursor = AtomicUsize::new(0);

        let run = || {
            loop
            {
                let chunk = cursor.fetch_add(1, Ordering::Relaxed);
                if chunk >= chunks
                {
                    break;
                }

                let start = chunk * batch;
                let mut accumulator = identity();
                for i in start..(start + batch).min(n)
                {
                    accumulator = fold(accumulator, i);
                }

                // Safety: chỉ số lô đến từ đúng một `fetch_add`, nên không có hai thread nào cùng
                // cầm một chỉ số, và cũng không có hai thread nào cùng chạm một ô.
                unsafe { slots.set(chunk, accumulator) };
            }
        };

        let helpers = self.worker_threads().min(chunks - 1);

        scope_in(&self.shared, |inner| {
            for _ in 0..helpers
            {
                inner.spawn(run);
            }
            // Cùng lý do như `parallel_for`.
            inner.shared.wake_many(helpers);
            run();
        });

        let mut accumulator = identity();
        for slot in slots.0
        {
            // Safety: mọi job đã xong, nên thread này là thread duy nhất còn lại.
            if let Some(partial) = slot.with_mut(|slot| unsafe { (*slot).take() })
            {
                accumulator = join(accumulator, partial);
            }
        }
        accumulator
    }
}

impl<'scope> Scope<'scope>
{
    /// Xếp `f` vào pool và trả về một handle để job khác bám vào.
    ///
    /// Giống hệt [`Scope::spawn`] về thời điểm chạy, tức là chạy ngay, không chờ gì, chỉ khác ở chỗ
    /// nó đưa lại cho bạn một thứ để treo việc sau lên.
    pub fn spawn_with_handle<F>(&self, f: F) -> JobHandle<'scope>
    where F: FnOnce() + Send + 'scope
    {
        self.spawn_after(&[], f)
    }

    /// Xếp `f` để chạy sau khi mọi job trong `deps` đã xong.
    ///
    /// Trả về ngay, đây không phải một điểm join. Không có gì bị chặn cho tới khi
    /// [`ThreadPool::scope`] bao ngoài kết thúc, và đó chính là ý: thread đang mô tả đồ thị thì đi
    /// mô tả tiếp, hoặc đi chạy job, chứ không ngồi lên một cái barrier.
    ///
    /// Phụ thuộc nào **đã** xong rồi thì không phải chờ. `deps` rỗng là hợp lệ và đúng bằng
    /// [`Self::spawn_with_handle`].
    ///
    /// # Không dựng được chu trình
    ///
    /// `deps` là các handle, mà handle chỉ tồn tại sau khi job của nó đã được tạo, nên mọi cạnh đều
    /// chỉ ngược về quá khứ. Không có cách nào gọi tên một job chưa tồn tại, nên cũng không có cách
    /// nào viết ra một chu trình. Điều đó quan trọng, vì một chu trình ở đây là một scope không bao
    /// giờ về 0, tức là một tiến trình không bao giờ ra khỏi điểm join.
    pub fn spawn_after<F>(&self, deps: &[&JobHandle<'scope>], f: F) -> JobHandle<'scope>
    where F: FnOnce() + Send + 'scope
    {
        let node = Arc::new(Node {
            // Bắt đầu từ một, cho cái chốt được thả ở cuối hàm này. Không có nó thì một phụ thuộc
            // vừa xong ở giữa hai lời `register` có thể kéo bộ đếm về 0 và đẩy job đi trong khi
            // thread này vẫn đang nối nốt các cạnh còn lại.
            pending:    AtomicUsize::new(1),
            work:       Mutex::new(Some(self.job(f))),
            successors: Mutex::new(Some(Vec::new())),
            shared:     Arc::clone(self.shared()),
        });

        for dep in deps
        {
            dep.node.register(&node);
        }

        // Thả cái chốt chính là thứ đẩy đi một nút không còn phụ thuộc nào, nên `deps` rỗng thì job
        // chạy ngay.
        Node::release(&node);

        JobHandle {
            node:   node,
            marker: PhantomData,
        }
    }
}

impl std::fmt::Debug for Scope<'_>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("Scope").field("remaining", &self.remaining()).finish_non_exhaustive()
    }
}
