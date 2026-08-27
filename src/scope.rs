//! Fork-join: chia việc ra, chờ nó xong, và trong lúc chờ thì vẫn làm việc khác.
//!
//! Đây là tầng mà mọi thứ phía trên gọi tới: một ECS scheduler chia system theo chunk, một frame
//! graph chạy nhiều pass song song, hay chỉ là một vòng `for` đủ nặng để đáng chia. Tất cả đều quy
//! về [`Scope`].
//!
//! # Một hệ quả phải biết trước khi dùng
//!
//! Thread đang chờ một scope **không nằm không**: nó lấy job khác của pool về chạy. Đó là cách duy
//! nhất để song song lồng nhau hoạt động được, xem [`Scope::spawn`]. Nghĩa là trong lúc một job
//! đang dừng ở điểm join, một job **không liên quan** có thể chạy ngay bên dưới nó, trên cùng
//! thread đó.
//!
//! Hệ quả thực dụng: **đừng giữ một cái khoá khi gọi vào crate này**. Nếu job chạy bên dưới lại đi
//! xin đúng cái khoá ấy, thread tự khoá chính mình, mà không dòng nào của cả hai job sai cả.

use std::any::Any;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

use crate::custom_type::Job;
use crate::latch::Latch;
use crate::pool::{Shared, ThreadPool};
use crate::sync::cell::UnsafeCell;
use crate::sync::{Arc, AtomicUsize, Mutex, Ordering};
use crate::utils::ignore_poison;

/// Chỗ giữ cái panic đầu tiên mà một job trong scope ném ra.
type PanicSlot = Mutex<Option<Box<dyn Any + Send + 'static>>>;

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
    shared: Arc<Shared>,
    latch:  Latch,
    panic:  PanicSlot,
    /// Bất biến theo `'scope`, để tham chiếu mà job bắt được không bị co ngắn hay kéo dài ra khỏi
    /// lời hứa của scope.
    marker: PhantomData<&'scope mut &'scope ()>,
}

/// Con trỏ tới chính scope, đưa được vào job.
///
/// Một `&Scope` thường không đi vào job được: tham chiếu ấy mượn cái scope nằm trên stack của người
/// mở nó, còn job thì phải sống độc lập với khung stack đó về mặt kiểu. Con trỏ thô cắt đứt quan hệ
/// mượn ấy, và thứ giữ cho nó hợp lệ vẫn là lời hứa cũ: scope không trả về khi còn job chưa xong.
struct ScopePtr<'scope>(*const Scope<'scope>);
unsafe impl Send for ScopePtr<'_> {}

/// Con trỏ tới ô panic, đưa được vào job.
///
/// Ô đó nằm trong scope, mà scope thì sống chừng nào còn vé latch chưa thả, nên con trỏ này luôn
/// hợp lệ ở mọi chỗ nó được dùng.
struct PanicSink(*const PanicSlot);
unsafe impl Send for PanicSink {}

impl ThreadPool
{
    /// Mở một scope, chạy `f`, rồi chờ mọi thứ `f` spawn ra.
    ///
    /// Chờ cả khi `f` panic: bỏ mặc job chạy tiếp trong lúc khung stack chúng đang mượn bị tháo dỡ
    /// thì đó đúng là cái use-after-free mà scope sinh ra để chặn.
    ///
    /// ```
    /// use xynok_concurrency::pool::{Config, ThreadPool};
    ///
    /// let pool = ThreadPool::new(Config {
    ///     threads: 3,
    ///     ..Default::default()
    /// });
    /// let mut totals = [0usize; 4];
    ///
    /// pool.scope(|s| {
    ///     for (i, slot) in totals.iter_mut().enumerate()
    ///     {
    ///         s.spawn(move || *slot = i * i);
    ///     }
    /// });
    ///
    /// assert_eq!(totals, [0, 1, 4, 9]);
    /// ```
    ///
    /// # Panics
    ///
    /// Nếu `f` panic, hoặc bất cứ job nào bên trong nó panic. Panic của job được bắt ngay tại chỗ,
    /// giữ lại, và ném lại ở đây sau khi mọi job khác đã xong: một panic thoát thẳng ra khỏi worker
    /// sẽ làm bộ đếm job thiếu một, và điểm chờ này treo mãi mãi. Nhiều job cùng panic thì cái được
    /// ghi nhận đầu tiên thắng, phần còn lại bị thả.
    pub fn scope<'scope, R>(&self, f: impl FnOnce(&Scope<'scope>) -> R) -> R
    {
        scope_in(self.shared(), f)
    }

    /// Chạy hai việc song song và trả về cả hai kết quả.
    ///
    /// `b` chạy ngay trên thread gọi, `a` được giao cho pool. Nếu không ai bốc `a` thì chính thread
    /// này sẽ chạy nó trong lúc chờ, nên `join` không bao giờ tệ hơn chạy tuần tự quá một chút chi
    /// phí ghi sổ.
    ///
    /// ```
    /// use xynok_concurrency::pool::{Config, ThreadPool};
    ///
    /// let pool = ThreadPool::new(Config {
    ///     threads: 2,
    ///     ..Default::default()
    /// });
    /// let (left, right) = pool.join(|| (1..=50u64).sum::<u64>(), || (51..=100u64).sum::<u64>());
    /// assert_eq!(left + right, 5050);
    /// ```
    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB,
        RA: Send,
    {
        let mut left = None;

        let right = self.scope(|s| {
            let slot = &mut left;
            s.spawn(move || *slot = Some(a()));
            b()
        });

        // Scope đã trả về, nên job của `a` chắc chắn đã chạy xong và đã ghi vào ô.
        (left.expect("job của `a` xong mà không để lại kết quả"), right)
    }

    /// Chia `0..n` thành từng lô rồi chạy `f` trên mọi chỉ số. Xem [`Scope::parallel_for`].
    pub fn parallel_for<F>(&self, n: usize, batch: usize, f: F)
    where F: Fn(usize) + Sync
    {
        self.scope(|s| s.parallel_for(n, batch, f));
    }

    /// Gộp `0..n` thành một giá trị, song song, kết quả không phụ thuộc lúc nào thread nào xong.
    /// Xem [`Scope::par_reduce`].
    pub fn par_reduce<T, I, F, J>(&self, n: usize, batch: usize, identity: I, fold: F, join: J) -> T
    where
        T: Send,
        I: Fn() -> T + Sync,
        F: Fn(T, usize) -> T + Sync,
        J: Fn(T, T) -> T,
    {
        self.scope(|s| s.par_reduce(n, batch, identity, fold, join))
    }
}

/// Thân của [`ThreadPool::scope`], gọi được từ bất cứ chỗ nào đang cầm phần dùng chung của pool.
pub(crate) fn scope_in<'scope, R>(shared: &Arc<Shared>, f: impl FnOnce(&Scope<'scope>) -> R) -> R
{
    let scope = Scope {
        shared: Arc::clone(shared),
        latch:  Latch::new(0),
        panic:  Mutex::new(None),
        marker: PhantomData,
    };

    let outcome = catch_unwind(AssertUnwindSafe(|| f(&scope)));
    // Chờ cả khi `f` đã panic: bỏ mặc job chạy tiếp trong lúc khung stack chúng mượn đang bị tháo
    // dỡ thì đó đúng là cái use-after-free mà scope sinh ra để chặn.
    shared.run_until(|| scope.latch.is_done());

    let job_panic = ignore_poison(scope.panic.lock()).take();

    match (outcome, job_panic)
    {
        (Err(payload), _) => resume_unwind(payload),
        (Ok(_), Some(payload)) => resume_unwind(payload),
        (Ok(value), None) => value,
    }
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
    ///
    /// let pool = ThreadPool::new(Config {
    ///     threads: 3,
    ///     ..Default::default()
    /// });
    /// let total = pool.par_reduce(1_000, 64, || 0u64, |acc, i| acc + i as u64, |a, b| a + b);
    /// assert_eq!(total, (0..1_000u64).sum::<u64>());
    /// ```
    pub fn par_reduce<T, I, F, J>(&self, n: usize, batch: usize, identity: I, fold: F, join: J) -> T
    where
        T: Send,
        I: Fn() -> T + Sync,
        F: Fn(T, usize) -> T + Sync,
        J: Fn(T, T) -> T,
    {
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

/// Cho một lát các ô kết quả theo lô đi được vào thread worker.
///
/// Ghi qua [`Self::set`] chứ không qua trường, vì một closure chạm thẳng `slots.0` sẽ bắt lấy cái
/// lát trần, mà lát trần thì không `Sync`.
struct ChunkSlots<'a, T>(&'a [UnsafeCell<Option<T>>]);

/// An toàn vì mỗi ô chỉ có đúng một thread chạm vào: thread nào giành được chỉ số lô đó từ bộ đếm.
unsafe impl<T: Send> Sync for ChunkSlots<'_, T> {}

impl<T> ChunkSlots<'_, T>
{
    /// # Safety
    ///
    /// Không thread nào khác được cầm cùng `chunk` này.
    #[inline]
    unsafe fn set(&self, chunk: usize, value: T)
    {
        self.0[chunk].with_mut(|slot| unsafe { *slot = Some(value) });
    }
}

impl std::fmt::Debug for Scope<'_>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("Scope").field("remaining", &self.remaining()).finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
#[path = "scope_tests.rs"]
mod test;
