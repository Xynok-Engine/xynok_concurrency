# Lộ trình dựng ThreadPool và work stealing

Tài liệu này mô tả sáu bước để đưa `xynok_concurrency` từ bản phác thảo hiện tại
(`src/thread_pool.rs`, một `VecDeque` dưới một `Mutex`, nằm trọn trong `#[cfg(test)]`)
lên tới kiến trúc của `xynok_workers`: N hàng đợi lock-free, work stealing, và fork-join
có bảo hộ lifetime.

Mỗi bước có: **mục tiêu**, **khung code**, **cơ chế**, **bẫy**, **test**, và **xong khi nào**.

---

## Bản đồ

Luồng dữ liệu đích:

```
thread ngoài pool ──push──> injector (1 cái, dùng chung)
                                 │
participant i ────push──> local[i].queue  ──steal──> participant j
                     └──> local[i].mailbox (ghim, không ai steal)
                                 │
                            find_job(i)  ──> run_job ──> Scope::remaining -= 1
                                 │
                            Sleep::wait(seen) ←── Sleep::wake_one/wake_all
```

Hiện trạng:

| Mảnh | Trạng thái |
|---|---|
| `CachePadded`, `SpinLock`, `Waker`, `sync.rs` (loom shim) | ✅ có sẵn |
| `Backoff::rounds()` / `reset()` | ✅ đã thêm |
| `Priority::apply_to_current_thread` | ✅ đã lên `src/apis/priority.rs` — xem `docs/priority-and-qos.md` |
| Bước 1 — `Injector<T>` | ❌ |
| Bước 2 — `Sleep` | ❌ |
| Bước 3 — đăng ký thread + `Local` | ❌ |
| Bước 4 — `find_job` + `worker_loop` | ❌ |
| Bước 5 — `Scope` | ❌ |
| Bước 6 — `PerWorker` và tầng trên | ❌ |

Thứ tự là bắt buộc: mỗi bước đứng trên bước trước. Bước 1–2 là hai mảnh khó nhất và có
thể test hoàn toàn độc lập; bước 3–4 chủ yếu là ráp; bước 5 mới là thứ engine gọi tới.

---

## Bước 1 — `Injector<T>`: hàng đợi bounded MPMC lock-free

**Mục tiêu.** Một hàng đợi cố định kích thước mà *mọi* thread push được và *mọi* worker
pop được, không lock, không cấp phát lúc chạy.

**File.** `src/utils/injector.rs` (hoặc `src/queue/injector.rs`).

### Khung

```rust
pub struct Injector<T>
{
    buffer: Box<[Slot<T>]>,
    mask:   usize,                     // capacity - 1
    head:   CachePadded<AtomicUsize>,  // vị trí pop kế tiếp
    tail:   CachePadded<AtomicUsize>,  // vị trí push kế tiếp
}

struct Slot<T>
{
    sequence: AtomicUsize,
    value:    crate::sync::cell::UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Send for Injector<T> {}
unsafe impl<T: Send> Sync for Injector<T> {}

impl<T> Injector<T>
{
    pub fn new(capacity: usize) -> Self;            // làm tròn lên lũy thừa 2, tối thiểu 2
    pub fn capacity(&self) -> usize;
    pub fn push(&self, value: T) -> Result<(), T>;  // đầy → TRẢ LẠI value
    pub fn pop(&self) -> Option<T>;
}
```

### Cơ chế — giao thức sequence của Vyukov

Mỗi slot mang một số `sequence` nói **đến lượt ai**. Đó là toàn bộ lý do producer và
consumer dùng chung một mảng mà không cần lock.

- Slot `i` khởi tạo `sequence = i` — "trống, đang đợi producer ở vị trí `i`".
- Producer đọc `tail = t`, xem slot `t & mask`:
  - `sequence == t` → slot là của mình. CAS `tail`: `t → t+1`. Thắng thì ghi value, rồi
    `sequence.store(t + 1, Release)` — "đầy, đang đợi consumer ở vị trí `t`".
  - `sequence < t` → vòng ring chưa quay tới, **queue đầy** → trả `Err(value)`.
  - `sequence > t` → người khác đã chiếm, đọc lại `tail`.
- Consumer đọc `head = h`, xem slot `h & mask`:
  - `sequence == h + 1` → có hàng. CAS `head`: `h → h+1`. Thắng thì đọc value, rồi
    `sequence.store(h + mask + 1, Release)` — trạng thái "trống" của **vòng sau**.
  - `sequence < h + 1` → **queue rỗng** → `None`.

So sánh phải viết là `sequence.wrapping_sub(t) as isize`, không phải `<` / `>` thẳng:
cursor được phép chạy quá `usize::MAX` rồi quay vòng, và phép trừ wrapping đọc như số có
dấu vẫn đúng qua ranh giới đó (queue không bao giờ chứa quá `isize::MAX` phần tử).

Điều đẹp nhất của sơ đồ này: **không thread nào phải hỏi "queue có đầy không"** — một câu
hỏi mà câu trả lời đã cũ ngay lúc đọc xong. Nó so `sequence` của *một* slot với vị trí của
chính mình và nhận câu trả lời dứt khoát về slot đó.

### Bẫy

1. **`head`/`tail` chỉ cần `Relaxed` CAS.** Chúng là *lời tuyên bố chiếm chỗ*, không công bố
   dữ liệu gì. Toàn bộ ordering nằm ở `sequence`: load `Acquire`, store `Release`. Rắc
   `SeqCst` lên đây là mất tiền không mua được gì.
2. **Phải dùng `crate::sync::cell::UnsafeCell`**, không phải `std::cell::UnsafeCell` —
   nếu không loom mù với ô nhớ đó và toàn bộ bước test coi như không có.
3. **`capacity` tối thiểu 2.** Với một slot, "đầy ở vị trí `p`" là `p + 1` và "trống cho
   vòng sau" là `p + capacity` — trùng nhau. Producer sẽ ghi đè giá trị mà consumer đang
   đọc dở. Loom gọi đó là causality violation, và đúng là như vậy.
4. **`Drop for Injector<T>` phải vét cạn.** Với `T = Job = Box<dyn FnOnce()>`, quên là leak
   thật; miri sẽ bắt. Vét bằng `while self.pop().is_some() {}` là đủ và đơn giản nhất.
5. **`unsafe impl Sync` chỉ cần `T: Send`**, không cần `T: Sync`: mỗi value chỉ do đúng một
   thread chạm vào — producer đã lấp slot, rồi consumer đã chiếm nó. Đây là *move* giữa các
   thread, không phải share.

### Test

- `cargo test`: 4 producer × 4 consumer, mỗi item xuất hiện **đúng một lần** (so tổng, hoặc
  bitset).
- Queue đầy → `push` trả `Err(value)` và value còn nguyên vẹn (không bị move mất).
- Drop counter: push N phần tử có `impl Drop`, drop queue, đếm đủ N.
- Wrap-around: đẩy/rút vài nghìn lần trên queue 2 slot, ép cursor quay vòng nhiều lần.
- `cargo miri test --lib injector` — bắt thiếu cặp Acquire/Release.
- `RUSTFLAGS="--cfg loom" cargo test --lib injector` với **capacity 2, đúng 2 thread**.
  Loom nổ theo cấp số nhân; model càng nhỏ càng dùng được.

### Xong khi nào

Miri và loom đều xanh với model nhỏ, và test drop-counter chứng minh không leak. Chưa cần
tích hợp gì với pool.

---

## Bước 2 — `Sleep`: ngủ mà không mất wakeup

**Mục tiêu.** Worker hết việc phải ngủ được (không đốt core), nhưng **không bao giờ** ngủ
quên qua một job vừa được đẩy vào.

**File.** `src/thread_pool.rs` (hoặc tách `src/pool/sleep.rs`).

Bản phác thảo hiện tại né vấn đề này bằng cách nhốt mọi thứ trong một `MutexCondition`:
worker chỉ ngủ khi đang giữ lock, và `Condvar::wait` nhả lock nguyên tử, nên khe hở không
tồn tại. Khi hàng đợi trở thành lock-free, cái lock đó biến mất và **bạn phải dựng lại
giao thức bằng tay**.

### Khung

```rust
pub(crate) struct Sleep
{
    state:   CachePadded<AtomicUsize>,  // events << SLEEPER_BITS | sleepers
    lock:    Mutex<()>,
    condvar: Condvar,
}

const SLEEPER_BITS: u32 = 16;
const SLEEPERS:  usize = (1 << SLEEPER_BITS) - 1;
const ONE_EVENT: usize = 1 << SLEEPER_BITS;

impl Sleep
{
    pub(crate) fn counter(&self) -> usize;      // load Acquire >> SLEEPER_BITS
    pub(crate) fn wake_one(&self);
    pub(crate) fn wake_all(&self);
    pub(crate) fn wait(&self, seen: usize);
}
```

### Cơ chế

Cuộc đua cần bịt:

```
worker:   tìm việc → không có
                            producer: push(job)
                            producer: wake()      ← rơi vào hư không, worker đang thức
worker:   ngủ                                     ← và ngủ xuyên qua job đó
```

Sai lầm là worker quyết định ngủ dựa trên thứ nó thấy **lúc trước**. Cách chữa: một bộ đếm
sự kiện mà mọi producer đều tăng. Worker đọc nó **trước khi** đi tìm, và chỉ được ngủ nếu
nó chưa nhúc nhích.

```rust
fn wake(&self, all: bool)
{
    let previous = self.state.fetch_add(ONE_EVENT, Ordering::AcqRel);
    if previous & SLEEPERS == 0 { return; }        // không ai ngủ → khỏi đụng mutex
    let _guard = self.lock.lock().unwrap();
    if all { self.condvar.notify_all(); } else { self.condvar.notify_one(); }
}

pub(crate) fn wait(&self, seen: usize)
{
    let mut guard = self.lock.lock().unwrap();
    let previous = self.state.fetch_add(1, Ordering::AcqRel);   // đăng ký + kiểm lại, một RMW
    if previous >> SLEEPER_BITS == seen
    {
        guard = self.condvar.wait(guard).unwrap();
    }
    self.state.fetch_sub(1, Ordering::AcqRel);
    drop(guard);
}
```

**Vì sao hai bộ đếm phải ở chung một word** — đây là đoạn đáng học nhất của cả kiến trúc.
Producer muốn bỏ qua mutex khi không ai ngủ, nên hai bên đọc lẫn nhau: producer tăng
`events` rồi đọc `sleepers`; worker tăng `sleepers` rồi đọc `events`. Trên **hai ô nhớ
riêng**, đó chính xác là hình store-buffer: cả hai cùng đọc được giá trị cũ — producer kết
luận "không ai ngủ" đúng lúc worker kết luận "không có gì xảy ra" — và worker ngủ vĩnh viễn.
Loại trừ nó trên hai ô nhớ đòi `SeqCst` cả hai bên.

Gói vào **một** word thì vấn đề biến mất chứ không phải được trả tiền để né. Hai bên cùng
RMW trên một địa chỉ, mà mọi RMW trên một địa chỉ đều được sắp thứ tự toàn phần theo
modification order của địa chỉ đó: cái đến sau **nhất định thấy** cái đến trước. Hoặc worker
thấy event count mới và không ngủ, hoặc producer thấy có người ngủ và đi lấy lock. `AcqRel`
là đủ, chỉ còn một atomic trên đường đi, và — khác với bản `SeqCst` — lập luận này là thứ
loom kiểm được.

Mutex vẫn cần, vì "đọc counter" và "ngủ" là hai bước và producer có thể chen vào giữa. Việc
kiểm lại diễn ra dưới đúng cái lock mà producer phải lấy để báo hiệu.

### Bẫy

1. **Luôn đọc `counter()` TRƯỚC khi đi tìm việc.** Đọc sau là tự mở lại đúng khe hở vừa bịt.
   Quy tắc này áp cho cả `worker_loop` (bước 4) lẫn `Scope::wait` (bước 5).
2. `wake_one` cho "có một job mới" (một job cần đúng một thread), `wake_all` cho "mọi người
   phải xem lại" — scope xong, shutdown, job ghim vào mailbox.
3. Spurious wakeup là hợp lệ và phải chấp nhận được: mọi caller đều kiểm lại điều kiện của
   mình trong vòng lặp.
4. `SLEEPER_BITS = 16` cho 65 535 thread ngủ và để lại 48 bit cho event count trên máy
   64-bit — tăng một lần mỗi job thì dùng được vài thế kỷ.

### Test

- Loom, và đây là chỗ loom **bắt buộc**: 1 producer (`push` + `wake_one`) và 1 worker
  (`seen = counter()` → `pop()` → `wait(seen)`). Assert worker không kết thúc ở trạng thái
  ngủ khi queue còn job. Đây là bug mà `cargo test` không bao giờ tìm ra.
- Test thường: N thread ngủ, một `wake_all` phải đánh thức tất cả.
- Test thường: `wait(seen)` với `seen` đã cũ phải trả về ngay, không ngủ.

### Xong khi nào

Loom model lost-wakeup xanh. Đây là điều kiện cần trước khi động vào bước 4.

---

## Bước 3 — Đăng ký thread và hàng đợi cục bộ

**Mục tiêu.** Mỗi participant có hàng đợi riêng, và mỗi thread biết mình là participant số
mấy — cả hai đều không tốn một lần lấy lock nào.

### Khung

```rust
thread_local! {
    static WORKER_INDEX: Cell<(u64, usize)> = const { Cell::new((NO_POOL, usize::MAX)) };
}

pub(crate) const NO_POOL: u64 = 0;   // id thật bắt đầu từ 1

struct Local
{
    queue:   Injector<Job>,   // việc thread này spawn — ai cũng steal được
    mailbox: Injector<Job>,   // việc ghim vào thread này — không ai steal
}

pub(crate) struct Shared
{
    injector:    Injector<Job>,           // cửa vào cho thread NGOÀI pool
    local:       Box<[CachePadded<Local>]>,
    sleep:       Sleep,
    id:          u64,
    shutdown:    AtomicBool,
    workers:     usize,
    host:        thread::Thread,
    spin_rounds: u32,
    priority:    Priority,
}
```

### Cơ chế

**`participants = threads + 1`.** Thread gọi `ThreadPool::new` là *host*, và nó **cũng chạy
job** (bước 5 làm cho điều đó thành thật). Nên nó có slot riêng ở index `workers`. Hệ quả
trực tiếp: `Config::frame()` phải đổi từ `cores()` sang `cores() - 1`. Bản phác thảo hiện
tại dùng `cores()` và ghi đúng lý do ngược lại — ở đó thread gọi chỉ ngủ trong `wait_idle`
nên không tính là participant.

`inject` trở thành:

```rust
pub(crate) fn inject(&self, job: Job)
{
    let queue = match self.participant()
    {
        Some(i) => &self.local[i].queue,   // của chính mình: không tranh chấp
        None    => &self.injector,
    };

    match queue.push(job)
    {
        Ok(())   => self.sleep.wake_one(),
        Err(job) => run_job(job),          // đầy → chạy tại chỗ
    }
}

fn participant(&self) -> Option<usize>
{
    let (id, index) = WORKER_INDEX.get();
    (id == self.id).then_some(index)
}
```

**Vì sao queue đầy thì chạy tại chỗ chứ không chờ.** Thread đang push thường *chính là* một
worker. Nếu nó đi ngủ, nó là một trong những thread lẽ ra phải rút cạn queue — deadlock.
Panic thì biến một đợt tải cao thành crash. Chạy tại chỗ vừa hoàn thành job vừa tạo
backpressure tự nhiên: một thread đang bận thì không push thêm gì nữa. Đây cũng là lý do
`Injector::push` trả lại giá trị chứ không trả `bool` — caller cần nó lại để chạy.

**Con số biện minh cho cả bước này.** `xynok_workers` đo trước khi tách queue: spawn job
rỗng tốn **171 ns ở 3 worker và 873 ns ở 7 worker**. Thêm core thì *chậm đi*, vì mọi push và
pop đều CAS lên cùng hai cache line. Tách queue không làm cho việc chia sẻ rẻ hơn — nó xóa
việc chia sẻ đi.

### Bẫy

1. **Thread-local giữ `(pool_id, index)` chứ không chỉ `index`.** Một thread có thể gặp
   nhiều pool: nó dựng pool A rồi dựng pool B, và ô này chỉ giữ lần đăng ký gần nhất. Không
   có id thì một index do pool khác để lại vẫn lọt qua kiểm tra "có nằm trong khoảng không",
   và thread được phát **slot của người khác** — hai thread ghi chung một ô `PerWorker`,
   đúng cái data race mà toàn bộ cơ chế này tồn tại để loại trừ.
2. **`mailbox` không bao giờ bị steal** — nó tồn tại cho tài nguyên thread-affine (command
   pool của Vulkan/D3D12 phải được record bởi đúng một thread). Ngoại lệ duy nhất ở bước 4.
3. `Shared` **không được** giữ join handle. Worker giữ `Arc<Shared>`; nếu handle nằm trong
   đó thì worker tự giữ sống tín hiệu shutdown của chính nó. Đây là lý do tách `Owner` —
   bản phác thảo hiện tại đã làm đúng, giữ nguyên.

### Test

- `worker_index()` từ mỗi worker trả về giá trị đôi một khác nhau, và host được index cuối.
- Dựng pool A rồi shutdown, dựng pool B từ thread khác: thread từng host A phải **panic**
  khi gọi `worker_index()`, không được nhận đại một số.
- `threads: 0` → `worker_count() == 1`, host là index 0.

---

## Bước 4 — `find_job` và `worker_loop`: work stealing

**Mục tiêu.** Không worker nào ngồi không khi trong pool còn việc, ở bất kỳ hàng đợi nào.

### Khung

```rust
fn find_job(&self, index: Option<usize>) -> Option<Job>
{
    if let Some(i) = index
    {
        if let Some(job) = self.local[i].mailbox.pop() { return Some(job); }  // 1
        if let Some(job) = self.local[i].queue.pop()   { return Some(job); }  // 2
    }
    if let Some(job) = self.injector.pop() { return Some(job); }              // 3

    let n = self.local.len();
    let start = index.map(|i| i + 1).unwrap_or(0);
    for offset in 0..n                                                        // 4
    {
        let victim = (start + offset) % n;
        if Some(victim) == index { continue; }
        if let Some(job) = self.local[victim].queue.pop() { return Some(job); }
    }

    if self.shutdown.load(Ordering::Acquire)                                  // 5
    {
        for local in &self.local
        {
            if let Some(job) = local.mailbox.pop() { return Some(job); }
        }
    }
    None
}
```

Thứ tự 1→4 là thứ tự giữ thread ở lại trên cache line của chính nó lâu nhất: việc ghim
(không ai chạy hộ được), rồi việc mình vừa spawn (nóng nhất trong cache), rồi việc từ ngoài
(không của riêng ai), rồi mới đi cướp.

`worker_loop` — mỗi dòng một lý do:

```rust
fn worker_loop(shared: Arc<Shared>, index: usize)
{
    WORKER_INDEX.set((shared.id, index));
    shared.priority.apply_to_current_thread();   // từ chính worker, không phải từ thread spawn nó

    let mut backoff = Backoff::new();

    loop
    {
        let seen = shared.sleep.counter();                     // TRƯỚC khi tìm
        if shared.run_one() { backoff.reset(); continue; }
        if shared.shutdown.load(Ordering::Acquire) { break; }   // SAU khi tìm

        if backoff.rounds() >= shared.spin_rounds
        {
            shared.sleep.wait(seen);
            backoff.reset();
        }
        else
        {
            backoff.snooze();
        }
    }
}
```

### Bẫy

1. **Steal bắt đầu từ `index + 1`, không phải từ 0.** Bắt đầu từ 0 thì mọi thread rảnh xếp
   hàng sau lưng participant 0; lệch đi thì chúng tản ra các nạn nhân khác nhau.
2. **`index: Option<usize>`.** Một thread *không* thuộc pool vẫn phải steal được — nó đang
   kẹt tại điểm join của `Scope::wait` (bước 5). Nó chỉ không có queue riêng để thử trước.
3. **Kiểm `shutdown` SAU khi tìm việc**, để shutdown làm *cạn* hàng đợi chứ không *bỏ* nó.
   Một job bị bỏ có thể là job mà một scope đang đếm — scope không bao giờ về 0 là một tiến
   trình không bao giờ thoát.
4. **Bước 5 trong `find_job` phá lời hứa "mailbox không bị steal", và đó là lựa chọn đúng.**
   Việc ghim thuộc về chủ của nó — cho tới khi chủ ngừng tồn tại. Khi pool đang tắt, job nằm
   trong mailbox của worker đã thoát thì không còn ai chạy. Chạy nó nhầm thread là phá lời
   hứa; không chạy nó là treo tiến trình. Giữa hai cái đó thì không phải cân nhắc gì nhiều.
   Nó chỉ được với tới khi mọi thứ khác đã rỗng, nên fast path không tốn gì.
5. `spin_rounds` là một đánh đổi đo được, không phải hằng số thẩm mỹ. `xynok_workers` đo với
   7 worker, spawn job rỗng từ một thread: **851 ns/job ở 10 vòng, 458 ở 40, 403 ở 80**. Cùng
   benchmark đó với fork-join lồng nhau — nơi worker luôn có việc — thì **không đổi chút
   nào**, vì không ai đi ngủ cả. Giá phải trả khi nâng nó lên là một core giữ ấm trên máy
   rảnh. 40 là chỗ đường cong bắt đầu phẳng.

### Test

- Một producer spawn 100 000 job rỗng, N worker: tất cả phải chạy, và chạy trên nhiều thread
  khác nhau (đếm bằng `PerWorker` hoặc một mảng atomic).
- Job chỉ được đẩy vào queue của **một** worker → các worker khác phải steal hết. Đây là test
  chứng minh work stealing hoạt động, không phải test throughput.
- Shutdown khi hàng đợi còn 8 job → cả 8 phải chạy (test này đã có sẵn, giữ nguyên).
- Job panic không giết worker (test đã có sẵn, giữ nguyên).

---

## Bước 5 — `Scope`: fork-join, và xóa `wait_idle`

**Mục tiêu.** Một điểm join đóng lại **đúng nhóm job của nó** (chứ không phải toàn bộ pool),
cho phép job mượn biến trên stack của người gọi, và — quan trọng nhất — **đợi bằng cách làm
việc**.

`wait_idle` hiện tại có một `assert!` chống "đợi chính mình". Cái assert đó là dấu hiệu của
một thiết kế còn thiếu, không phải một tính năng: `Scope` xóa nó đi, và test
`wait_idle_tu_trong_job_thi_panic` bị thay bằng test ngược lại — **scope lồng nhau phải chạy
được**.

### Khung

```rust
pub struct Scope<'scope>
{
    shared: Arc<Shared>,
    state:  Arc<ScopeState>,
    marker: PhantomData<fn(&'scope ()) -> &'scope ()>,
}

struct ScopeState
{
    remaining: AtomicUsize,
    panic:     Mutex<Option<Box<dyn Any + Send>>>,
}

impl<'scope> Scope<'scope>
{
    pub fn spawn<F>(&self, f: F) where F: FnOnce() + Send + 'scope;
    pub fn spawn_on<F>(&self, worker: usize, f: F) where F: FnOnce() + Send + 'scope;
    pub fn parallel_for<F>(&self, n: usize, batch: usize, f: F) where F: Fn(usize) + Sync;
}
```

Vòng đời của một scope:

```rust
pub(crate) fn scope_in<'scope, R>(shared: &Arc<Shared>, f: impl FnOnce(&Scope<'scope>) -> R) -> R
{
    let scope = /* ... */;

    let outcome = catch_unwind(AssertUnwindSafe(|| f(&scope)));
    scope.wait();                                     // đợi KỂ CẢ khi đang unwind

    let job_panic = scope.state.panic.lock().unwrap().take();

    match (outcome, job_panic)
    {
        (Err(payload), _)      => resume_unwind(payload),
        (Ok(_), Some(payload)) => resume_unwind(payload),
        (Ok(value), None)      => value,
    }
}
```

Điểm join — đây là chỗ work stealing trở nên có ý nghĩa:

```rust
fn wait(&self)
{
    let mut backoff = Backoff::new();

    loop
    {
        if self.is_complete() { return; }
        let seen = self.shared.sleep.counter();
        if self.shared.run_one() { backoff.reset(); continue; }   // ← đợi bằng cách LÀM VIỆC
        if self.is_complete() { return; }

        if backoff.rounds() >= self.shared.spin_rounds
        {
            self.shared.sleep.wait(seen);
            backoff.reset();
        }
        else
        {
            backoff.snooze();
        }
    }
}
```

### Bẫy

1. **`remaining.fetch_add(1)` phải xảy ra TRƯỚC khi push job**, không phải trong job. Nếu
   không, `wait` có thể thấy 0 ở khe giữa push và increment rồi trả về khi job còn đang bay.
2. **Job cuối (`fetch_sub(..) == 1`) phải `sleep.wake_all()`.** Người đợi có thể đang park
   trên condvar của pool chứ không đang spin — scope kết thúc là một sự kiện pool phải công
   bố như mọi sự kiện khác.
3. **`transmute` xóa `'scope` là `unsafe`, và tính đúng đắn nằm ở chỗ `scope()` không return
   khi `remaining > 0` — kể cả trên đường unwind.** Vì thế phải `catch_unwind(f)` → `wait()`
   → rồi mới `resume_unwind`. Đảo thứ tự là dangling reference, và là loại bug tệ nhất có
   thể viết ra ở đây.
4. **Panic đầu tiên thắng.** Giữ panic sau nghĩa là chọn giữa chúng theo thứ tự hoàn thành,
   mà thứ tự đó không tái lập được.
5. **`spawn_on` không có đường "chạy tại chỗ".** Chạy tại chỗ chính là điều caller yêu cầu
   *đừng* làm. Mailbox đầy thì phải thử lại — và thử lại bằng cách `run_one()`, vì mailbox
   chỉ có chủ của nó mới vét được, và thread đang gọi rất có thể *chính là* chủ đó.
   `inject_on` cũng phải `wake_all` chứ không `wake_one`: một condvar phục vụ cả pool, nên
   `notify_one` đánh thức một sleeper bất kỳ, kẻ đó nhìn quanh, không thấy gì mình được phép
   chạy, rồi ngủ lại — job nằm trong mailbox mà chủ của nó chưa hề được đánh thức.
6. **`parallel_for` spawn một job mỗi participant, không phải mỗi phần tử.** Chia việc qua
   một `AtomicUsize` cursor cộng `batch`. Đây là lý do một `Box` mỗi spawn là chấp nhận
   được: số lần cấp phát tỉ lệ với số core, không phải với khối lượng công việc. Thêm hai
   điều: nếu `n <= batch` thì chạy tuần tự ngay tại chỗ (spawn + wake + join tốn micro giây,
   đáng cho mili giây công việc và lỗ trắng cho nano giây), và **thread gọi phải chạy `run`
   cùng** chứ không phát việc rồi đợi — đẩy hết cho worker rồi park là bỏ không một core,
   thường là core nhanh nhất.
7. Trên máy có P-core và E-core, chia tĩnh `n/8` cho 8 worker là sai ngay từ đầu: worker trên
   E-core không tiến triển bằng worker trên P-core, nên frame kết thúc theo nhịp của E-core.
   Xem `docs/priority-and-qos.md` — Apple khuyến nghị thẳng work stealing vì lý do này.
8. **Điểm join là chỗ priority inversion sinh ra.** Nếu `wait` spin vô hạn trên atomic, một
   thread ưu tiên cao đốt P-core để đợi một thread ưu tiên thấp đang nằm trên E-core mà không
   được lên lịch. `spin_rounds` rồi park thật không chỉ là chuyện tiết kiệm điện — nó là điều
   kiện đúng đắn trên hệ core dị nhất.

### Test

- `scope` với 4 job ghi vào 4 phần tử của một mảng trên stack — chứng minh borrow hoạt động.
- Job panic trong scope → panic được ném lại **tại điểm join**, và mọi job anh em vẫn chạy
  xong trước khi nó nổ.
- **Scope lồng nhau**: một job mở scope con và spawn tiếp. Phải chạy được, không treo.
- `parallel_for(100_000, 16, ..)` → mọi index chạy đúng một lần; và với `n <= batch` thì
  không thread nào ngoài thread gọi tham gia.
- `spawn_on(i, ..)` → job chạy đúng trên participant `i`, kiểm bằng `worker_index()`.

### Xong khi nào

`scope` thay được `wait_idle` ở mọi chỗ trong test vòng lặp frame, và test scope lồng nhau
xanh. Đến đây pool đã tương đương `xynok_workers` về mặt cơ chế.

---

## Bước 6 — Tầng trên

Không mảnh nào ở đây chặn mảnh nào; làm theo thứ tự cần dùng.

**`PerWorker<T>`** — mảng một slot mỗi participant, đánh chỉ số bằng `worker_index()`.
`unsafe impl Sync for PerWorker<T> where T: Send` — mỗi thread chỉ chạm slot của nó, nên đây
là *move* giữa thread chứ không phải share, cùng điều kiện như một channel. Mỗi slot cần một
cờ `borrowed: AtomicBool` để bắt trường hợp mượn hai lần: một job có thể bắt đầu *bên dưới*
một job khác trên cùng thread (tại điểm join), và xin lại đúng slot đó — hai `&mut T` tới một
ô là UB. Cờ phải là `AtomicBool` chứ không phải `Cell`, vì host đọc cờ của mọi slot.
`iter_mut()` duyệt theo **chỉ số**, không theo thứ tự hoàn thành — đó là chỗ tính tái lập
được giữ hay mất.

**`Bump` / `scratch`** — một arena mỗi participant, reset ở cuối frame. Borrow phải scoped
theo closure vì lý do ở trên.

**Registry hai lane** — bản phác thảo `LANES: [Mutex<Option<ThreadPool>>; N]` đánh chỉ số
bằng enum của bạn **tốt hơn** hai `static Mutex` rời của `xynok_workers`: nó cho thấy hai
lane hoàn toàn đối xứng, và thêm lane thứ ba (audio, io) chỉ là thêm một phần tử. Giữ nguyên.
Dùng `Mutex` chứ không `OnceLock`, vì shutdown phải *rút pool ra được*.

**`inject_blocking`** — lane nền, queue đầy thì **chờ**, không chạy tại chỗ. Đây là điểm khác
biệt duy nhất so với `inject`, và nó ngược lại có chủ đích: lý do gửi `File::read` sang pool
I/O là để nó **không** chạy trên thread này; lặng lẽ chạy nó ở đây là phá đúng mục đích đó,
vào đúng lúc tệ nhất (khi máy đang quá tải).

**Ghi chú hiệu năng đáng nhớ**: đừng đổi registry `Mutex` sang `RwLock` vì "đọc nhiều hơn
ghi". Đo với 8 worker gọi `scratch` trong vòng lặp: `RwLock` **428 ns/lần** so với **22 ns**
của mutex. Đường đọc của `RwLock` nhiều việc hơn một cặp lock/unlock, và hai bên vẫn ghi cùng
một cache line nên 8 reader ping-pong nó y hệt 8 kẻ lấy lock. Bài học không nằm ở loại lock:
cách chữa cho một registry bị tranh chấp không phải lấy nó khéo hơn, mà là **không lấy nó**
— `worker_index()` trả lời từ thread-local, và đó là lý do hot path không tốn gì.

**Sau cùng**: `profile::Sink` (profiler quan tâm khoảng *giữa* các job — đó là chỗ frame biến
mất), `job_graph` / `JobHandle` (thứ tự giữa các job mà không cần barrier), `os_workgroup`
trên Apple silicon (xem `docs/priority-and-qos.md` mục 8).

---

## Thứ tự và cách kiểm

```
[✅ Backoff + Priority]  →  1. Injector  →  2. Sleep  →  3. Local + registration
                                                              ↓
                            6. PerWorker  ←  5. Scope  ←  4. find_job + worker_loop
```

Sau **mỗi** bước, cả ba:

```bash
cargo test --lib
cargo miri test --lib <module>
MIRIFLAGS="-Zmiri-many-seeds=0..16" cargo miri test --lib
LOOM_LOCATION=1 RUSTFLAGS="--cfg loom" cargo test --lib <module>
```

`cargo test` xanh chỉ chứng minh **một** interleaving chạy được. Với `Injector` và `Sleep`
thì điều đó gần như không chứng minh gì. Miri kiểm một lịch chạy để tìm UB; loom chạy lại
test dưới **mọi** lịch hợp lệ và **mọi** thứ tự bộ nhớ mà mô hình cho phép — nó là công cụ
duy nhất tìm ra lost wakeup, và `Sleep` chính là nơi bug đó sống.

Giữ nguyên năm test đang có trong `src/thread_pool.rs` làm lưới an toàn: chúng là đặc tả
**hành vi**, độc lập với việc bên dưới là một `VecDeque` hay N `Injector`.

- `hai_lane_song_song_trong_mot_vong_lap_frame` — giữ, đổi `wait_idle` thành `scope`
- `mot_job_panic_khong_giet_worker` — giữ nguyên
- `pool_khong_co_worker_chay_inline` — giữ nguyên
- `shutdown_chay_not_hang_doi` — giữ nguyên
- `wait_idle_tu_trong_job_thi_panic` — **xóa ở bước 5**, thay bằng test scope lồng nhau
