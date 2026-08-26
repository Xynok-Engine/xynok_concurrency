---
excerpt:
cover img:
---

# Lanes and the job system in Xynok Engine

![test image ref](images/test_img.jpg)

Tài liệu này chốt hai thứ:

1. **Flow của engine khi nhiều lane cùng chạy**: ai chạy ở đâu, nói chuyện với nhau bằng gì.
2. **Kế hoạch sửa** `xynok_concurrency` và `xynok_ecs` để flow đó thành hiện thực.

| phần | nội dung |
|---|---|
| [1](#1-ba-crate-đang-ở-đâu) | Ba crate đang ở đâu |
| [2](#2-lane-không-phải-là-pool) | Lane không phải là pool |
| [3](#3-flow-một-frame) | Flow một frame |
| [4](#4-các-kênh-giữa-lane) | Các kênh giữa lane |
| [5](#5-kế-hoạch-xynok_concurrency) | Kế hoạch `xynok_concurrency` |
| [6](#6-kế-hoạch-xynok_ecs) | Kế hoạch `xynok_ecs` |
| [7](#7-thứ-tự-thực-hiện) | Thứ tự thực hiện |
| [8](#8-ba-quyết-định-đã-chốt) | Ba quyết định đã chốt |

---

## 1. Ba crate đang ở đâu

| crate | vai trò | trạng thái |
|---|---|---|
| `xynok_concurrency` | mọi thứ song song sống ở đây | nguyên liệu tầng thấp xong, **chưa có pool** |
| `xynok_workers` | pool đời trước, 13.6k dòng | chạy được, nhưng một injector bounded dùng chung, không work-stealing |
| `xynok_ecs` | dữ liệu và system | chunk archetype xong, scheduler chạy tuần tự |

**`xynok_concurrency` đang có**: ring FIFO/LIFO SPMC kèm batch steal (có loom, miri, stress),
`InlineFn` 64 byte vừa một cache line, `QueueBatching` (injector thô), `Waker` (latch), `Priority`
mới có macOS, `pack`/`unpack` trong [`utils/mod.rs`](../src/utils/mod.rs) đúng cái sẽ dùng cho ô
atomic của sleep protocol. Thiếu đúng tầng giữa: **không có pool nào được export**.

**`xynok_workers` có sẵn thứ đáng mang sang**: `Latch`, `Scope` (`parallel_for`, `par_reduce`,
`spawn_on`), `PerWorker`, `JobGraph` (`spawn_after`), `Bump` (arena mỗi frame), `OneshotChannel`,
`profile::Sink`, `Priority` ba nền tảng. Thứ không mang sang là `Injector` bounded: `push` trả
`Result<(), T>` và đó chính là lỗ hổng mà [docs/injector.md](./injector.md) mô tả.

**`xynok_ecs` có sẵn thứ quan trọng nhất**: `AccessScopes::can_parallel_with`
([`query/access_scope.rs:124`](../../xynok_ecs/src/query/access_scope.rs)). Câu hỏi khó nhất của
một ECS scheduler song song đã được trả lời rồi. Chỉ là `DefaultScheduler::run`
([`schedule/scheduler.rs:80`](../../xynok_ecs/src/schedule/scheduler.rs)) vẫn đang chạy vòng `for`
tuần tự và chưa ai hỏi tới nó.

`src/thread_pool.rs` là bản nháp trong `#[cfg(test)]`, sẽ xóa ở bước đầu tiên.

---

## 2. Lane không phải là pool

Sai lầm dễ mắc nhất khi nghe "engine có lane physics, lane render, lane compute, lane audio, lane
IO" là cho mỗi lane một pool với N thread riêng. Sáu lane nhân tám thread là 48 thread trên 8 core.
Work-stealing chỉ có ý nghĩa khi số worker xấp xỉ số core; vượt qua đó thì thứ bạn mua được là
preempt giữa các worker và cache bị đá qua đá lại.

Chia lane theo **tính chất chạy**, không theo tên miền:

| loại | gồm | thread | vì sao tách |
|---|---|---|---|
| **A. Compute** | frame logic, ECS system, physics, culling, animation, ghi command buffer, chuẩn bị dữ liệu compute | **một** pool work-stealing, `N = cores - 1`, main thread tham gia thành người thứ N | CPU-bound, không bao giờ block, muốn cache nóng và muốn ăn hết core |
| **B. Blocking** | đọc file, decode texture, compile shader, giải nén | pool riêng 2 tới 4 thread, priority thấp | thời gian nằm trong syscall. Một thread block không được phép chiếm core của lane A |
| **C. Chuyên dụng** | audio callback, main/window thread, GPU submit nếu driver đòi | thread riêng, không thuộc pool nào, không tham gia steal | ràng buộc cứng: deadline realtime, hoặc bắt buộc phải là đúng thread đó |

Physics, rendering, compute **không** phải ba lane riêng. Chúng là ba nhóm job trong lane A, phân
biệt nhau bằng vị trí trong DAG chứ không bằng thread riêng. Cho physics một pool riêng nghĩa là
trong lúc physics chạy thì các core dành cho render ngồi không, và ngược lại. Một pool duy nhất với
DAG thì lúc physics của archetype này xong, core đó nhảy sang việc khác ngay.

Audio thì ngược lại hoàn toàn, và phải nói thẳng: **audio thread không bao giờ là worker của pool**.
Nó bị driver gọi lại mỗi vài ms, nó không được allocate, không được lấy khoá, không được park chờ
steal. Nó nhận lệnh qua một ring SPSC và chỉ đọc.

---

## 3. Flow một frame

```text
                    ┌──────────────────── frame N ─────────────────────────┐
 main thread        │ event │ begin │ ==== tham gia pool lane A ==== │ present
                    │       │       │                                │
 pool A · worker 0  │       │       │ [physics] [ecs] [cull] [record] │
 pool A · worker 1  │       │       │ [physics] [ecs] [cull] [record] │
 pool A · worker 2  │       │       │ [physics] [ecs] [cull] [record] │
                    │       │       │                                │
 pool B · io 0      │ ······· đọc scene.pak, vắt qua nhiều frame ················>
 pool B · io 1      │ ······· decode texture ····································>
                    │
 audio thread       │ cb    cb    cb    cb    cb    cb    cb    cb    cb    cb
                    └──────────────────────────────────────────────────────┘
```

Bốn điều đáng chú ý trong hình:

**Main thread tham gia pool.** Nó không submit rồi ngủ. Trong lúc chờ schedule xong nó lấy job về
chạy như một worker. Đây là lý do `N = cores - 1`: main thread là người thứ N. Nếu để nó park thì
bạn mất một core suốt cả frame, và trên máy 4 core đó là 25%.

**Lane B vắt qua ranh giới frame.** `present` không đợi nó. Một asset load xong ở frame N+7 thì kết
quả đi ngược về lane A qua injector, và job nào đang đợi asset đó được đánh thức bằng job graph chứ
không bằng polling.

**Audio chạy độc lập với nhịp frame.** Nó không nằm trong barrier nào. Lane A gửi lệnh cho nó (phát
âm thanh này, đổi volume kia) qua ring SPSC, audio thread đọc trong callback tiếp theo. Nếu frame
tụt xuống 20 FPS thì audio vẫn 48 kHz.

**Trong lane A, thứ tự đến từ DAG.** `[physics] [ecs] [cull] [record]` vẽ như bốn giai đoạn nối
tiếp cho dễ nhìn, nhưng thực tế chúng chồng lấn: culling của archetype đã xong physics có thể bắt
đầu trong khi archetype khác còn đang chạy physics. Đó là điểm khác biệt giữa job graph và một chuỗi
barrier.

---

## 4. Các kênh giữa lane

| từ | tới | bằng gì | vì sao |
|---|---|---|---|
| main / thread ngoài | pool A | injector của lane | không sở hữu ring nào, xem [injector.md](./injector.md) |
| worker | worker (cùng lane) | ring local, rồi steal | đường nóng, phần lớn job đi lối này |
| worker | injector cùng lane | `spill_half` khi ring đầy | biến biên cứng của ring thành ngưỡng xả |
| lane A | lane B | injector của lane B | submit từ ngoài, cùng cơ chế |
| lane B | lane A | injector của lane A, hoặc `JobHandle` hoàn thành | asset xong thì đánh thức job đang đợi |
| lane A | audio | **ring SPSC bounded**, chưa có, phải viết | audio không được lock, không được allocate |
| lane A | main thread | hàng đợi "chỉ main chạy" | present, window API, một số lời gọi driver bắt buộc đúng thread |
| job | job cha | `OneshotChannel`, hoặc latch | trả kết quả về điểm join |

Ô duy nhất trong bảng cần viết mới hoàn toàn là ring SPSC cho audio. Hai ring hiện có đều là SPMC,
và audio cần thứ ngược lại: một người ghi, một người đọc, wait-free ở phía đọc.

---

## 5. Kế hoạch `xynok_concurrency`

### M1. Lõi pool work-stealing

Không có mốc này thì không có gì khác chạy được.

| bước | việc |
|---|---|
| M1.1 | **Xong.** Xóa `src/thread_pool.rs`, đổi `custom_type::Job` sang `InlineFn`, thêm test job chạy qua ring và bị thả cùng ring |
| M1.2 | **Xong.** [`src/injector.rs`](../src/injector.rs): độ dài đọc được không cần khoá, `steal_batch_and_pop` nạp thẳng vào ring bằng một lần publish, trait `LocalQueue` để dùng chung cho cả hai loại ring. Bản linked list block lock-free để sau, xem [mục 8](#8-ba-quyết-định-đã-chốt) |
| M1.3 | Nối `spill_half` (đã có ở [`ring_buffer_fifo/owner.rs:251`](../src/ring_buffer_fifo/owner.rs), chưa ai gọi). Viết `spill_half` cho `ring_buffer_lifo` |
| M1.4 | **Sleep protocol**: một ô atomic đóng gói `(num_searching, num_unparked)` bằng `pack`/`unpack` sẵn có, cộng danh sách thread đang park. Trần searcher ở 50% worker |
| M1.5 | Worker loop: `lifo_slot -> ring -> injector -> steal -> injector -> park`, kèm `tick % 61` ép ngó injector |
| M1.6 | `ThreadPool`: `new(Config)`, `inject`, `worker_index`, `shutdown` có thứ tự, `threads: 0` chạy inline. Export ở `lib.rs` |
| M1.7 | Loom cho sleep protocol. Đây là phần dễ sai nhất và là thứ duy nhất bắt được lost wakeup |

M1.4 là phần khó nhất của cả kế hoạch. `Waker` hiện tại chộp `thread::current()` ngay trong `new()`
([`utils/waker.rs:28`](../src/utils/waker.rs)) nên nó là latch một lần dùng, không phải giao thức
ngủ của pool.

### M2. Fork-join, thứ ECS gọi trực tiếp

| bước | việc |
|---|---|
| M2.1 | `Latch` dùng được từ trong job. `WakerSignal<'a>` mượn `&'a Waker` nên không nhét vào `InlineFn` (`'static`) được. Giải bằng raw pointer kiểu `HeapMut`, an toàn vì `wait()` chặn cho tới khi mọi ticket drop |
| M2.2 | **Work-while-waiting**. Người đợi lấy job về chạy thay vì park. Không có cái này thì mọi barrier lồng nhau hoặc là deadlock, hoặc là bỏ phí core |
| M2.3 | `scope()`, `join(a, b)`, `parallel_for`, `par_reduce`. Port từ `xynok_workers/src/scope.rs`, đổi nền sang ring work-stealing |
| M2.4 | `JobGraph` với `spawn_after(deps)`. Port từ `xynok_workers/src/job_graph.rs`. Đây là thứ ECS scheduler và frame graph đều dùng |
| M2.5 | `PerWorker<T>` và `worker_index()` ổn định trong `0..N`. Vulkan bắt buộc: `VkCommandPool` không thread-safe nên mỗi worker một pool riêng |
| M2.6 | `Bump` arena mỗi worker mỗi frame, `end_frame()` reset một nhát. Port từ `xynok_workers/src/bump.rs` |

`scope()` ở đây **không phải** để ECS mượn `World`: `World` đi qua `HeapMut` nên đã `'static` sẵn.
Nó cần cho hai chỗ khác: chia chunk bên trong một system, và mọi code engine ngoài ECS muốn
fork-join trên dữ liệu stack.

### M3. Lane và kênh chuyên dụng

| bước | việc |
|---|---|
| M3.1 | `Priority` cho Linux (`nice`, `sched_setattr`) và Windows (`SetThreadPriority`, `THREAD_POWER_THROTTLING`). Hiện chỉ có macOS |
| M3.2 | Lane B: pool blocking riêng, cộng `block_in_place` làm cửa thoát khi một job lane A lỡ phải chờ fence |
| M3.3 | **Ring SPSC bounded** cho audio. Wait-free phía đọc, không allocate, không lock. Viết mới |
| M3.4 | Hàng đợi main-thread: `spawn_on_main` và `run_pending_on_main` gọi trong vòng lặp frame |
| M3.5 | `OneshotChannel` port từ `xynok_workers/src/channel.rs` |
| M3.6 | Registry lane: `LaneId`, mỗi lane một pool, config đọc từ env để tune không cần build lại |

### M4. Đo đạc và hardening

| bước | việc |
|---|---|
| M4.1 | `profile::Sink` port, một zone mỗi job, cắm được Tracy hoặc Superluminal |
| M4.2 | Counter: steal trúng/trượt, park/unpark, độ sâu injector, thời gian tìm việc. Không có số thì không tune được kích thước ring, số worker, hằng 61 |
| M4.3 | Loom cho scope và job graph. Miri. Stress test **phải có trần cứng** cho mọi vòng gom kết quả |
| M4.4 | Chế độ `threads: 0` chạy inline, để trả lời câu "bug này có phải do đa luồng không" |

M4.4 nên làm sớm hơn vị trí của nó trong bảng, ngay khi pool chạy được.

---

## 6. Kế hoạch `xynok_ecs`

### E1. Scheduler song song, do người dùng khai báo

Xynok **không** tự suy ra song song. Người viết game nói rõ system nào được chạy cùng nhau, qua một
API kiểu `add_system_parallel` hoặc một system group. Đây là quyết định thiết kế chứ không phải chỗ
còn thiếu, và nó đổi vai trò của `can_parallel_with`: từ "cỗ máy suy ra DAG" thành "trọng tài kiểm
tra lời khai của người dùng".

Đổi lại được gì: DAG tự suy có thể đổi thứ tự chạy giữa hai lần build chỉ vì ai đó thêm một
component vào một query, và bug do thứ tự system đổi thì cực kỳ khó lần. Khai báo tay thì thứ tự
nằm trong code người dùng, đọc được và diff được.

| bước | việc |
|---|---|
| E1.1 | `add_system_parallel(session, (a, b, c))`: một nhóm chạy cùng lúc. `SystemSpecs` kiểm mọi cặp trong nhóm bằng `can_parallel_with`, sai thì panic ngay tại call site, giống cách `add_system` đang bắt system tự alias |
| E1.2 | Session thành danh sách **bước**: mỗi bước là một system đơn hoặc một nhóm song song. Chạy tuần tự qua các bước, trong một bước thì spawn rồi join |
| E1.3 | Mỗi job cần `&mut Box<dyn TSystem>`, không `'static`. Giải bằng `HeapMut<SystemTypeStorage>`, đúng thủ thuật đã dùng cho `World`. Scheduler bảo đảm mỗi system vào đúng một job |
| E1.4 | Main thread tham gia pool trong lúc chờ một bước xong, không park |

`TSystem` đã là `Send + Sync + 'static` và `run` nhận `HeapMut<World>`, nên kiểu dữ liệu phía ECS
gần như không phải đổi. Phần sửa thật nằm gọn trong `DefaultScheduler`.

`JobGraph` của M2.4 vẫn cần, nhưng cho frame graph phía engine (render phụ thuộc culling phụ thuộc
transform), không phải cho ECS scheduler. Với ECS thì "danh sách bước" là đủ và đơn giản hơn nhiều.

### E2. Song song bên trong một system

| bước | việc |
|---|---|
| E2.1 | `Query` chia theo chunk. Chunk archetype là đơn vị chia tự nhiên, `get_components` đã trả `&[C]` nguyên cột nên mỗi job nhận một lát liền mạch |
| E2.2 | Ngưỡng chia: dưới ngưỡng thì chạy thẳng, không spawn. Xem [mục 8](#8-ba-quyết-định-đã-chốt) |
| E2.3 | `par_for_each_chunk` trên `Query`, dựng trên `scope()` của M2.3. Cũng do người dùng gọi tay, cùng tinh thần với E1 |

### E3. Structural change

Đây là thứ chặn ECS song song mà chưa ai đụng tới: `cmd_buffer/mod.rs` đang rỗng.

| bước | việc |
|---|---|
| E3.1 | Command buffer mỗi worker, dựng trên `PerWorker` của M2.5 |
| E3.2 | Áp dụng tại điểm đồng bộ cuối bước, một thread, thứ tự xác định |
| E3.3 | `World::create`, `destroy`, `add_component`, `remove_component` đều nhận `&mut World` nên không gọi được từ job song song. Chúng phải đi qua command buffer. Đây là quy ước cần ghi vào tài liệu người dùng, không phải giới hạn tạm thời |

---

## 7. Thứ tự thực hiện

```text
M1  lõi pool  ──────────────────────────────────▶ mọi thứ khác phụ thuộc vào đây
      │
      ├──▶ M2.1  latch                    ─┐
      ├──▶ M2.2  work-while-waiting        ├──▶ E1  scheduler song song
      └──▶ M2.3  scope, parallel_for      ─┘        (ECS lần đầu ăn nhiều core)
                  │
                  ├──▶ M2.4  job graph  ──▶ frame graph phía engine
                  ├──▶ M2.5  PerWorker  ──▶ E3  command buffer
                  ├──▶ M2.6  Bump
                  └──▶ E2  parallel query
                            │
                            └──▶ M3  lane B, audio SPSC, main queue
                                       │
                                       └──▶ M4  đo đạc, hardening
```

Mốc đáng ăn mừng là hết **E1**: lần đầu ECS chạy trên nhiều core thật, và cũng là lần đầu có số
liệu để tune thay vì đoán.

`xynok_workers` đóng băng từ bây giờ, dùng làm tham chiếu, cho nghỉ khi M2 xong.

---

## 8. Ba quyết định đã chốt

### 8.1. Injector dựng trên `QueueBatching`

Nâng cấp thứ đang có, không dựng linked list block lock-free ngay. Lý do thực dụng: mỗi lane một
injector riêng nên tranh chấp vốn đã thấp, và một cấu trúc mà mình hiểu rõ thì sửa được lúc 2 giờ
sáng.

Bản block lock-free vẫn nằm trong kế hoạch, để sau. Ý tưởng của nó, ghi lại ở đây để lần sau khỏi
phải đi tìm: hàng đợi là một **danh sách liên kết các block**, mỗi block chứa vài chục slot.

```text
        head                                          tail
         │                                              │
         ▼                                              ▼
   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
   │ [x][x][ ][ ] │ ───▶ │ [ ][ ][ ][ ] │ ───▶ │ [ ][ ][ ][ ] │ ───▶ null
   │  block 0     │      │  block 1     │      │  block 2     │
   └──────────────┘      └──────────────┘      └──────────────┘
     đã lấy hết            đang đọc              đang ghi
```

Đa số lần `push` chỉ là một `fetch_add` vào chỉ số trong block hiện tại, không allocate. Chỉ khi
block đầy mới cấp block mới và nối vào, tức là một lần allocate cho vài chục job. Phần khó là thu
hồi block khi vẫn còn thread đang đọc trong đó, và cách crossbeam giải là đếm tham chiếu trên từng
block: thread cuối cùng rời block là thằng giải phóng nó. Đó cũng chính là phần đáng để hiểu kỹ
trước khi viết. Mục 7 của [injector.md](./injector.md) nói thêm về nó.

Cột mốc để quay lại quyết định này: số liệu ở M4.2 cho thấy worker tốn đáng kể thời gian chờ khoá
injector.

### 8.2. Asset IO: thread block thuần trước, chừa chỗ cho async

Mặc định của engine là asset IO async, nhưng giai đoạn này dùng lane B với thread block thuần. Nó
đơn giản hơn hẳn và đã đủ nhanh cho asset loading: `File::read` trên một thread priority thấp không
tệ hơn `io_uring` bao nhiêu khi bạn đọc vài file lớn thay vì hàng vạn file nhỏ.

Ba chỗ cố ý chừa sẵn để sau này nâng lên async mà không phải viết lại:

- **Job trả kết quả qua `OneshotChannel` hoặc `JobHandle`**, không qua giá trị trả về của một lời
  gọi chặn. Người gọi vốn đã không giả định "gọi xong là có kết quả", nên đổi nền phía dưới không
  đụng tới họ.
- **Lane B là một `LaneId` trong registry**, không phải một hàm `load_file` gắn cứng. Đổi cái chạy
  bên trong lane thành một reactor là đổi một chỗ.
- **`Job` là type alias** ([`custom_type.rs`](../src/custom_type.rs)), nên ngày cần thêm biến thể
  "job có thể trả `Pending`" thì đó là sửa một định nghĩa chứ không phải sửa mọi call site.

### 8.3. Ngưỡng chia job

Spawn một job không miễn phí. Chi phí gồm một lần đẩy vào ring, có thể một lần bị steal (CAS vào
`head` của bạn, cộng một cache line bay từ core này sang core kia), một lần chạm latch lúc xong, và
có thể một lần `unpark`. Tổng cỡ **1 tới 5 micro giây**, và phần lớn nằm ở cache line di chuyển chứ
không phải ở lệnh atomic.

Chia 1000 entity cho 8 worker, mỗi job 125 entity:

| việc mỗi entity | job chạy trong | chi phí spawn | kết quả |
|---|---|---|---|
| 2 ns (cộng vector) | 250 ns | ~2000 ns | **chậm hơn chạy tuần tự nhiều lần** |
| 200 ns (skinning) | 25 µs | ~2000 ns | lãi thật, gần 8x |

Nên `parallel_for` không chia mù. Nó nhận thêm tham số `batch`, tức bao nhiêu phần tử một job:

```rust
scope.parallel_for(n, batch, |i| { ... });
// n = 1000, batch = 64   -> 16 job
// n = 1000, batch = 1000 -> 1 job, tức là chạy tuần tự
```

Con số **20 micro giây** là thời gian mục tiêu cho một job, không phải số phần tử. Từ đó suy ngược
ra `batch`: đo được mỗi entity tốn 200 ns thì `batch = 20µs / 200ns = 100`. Chọn 20 µs vì nó đủ lớn
để chi phí spawn (~2 µs) chỉ chiếm 10%, và đủ nhỏ để 8 core vẫn chia đều được việc.

Hai điều đi kèm:

- **Không đoán được từ trên bàn.** Nó phụ thuộc máy, cache, và bản thân công việc. Nên nó là tham
  số có default, và M4.2 sinh ra để đo lại.
- **Có cách bỏ hẳn tham số này, để sau.** Rayon dùng adaptive splitting: chia đôi, và chỉ chia tiếp
  khi thật sự có kẻ đến steal. Không ai rảnh thì không chia thêm. Đẹp hơn nhưng phức tạp hơn.

Với ECS thì đơn vị chia là **chunk**, không phải entity: một chunk đã là khối liền mạch trong bộ
nhớ và số entity mỗi chunk là cố định, nên `batch` đếm bằng chunk và câu hỏi thành "mỗi chunk tốn
bao lâu".
