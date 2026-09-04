---
excerpt:
cover img:
---

# Lanes and the job system in Xynok Engine

![test image ref](images/test_img.jpg)

Tài liệu này chốt hai thứ:

1. **Flow của engine khi nhiều lane cùng chạy**: ai chạy ở đâu, nói chuyện với nhau bằng gì.
2. **Kế hoạch sửa** `xynok_concurrency` và `xynok_ecs` để flow đó thành hiện thực.

Trạng thái tính tới 28/08/2026: **M1 tới M5 và E1 tới E3 đều đã làm xong**. `xynok_ecs` giờ chạy
được nhóm system song song, chia query theo chunk, và có command buffer cho structural change. Lane B
không còn là mấy thread ngồi chặn nữa mà là một **lane async**: nó chạy future, và một task đang chờ
thì không nằm trên thread nào ([mục 5, M5](#m5-lane-b-thành-lane-async)). Cái còn lại trong tài liệu
này là mấy chỗ cố ý để lại, đều ghi rõ lý do tại chỗ: reactor thật cho IO
([mục 8.2](#82-lane-async-executor-trước-reactor-sau)), loom cho scope và job graph (M4.3),
`sched_setattr` trên Linux (M3.1), và bản lane queue block lock-free
([mục 8.1](#81-lane-queue-dựng-trên-queuebatching)).

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
| `xynok_concurrency` | mọi thứ song song sống ở đây | **M1 tới M5 xong**, xem [mục 5](#5-kế-hoạch-xynok_concurrency) |
| `xynok_workers` | pool đời trước, khoảng 7.5k dòng | đóng băng, chỉ còn dùng làm tham chiếu |
| `xynok_ecs` | dữ liệu và system | chunk archetype xong, **E1 tới E3 xong**: scheduler chạy được nhóm song song |

**`xynok_concurrency` đang có**: ba ring (FIFO/LIFO SPMC kèm batch steal, và SPSC cho audio, đều có
loom và stress), `InlineFn` 64 byte vừa một cache line, [`QueueBatching`](../src/utils/queue_batching/),
[`ThreadPool`](../src/pool/mod.rs) work-stealing với giao thức ngủ riêng, `Scope` (`join`,
`parallel_for`, `par_reduce`), `JobGraph`, `PerWorker`, `Bump`, kênh oneshot, registry
[`Lanes`](../src/lanes.rs), `profile::Sink` và bộ đếm. `Priority` giờ có cả ba nền tảng.

**Thứ đã mang sang từ `xynok_workers`**: `Latch` (viết lại để vé đi được vào job), `Scope`,
`PerWorker`, `JobGraph`, `Bump`, `OneshotChannel`, `profile::Sink`. Thứ không mang sang là hàng đợi
bounded: `push` trả `Result<(), T>` và đó chính là lỗ hổng mà [lane_queue.md](./lane_queue.md) mô
tả.

**`xynok_ecs` có sẵn thứ quan trọng nhất**: `AccessScopes::can_parallel_with`
([`query/access_scope.rs`](../../xynok_ecs/src/query/access_scope.rs)). Câu hỏi khó nhất của một
ECS scheduler song song đã được trả lời từ trước, và giờ `DefaultScheduler`
([`schedule/scheduler.rs`](../../xynok_ecs/src/schedule/scheduler.rs)) dùng nó thật: session là một
danh sách bước, mỗi bước là một system đơn hoặc một nhóm song song. `xynok_ecs` phụ thuộc thẳng vào
`xynok_concurrency` bằng path, xem [mục 6](#6-kế-hoạch-xynok_ecs).

`src/thread_pool.rs` là bản nháp trong `#[cfg(test)]`, đã xóa ở bước đầu tiên.

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
| **B. Async** | nạp asset, decode texture, compile shader, giải nén | pool riêng 2 tới 4 thread, priority thấp, chạy **task** (future) | việc ở đây là một chuỗi chờ nối nhau. Một cái chờ không được phép chiếm core của lane A |
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
 pool A · worker 0  │       │       │ [physics] [ecs] [cull] [record]│
 pool A · worker 1  │       │       │ [physics] [ecs] [cull] [record]│
 pool A · worker 2  │       │       │ [physics] [ecs] [cull] [record]│
                    │       │       │                                │
 pool B · task 0    │ ··· đọc scene.pak ···▷ await ▷··· decode ··· vắt qua nhiều frame ···>
 pool B · task 1    │ ··· compile shader ··▷ await ▷··· link ····························>
                    │
 audio thread       │ cb    cb    cb    cb    cb    cb    cb    cb    cb    cb
                    └──────────────────────────────────────────────────────┘
```

Bốn điều đáng chú ý trong hình:

**Main thread tham gia pool.** Nó không submit rồi ngủ. Trong lúc chờ schedule xong nó lấy job về
chạy như một worker. Đây là lý do `N = cores - 1`: main thread là người thứ N. Nếu để nó park thì
bạn mất một core suốt cả frame, và trên máy 4 core đó là 25%.

**Lane B vắt qua ranh giới frame.** `present` không đợi nó. Một asset load xong ở frame N+7 thì kết
quả đi ngược về lane A qua lane queue, và job nào đang đợi asset đó được đánh thức bằng job graph
không bằng polling.

Chỗ `▷ await ▷` trong hình là chỗ đáng nhìn nhất: giữa hai bước, task **không** nằm trên thread nào
cả. Hai thread của lane B phục vụ được hàng chục asset đang dở dang, vì cái chiếm thread chỉ là đoạn
syscall thật, không phải cả chuỗi.

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
| main / thread ngoài | pool A | lane queue của lane | không sở hữu ring nào, xem [lane_queue.md](./lane_queue.md) |
| worker | worker (cùng lane) | ring local, rồi steal | đường nóng, phần lớn job đi lối này |
| worker | lane queue cùng lane | `spill_half` khi ring đầy | biến biên cứng của ring thành ngưỡng xả |
| lane A | lane B | lane queue của lane B | submit từ ngoài, cùng cơ chế |
| lane B | lane B | waker của task, xếp task lại vào chính lane queue đó | một `.await` xong là một lần task tự xếp hàng lại |
| lane B | lane A | lane queue của lane A, hoặc `JobHandle` hoàn thành | asset xong thì đánh thức job đang đợi |
| lane A | audio | **ring SPSC bounded** ([`ring_buffer_spsc`](../src/ring_buffer_spsc/mod.rs)) | audio không được lock, không được allocate |
| lane A | main thread | hàng đợi "chỉ main chạy" | present, window API, một số lời gọi driver bắt buộc đúng thread |
| job | job cha | `OneshotChannel`, hoặc latch | trả kết quả về điểm join |

Ô duy nhất trong bảng phải viết mới hoàn toàn là ring SPSC cho audio, vì hai ring cũ đều là SPMC còn
audio cần thứ ngược lại: một người ghi, một người đọc, wait-free ở **cả hai** phía chứ không riêng
phía đọc. Nó xong rồi, và cả bảng này giờ đã có mặt đủ trong code.

Hàng "lane B tới lane B" là hàng mới, và nó là toàn bộ cái khác biệt của lane async: đường quay lại
của một task không phải một kênh riêng, nó vẫn là lane queue cũ. Waker chỉ là cái nút bấm để đẩy
task vào đó thêm một lượt nữa.

---

## 5. Kế hoạch `xynok_concurrency`

### M1. Lõi pool work-stealing

Không có mốc này thì không có gì khác chạy được.

| bước | việc |
|---|---|
| M1.1 | **Xong.** Xóa `src/thread_pool.rs`, đổi `custom_type::Job` sang `InlineFn`, thêm test job chạy qua ring và bị thả cùng ring |
| M1.2 | **Xong.** [`src/utils/queue_batching/`](../src/utils/queue_batching/): độ dài đọc được không cần khoá, `steal_batch_and_pop` nạp thẳng vào ring bằng một lần publish, trait `LocalQueue` để dùng chung cho cả hai loại ring. Bản linked list block lock-free để sau, xem [mục 8](#8-ba-quyết-định-đã-chốt) |
| M1.3 | **Xong.** `spill_half` được `Shared::push_local` gọi khi ring đầy, và `ring_buffer_lifo` giờ có bản của riêng nó ([`ring_buffer_lifo/owner.rs`](../src/ring_buffer_lifo/owner.rs)): chủ bốc được cả lô ở phía `top`, khác kẻ trộm, vì `bottom` không đổi dưới lưng nó |
| M1.4 | **Xong.** [`src/pool/sleep.rs`](../src/pool/sleep.rs). Một ô atomic gói **ba** con số chứ không phải hai, xem ghi chú bên dưới. Trần searcher ở 50% worker, sàn là một |
| M1.5 | **Xong.** Worker loop trong [`src/pool/mod.rs`](../src/pool/mod.rs): `lifo_slot -> ring -> lane_queue -> steal -> lane_queue -> park`, kèm `tick % 61` ép ngó lane queue |
| M1.6 | **Xong.** `ThreadPool::new(Config)`, `spawn`, `worker_index`, `shutdown` có thứ tự, `threads: 0` chạy inline |
| M1.7 | **Xong.** [`src/pool/tests/loom.rs`](../src/pool/tests/loom.rs), bốn mô hình, cộng stress test có trần cứng |

M1.4 đúng là phần khó nhất, và nó không chạy đúng ngay từ bản đầu. Hai chỗ phải sửa, cả hai đều đáng
ghi lại:

**Trần searcher phải có sàn.** Lấy đúng 50% thì pool một hoặc hai worker có trần bằng 0, tức là
không worker nào được phép đi trộm, và việc nằm trong ring của người khác thì nằm đó mãi. Một test
treo 60 giây mới lòi ra chuyện này.

**Hai con số là chưa đủ, phải có ba.** Bản đầu chỉ gói `(searching, unparked)` và `notify` đọc ô đó
bằng một `load` thường. Loom dựng lại được cảnh: worker đọc hàng đợi thấy rỗng (một giá trị cũ, mà
mô hình bộ nhớ cho phép), còn người đẩy job thì đọc thấy "chưa ai ngủ" và bỏ đi. Cách bịt là thêm
một **bộ đếm sự kiện** vào cùng ô, và bắt cả hai phía chạm ô đó bằng một read-modify-write: worker
đọc bộ đếm trước khi đi tìm việc, rồi so lại lúc sắp ngủ. Hai RMW trên cùng một địa chỉ thì luôn có
một cái đến trước, nên không còn khe nào ở giữa.

`Waker` cũ vẫn ở nguyên chỗ của nó ([`utils/waker.rs`](../src/utils/waker.rs)): nó chộp
`thread::current()` ngay trong `new()` nên là latch một lần dùng. [`Latch`](../src/latch.rs) mới là
bản dùng được từ trong job.

Một cái bẫy nữa ở M1.6, và nó chỉ lộ ra khi chạy ví dụ thật chứ không test nào bắt được: **một
thread chỉ nhớ được chỗ đứng ở một pool**. `Lanes` dựng hai pool trên cùng main thread, nên pool
dựng sau ghi đè chỗ đứng của pool trước, và main thread mất cái ring của nó ở pool kia. Không sai
kết quả, chỉ là mọi job nó spawn phải đi vòng qua lane queue, tức là qua một cái khoá, cho từng job
một. Bộ đếm ở M4.2 nói ra ngay: `lane_pops` bằng hơn nửa số job đã chạy. Bịt bằng hai thứ, một cái
chữa triệu chứng và một cái chữa gốc: `Lanes` dựng lane async trước lane compute, và
`Shared::context` có thêm một đường lùi so id thread với host. Ví dụ `frame` chạy nhanh hơn 30% sau
khi sửa, và `lane_pops` về 0.

### M2. Fork-join, thứ ECS gọi trực tiếp

| bước | việc |
|---|---|
| M2.1 | **Xong.** [`src/latch.rs`](../src/latch.rs). Vé mang con trỏ thô thay cho tham chiếu nên đi được vào `InlineFn`, an toàn vì lần chờ không trả về khi còn vé chưa thả. Vé trừ bộ đếm lúc `Drop`, nên job panic cũng không mang nó xuống mồ |
| M2.2 | **Xong.** `ThreadPool::run_until`, và mọi điểm join đều đi qua nó |
| M2.3 | **Xong.** [`src/scope.rs`](../src/scope.rs): `scope()`, `join`, `parallel_for`, `par_reduce`. Job của scope không phải `'static`, và cũng không phải box: `InlineFn::new_unbound` nhận closure mượn stack, đổi lại người gọi phải bảo đảm job xong trước khi stack đó biến mất, mà đó đúng là thứ scope làm |
| M2.4 | **Xong.** [`src/job_graph.rs`](../src/job_graph.rs): `spawn_with_handle` và `spawn_after(deps)` |
| M2.5 | **Xong.** [`src/per_worker.rs`](../src/per_worker.rs), kèm cờ bắt lỗi mượn hai lần cùng một ô, vì work-while-waiting làm chuyện đó với tới được |
| M2.6 | **Xong.** [`src/bump.rs`](../src/bump.rs), cộng `ThreadPool::scratch` và `end_frame` |

`scope()` ở đây **không phải** để ECS mượn `World`: `World` đi qua `HeapMut` nên đã `'static` sẵn.
Nó cần cho hai chỗ khác: chia chunk bên trong một system, và mọi code engine ngoài ECS muốn
fork-join trên dữ liệu stack.

Một cái bẫy chung của M2.3 và M2.4, và miri là thứ chỉ ra nó với đúng bốn chữ **"trying to join
itself"**: `Scope` và nút của `JobGraph` ban đầu giữ nguyên một `ThreadPool`, mà `ThreadPool` thì
giữ đám join handle, tức là nó là một tay cầm **giữ cho pool sống**. Trình tự dẫn tới tai nạn:

```text
worker:  chạy xong job của nút, thả vé latch
scope:   thấy bộ đếm về 0, trả về
gọi:     thả nốt tay cầm pool cuối cùng của mình
worker:  vẫn còn trong Node::run một nhịp nữa, dọn danh sách kế nhiệm rồi thả Arc<Node>
worker:  đó là tay cầm pool cuối cùng  ->  Drop  ->  shutdown  ->  join chính mình
```

Cách bịt là tách "phần dùng chung" khỏi "phần giữ cho sống": cả hai giờ giữ `Arc<Shared>`, và
`Shared` cố ý không chứa join handle nào. Ai muốn pool sống thì phải cầm một `ThreadPool` thật.

Cùng cái bẫy ấy vẫn với tới được từ phía người dùng: chỉ cần một job bắt được một `ThreadPool`
clone rồi tình cờ là kẻ thả cái tay cầm cuối cùng. Nên `shutdown` không cấm suông nữa mà xử lý hẳn:
nếu thread đang gọi là worker của chính pool đó thì nó bỏ bước join và chỉ thả các handle ra, để
thread tự kết thúc khi thấy cờ shutdown. Đổi lại, `shutdown` trả về trước khi worker dừng hẳn, nên
đường tử tế vẫn là tắt pool từ thread đã dựng nó.

### M3. Lane và kênh chuyên dụng

| bước | việc |
|---|---|
| M3.1 | **Xong.** Ba nền tảng, kèm `THREAD_POWER_THROTTLING` để thread frame không bị Windows dọn xuống core tiết kiệm điện. Util clamp trên Linux (`sched_setattr`) để sau, xem ghi chú bên dưới |
| M3.2 | **Xong.** `Lanes::async_lane()` là pool riêng, priority thấp, ngủ sớm. `block_in_place` có, kèm ghi rõ giới hạn: nó gọi thêm một worker dậy chứ không spawn worker thay thế. Lane này về sau thành lane async, xem [M5](#m5-lane-b-thành-lane-async) |
| M3.3 | **Xong.** [`src/ring_buffer_spsc`](../src/ring_buffer_spsc/mod.rs). Không CAS ở đâu cả nên cả hai phía đều wait-free, và có loom kiểm ba tính chất: không mất phần tử, nội dung hiện ra cùng lúc với chỉ số, người ghi không đè lên ô chưa đọc |
| M3.4 | **Xong.** `Lanes::spawn_on_main` và `run_pending_on_main`. Lần vét chỉ chạy những job đã có mặt lúc bắt đầu, để một job đẻ job không giữ main thread lại vô hạn |
| M3.5 | **Xong.** [`src/channel.rs`](../src/channel.rs), kèm một thứ bản cũ không có: đầu gửi biến mất mà chưa gửi gì thì người chờ nhận `None` chứ không treo. Job panic là chuyện xảy ra thật |
| M3.6 | **Xong.** [`src/lanes.rs`](../src/lanes.rs): `LaneId`, `Lanes`, config đọc từ `XYNOK_LANE_THREADS` và `XYNOK_ASYNC_THREADS` |

`sched_setattr` để lại có lý do chứ không phải quên: nó là một syscall thô, số hiệu khác nhau theo
kiến trúc, và nó chỉ đáng làm khi đã có số đo cho thấy governor đang hạ tần số nhầm chỗ.

### M4. Đo đạc và hardening

| bước | việc |
|---|---|
| M4.1 | **Xong.** [`src/profile.rs`](../src/profile.rs), một zone mỗi job, cộng `worker_park`/`worker_unpark` là thứ profiler không nhìn thấy từ ngoài |
| M4.2 | **Xong.** [`src/pool/counters.rs`](../src/pool/counters.rs): job đã chạy, trộm trúng/trượt, lần lấy từ lane queue, lần xả, lần park, độ sâu lane queue. Mỗi worker một ô riêng trên một cache line riêng, vì đo mà làm chậm thứ đang đo thì con số đọc ra không nói về hệ thống thật nữa |
| M4.3 | **Một phần.** Loom phủ giao thức ngủ, latch, ring SPSC, hai ring cũ. Scope và job graph thì đang dựa vào stress test có trần cứng chứ chưa có mô hình loom, xem ghi chú bên dưới. Miri chạy sạch trên nhóm test của pool |
| M4.4 | **Xong.** `Config::inline()`, cộng `XYNOK_LANE_THREADS=1` để tắt song song mà không sửa dòng code nào |

Vì sao scope và job graph chưa có loom: cả hai đều dựng trên một `ThreadPool` thật, mà pool thì dùng
`thread_local` để biết mình là ai. Dưới loom thì các "thread" là coroutine chạy trên một thread OS
duy nhất, nên `thread_local` của std không tách được chúng ra, và mô hình sẽ nói dối. Muốn làm cho
đúng thì phải bọc chỗ đăng ký đó lại sau một lớp thay được, và đó là việc đáng làm riêng chứ không
nên nhét vào đây.

M4.4 đã làm sớm hơn vị trí của nó trong bảng, đúng như ghi chú cũ đề nghị: `threads: 0` có từ lúc
`ThreadPool` chạy được lần đầu.


### M5. Lane B thành lane async

Mục 8.2 hồi đầu chốt "thread block thuần trước, chừa chỗ cho async", và ba chỗ chừa sẵn ở đó giờ được
dùng tới. Lane B không còn là mấy thread ngồi trong `read()` nữa: nó chạy **task**, và một task là
một future, poll ra `Pending` thì trả thread lại cho lane ngay tại chỗ.

| bước | việc |
|---|---|
| M5.1 | **Xong.** [`src/task.rs`](../src/task.rs): một task là future cộng một ô trạng thái, waker của nó đẩy chính nó về lane queue |
| M5.2 | **Xong.** [`Receiver`](../src/channel.rs) là một `Future`. Cùng một cái kênh giờ phục vụ cả ba kiểu chờ: `recv` (ngủ), `recv_in` (chạy job giúp lane), và `.await` (không giữ thread nào) |
| M5.3 | **Xong.** `LaneId::Async`, `Lanes::spawn_async`, `Lanes::async_lane()`, `Lanes::block_on`. `run_blocking` giữ nguyên tên nhưng đổi vai: nó là **đáy** của một chuỗi async, chỗ gọi cái syscall thật |
| M5.4 | **Xong.** `task::block_on` cho thread ngoài pool, `task::block_on_in` chờ bằng cách chạy job của lane, cộng `task::yield_now` |
| M5.5 | **Xong.** Unit test, một stress test có trần cứng (số task và số lượt nhường đều là hằng), và miri chạy sạch trên cả nhóm `task::` lẫn `channel::` |

Ba chỗ đáng ghi lại.

**Một cờ `queued` là không đủ, phải là một ô trạng thái.** Waker được phép gọi bất cứ lúc nào và gọi
bao nhiêu lần cũng được, kể cả đúng lúc worker đang poll. Với một cờ `bool` thì lần gọi giữa chừng ấy
hoặc rơi mất (task ngủ luôn), hoặc đẩy task vào hàng đợi lần thứ hai trong khi bản thứ nhất còn đang
chạy, tức là hai worker cùng poll một future. Ô trạng thái có năm nhánh, và `NOTIFIED` chính là chỗ
ghi lại "có người gọi dậy trong lúc đang poll" cho tới khi poll xong:

```text
   IDLE ──wake──▶ SCHEDULED ──worker lấy ra──▶ RUNNING ──Ready──▶ DONE
     ▲                 ▲                          │
     │                 │                       Pending
     └── poll xong ────┴── xếp lại ◀── NOTIFIED ◀─┴── wake giữa lúc poll
```

**Task giữ `Arc<Shared>`, không giữ `ThreadPool`.** Đúng cái bẫy "trying to join itself" ở M2.3 và
M2.4, chỉ khác chỗ đứng: một task nằm trong hàng đợi của lane mà lại giữ tay cầm giữ-cho-pool-sống
thì tay cầm cuối cùng rất dễ bị thả bởi chính worker đang chạy nó.

**Và nó lôi ra một lỗi thật trong giao thức ngủ.** `Sleep::notify_all` trước đây thấy danh sách ngủ
rỗng là trả về ngay, không nhích bộ đếm sự kiện. Nhưng "chưa có ai trong danh sách" không có nghĩa là
"không có ai sắp ngủ": một worker vừa kiểm cờ shutdown thấy `false` và đang trên đường tới `park` thì
chưa kịp ghi tên, nên nó không nhận `unpark` nào, mà bộ đếm cũng không đổi để nó biết mà quay lại.
Nó ngủ qua luôn lần shutdown, và thread đi `join` nó thì đợi tới hết đời tiến trình.

Khe ấy rất hẹp, hẹp tới mức chạy test bình thường bao nhiêu lần cũng không thấy. Miri dựng lại được
ngay khi nhiều test cùng dựng rồi tắt pool trong một tiến trình. Cách bịt là một dòng: `notify_all`
nhích bộ đếm **luôn luôn**, kể cả khi không gọi ai dậy cả. Mô hình loom `notify_all_khong_bo_sot_ai`
cũng được nới theo, vì giờ worker ra khỏi `park` bằng `Cancelled` (tự rút tên trước khi kịp ngủ) là
chuyện hợp lệ, và còn rẻ hơn `Notified` một lần park.

Cái **chưa** có, và cố ý chưa có: một reactor thật. Xem [mục 8.2](#82-lane-async-executor-trước-reactor-sau).

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
| E1.1 | **Xong.** `add_system_parallel(session, (a, b, c))`, cộng `TIntoSystems` cho tuple tới 16 phần tử. `SystemSpecs::check_group_can_parallel` kiểm mọi cặp bằng `can_parallel_with`, sai thì panic ngay tại call site, giống cách `add_system` đang bắt system tự alias |
| E1.2 | **Xong.** `ScheduleStep::Single` và `ScheduleStep::Parallel`, session là một `Vec<ScheduleStep>`. Chạy tuần tự qua các bước, trong một bước thì spawn rồi join bằng `pool.scope` |
| E1.3 | **Xong.** Không cần `HeapMut<SystemTypeStorage>`: `group.iter_mut()` đã cho ra đúng những `&mut` rời nhau mà mỗi job cần, và borrow checker kiểm giúp luôn. `HeapMut` vẫn dùng, nhưng cho `World` |
| E1.4 | **Xong.** `pool.scope` chờ bằng work-while-waiting, nên thread gọi bốc luôn một trong mấy job vừa spawn |

`TSystem` đã là `Send + Sync + 'static` và `run` nhận `HeapMut<World>`, nên kiểu dữ liệu phía ECS
gần như không phải đổi. Phần sửa thật nằm gọn trong `DefaultScheduler`.

Một cái bẫy mà chỉ miri chỉ ra được, và nó không nằm trong bảng trên: **khởi tạo một `Query` là ghi
vào `World`**. Lần đầu gặp một kiểu query, world đăng ký component chưa từng thấy, thêm một
`QuerySpec`, và dựng lại danh sách archetype của query mỗi khi có archetype mới. `TSystemParam::init`
làm đúng chuyện đó, mà `init` thì chạy bên trong job. Hai system của một nhóm là hai đường ghi vào
một registry.

Cách bịt là tách `TSystemParam::prepare` khỏi `TSystemParam::init`: `prepare` giữ toàn bộ phần ghi và
chạy tuần tự trên thread gọi trước khi spawn, còn `init` chỉ còn tra bảng qua
`World::query_src_access`, tức là chỉ đọc. Miri gọi tên nó ra ngay cả khi không ai ghi gì thật, vì
chỉ riêng việc dựng `&mut World` từ hai thread đã là data race rồi.

`JobGraph` của M2.4 vẫn cần, nhưng cho frame graph phía engine (render phụ thuộc culling phụ thuộc
transform), không phải cho ECS scheduler. Với ECS thì "danh sách bước" là đủ và đơn giản hơn nhiều.

### E2. Song song bên trong một system

| bước | việc |
|---|---|
| E2.1 | **Xong.** `TQueryParam::ChunkColumns` cho ra `&[C]`, `&mut [C]`, hoặc tuple các lát. `ChunkView` gói chúng lại kèm `&[Entity]` |
| E2.2 | **Xong.** `batch` đếm bằng chunk, và `parallel_for` tự chạy thẳng khi `batch` lớn hơn tổng số chunk. Xem [mục 8](#8-ba-quyết-định-đã-chốt) |
| E2.3 | **Xong.** `Query::par_for_each_chunk` và `Query::for_each_chunk`, dựng trên `parallel_for` của M2.3. Cũng do người dùng gọi tay, cùng tinh thần với E1 |

Một điểm đáng ghi lại: **`Entity` không phải một cột component**, nó nằm trong header của chunk. Nên
`Query<&Entity>` không chạy được, và `ChunkView` là chỗ duy nhất lấy được id entity. Đó cũng là thứ
mà E3 cần, vì một lệnh `destroy` thì phải biết huỷ ai.

Chỗ nối giữa hai mảng: `ChunkIndex` dựng một bảng prefix sum cho mỗi **archetype**, không phải cho
mỗi chunk. Một world lớn có hàng vạn chunk nhưng chỉ vài chục archetype, nên bảng ấy bé và dựng lại
mỗi lần gọi cũng không đáng kể.

### E3. Structural change

| bước | việc |
|---|---|
| E3.1 | **Xong.** `CommandBuffers` là một `PerWorker<CommandBuffer>` sống trong `World`, vì world là thứ duy nhất một `TSystemParam` với tới được. Param tên `Commands` |
| E3.2 | **Xong.** `World::apply_commands` đi theo thứ tự ô rồi tới thứ tự ghi trong ô, không theo thứ tự worker nào xong trước. Scheduler gọi nó ở cuối mỗi bước |
| E3.3 | **Xong.** `Commands` có `create`, `destroy`, `add_component`, `merge_component`, `remove_component`, cộng `push` cho việc tuỳ ý. Ghi trong README và trong `examples/multi_thread_system.rs`. Đây là quy ước lâu dài, không phải giới hạn tạm thời |

Hai chỗ đáng nói:

**`Commands` không góp gì vào access scope.** Nó không chạm kho component nào, nên hai system cùng
cầm nó vẫn đứng chung một nhóm song song được. Mỗi cái ghi vào ô của worker mình.

**`create` chưa trả về `Entity`.** Lúc ghi lệnh thì entity chưa tồn tại. Muốn có id ngay thì phải đặt
chỗ trước trong bảng entity, và đó là việc riêng, đáng làm khi có chỗ dùng thật cần nó.

---

## 7. Thứ tự thực hiện

```text
M1  lõi pool  ✓ ────────────────────────────────▶ mọi thứ khác phụ thuộc vào đây
      │
      ├──▶ M2.1  latch                 ✓  ─┐
      ├──▶ M2.2  work-while-waiting    ✓   ├──▶ E1  scheduler song song  ✓
      └──▶ M2.3  scope, parallel_for   ✓  ─┘        (ECS đã ăn nhiều core)
                  │
                  ├──▶ M2.4  job graph ✓ ──▶ frame graph phía engine  ◀── ở đây
                  ├──▶ M2.5  PerWorker ✓ ──▶ E3  command buffer  ✓
                  ├──▶ M2.6  Bump      ✓
                  └──▶ E2  parallel query  ✓
                            │
                            └──▶ M3  lane B, audio SPSC, main queue  ✓
                                       │
                                       ├──▶ M4  đo đạc, hardening  ✓
                                       └──▶ M5  lane B thành lane async  ✓
```

Cả `xynok_concurrency` lẫn E1 tới E3 của `xynok_ecs` đã xong. Mốc đáng ăn mừng là hết **E1**: lần đầu
ECS chạy trên nhiều core thật, và cũng là lần đầu có số liệu để tune thay vì đoán. M5 thì đóng nốt
món nợ mà mục 8.2 ghi từ đầu: lane B giờ đúng hình dạng async như thiết kế nói, chứ không còn là mấy
thread ngồi chặn.

Việc tiếp theo là **frame graph phía engine**, dựng trên `JobGraph` của M2.4: render phụ thuộc
culling phụ thuộc transform. ECS thì không cần nó, vì "danh sách bước" đã đủ, nhưng phía engine thì
mấy pass có phụ thuộc chéo thật.

Bốn chỗ cố ý để lại, xếp theo thứ tự đáng làm trước:

0. **Reactor cho lane async** ([mục 8.2](#82-lane-async-executor-trước-reactor-sau)). Hình dạng async
   đã xong, phần dưới đáy thì vẫn là một syscall chặn trên một thread của lane. Cắm reactor vào là
   sửa đúng `Lanes::run_blocking`, không đụng call site nào.
1. **Loom cho scope và job graph** (M4.3). Phải bọc chỗ đăng ký `thread_local` lại sau một lớp thay
   được thì loom mới không nói dối. Đến giờ chỗ trống ấy được lấp bằng stress test và bằng miri, mà
   miri thì đã bắt được một lỗi thật ở E1, nên nó không phải là không có giá trị.
2. **Số đo thật của E1 và E2.** Bảng benchmark ở [mục 8.3](#83-ngưỡng-chia-job) mới đo `parallel_for`
   trần. Con số cần tiếp theo là "một chunk ECS tốn bao lâu", vì đó là thứ suy ra `batch`.
3. **Lane queue block lock-free** ([mục 8.1](#81-lane-queue-dựng-trên-queuebatching)) và
   `sched_setattr` trên Linux (M3.1). Cả hai đều đợi số đo chỉ ra rằng chúng đáng làm.

`xynok_workers` đóng băng từ đây, chỉ còn dùng làm tham chiếu.

---

## 8. Ba quyết định đã chốt

### 8.1. Lane queue dựng trên `QueueBatching`

Nâng cấp thứ đang có, không dựng linked list block lock-free ngay. Lý do thực dụng: mỗi lane một
hàng đợi riêng nên tranh chấp vốn đã thấp, và một cấu trúc mà mình hiểu rõ thì sửa được lúc 2 giờ
sáng.

Lựa chọn đó chỉ đứng được nhờ một điều kiện, và cần nói rõ vì ngày nào nó gãy thì quyết định này
gãy theo: **mọi đường nóng chạm khoá theo cụm**. Submit từ ngoài đi `push` một lần rồi thôi, worker
lấy việc thì `steal_batch_and_pop` bốc cả cụm, ring đầy thì `spill_half` xả nửa hàng một lượt. Chi
phí một lần giành khoá được chia đều cho vài chục job. Nếu có chỗ nào chạm khoá một lần cho một
job, spinlock không sống nổi, và đó là dấu hiệu phải sửa chỗ gọi trước khi nghĩ tới việc đổi cấu
trúc. Bộ đếm `spills` và `lane_pops` ở M4.2 là chỗ nhìn ra chuyện đó.

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
trước khi viết. Mục 7 của [lane_queue.md](./lane_queue.md) nói thêm về nó.

Hai cột mốc để quay lại quyết định này:

- Số liệu ở M4.2 cho thấy worker tốn đáng kể thời gian chờ khoá lane queue.
- **Priority inversion**: một thread lane B priority thấp bị OS cắt ngang trong lúc đang giữ khoá,
  còn một worker lane A thì đang quay tại chỗ chờ đúng cái khoá đó. Số liệu của nó không phải
  throughput trung bình mà là p99 frame time lúc máy tải nặng, và triệu chứng này đủ để đổi cấu
  trúc kể cả khi throughput vẫn đẹp.

### 8.2. Lane async: executor trước, reactor sau

Bản đầu của mục này chốt "thread block thuần trước, chừa chỗ cho async", và ghi ba chỗ chừa sẵn. Ba
chỗ đó đã dùng tới ở [M5](#m5-lane-b-thành-lane-async), và giờ mục này chốt phần còn lại: đã có
executor, chưa có reactor, và đó là hai thứ khác nhau.

**Executor** là thứ quyết định "task nào được poll, poll ở đâu". Nó xong rồi:
[`src/task.rs`](../src/task.rs) chạy trên chính lane queue và pool work-stealing đang có, không thêm
thread nào, không thêm hàng đợi nào. Một task đang `.await` thì không nằm trên thread nào cả, nên hai
tới bốn thread của lane B phục vụ được hàng chục asset dở dang.

**Reactor** là thứ hỏi OS "cái file này đọc xong chưa" mà không chặn thread: `io_uring`, `epoll`,
`kqueue`, IOCP. Cái này **chưa** có. Đáy của mọi chuỗi async hiện giờ vẫn là `Lanes::run_blocking`,
tức là một closure đồng bộ chạy trên một thread của lane. Nói thẳng ra thì hình dạng lời gọi là async
còn cái syscall thì vẫn chặn, y như `spawn_blocking` của tokio.

Nói vậy không phải để xin lỗi. Ở tải thật của một engine, tức là đọc vài file lớn chứ không phải hàng
vạn file nhỏ, `File::read` trên một thread priority thấp không thua `io_uring` bao nhiêu, còn phần
code phải viết và phải debug thì chênh nhau cả một bậc. Cái đắt của kiểu chặn không phải bản thân
syscall, mà là **giữ thread** trong lúc chờ, và đúng cái đó thì executor đã gỡ xong: chỉ đoạn syscall
thật mới chiếm thread, không phải cả chuỗi nạp asset.

Hai chỗ để quay lại quyết định này:

- **Số file đang bay vượt số thread của lane.** Lúc đó `run_blocking` thành nút cổ chai thật, và
  `parks` cùng `lane_pops` của lane B ở M4.2 là chỗ nhìn ra.
- **Có nhu cầu huỷ giữa chừng.** Đổi scene mà bỏ dở mười file đang đọc thì với thread chặn là không
  huỷ được, phải đợi syscall xong; với reactor thì thả task là xong.

Ngày làm, chỗ phải sửa là ruột của `Lanes::run_blocking`: thay vì đẩy closure cho một thread, nó đăng
ký một sự kiện với reactor và trả về đúng cái `Receiver` như cũ. Waker mà task để lại chính là thứ
reactor cần để gọi task dậy, và nó đã nằm sẵn ở đó. Call site không đổi một dòng nào, và đó là điểm
của cả cái thiết kế này.

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

### Số đo thật, thay cho phỏng đoán

`benches/pool.rs` đo lại đúng mấy con số ở trên. Trên M-series 12 core, pool 4 worker cộng thread
gọi, 65 536 phần tử, lấy median:

| việc mỗi phần tử | batch 64 | batch 1024 | một job (tuần tự) |
|---|---|---|---|
| 16 phép nhân | 102 µs | **69 µs** | 266 µs |
| 1 phép nhân | 84 µs | 18 µs | **20 µs** |

Hàng trên là chỗ chia có lãi: nhanh gấp 3.8 lần chạy tuần tự, và `batch = 64` đã bắt đầu lỗ so với
`1024` vì chi phí spawn. Hàng dưới là chỗ chia thành lỗ: một phép nhân mỗi phần tử thì `batch = 64`
chậm gấp bốn lần cứ để yên mà chạy, còn `batch = 1024` chỉ ngang bằng.

Chi phí spawn đo được thấp hơn khoảng ước lượng ở trên: một job rỗng qua `scope` tốn chừng **120
nano giây** khi pool đang nóng, và 512 job rỗng trong một scope là 191 µs, tức chừng **370 nano
giây một job**. Khoảng 1 tới 5 micro giây ở đầu mục vẫn đúng cho trường hợp xấu, khi job bị trộm
sang core khác và kéo theo một cache line đi cùng, nhưng nó không phải con số thường gặp.

Hai điều đi kèm:

- **Không đoán được từ trên bàn.** Nó phụ thuộc máy, cache, và bản thân công việc. Bảng trên là số
  của một máy, một tải; đổi máy thì đo lại, đó chính là việc `benches/pool.rs` và M4.2 sinh ra để
  làm.
- **Có cách bỏ hẳn tham số này, để sau.** Rayon dùng adaptive splitting: chia đôi, và chỉ chia tiếp
  khi thật sự có kẻ đến steal. Không ai rảnh thì không chia thêm. Đẹp hơn nhưng phức tạp hơn.

Với ECS thì đơn vị chia là **chunk**, không phải entity: một chunk đã là khối liền mạch trong bộ
nhớ và số entity mỗi chunk là cố định, nên `batch` đếm bằng chunk và câu hỏi thành "mỗi chunk tốn
bao lâu".


