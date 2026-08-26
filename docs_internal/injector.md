# `injector` - hàng đợi chung của pool

Tài liệu này giải thích **injector**: mảnh còn thiếu giữa các ring local trong
[`ring_buffer_fifo`](./ring-buffer-fifo.md) / [`ring_buffer_lifo`](./ring-buffer-lifo.md) và một
thread pool chạy được thật.

Nó trả lời ba câu, theo đúng thứ tự nên đọc:

| phần | nội dung |
|---|---|
| [1](#1-hai-lỗ-hổng-mà-ring-local-không-tự-bịt-được) | Hai lỗ hổng mà ring local không tự bịt được |
| [2](#2-injector-là-gì-và-không-là-gì) | Injector là gì, và quan trọng hơn: nó **không** là gì |
| [3](#3-ba-đường-vào-một-đường-ra) | Ba đường vào, một đường ra |
| [4](#4-chỗ-nó-đứng-trong-vòng-lặp-worker) | Chỗ nó đứng trong vòng lặp worker |
| [5](#5-fairness-vì-sao-phải-ép-worker-ngó-injector) | Fairness: vì sao phải **ép** worker ngó nó |
| [6](#6-vì-sao-nó-không-thể-là-thêm-một-ring-nữa) | Vì sao nó không thể chỉ là thêm một ring nữa |
| [7](#7-dựng-nó-thế-nào) | Dựng nó thế nào: danh sách block |
| [8](#8-queuebatching-đang-ở-đâu-trong-hình-này) | `QueueBatching` đang ở đâu, và cần sửa gì |
| [9](#9-tra-nhanh) | Tra nhanh |

---

## 1. Hai lỗ hổng mà ring local không tự bịt được

Hai ring trong crate này đều là **SPMC**: một chủ ghi, nhiều kẻ trộm đọc. `Producer` là `!Sync`,
và đó không phải chi tiết cài đặt mà là cả lý do nó nhanh. Nhìn `push`
([`ring_buffer_fifo/owner.rs`](../src/ring_buffer_fifo/owner.rs)):

```rust
unsafe { self.ring.slots.write(tail, val) };
self.ring.tail.store(tail.wrapping_add(1), Ordering::Release);
```

Không CAS, không khoá, không vòng lặp thử lại. Một lần ghi ô, một lần `store(Release)`. Rẻ đúng vì
độc quyền. Cho thread thứ hai ghi vào đó là mất sạch tính chất này.

Từ đó rơi ra hai lỗ hổng.

### Lỗ hổng 1: thread ngoài pool không có ring nào để push

Main thread gọi `spawn(job)`. Nó không phải worker, nó không sở hữu `Producer` nào cả. Nó cũng
không được phép thò tay vào ring của worker 3, vì như trên, hợp đồng nói chỉ một chủ được ghi.

Vậy job đó đi đâu?

Không có chỗ nào. Đây là bế tắc kiến trúc chứ không phải thiếu một hàm tiện ích.

### Lỗ hổng 2: ring có biên, còn công việc thì không

`Slots::new` cấp phát cố định lúc khởi tạo. Ring là một mảng, không phải `Vec`.

Bây giờ một job chạy trên worker 0 và nó spawn ra 10.000 job con. Ring của worker 0 có 256 ô.
`push` trả `Err(val)` ở job thứ 257.

Trả `Err` về cho ai? Người gọi `spawn` là code người dùng, họ không có cách nào xử lý ngoài việc
spin chờ chỗ trống, mà spin ở đây là deadlock: chỗ trống chỉ xuất hiện khi worker chạy tiếp, và
worker thì đang kẹt trong `spawn`.

Cả hai lỗ hổng có chung một lời giải.

---

## 2. Injector là gì, và không là gì

**Injector là một hàng đợi MPMC không giới hạn, dùng chung cho cả pool, nằm cạnh các ring local.**

```text
                        ┌─────────────────────────────┐
   main thread ────────▶│         INJECTOR            │  nhiều người ghi
   thread ngoài ───────▶│  [j][j][j][j][j][j][j][j]   │  nhiều người đọc
   worker spill ───────▶└──────────┬──────────────────┘  không giới hạn
                                   │
                                   │ steal_batch (lấy cả cụm)
                 ┌─────────────────┼─────────────────┐
                 ▼                 ▼                 ▼
            ┌─────────┐       ┌─────────┐       ┌─────────┐
            │ ring W0 │◀─────▶│ ring W1 │◀─────▶│ ring W2 │  một người ghi
            └─────────┘ steal └─────────┘ steal └─────────┘  có biên
                 ▲                 ▲                 ▲
              worker 0          worker 1          worker 2
```

Chỗ dễ hiểu nhầm nhất, nên nói thẳng ra: **injector không thay thế ring local.**

Nó là tầng chậm, và nó phải chậm. Ring local mới là nơi phần lớn job đi qua, và mỗi lần một job
phải vòng qua injector là một lần bạn trả giá tranh chấp. Một pool khoẻ mạnh có injector gần như
lúc nào cũng rỗng: job rơi vào rồi bị hút lên ring local ngay.

Nếu bạn đo thấy injector lúc nào cũng đầy, đó không phải injector hoạt động tốt. Đó là dấu hiệu
ring local quá nhỏ hoặc worker đang bị bỏ đói.

---

## 3. Ba đường vào, một đường ra

### Đường vào 1: submit từ ngoài pool

Đường hiển nhiên nhất. Main thread, thread I/O, thread mạng, bất cứ ai không phải worker.

```rust
pub fn spawn(&self, job: Job)
{
    match Worker::current()          // thread này có phải worker của pool không?
    {
        Some(worker) => worker.push(job),   // có: đi thẳng vào ring local
        None => self.injector.push(job),    // không: rơi vào injector
    }
}
```

Nhánh `Some` là lý do work-stealing nhanh: job do job sinh ra không bao giờ chạm injector.

### Đường vào 2: ring local đầy, worker xả bớt

Đây là lý do `spill_half` tồn tại ở
[`ring_buffer_fifo/owner.rs:251`](../src/ring_buffer_fifo/owner.rs). Hàm đã viết xong và hiện
**chưa có ai gọi**, vì tầng gọi nó chính là tầng còn thiếu.

```rust
fn push_local(&mut self, job: Job)
{
    if self.worker.push(job).is_ok()
    {
        return;
    }

    // ring đầy: rút nửa cũ ra, ném xuống injector, giờ chắc chắn có chỗ
    let mut spill = Vec::with_capacity(self.worker.capacity() / 2 + 1);
    self.worker.spill_half(&mut spill);
    spill.push(job);
    self.injector.push_batch(spill);
}
```

Hai chi tiết đáng dừng lại:

**Vì sao xả nửa *cũ* chứ không phải nửa mới?** Với ring FIFO, nửa cũ là phần ít khả năng còn nóng
trong cache nhất. `spill_half` gọi `pop_batch` từ đầu `head`, đúng phía đó. Nửa mới ở lại vì job
vừa spawn thường thao tác trên dữ liệu mà worker vừa chạm.

**Vì sao đây mới là mảnh làm pool không giới hạn.** Sau khi có spill, biên của ring không còn là
giới hạn cứng nữa mà thành **ngưỡng xả**. `push` không bao giờ thất bại từ góc nhìn người dùng,
trong khi ring local vẫn giữ được kích thước cố định và không allocate trên đường nóng.

### Đường vào 3: đẩy job giữa các lane

Job trên lane `Background` tải xong texture và muốn đẩy bước upload GPU về lane `Frame`. Nó không
sở hữu ring nào của lane `Frame`, nên nó đi qua injector của lane đó. Cùng cơ chế với đường vào 1.

### Đường ra: lấy theo cụm, không lấy từng cái

```rust
// lấy nhiều, giữ 1 chạy ngay, phần còn lại nhét vào ring local
let job = injector.steal_batch_and_pop(&mut self.worker, BATCH);
```

Lý do bắt buộc phải batch: injector là điểm dùng chung của mọi thread trong pool. Lấy một job mỗi
lượt nghĩa là mỗi job phải trả một lần tranh chấp trên cùng một cache line. Lấy 32 cái thì bạn trả
giá một lần, rồi 31 job sau chạy từ ring local và không đụng vào ai.

Kích thước cụm thường lấy `min(injector.len() / n_workers + 1, ring.capacity() / 2)`. Chia cho số
worker để không có một thằng hốt sạch rồi những thằng khác vẫn đói.

---

## 4. Chỗ nó đứng trong vòng lặp worker

Injector xuất hiện đúng hai lần trong thứ tự tìm việc, và cả hai lần đều có lý do riêng.

```text
 1. lifo_slot            job nóng nhất, vừa spawn xong, cache còn ấm
 2. ring local           rẻ, không tranh chấp
 3. INJECTOR             ◀── job từ ngoài vào, và job đã bị spill
 4. steal từ sibling     đắt: phải CAS vào head của người khác
 5. INJECTOR lần nữa     ◀── kiểm lại ngay trước khi ngủ
 6. park
```

**Vì sao bước 3 đứng trước bước 4?** Vì injector rẻ hơn steal. Bước 4 phải chọn nạn nhân ngẫu
nhiên, CAS vào `head` của họ, có thể thua rồi thử lại, và có thể nhận về `Steal::Busy` khi một
thief khác đang dở tay. Injector chỉ có một điểm để hỏi.

Ngoài ra job trong injector thường **già hơn**, và job già hơn nên chạy trước.

**Vì sao có bước 5?** Đây là chỗ tinh tế. Giữa lúc worker làm xong bước 3 và lúc nó thực sự park,
main thread có thể vừa push một job vào injector. Nếu main thread kiểm "có worker nào đang tìm
việc không?" ngay trước khi worker này chuyển sang trạng thái ngủ, nó sẽ thấy "có" và quyết định
không đánh thức ai. Kết quả: job nằm im, worker ngủ, không ai chạy.

Đó là **lost wakeup**. Bước 5 thu hẹp khe hở lại nhưng không đóng hẳn được nó; phần còn lại là
việc của giao thức ngủ/thức (ô atomic đóng gói `num_searching` và `num_unparked`). Đừng nhầm bước
5 là đủ.

---

## 5. Fairness: vì sao phải ép worker ngó injector

Một worker chỉ chạy `lifo_slot -> ring local` có thể tự nuôi mình **mãi mãi**. Job sinh job con,
job con sinh job cháu, ring local không bao giờ cạn, và nó không bao giờ tới được bước 3.

Trong khi đó job của main thread nằm trong injector chết đói. Không phải chậm, mà là chết đói:
không có giới hạn trên cho thời gian chờ.

Cách chữa là một bộ đếm ép ngó:

```rust
self.tick = self.tick.wrapping_add(1);

let job = match self.tick % GLOBAL_QUEUE_INTERVAL == 0
{
    // ép ngó injector trước, kể cả ring local đang đầy việc
    true => self.injector.pop().or_else(|| self.worker.pop()),
    false => self.worker.pop().or_else(|| self.injector.pop()),
};
```

Tokio dùng `GLOBAL_QUEUE_INTERVAL = 61`. Con số là số nguyên tố, chọn vậy để nó không cộng hưởng
với các chu kỳ chẵn khác trong hệ thống (kích thước batch, số worker, chu kỳ frame). Nếu bạn để 64
và batch cũng là 32 thì hai chu kỳ khoá pha với nhau và luôn có worker rơi vào đúng nhịp xấu.

Đây là đánh đổi rõ ràng: bạn trả một phép chia dư mỗi vòng lặp để mua một giới hạn trên cho độ trễ
của job submit từ ngoài. Với một engine có vòng lặp frame thì cái giới hạn trên đó chính là thứ
bạn cần, vì p99 mới quyết định frame có kịp hay không, không phải trung bình.

---

## 6. Vì sao nó không thể là thêm một ring nữa

Câu hỏi tự nhiên: đã có ring rồi, sao không dùng thêm một ring làm hàng đợi chung?

Vì hai bài toán ngược nhau:

| | ring local | injector |
|---|---|---|
| người ghi | 1 (chủ, `!Sync`) | nhiều |
| có biên | có, cố định lúc init | không |
| cấp phát | một lần | theo block, khi cần |
| tần suất chạm | rất cao | thấp |
| tối ưu cho | latency | không bao giờ từ chối |
| giá một lần push | một `store(Release)` | một `fetch_add`, thỉnh thoảng cấp block |

Ring đánh đổi tính linh hoạt lấy tốc độ. Injector đánh đổi tốc độ lấy tính không bao giờ từ chối.
Ép một cấu trúc gánh cả hai thì bạn được một cấu trúc dở ở cả hai.

Cụ thể hơn, nếu dùng ring làm injector:

- **Nhiều người ghi**: `push` phải thành CAS loop, mất luôn cái `store(Release)` một nhịp.
- **Có biên**: bạn quay lại đúng lỗ hổng 2, chỉ là dời nó lên một tầng. Injector đầy thì spill đi
  đâu?

---

## 7. Dựng nó thế nào

Cách phổ biến (crossbeam làm vậy) là **danh sách liên kết các block**, mỗi block chứa vài chục
slot.

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

Vì sao chia block thay vì linked list từng phần tử:

- **Đa số lần push chỉ là một `fetch_add`** vào chỉ số trong block hiện tại. Không allocate.
- **Chỉ khi block đầy** mới cấp block mới và nối vào. Một lần allocate cho vài chục job.
- **Không giới hạn** nhưng vẫn không allocate trên đường nóng.
- **Block đã lấy hết được thu hồi**, nên bộ nhớ không phình theo đỉnh lịch sử.

Phần khó là thu hồi block an toàn khi vẫn còn thread đang đọc trong đó. Crossbeam giải bằng bộ đếm
tham chiếu trên từng block: thread cuối cùng rời block là thằng giải phóng nó.

Nếu bạn không muốn tự dựng phần đó, phương án thay thế hợp lý là giữ một hàng đợi dưới khoá nhưng
**chỉ chạm vào nó theo batch**, để chi phí khoá được chia đều cho vài chục job. Đó chính là hình
dạng của `QueueBatching` hiện tại.

---

## 8. `QueueBatching` đang ở đâu trong hình này

[`src/utils/queue_batching.rs`](../src/utils/queue_batching.rs) **chính là một injector**, phiên
bản đơn giản: một `Queue<T>` dưới spinlock. Nó đã có sẵn thứ quan trọng nhất là `push_batch` và
`pop_batch`, tức là đúng hình dạng cần thiết.

Hai chỗ cần sửa trước khi giao vai trò này cho nó thật.

### 8.1. `len()` và `is_empty()` đang lấy khoá

[`queue_batching.rs:150`](../src/utils/queue_batching.rs)

```rust
pub fn len(&self) -> usize
{
    self.get().len()      // self.get() giành khoá
}
```

Worker ở bước 3 muốn hỏi "có việc không?" mà phải giành spinlock là sai trên đường nóng. Tệ hơn:
nó có thể phải xếp hàng sau lưng một thread đang `drain_into` cả hàng đợi, tức là một câu hỏi đáng
lẽ tốn một lần đọc lại tốn cả một lượt drain.

Cách chữa: một `AtomicUsize` đếm độ dài, cập nhật trong lúc giữ khoá, đọc bằng `Relaxed`.

```rust
#[inline]
pub fn len(&self) -> usize
{
    self.length.load(Ordering::Relaxed)
}
```

Con số đọc ra có thể cũ vài nhịp, và điều đó không sao: worker chỉ dùng nó để quyết định có bõ
công lấy khoá hay không. Nếu nó sai thì lần lấy khoá tiếp theo sẽ nói sự thật.

### 8.2. Spinlock không park, mà crate lại có priority lane

[`queue_batching.rs:171`](../src/utils/queue_batching.rs), `get_contended` quay vòng với `Backoff`
và không bao giờ nhường hẳn.

Ghép với [`apis/priority.rs`](../src/apis/priority.rs): worker `Background` chạy ở
`QOS_CLASS_UTILITY`, và trên Apple Silicon kernel lùa hẳn nó sang E-core. Trên Linux nó ở
`nice(10)`.

Bây giờ dựng kịch bản:

1. Worker `Background` giành được khoá injector.
2. Nó bị preempt, vì nó là thứ đáng bị preempt nhất trong máy.
3. Worker `Frame` trên P-core spin chờ khoá đó.
4. Scheduler nhìn thấy P-core đang bận (spin cũng là bận) nên không vội cho thằng `Background`
   chạy lại.

Đây là **priority inversion** thật, không phải lý thuyết. Nó tệ đúng ở chỗ nó chỉ xuất hiện khi
máy đã tải nặng, tức là đúng lúc bạn cần frame kịp nhất.

Ba hướng chữa, chọn một:

- **Mỗi lane một injector riêng.** Đơn giản nhất, và đằng nào các lane cũng không chia việc cho
  nhau. Chỉ còn tranh chấp giữa các thread cùng priority.
- **Spinlock có đường lui về park.** Sau vài vòng backoff thì ngủ hẳn thay vì đốt core.
- **Chuyển sang cấu trúc block lock-free** ở mục 7. Đắt nhất về công viết, nhưng bỏ hẳn vấn đề.

---

## 9. Tra nhanh

**Injector giải quyết cái gì**

- Thread không phải worker thì không có ring để push.
- Ring local có biên, còn công việc thì không.

**Ba đường vào**

| đường | ai đẩy | khi nào |
|---|---|---|
| submit ngoài | thread không phải worker | mỗi lần `spawn` từ ngoài pool |
| spill | worker | ring local đầy, xả nửa cũ |
| chuyển lane | worker lane khác | đẩy việc sang lane khác |

**Một đường ra**

Luôn theo cụm. `min(len / n_workers + 1, ring.capacity() / 2)`. Giữ một job chạy ngay, phần còn
lại nhét vào ring local.

**Hai chỗ trong vòng lặp worker**

- Sau ring local, trước khi steal.
- Một lần nữa ngay trước khi park, để thu hẹp khe hở lost wakeup.

**Một luật fairness**

Cứ 61 vòng thì ép ngó injector trước, kể cả ring local đang đầy việc.

**Dấu hiệu injector đang sai**

| triệu chứng | nguyên nhân thường gặp |
|---|---|
| injector lúc nào cũng đầy | ring local quá nhỏ, hoặc worker bị bỏ đói |
| job submit từ main thread trễ bất thường | thiếu luật fairness ở mục 5 |
| p99 frame dựng đứng khi máy tải nặng | priority inversion ở mục 8.2 |
| CPU 100% mà throughput thấp | thiếu chặn số searcher, mọi worker cùng hỏi injector |

**Còn thiếu gì để nối vào**

- `spill_half` đã có ở [`ring_buffer_fifo/owner.rs:251`](../src/ring_buffer_fifo/owner.rs), chưa
  có ai gọi.
- `ring_buffer_lifo` chưa có `spill_half` tương ứng.
- `QueueBatching` cần bộ đếm len atomic và một quyết định về spinlock giữa các lane.
- Vòng lặp worker, nơi mọi thứ ở trên được lắp lại, vẫn chưa tồn tại.
