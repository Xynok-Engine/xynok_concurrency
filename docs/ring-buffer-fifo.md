# `ring_buffer_fifo` — ba ngón tay `steal`, `real`, `tail`

Tài liệu này giải thích `src/ring_buffer_fifo/` từ **cội nguồn**: vì sao nó có hình dạng như bây giờ,
và ba chỉ số bên trong nó hoạt động ra sao.

Bản song sinh của nó là [`ring_buffer_lifo`](./ring-buffer-lifo.md) — chủ lấy ở đuôi, giữ được
tính LIFO, đổi lại kẻ trộm chỉ bốc được **một** job mỗi lượt. Mục [1 nấc 5](#nấc-5--chase-lev-né-bằng-cách-đứng-hai-đầu)
giải thích vì sao hai ràng buộc đó đi liền nhau. Chọn cái nào: xem cuối tài liệu này.

Bố cục:

| phần | nội dung |
|---|---|
| [1](#1-cội-nguồn-sáu-nấc-thang) | Sáu nấc thang lịch sử — mỗi nấc chết vì cái gì |
| [2](#2-ba-chỉ-số-là-gì) | Ba chỉ số là gì, và hai phép trừ duy nhất cần nhớ |
| [3](#3-ví-dụ-chạy-tay-trên-một-mảng-4-ô) | Chạy tay từng bước trên một mảng 4 ô |
| [4](#4-bốn-hành-vi-thật-trong-code) | Bốn hành vi thật: `push`, `pop`, `claim`, `release` |
| [5](#5-ai-ghi-chỉ-số-nào-lúc-nào) | Bảng tra: ai ghi cái gì, lúc nào |
| [6](#6-những-cách-làm-sai-và-chúng-vỡ-ở-đâu) | Bốn cách làm sai và chúng vỡ ở đâu |
| [7](#7-tra-nhanh) | Tra nhanh API và ví dụ chạy được |

---

## 1. Cội nguồn: sáu nấc thang

Thiết kế hiện tại không phải ý tưởng đầu tiên. Nó là nấc cuối của một chuỗi, và mỗi nấc sinh ra để
chữa đúng cái chết của nấc trước.

### Nấc 0 — Một hàng đợi chung dưới một `Mutex`

Mọi worker chung một `VecDeque`. Muốn lấy việc thì khoá, thò tay vào, mở khoá.

**Chết vì:** 8 worker thì 7 đứng đợi. Tệ hơn, đợi lâu quá thì hệ điều hành cho ngủ, và một lần
đánh thức tốn cỡ chục nghìn nhịp máy — trong khi bản thân công việc chỉ vài trăm nhịp. Thêm nữa,
tất cả cùng đập vào một cache line, nên mỗi lần chạm là một lần giằng cache giữa các lõi.

### Nấc 1 — Mỗi worker một hàng đợi riêng

Hết giằng, hết đợi. Nhanh tuyệt đối.

**Chết vì:** mất cân bằng tải. Worker A ngập 500 job, worker B ngồi không mà chẳng giúp được.
Máy 8 lõi chạy như máy 1 lõi.

### Nấc 2 — Hàng đợi riêng + cho phép ăn cắp việc

Worker hết việc thì sang hàng đợi người khác bốc bớt. Đây là **work stealing**, từ Cilk
(Blumofe & Leiserson, 1994); ngày nay nằm trong Tokio, Rayon, Go scheduler, Java `ForkJoinPool`.

Nhưng nếu hàng đợi vẫn có khoá, chủ của nó phải khoá/mở **mỗi lần** đụng vào hàng đợi của chính
mình, trong khi phần lớn thời gian không ai đến trộm.

**Chết vì:** trả giá thường trực cho một tình huống hiếm.

### Nấc 3 — Bỏ khoá: một người ghi, một người đọc

Ring buffer kinh điển, hai chỉ số — đúng như tài liệu kernel Linux mà `mod.rs` dẫn nguồn:

```text
   người đọc ─→ head              tail ←─ người ghi
                  │                 │
                  ▼                 ▼
        [ ][ ][ A  B  C  D  E ][ ][ ][ ]
```

Không khoá, chỉ một luật: **ghi giá trị vào ô trước, rồi mới nhích `tail`.** Người đọc thấy `tail`
nhích thì chắc chắn giá trị đã nằm sẵn ở đó.

**Chết vì:** luật đó chỉ đúng khi có **đúng một** người đọc. Thêm người thứ hai là vỡ.

### Nấc 4 — Nhiều người đọc: hai chỉ số không đủ nữa

Đây là chỗ ngón tay thứ ba ra đời.

Với hai chỉ số, người đọc chỉ có hai lựa chọn, **cả hai đều sai**:

| cách | làm gì | vỡ ở đâu |
|---|---|---|
| A | nhích `head` **trước**, rồi chép giá trị ra | người ghi thấy ô trống, ghi đè lên đúng lúc ta đang chép |
| B | chép giá trị ra **trước**, rồi mới nhích `head` | người đọc thứ hai thấy `head` chưa nhích, chép lại đúng ô đó — hai người cùng cầm một giá trị, thả hai lần là **double free** |

Vấn đề gốc: một chỉ số bị bắt nói hai câu trái ngược nhau cùng lúc —

- *"chỗ này có chủ rồi, đừng lấy nữa"* (phải nói **trước** khi chép), và
- *"chỗ này trống rồi, ghi đè được"* (chỉ được nói **sau** khi chép xong).

Không thể. **Nên tách làm hai:**

| chỉ số | nghĩa | nhích lúc nào |
|---|---|---|
| `real` | "đã có chủ, đừng lấy nữa" | **trước** khi chép |
| `steal` | "chép xong rồi, ghi đè được" | **sau** khi chép |

Khoảng `[steal, real)` giữa hai chỉ số là **tấm biển đang thi công**.

Với đúng một người đọc thì hai câu đó phát ra cùng lúc, nên một chỉ số là đủ — đó là lý do nấc 3
sống khoẻ mấy chục năm trong kernel.

### Nấc 5 — Chase-Lev: né bằng cách đứng hai đầu

Thiết kế work-stealing kinh điển (Chase & Lev, 2005): **chủ làm việc ở đuôi (LIFO), kẻ trộm lấy ở
đầu (FIFO).** Hai bên ở hai cực nên hầu như không đụng nhau — chủ `push`/`pop` gần như miễn phí,
không CAS. Lợi thêm: job chủ lấy ra là job vừa bỏ vào, dữ liệu còn nóng trong cache.

**Chết vì (trong bối cảnh này):** để làm vậy, chủ phải **hạ `tail` xuống đầu cơ rồi khôi phục** khi
thấy hụt — tức là có chỉ số **đi lùi**. Mà mọi so sánh ở đây là phép trừ trên số đếm vòng, chỉ
diễn giải được khi mọi chỉ số luôn tiến; cho phép lùi thì mọi lập luận đều phải mang theo ngoại lệ.

Nặng hơn: khi kẻ trộm bốc **cả lô** thay vì một job, vùng chủ nhắm (ở đuôi) và vùng trộm nhắm (ở
đầu) có thể chồng lên nhau — mà `tail` chỉ mình chủ ghi, `head` chỉ mình trộm ghi, **không lệnh
nguyên tử nào nhìn được cả hai** để phân xử. Hàng rào `SeqCst` cứu được ca một-job, không cứu được
ca một-vùng.

### Nấc 6 — Thiết kế hiện tại

Bỏ luôn chuyện đứng hai đầu: **cả chủ lẫn kẻ trộm cùng lấy ở `real`.**

```text
           steal          real                      tail
             │              │                         │
             ▼              ▼                         ▼
  [ ][ ][ ][ X  X  X ][ A  B  C  D  E ][ ][ ][ ][ ][ ][ ]
            └đang bê ┘└ chưa ai nhận ┘
```

| được | mất |
|---|---|
| mọi lần lấy — một job hay cả lô — là **một lần CAS trên `head`**, ai thắng thì rõ ràng | chủ mất tính LIFO, job vừa đặt không còn nóng trong cache |
| **ba chỉ số chỉ tiến, không bao giờ lùi** → đọc lệch nhịp chỉ ra số cũ hơn, không bao giờ ra số vô nghĩa | mỗi `pop` của chủ phải trả một CAS |
| `push` vẫn hoàn toàn không CAS | |

Dòng thứ hai mới là món lời lớn nhất: nó cắt đôi độ dài của mọi lập luận về sau.

Đây gần đúng hàng đợi cục bộ của **Tokio** hiện nay: ring buffer cố định, `head` gói hai nửa
`steal`/`real`, `tail` riêng, kẻ trộm bốc một nửa. Tokio bù chỗ "mất LIFO" bằng một ô riêng bên
ngoài ring buffer, giữ đúng một job nóng nhất.

### Đường đi tóm tắt

```text
hàng đợi chung + khoá      → ai cũng đợi ai, đánh thức tốn kém
   ↓ bỏ dùng chung
hàng đợi riêng mỗi worker  → nhanh, nhưng kẻ ngập việc người ngồi chơi
   ↓ cho ăn cắp việc
riêng + khoá               → trả giá khoá thường trực cho chuyện hiếm
   ↓ bỏ khoá: 1 ghi, 1 đọc
2 chỉ số (kernel)          → vỡ ngay khi có 2 người đọc
   ↓ tách "đã có chủ" khỏi "đã xong"
3 chỉ số                   → chạy được với nhiều người đọc
   ↓ hai bên đứng hai đầu (Chase-Lev)
chủ gần như miễn phí       → nhưng chỉ số phải lùi, và bốc cả lô thì không phân xử được
   ↓ kéo cả hai về cùng một đầu
thiết kế này               → mọi thứ chỉ tiến, mọi tranh chấp gói vào một CAS
```

---

## 2. Ba chỉ số là gì

### Chỉ số đếm vô hạn, ô thì quay vòng

Đây là điều phải nắm trước mọi thứ khác:

> **Ba chỉ số không phải "vị trí trong mảng". Chúng là bộ đếm chạy thẳng, không bao giờ quay lại.**

Đặt 10 000 job vào một ring buffer 256 ô thì `tail` đếm tới 10 000. Chỉ khi cần biết *ô nào* mới lấy phần
dư — và vì sức chứa luôn là lũy thừa của 2 nên phép chia dư đó là một phép `AND`:

```rust
fn slot(&self, idx: u32) -> &UnsafeCell<MaybeUninit<T>>
{
    &self.slots[(idx & self.mask) as usize] // mask = capacity - 1
}
```

`src/ring_buffer_fifo/mod.rs:201`. Đây là **chỗ duy nhất** trong toàn bộ module có phép mask. `tail`,
`real`, `steal` không bao giờ bị mask.

### Ba vai

| chỉ số | trỏ vào | ai đẩy lên | ý nghĩa một câu |
|---|---|---|---|
| `tail` | ô trống kế tiếp | **chỉ mình chủ**, lúc `push` | "chỗ ghi tiếp theo" |
| `real` | phần tử đầu **chưa ai nhận** | chủ lúc `pop`, kẻ trộm lúc `claim` | "đã có chủ, đừng lấy nữa" |
| `steal` | phần tử đầu của vùng **đang bị bê** | kẻ trộm lúc `release`, chủ lúc `pop` | "chép xong rồi, ghi đè được" |

Bất biến toàn cục: **`steal ≤ real ≤ tail`**, và cả ba **chỉ tăng**.

### Đời một phần tử

Mỗi phần tử bị ba chỉ số vượt qua, luôn theo đúng thứ tự này:

```text
  ①  tail vượt qua nó   → đã nằm trong ring buffer, ai cũng lấy được
  ②  real vượt qua nó   → có chủ rồi, người khác đừng đụng
  ③  steal vượt qua nó  → đã chép xong ra ngoài, ô được phép ghi đè
```

- Chủ `pop`: ② và ③ xảy ra **cùng một lúc**, trong một CAS.
- Kẻ trộm `steal_batch`: ② và ③ cách nhau một quãng — quãng đang chép dữ liệu.

### Hai phép trừ, và chỉ hai

Mọi câu hỏi về ring buffer đều quy về một trong hai phép trừ này:

| câu hỏi | công thức | dùng ở | code |
|---|---|---|---|
| còn mấy chỗ trống? (cho `push`) | `capacity - (tail - steal)` | chủ | `owner.rs:73` `free_from` |
| còn mấy job lấy được? (cho `pop`/`steal`) | `tail - real` | cả hai | `mod.rs:183` `available` |

**`real` không tham gia vào phép tính chỗ trống.** Vùng `[steal, real)` đã có chủ nhưng dữ liệu vẫn
còn nằm trong ô, nên vẫn tính là **bị chiếm** — mốc của phép tính chỗ trống bắt buộc là `steal`.

Cũng vì hai câu hỏi này khác nhau mà kiểu này **không có `len()`**: `occupied()` (mốc `steal`) và
`available()` (mốc `real`) trả lời hai chuyện khác nhau, và `is_empty()` cặp với `available()`.

### `steal` và `real` nằm chung một ô nhớ

```rust
pub const fn pack(a: u32, b: u32) -> u64 { (a as u64) << 32 | (b as u64) }
pub const fn unpack(val: u64) -> (u32, u32) { ((val >> 32) as u32, val as u32) }
```

`src/utils/mod.rs:14`. Hai chỉ số là hai nửa của **một** `AtomicU64` tên là `head`.

Lý do: kẻ trộm cần vừa kiểm tra *"có ai đang bê không"* (`steal == real`?) vừa giành vùng, trong
**một** thao tác nguyên tử. Phần cứng không có lệnh CAS trên hai ô nhớ, nên hai chỉ số phải ở chung
một ô.

---

## 3. Ví dụ chạy tay trên một mảng 4 ô

Dưới đây là toàn bộ cơ chế, rút gọn còn một mảng thường và ba biến `u32` — không atomic, không
thread. Phép toán chỉ số thì giống hệt bản thật, từng dòng.

```rust
const CAP: u32 = 4;
const MASK: u32 = CAP - 1; // = 3

struct RingBufferRutGon
{
    o:     [char; CAP as usize], // '.' = chưa từng ghi gì
    steal: u32,
    real:  u32,
    tail:  u32,
}

impl RingBufferRutGon
{
    /// Chỗ trống — mốc là `steal`, KHÔNG phải `real`.
    fn con_trong(&self) -> u32
    {
        CAP - self.tail.wrapping_sub(self.steal)
    }

    /// Job còn lấy được — mốc là `real`.
    fn con_lay_duoc(&self) -> u32
    {
        self.tail.wrapping_sub(self.real)
    }

    /// Chủ đặt job. Ghi ô TRƯỚC, nhích `tail` SAU.
    fn push(&mut self, val: char) -> Result<(), char>
    {
        if self.con_trong() == 0
        {
            return Err(val);
        }
        self.o[(self.tail & MASK) as usize] = val; // ① ghi
        self.tail = self.tail.wrapping_add(1); // ② công bố
        Ok(())
    }

    /// Chủ lấy job ở `real`. Bản thật: đúng một lần CAS trên `head`.
    fn pop(&mut self) -> Option<char>
    {
        if self.con_lay_duoc() == 0
        {
            return None;
        }
        let idx = self.real;
        self.real = self.real.wrapping_add(1);
        if self.steal == idx
        {
            // Không ai đang bê → kéo `steal` theo, trả ô lại cho `push` ngay lập tức.
            self.steal = self.real;
        }
        // Còn nếu có kẻ trộm đang bê thì `steal` phải nằm yên, chỉ `real` đi tiếp.
        Some(self.o[(idx & MASK) as usize])
    }

    /// Kẻ trộm, nhịp 1: dán biển. `real` tiến, `steal` đứng yên.
    fn claim(&mut self, max: u32) -> Option<(u32, u32)>
    {
        if self.steal != self.real
        {
            return None; // đã có kẻ trộm khác đang bê — đi tìm ring buffer khác
        }
        let n = self.con_lay_duoc().min(max);
        if n == 0
        {
            return None;
        }
        let start = self.real;
        self.real = self.real.wrapping_add(n);
        Some((start, n))
    }

    /// Kẻ trộm, nhịp 2: gỡ biển — CHỈ sau khi đã chép xong.
    /// `steal` nhảy thẳng lên `real` HIỆN TẠI, không phải mốc cuối vùng đã nhận.
    fn release(&mut self)
    {
        self.steal = self.real;
    }
}
```

### Bảng chạy

Bắt đầu: mảng `[. . . .]`, `steal = real = tail = 0`.

| # | thao tác | mảng sau đó | steal | real | tail | ghi chú |
|---|---|---|---|---|---|---|
| 0 | *(khởi tạo)* | `. . . .` | 0 | 0 | 0 | rỗng: `tail - real = 0` |
| 1 | `push('A')` | `A . . .` | 0 | 0 | 1 | ghi ô `0 & 3 = 0` |
| 2 | `push('B')` | `A B . .` | 0 | 0 | 2 | |
| 3 | `push('C')` | `A B C .` | 0 | 0 | 3 | |
| 4 | `push('D')` | `A B C D` | 0 | 0 | 4 | `tail - steal = 4 = CAP` → đầy |
| 5 | `push('E')` | `A B C D` | 0 | 0 | 4 | **`Err('E')`** — trả lại nguyên giá trị |
| 6 | `pop()` → `'A'` | `A B C D` | 1 | 1 | 4 | `steal == real` nên **cả hai** cùng nhích. Chữ `A` vẫn nằm trong ô — byte rác vô hại, chỉ số mới là chân lý |
| 7 | `push('E')` | `E B C D` | 1 | 1 | 5 | còn trống `4 - (4-1) = 1`; ghi ô `4 & 3 = 0`, đè lên rác `A` |
| 8 | `claim(2)` → `(1, 2)` | `E B C D` | **1** | **3** | 5 | nhận `[1,3)` = ô 1, 2 = `B`, `C`. **`steal != real`: biển đã dán** |
| 9 | `push('F')` | `E B C D` | 1 | 3 | 5 | **`Err('F')`** — còn trống `4 - (5-1) = 0`. Vùng đang bê vẫn tính là chiếm |
| 10 | `pop()` → `'D'` | `E B C D` | 1 | 4 | 5 | `steal != real` nên **chỉ `real`** nhích. Chủ lấy được `D` trong lúc kẻ trộm còn đang bê `B`, `C` |
| 11 | *(kẻ trộm chép xong `B`, `C`)* | `E B C D` | 1 | 4 | 5 | |
| 12 | `release()` | `E B C D` | **4** | 4 | 5 | `steal` nhảy lên `real` **hiện tại** = 4, không phải mốc 3 của vùng đã nhận |
| 13 | `push('G')` | `E G C D` | 4 | 4 | 6 | còn trống `4 - (5-4) = 3`; ghi ô `5 & 3 = 1` |
| 14 | `push('H')` | `E G H D` | 4 | 4 | 7 | ô `6 & 3 = 2` |
| 15 | `push('I')` | `E G H I` | 4 | 4 | 8 | ô `7 & 3 = 3`; giờ `tail - steal = 4` → đầy trở lại |

Bốn điều đọc ra từ bảng:

1. **Bước 6 và 7** — lấy job ra không hề xoá ô. Chữ `A` còn nguyên tới khi `E` ghi đè. "Ô trống"
   không phải trạng thái của ô, nó là **lời cho phép ghi đè**, và lời đó nằm ở chỉ số `steal`.
2. **Bước 8 → 12** — quãng `steal != real` là tấm biển. Trong quãng đó `push` bị từ chối
   (bước 9) dù nhìn vào mảng thì có vẻ vẫn còn chỗ.
3. **Bước 10** — chủ và kẻ trộm làm việc song song trên cùng một ring buffer, không ai chờ ai, và mỗi bên
   lấy phần khác nhau.
4. **Bước 12** — `release` kéo `steal` lên `real` **hiện tại**. Nếu đặt bằng mốc cũ (3) thì
   `steal = 3 ≠ real = 4` vĩnh viễn: mọi kẻ trộm sau đó tưởng có người đang bê và bỏ đi, `push`
   tưởng ring buffer đầy hơn thực tế — ring buffer kẹt cứng mà không ai báo lỗi.

   Kéo lên `real` hiện tại là an toàn vì `[3, 4)` đã bị **chính chủ ring buffer** lấy đi, mà chủ chỉ có một
   thread: nó không thể vừa ở trong `pop` vừa quay ra `push` đè lên.

---

## 4. Bốn hành vi thật trong code

Bản thật khác bản mô phỏng trên ở đúng một chỗ: mọi thay đổi `head` đều qua `compare_exchange`, và
mọi thay đổi có kèm thứ tự bộ nhớ.

### 4.1 `Producer::push` — không CAS

`src/ring_buffer_fifo/owner.rs:83`

```rust
let tail = self.ring.tail.load(Ordering::Relaxed);  // chỉ mình ta ghi → luôn chính xác
if self.free_from(tail) == 0 { return Err(val); }   // mốc là `steal`
self.ring.slot(tail).with_mut(|p| unsafe { (*p).write(val) });        // ① ghi ô
self.ring.tail.store(tail.wrapping_add(1), Ordering::Release);        // ② công bố
```

Toàn bộ khoản lời so với khoá nằm ở đây: **không CAS, không giành**. Đổi lại là ràng buộc "đúng một
`Producer`", được kiểu dữ liệu canh chứ không phải tài liệu (xem [§5](#5-ai-ghi-chỉ-số-nào-lúc-nào)).

`Release` ở ② ghép với `Acquire` khi người khác nạp `tail`: **thấy chỉ số mới là chắc chắn thấy cả
giá trị vừa ghi**. Đảo thứ tự ① và ② là công bố một ô chưa ghi xong.

`push_batch` (`owner.rs:111`) giống hệt, chỉ khác: ghi `n` ô rồi **công bố một lần duy nhất**. Cho
tới lệnh `store` đó, với mọi thread khác thì cả lô **chưa tồn tại**.

### 4.2 `Producer::pop` — một CAS, cùng đầu với kẻ trộm

`src/ring_buffer_fifo/owner.rs:142`

```rust
let next = match steal == real
{
    true  => pack(next_real, next_real), // không ai đang bê → kéo cả hai
    false => pack(steal, next_real),     // có kẻ trộm đang bê → `steal` nằm yên
};
self.ring.head.compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire)
```

Nhánh `true` là chỗ dễ hỏi nhất: vì sao chủ được kéo `steal` lên **trước** khi chép giá trị ra?

Kéo `steal` lên nghĩa là "ô này ghi đè được". Người duy nhất ghi đè là chủ — mà chủ đang đứng ngay
trong `pop`, không thể đồng thời ở trong `push`. Kẻ trộm không có đặc quyền đó, nên nó buộc phải
làm hai nhịp.

### 4.3 `Consumer::claim` — nhịp 1, dán biển

`src/ring_buffer_fifo/thief.rs:61`

```rust
let (steal, real) = unpack(head);
if steal != real { return None; }                    // đã có kẻ khác đang bê → đi luôn
let tail = self.ring.tail.load(Ordering::Acquire);   // đọc `tail` SAU `head`
let n = tail.wrapping_sub(real).min(max);
// `steal` đứng yên tại `real` cũ — đó là điều làm nên tấm biển
self.ring.head.compare_exchange_weak(head, pack(real, real.wrapping_add(n)), AcqRel, Acquire)
```

Hai chi tiết đắt giá:

- **Thứ tự đọc `head` trước, `tail` sau** là bắt buộc. Cả hai chỉ tiến, nên `tail` đọc sau luôn
  `≥ real` đọc trước, và hiệu không bao giờ âm. Đọc ngược lại thì `real` có thể đã vượt qua `tail`
  cũ, và `wrapping_sub` trả về một con số khổng lồ. Cùng lý do đó, `occupied()` (`mod.rs:173`) đọc
  `head` trước rồi mới `tail`.
- **Thấy `steal != real` thì bỏ đi ngay, không thử lại.** Đứng đợi ở một ring buffer đang bị bốc thì thà
  đi tìm ring buffer khác.

### 4.4 `Consumer::release` — nhịp 2, gỡ biển

`src/ring_buffer_fifo/thief.rs:109`

```rust
let (_, real) = unpack(head);
self.ring.head.compare_exchange_weak(head, pack(real, real), Ordering::Release, Ordering::Relaxed)
```

Gọi **sau** khi đã chép xong toàn bộ vùng đã nhận. `Release` chốt mọi lần đọc ô ở trên lại trước
thao tác này: chủ ring buffer chỉ thấy `steal` tiến lên sau khi từng byte đã được chép xong.

Ghép cả ba lại thành `steal_batch` (`thief.rs:146`):

```rust
let (start, n) = self.claim(max)?;                        // ① một CAS
unsafe { self.ring.drain_claimed(start, n, dst) };        // ② chép — mất thời gian
self.release();                                           // ③ một CAS
```

Quãng giữa ① và ③ chính là "đang bê dở". Nó **không** là khoảnh khắc lý thuyết: `n` có thể là hàng
trăm phần tử, và hệ điều hành có quyền dừng thread này giữa chừng cả mili giây. Chạy
`cargo run --example ring_buffer_oop` sẽ thấy một thread quan sát bắt gặp trạng thái đó vài chục
lần chỉ trong một lần chạy.

---

## 5. Ai ghi chỉ số nào, lúc nào

| chỉ số | ai được ghi | ghi bằng | ở đâu |
|---|---|---|---|
| `tail` | **chỉ `Producer`** | `store` trần, `Release` | `owner.rs:99`, `owner.rs:134` |
| `real` | `Producer` và `Consumer` | CAS trên `head` | chủ: `owner.rs:166`, `owner.rs:210` · trộm: `thief.rs:88` |
| `steal` | `Producer` và `Consumer` | CAS trên `head` | chủ: cùng chỗ trên · trộm: `thief.rs:121` |

| việc xảy ra | ai | `steal` | `real` | `tail` |
|---|---|---|---|---|
| `push` / `push_batch` | chủ | – | – | **+1 / +n** |
| `pop`, không ai đang bê | chủ | **+1** | **+1** | – |
| `pop`, đang có kẻ trộm bê | chủ | – | **+1** | – |
| `claim` (nhận vùng) | trộm | – | **+n** | – |
| `release` (bê xong) | trộm | **nhảy lên = `real`** | – | – |

Đọc theo cột: **`tail` chỉ nhích khi có giá trị đi vào ring buffer; `real` và `steal` chỉ nhích khi có giá
trị đi ra.**

### Ràng buộc "đúng một Producer" được canh bằng kiểu

| bất biến | canh bằng |
|---|---|
| đúng một thread ghi `tail` | `Producer` chứa `PhantomData<Cell<T>>` → `!Sync`, `!Clone` (`owner.rs:18`) |
| nhiều thread cùng bốc là hợp lệ | `Consumer: Copy` — `head` lo phần giành giật |
| không tách quyền hai lần | `split(&mut self)` mượn độc quyền suốt vòng đời hai thẻ (`mod.rs:126`) |
| không đọc một ô hai lần | `drain_claimed` là `unsafe`, chỉ gọi sau một CAS thắng (`mod.rs:213`) |

`RingBufferFifo::producer()` (`mod.rs:154`) là `unsafe` chính vì nó bỏ qua ràng buộc đầu: chỗ gọi phải
tự bảo đảm không bao giờ có hai `Producer` cùng sống. Hai `Producer` là hai thread cùng `store` vào
`tail` — mất job và ghi đè lên nhau, im lặng.

---

## 6. Những cách làm sai, và chúng vỡ ở đâu

### 6.1 Gỡ biển trước khi chép xong

Gọi `release()` ngay sau `claim()`, rồi mới chép.

**Vỡ:** chủ thấy ô trống, `push` giá trị mới đè lên đúng lúc kẻ trộm đang chép. Kẻ trộm bê về một
giá trị nửa cũ nửa mới; job mới của chủ biến mất trong im lặng.

Đây là lỗi mà stress test gần như không bao giờ bắt được — phải để `loom` bắt
(`src/ring_buffer_fifo/tests/loom.rs`).

### 6.2 Đánh dấu "đang bê" bằng cách **hạ** `steal` thay vì **đẩy** `real`

Ring buffer 4 ô, `steal = real = 4`, `tail = 8`, đang chứa `E F G H` ở ô 0–3.

Kẻ trộm muốn 2 job. Hạ `steal` 4 → 2 thì vùng đang bê thành `[2, 4)` = ô 2, 3 — nhưng ô 2, 3 giờ
đang chứa `G`, `H`, hai job **mới**:

| hỏng | chi tiết |
|---|---|
| bê nhầm | tưởng nhận job ở chỉ số 2, 3 (đã bị lấy từ lâu), thực ra chạm vào `G`, `H` |
| đọc hai lần | `real` vẫn ở 4, nên với mọi người khác `G`, `H` vẫn "chưa ai nhận" → sẽ bị lấy lần nữa → **double free** |
| tính toán nổ | `còn trống = 4 - (8 - 2) = 4 - 6` → tràn `u32` thành số khổng lồ → chủ ghi đè lên tất cả |

Gốc rễ: job chưa ai nhận nằm ở `[real, tail)`, nên nhận job = **cắt bớt từ đầu bên trái** = đẩy
`real`. `steal` nằm phía sau `real`, và phía sau là quá khứ.

### 6.3 Dùng `real` làm mốc tính chỗ trống

`còn trống = capacity - (tail - real)` bỏ qua vùng `[steal, real)`.

**Vỡ:** vùng đó đã có chủ nhưng dữ liệu vẫn nằm trong ô. Chủ sẽ `push` đè thẳng lên tay kẻ trộm.
Mốc bắt buộc là `steal` (`owner.rs:73`).

### 6.4 Cho chỉ số đi lùi

**Vỡ:** ba chỉ số là bộ đếm vòng trên `u32`; so sánh chúng chỉ có một cách là lấy hiệu, và hiệu chỉ
diễn giải được duy nhất khi mọi chỉ số luôn tiến và khoảng cách giữa chúng không bao giờ chạm nửa
vòng. Đó là lý do `RingBufferFifo::new` chặn sức chứa ở `2^31` (`mod.rs:87`).

Giữ được luật này thì mọi lần đọc lệch nhịp chỉ cho ra một con số **cũ hơn** — không bao giờ ra một
con số vô nghĩa. Đó là món lời lớn nhất của nấc 6 so với Chase-Lev.

### 6.5 Thả sai vùng khi `Drop`

`impl Drop` (`mod.rs:239`) đi từ **`real`** tới `tail`, không phải từ `steal`.

Vùng `[steal, real)` đã có chủ: hoặc kẻ trộm đang chép nó ra, hoặc `pop` đã bê đi rồi mà `steal`
chưa kịp đuổi theo. Cả hai trường hợp, **giá trị đã được chuyển ra khỏi ô** — thả thêm lần nữa là
double free.

---

## 7. Tra nhanh

### API

| muốn | gọi | ai gọi được |
|---|---|---|
| đặt một job | `Producer::push` → `Result<(), T>` | chủ |
| đặt cả lô, công bố một lần | `Producer::push_batch(&mut Vec<T>)` → `usize` | chủ |
| đặt từ một iterator, không cần `Vec` | `Producer::push_iter(impl IntoIterator)` → `usize` | chủ |
| lấy một job | `Producer::pop` → `Option<T>` | chủ |
| lấy cả lô, một CAS | `Producer::pop_batch(&mut Vec<T>, max)` → `usize` | chủ |
| lấy cả lô vào một sink bất kỳ | `Producer::pop_batch_with(max, impl FnMut(T))` | chủ |
| ring đầy: nhả nửa hàng đợi ra ngoài | `Producer::spill_half(&mut Vec<T>)` → `usize` | chủ |
| vét sạch (lúc shutdown) | `Producer::drain(&mut Vec<T>)` → `usize` | chủ |
| bốc một job | `Consumer::steal` → `Option<T>` | ai cũng được |
| bốc một job, phân biệt rỗng/bận | `Consumer::try_steal` → `Steal<T>` | ai cũng được |
| bốc cả lô | `Consumer::steal_batch(&mut Vec<T>, max)` → `usize` | ai cũng được |
| bốc cả lô, phân biệt rỗng/bận | `Consumer::try_steal_batch(...)` → `Steal<usize>` | ai cũng được |
| bốc nửa số đang có | `Consumer::steal_half(&mut Vec<T>)` → `usize` | ai cũng được |
| bốc thẳng sang ring khác, **không cấp phát** | `Consumer::try_steal_into(&mut Producer)` → `Steal<usize>` | ai cũng được |
| bốc vào một sink bất kỳ | `Consumer::try_steal_with(max, impl FnMut(T))` → `Steal<usize>` | ai cũng được |
| còn chỗ trống không | `Producer::remaining` / `is_full` / `RingBufferFifo::occupied` | |
| còn job không | `available` / `is_empty` | |

`steal_half` là con số mặc định cho một vòng đi trộm: bốc sạch thì chủ ring buffer đói lại ngay và kẻ trộm
kế tiếp cũng chẳng còn gì — công việc chỉ dồn từ chỗ này sang chỗ kia chứ không được chia.

### `Steal<T>`: "rỗng" và "bận" là hai câu trả lời khác nhau

```rust
pub enum Steal<T> { Empty, Busy, Success(T) }
```

Một hàm trả `0` cho cả hai là một cái bẫy cho scheduler: **`Empty`** nghĩa là ring buffer này thật sự
hết việc, đi tìm nạn nhân khác hoặc cho phép mình ngủ. **`Busy`** nghĩa là *có* việc nhưng ngay lúc
này không lấy được — hoặc một kẻ trộm khác đang bê (`steal != real`), hoặc chỗ đích đã đầy. Ngủ khi
thấy `Busy` là ngủ quên trên đống việc.

`Steal::or_else` giữ đúng phân biệt đó khi duyệt nhiều nạn nhân: chỉ cần **một** nạn nhân trả `Busy`
là kết quả tổng không bao giờ tụt xuống `Empty`.

### Ba đường không cấp phát

Trên đường chạy nóng của một pool, mọi lần `Vec` phải xin thêm chỗ là một lần vào allocator giữa
lúc đang giành cache line. Ba đường sau tránh hẳn:

| đường | vì sao không cấp phát |
|---|---|
| `try_steal_into(&mut Producer)` | chép thẳng từ ô của nạn nhân sang ô của mình, không qua `Vec` nào |
| `try_steal_with` / `pop_batch_with` | nhận một closure `FnMut(T)`, chỗ gọi tự quyết chứa vào đâu |
| `push_iter` | nhận iterator, không cần dựng `Vec` trung gian để rồi `drain` |

`RingBufferFifo::new` cũng cấp phát mảng ô bằng `Box::new_uninit_slice` — một lần xin bộ nhớ, không
có vòng lặp khởi tạo `N` phần tử.

### Cache `steal` phía chủ

`push` đọc `head` — cache line **dùng chung** với mọi kẻ trộm — để biết còn chỗ không. Làm việc đó
mỗi lần `push` là trả giá tranh chấp cho một câu hỏi mà câu trả lời gần như luôn là "còn".

`Producer` giữ một bản sao `steal` trong `Cell`. Vì `steal` **chỉ tiến**, một bản sao cũ luôn cho ra
số chỗ trống **nhỏ hơn hoặc bằng** sự thật — nó có thể từ chối một `push` lẽ ra được, nhưng không
bao giờ cho phép một `push` lẽ ra phải bị chặn. Chỉ khi bản sao nói "đầy" thì mới nạp lại `head`
thật một lần.

Điều ngược lại **không** đúng cho `push_batch`/`push_iter`: ở đó một bản sao cũ sẽ cắt ngắn cả lô,
nên chúng luôn nạp `head` một lần trước khi tính. Một lần chạm cache line cho `n` job vẫn là món
lời của việc gộp lô. Đây từng là một lỗi thật, và test
`push_theo_lo_khong_bi_cache_cu_cat_ngan` là chỗ giữ nó không quay lại.

### Lấy thẻ quyền

| cách | chữ ký | khi nào dùng |
|---|---|---|
| `split` | `&mut self → (Producer, Consumer)` | ring buffer nằm trên stack, chia bằng `thread::scope` |
| `consumer` | `&self → Consumer` | an toàn, không điều kiện; ring buffer sau `Arc` |
| `producer` | `unsafe &self → Producer` | ring buffer sau `Arc`; chỗ gọi tự bảo đảm chỉ có một |

### Ví dụ chạy được

| lệnh | cho thấy gì |
|---|---|
| `cargo test --lib ring_buffer_fifo` | toàn bộ test đơn vị, stress và test hai nhịp của kẻ trộm |

### Test

| lệnh | phạm vi |
|---|---|
| `cargo test --lib` | `tests/unit.rs`, `tests/stress.rs`, `tests/thief.rs` |
| `cargo miri test --lib ring_buffer_fifo` | một lịch chạy, soi UB |
| `MIRIFLAGS="-Zmiri-many-seeds=0..16" cargo miri test --lib ring_buffer_fifo` | nhiều lịch chạy |
| `LOOM_LOCATION=1 RUSTFLAGS="--cfg loom" cargo test --lib ring_buffer_fifo` | `tests/loom.rs` — thử mọi thứ tự chen ngang |

`loom` là thứ duy nhất bắt được lớp lỗi ở [§6.1](#61-gỡ-biển-trước-khi-chép-xong): thứ tự chen ngang
gây ra nó hiếm tới mức chạy thật hàng triệu lần cũng có thể không gặp.

### Nguồn

- [Circular buffers — tài liệu kernel Linux](https://docs.kernel.org/next/core-api/circular-buffers.html)
- Blumofe & Leiserson, *Scheduling Multithreaded Computations by Work Stealing* (1994)
- Chase & Lev, *Dynamic Circular Work-Stealing Deque* (2005)
- `tokio/src/runtime/scheduler/multi_thread/queue.rs` — hàng đợi cục bộ với `head` gói `(steal, real)`

---

## 8. Chọn bản nào

| | `ring_buffer_fifo` | `ring_buffer_lifo` |
|---|---|---|
| chủ lấy job | cũ nhất (FIFO) | **mới nhất** (LIFO) |
| `pop` của chủ | một CAS mỗi lần | **không CAS** trên đường nóng |
| kẻ trộm bốc | **cả lô** / nửa hàng đợi | **một job** mỗi lượt |
| chỉ số đi lùi | không, cả ba chỉ tiến | có (`bottom`, đầu cơ) |
| ordering | `AcqRel` là đủ | `SeqCst` trên `top` và `pop` |
| trần sức chứa | `2^31` | `2^30` |
| hợp với | injector, hàng đợi vào/ra, chỗ cần san tải theo lô | fork-join, `scope`, `parallel_for`, đệ quy chia đôi |

Quy tắc ngắn: **hàng đợi cục bộ của worker → LIFO. Chỗ cần san một lô việc sang nơi khác → FIFO.**
Một pool đầy đủ thường dùng cả hai. Chi tiết bên kia: [`ring_buffer_lifo`](./ring-buffer-lifo.md).
