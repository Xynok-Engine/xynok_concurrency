# `ring_buffer_lifo` — chủ ăn ở đuôi, kẻ trộm ăn ở đầu

Tài liệu này giải thích `src/ring_buffer_lifo/`: một deque work-stealing kiểu Chase-Lev, **có
trần**, trong đó chủ lấy job **mới nhất** trước (LIFO) và gần như không phải trả một lệnh CAS nào.

Bản song sinh của nó là [`ring_buffer_fifo`](./ring-buffer-fifo.md). Hai bản **không** thay thế
được cho nhau, và mục [7](#7-chọn-cái-nào) nói rõ chọn cái nào.

| phần | nội dung |
|---|---|
| [1](#1-vì-sao-cần-một-bản-lifo-riêng) | Vì sao LIFO đáng có một cấu trúc riêng |
| [2](#2-hai-chỉ-số-rưỡi) | Hai chỉ số rưỡi: `bottom`, và `top` gói `(free, claim)` |
| [3](#3-bốn-hành-vi) | `push`, `pop`, `claim`, `release` |
| [4](#4-vì-sao-kẻ-trộm-chỉ-bốc-được-một-job) | Vì sao kẻ trộm **không** bốc được cả lô |
| [5](#5-vì-sao-seqcst-chứ-không-phải-acqrel) | Vì sao `SeqCst`, và một data race miri đã bắt được |
| [6](#6-những-cách-làm-sai) | Bốn cách làm sai |
| [7](#7-chọn-cái-nào) | Tra nhanh API và bảng chọn |

---

## 1. Vì sao cần một bản LIFO riêng

Bản FIFO cho cả chủ lẫn kẻ trộm cùng lấy ở một đầu. Điều đó làm cho mọi tranh chấp gói gọn vào một
lệnh CAS và cho phép bốc cả lô — nhưng nó trả hai cái giá:

1. **Mỗi `pop` của chủ là một CAS trên cache line dùng chung.** Với fork-join đệ quy, chủ `pop`
   liên tục, nên cái giá đó là thường trực chứ không phải hiếm.
2. **Job vừa đặt vào là job được lấy ra sau cùng.** Trong một cây fork-join, job vừa spawn là job
   có dữ liệu còn nóng nhất trong cache và là job có cây con nông nhất. Lấy nó trước vừa nhanh hơn
   vừa giữ số job đang sống ở mức `O(độ sâu)`; lấy theo FIFO thì con số đó là `O(bề rộng)` — và
   với `parallel_for` trên một triệu phần tử, hai con số đó khác nhau vài bậc.

Đó là lý do Cilk, Rayon, `ForkJoinPool` của Java đều chọn LIFO cho chủ và FIFO cho kẻ trộm. Chủ đi
sâu (depth-first, cache nóng, bộ nhớ có trần); kẻ trộm bốc từ đầu kia, nơi những job to nhất — gốc
của các cây con lớn — nằm sẵn, nên một lần đi trộm mang về nhiều việc.

```text
              top                                    bottom
  kẻ trộm ──→  │                                        │  ←── chủ push/pop
               ▼                                        ▼
     [ ][ ][ A  B  C  D  E ][ ][ ][ ][ ][ ]
             └cũ nhất┘      └mới nhất┘
```

## 2. Hai chỉ số rưỡi

Chase-Lev gốc có đúng hai chỉ số: `top` và `bottom`. Bản này có **hai rưỡi**, và nửa thêm ra là bắt
buộc vì một lý do duy nhất: **ring buffer này có trần, nên ô bị dùng lại.**

Trong Chase-Lev gốc (crossbeam), buffer *lớn lên* khi đầy. Kẻ trộm được phép đọc ô **trước** khi
CAS thành công — một cú đọc đầu cơ — và điều đó không hỏng vì ô đó không bao giờ bị chủ ghi đè
trong lúc kẻ trộm đọc: muốn ghi đè thì `bottom` phải quay đủ một vòng, mà buffer đã lớn lên trước
khi tới lúc đó. Bỏ khả năng lớn lên đi thì cú đọc đầu cơ ấy thành **data race thật**.

Nên bản này đảo lại: **nhận trước, đọc sau.** Và để "nhận" có nghĩa, phải có một chỉ số nói với chủ
rằng ô đó chưa được thả ra — đúng vai của `steal` bên bản FIFO.

| chỉ số | trỏ vào | ai đẩy lên | ý nghĩa một câu |
|---|---|---|---|
| `bottom` | ô trống kế tiếp ở **đuôi** | **chỉ mình chủ** | "chỗ ghi tiếp theo, cũng là chỗ `pop` lấy" |
| `claim` | job **cũ nhất chưa ai nhận** | kẻ trộm lúc nhận, chủ lúc giành job cuối | "đã có chủ, đừng lấy nữa" |
| `free` | ô đầu của vùng **đang bị bê** | kẻ trộm lúc chép xong | "chép xong rồi, ghi đè được" |

`free` và `claim` là hai nửa của **cùng một** `AtomicU64` (`pack`/`unpack`), vì kẻ trộm phải vừa
kiểm "có ai đang bê không" vừa nhận ô trong đúng một thao tác nguyên tử.

Bất biến: **`free ≤ claim ≤ bottom`**, và cả ba chỉ tăng — trừ đúng một ngoại lệ, là toàn bộ chỗ khó
của thiết kế này:

> `pop` **hạ `bottom` xuống một cách đầu cơ** rồi khôi phục nếu hụt.

Ngoại lệ đó là cái giá phải trả để `pop` không cần CAS, và nó là nguồn của mọi thứ ở mục 5.

Vì `bottom - claim` được đọc như **số có dấu** (`as i32`) để phân biệt "rỗng" (`< 0`) với "còn đúng
một job" (`== 0`), sức chứa bị chặn ở `2^30` chứ không phải `2^31` như bản FIFO.

## 3. Bốn hành vi

### 3.1 `Producer::push` — không CAS

```text
bottom = load(Relaxed)
nếu bottom - free ≥ capacity  → Err(val)      // đầy
ghi ô bottom
bottom.store(bottom + 1, Release)             // công bố
```

Mốc tính chỗ trống là `free`, **không** phải `claim`: ô trong `[free, claim)` đang có kẻ trộm chép
dở, dùng `claim` là ghi đè lên tay nó.

`free` được đọc từ một bản sao trong `Cell` (xem mục "Cache" của tài liệu FIFO — cơ chế giống hệt),
nên đường `push` thường không chạm cache line dùng chung. `push_batch`/`push_iter` thì luôn nạp lại
một lần, vì bản sao cũ sẽ cắt ngắn cả lô.

### 3.2 `Producer::pop` — LIFO, và đường không CAS

```text
bottom = load(Relaxed)
next   = bottom - 1
bottom.store(next, SeqCst)          // hạ xuống ĐẦU CƠ: "ô này của tôi"
fence(SeqCst)

(free, claim) = unpack(top.load(SeqCst))
size = (next - claim) as i32

size < 0  → khôi phục bottom, None                    // rỗng
size > 0  → đọc ô next, xong                          // KHÔNG CAS — đường nóng
size == 0 → còn đúng một job, và kẻ trộm cũng đang nhắm nó
            → CAS top: claim → claim + 1
              thắng: khôi phục bottom, đọc ô next
              thua : khôi phục bottom, None
```

Ba điều đáng nhớ:

- **`size > 0` là đường nóng và nó không có CAS nào cả.** Đó là toàn bộ lý do module này tồn tại.
- **Ô chỉ được đọc *sau* khi đã phân xử xong.** Bản Chase-Lev kinh điển đọc trước rồi `mem::forget`
  nếu thua; ở đây làm vậy là để kẻ trộm và chủ cùng đọc một ô mà ô đó có thể đang bị ghi đè.
- **Nhánh `size == 0` phải là một vòng lặp, không phải một lần CAS.** `top` gói *hai* trường, nên
  CAS có thể hỏng vì một lý do chẳng liên quan gì tới ta: một kẻ trộm vừa **gỡ biển**, đổi `free`
  chứ không đụng `claim`. Bỏ cuộc lúc đó là **đánh mất job** — và đó là lỗi loom bắt được ngay khi
  bản này vừa viết xong. Bản FIFO không có bẫy này vì ở đó chủ đã lặp sẵn.

### 3.3 `Consumer::claim` — nhận trước

```text
head = top.load(SeqCst); (free, claim) = unpack(head)
free != claim  → Busy                          // đã có kẻ trộm khác đang bê
fence(SeqCst)
bottom = bottom.load(SeqCst)
(bottom - claim) as i32 <= 0 → Empty           // rỗng
CAS top: (claim, claim) → (claim, claim + 1)
  thắng → nhận ô claim
  thua  → Busy
```

`free` đứng yên tại `claim` cũ — đó là tấm biển "đang thi công" giữ cho `push` không ghi đè.

### 3.4 `Consumer::release` — gỡ biển

```text
lặp:
  (_, claim) = unpack(top.load(SeqCst))
  CAS top → (claim, claim)
```

Kéo `free` thẳng lên bằng `claim` **hiện tại**, không phải mốc cũ — vì trong lúc ta chép, chủ có thể
đã giành mất job cuối và tự đẩy `claim` đi tiếp. Đặt lại mốc cũ thì `free != claim` vĩnh viễn: mọi
kẻ trộm sau đó tưởng có người đang bê, còn `push` tưởng ring đầy hơn thực tế. Ring kẹt cứng, không
ai báo lỗi.

## 4. Vì sao kẻ trộm chỉ bốc được một job

Đây là ràng buộc lớn nhất của bản LIFO, và nó **không** phải chuyện chưa làm tới. Nó là hệ quả trực
tiếp của việc chủ đứng ở đầu bên kia.

Giả sử kẻ trộm nhận cả vùng `[claim, claim + n)` bằng một CAS, với `n` tính từ một bản chụp `bottom`.
Chủ đang `pop` ở `next = bottom - 1` và chỉ CAS khi `size == 0`. Nếu bản chụp `bottom` của kẻ trộm
**cũ hơn một nhịp**, nó tính ra `n` lớn hơn thực tế và vùng nó nhận trùm qua đúng ô `next` mà chủ
đang lấy — trong khi chủ, thấy `size > 0`, **không CAS gì cả**. Không có lệnh nguyên tử nào nhìn
được cả `bottom` lẫn `top`, nên không có chỗ nào để phân xử.

Với `n = 1` thì không có kẽ hở đó: đụng nhau chỉ xảy ra khi ô kẻ trộm nhắm **chính là** ô chủ nhắm,
và lúc ấy `size == 0`, tức là cả hai đều CAS lên cùng một ô nhớ. Đó là toàn bộ chỗ dựa của
Chase-Lev, và nó chỉ đứng vững ở `n = 1`.

Hệ quả thực dụng: **một vòng đi trộm trên ring LIFO mang về một job.** Bù lại, job đó là gốc của cây
con to nhất, nên "một job" ở đây thường là rất nhiều việc. Cần bốc theo lô thì dùng bản FIFO.

## 5. Vì sao `SeqCst`, chứ không phải `AcqRel`

Chủ ghi `bottom` rồi đọc `top`; kẻ trộm đọc `top` rồi đọc `bottom`. Đó đúng là hình **store buffer**:
nếu cả hai bên đều được phép thấy giá trị cũ của bên kia, chủ kết luận "còn nhiều job, khỏi CAS"
đúng lúc kẻ trộm kết luận "còn job ở `claim`, nhận thôi" — và cả hai cùng lấy một ô.

Chase-Lev kinh điển chặn nó bằng một cặp `fence(SeqCst)` hai bên. **Ở bản này thế là chưa đủ**, và
đó không phải chuyện lý thuyết: `cargo miri test` với `-Zmiri-many-seeds` bắt được một data race
thật giữa `Slots::write` của chủ và `Slots::read` của kẻ trộm. Nguyên nhân: chỉ có `fence` thì lập
luận phải đi vòng qua *thứ tự của hàng rào* trong tổng thứ tự `SeqCst`, và lập luận đó vỡ khi có
**nhiều hơn một** kẻ trộm — `claim` có thể nhảy hai bậc trong lúc chủ đang cầm một bản đọc cũ, và
lúc đó `size > 0` là một câu trả lời sai mà chủ tin theo mà không CAS.

Cách chữa là bỏ hẳn lập luận vòng: **cho cả `top` lẫn phần `bottom` có thể đi lùi đều là thao tác
`SeqCst`.**

| ô nhớ | thao tác | ordering | vì sao |
|---|---|---|---|
| `top` | mọi `load` và mọi CAS | `SeqCst` | để mọi thao tác trên `top` nằm trong một tổng thứ tự duy nhất |
| `bottom` | `store` trong `pop` (hạ và khôi phục) | `SeqCst` | đây là lần **duy nhất** `bottom` đi lùi |
| `bottom` | `load` trong `claim` | `SeqCst` | nửa còn lại của cặp store-buffer |
| `bottom` | `store` trong `push` | `Release` | `push` chỉ đẩy `bottom` **lên**; thấy giá trị cũ chỉ làm kẻ trộm thấy ít việc hơn — an toàn |

Đọc `bottom` cũ mà **nhỏ hơn** thì kẻ trộm bỏ qua job — mất hiệu năng, không mất tính đúng. Đọc cũ
mà **lớn hơn** mới là lỗi, và chỉ `pop` mới tạo ra được tình huống đó. Nên chỉ `pop` phải trả giá
`SeqCst`; `push` thì không.

Cái giá: trên x86 một `store` `SeqCst` là `xchg` (hàng rào đầy đủ), trên ARM64 là `stlr` kèm `dmb`.
Nó nằm trên đường `pop`, tức là đường nóng — nhưng vẫn rẻ hơn một CAS trên cache line đang bị giành,
và đó chính là thứ nó thay thế.

## 6. Những cách làm sai

### 6.1 Đọc ô trước khi CAS (kiểu Chase-Lev gốc)

Sai vì ring này có trần: chủ có thể quay đủ một vòng và ghi đè đúng ô kẻ trộm đang đọc đầu cơ.
crossbeam làm được vì buffer của nó lớn lên. Có trần thì phải **nhận trước, đọc sau**.

### 6.2 Bỏ cuộc sau một lần CAS hỏng trong `pop`

`top` gói hai trường; CAS có thể hỏng vì kẻ trộm gỡ biển (`free` đổi) chứ không phải vì ai đó lấy
mất job. Bỏ cuộc lúc đó là **im lặng đánh mất job cuối cùng**. Test loom
`chu_va_trom_tren_hai_job` là chỗ giữ lỗi này không quay lại.

### 6.3 Cho kẻ trộm bốc cả lô

Xem mục 4. Không có lệnh nguyên tử nào phân xử được, và stress test gần như không bao giờ chỉ ra —
nó hiện ra dưới dạng một job chạy hai lần, hàng giờ sau.

### 6.4 Dùng `claim` làm mốc tính chỗ trống cho `push`

Ô trong `[free, claim)` đang bị bê. Dùng `claim` là ghi đè lên tay kẻ trộm — đúng loại lỗi mà chỉ
loom và miri bắt được.

### 6.5 `Drop` thả nhầm vùng

`Drop` chỉ được thả `[claim, bottom)`. Vùng `[free, claim)` đã được **chuyển ra khỏi ô** rồi; thả
thêm lần nữa là double free.

## 7. Chọn cái nào

| | `ring_buffer_lifo` | `ring_buffer_fifo` |
|---|---|---|
| chủ lấy job | **mới nhất** (LIFO) | cũ nhất (FIFO) |
| `pop` của chủ | **không CAS** (đường nóng) | một CAS mỗi lần |
| kẻ trộm bốc | **một job** mỗi lượt | **cả lô** / nửa hàng đợi |
| chỉ số đi lùi | có (`bottom`, đầu cơ) | không, cả ba chỉ tiến |
| ordering | `SeqCst` trên `top` và `pop` | `AcqRel` là đủ |
| trần sức chứa | `2^30` | `2^31` |
| hợp với | fork-join, `scope`, `parallel_for`, đệ quy chia đôi | injector, hàng đợi vào/ra, chỗ cần san tải theo lô |

Quy tắc ngắn: **hàng đợi cục bộ của worker → LIFO. Chỗ cần san một lô việc sang nơi khác → FIFO.**
Một pool đầy đủ thường dùng cả hai.

### API

| muốn | gọi | ai gọi được |
|---|---|---|
| đặt một job | `Producer::push` → `Result<(), T>` | chủ |
| đặt cả lô | `Producer::push_batch(&mut Vec<T>)` → `usize` | chủ |
| đặt từ iterator | `Producer::push_iter(impl IntoIterator)` → `usize` | chủ |
| lấy job mới nhất | `Producer::pop` → `Option<T>` | chủ |
| lấy nhiều job mới nhất | `Producer::pop_batch(&mut Vec<T>, max)` → `usize` | chủ |
| lấy vào một sink bất kỳ | `Producer::pop_batch_with(max, impl FnMut(T))` | chủ |
| vét sạch (lúc shutdown) | `Producer::drain(&mut Vec<T>)` → `usize` | chủ |
| bốc một job | `Consumer::steal` → `Option<T>` | ai cũng được |
| bốc một job, phân biệt rỗng/bận | `Consumer::try_steal` → `Steal<T>` | ai cũng được |
| bốc thẳng sang ring khác, không cấp phát | `Consumer::try_steal_into(&mut Producer)` → `Steal<usize>` | ai cũng được |
| còn chỗ trống không | `Producer::remaining` / `is_full` / `RingBufferLifo::occupied` | |
| còn job không | `available` / `is_empty` | |

`pop_batch` là một vòng lặp `pop`, không phải một CAS cho cả lô — ở bản LIFO không có "một lô" nào
để nhận trong một thao tác, vì mỗi lần hạ `bottom` là một lần phân xử riêng. Cái giá đó là thật,
và nó là lý do bản FIFO tồn tại.

Không có `spill_half`: khi ring LIFO đầy, thứ đúng để làm là chạy ngay job vừa `push` không vào
được — nó là job nóng nhất và là job sẽ được chạy tiếp theo. Nhả *nửa mới nhất* ra ngoài là ném đi
đúng phần cache đang nóng.

### Lấy thẻ quyền

| cách | chữ ký | khi nào dùng |
|---|---|---|
| `split` | `&mut self → (Producer, Consumer)` | ring nằm trên stack, chia bằng `thread::scope` |
| `consumer` | `&self → Consumer` | an toàn, không điều kiện; ring sau `Arc` |
| `producer` | `unsafe &self → Producer` | ring sau `Arc`; chỗ gọi tự bảo đảm **chỉ có một** |

### Test

| lệnh | phạm vi |
|---|---|
| `cargo test --lib ring_buffer_lifo` | `tests/unit.rs`, `tests/stress.rs` |
| `cargo miri test --lib ring_buffer_lifo` | một lịch chạy, soi UB |
| `MIRIFLAGS="-Zmiri-many-seeds=0..64" cargo miri test --lib ring_bon_o_hai_ke_trom` | nhiều lịch chạy trên ring bốn ô — đây là test đã bắt được data race ở mục 5 |
| `LOOM_LOCATION=1 RUSTFLAGS="--cfg loom" cargo test --lib ring_buffer_lifo` | `tests/loom.rs` — mọi thứ tự chen ngang |

### Nguồn

- Chase & Lev, *Dynamic Circular Work-Stealing Deque* (2005)
- Lê, Pop, Cohen, Zappa Nardelli, *Correct and Efficient Work-Stealing for Weak Memory Models* (PPoPP 2013)
- `crossbeam-deque/src/deque.rs` — bản Chase-Lev tự lớn lên, để đối chiếu với bản có trần ở đây
- Blumofe & Leiserson, *Scheduling Multithreaded Computations by Work Stealing* (1994)
