---
title: Thread Pool giải thích từ đầu
excerpt: ThreadPool là gì, vì sao cần nó, và một job đi qua những chặng nào trước khi được chạy.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/a/a5/Multithreaded_process.svg/1280px-Multithreaded_process.svg.png
tags:
  - concurrency
  - scheduling
---

## Ý chính trong một đoạn

`ThreadPool` là một nhóm thread được tạo sẵn một lần rồi dùng lại mãi. Bạn không tự tạo thread nữa,
bạn đưa cho pool những mẩu việc nhỏ (gọi là **job**), rồi các thread trong pool tự chia nhau chạy.
Ai rảnh trước thì lấy việc trước, ai hết việc thì đi "trộm" việc của người bên cạnh. Nhờ vậy các
core của CPU không có ai ngồi không trong khi người khác ôm cả đống việc.

Nếu bạn cần ghi nhớ: pool cung cấp sẵn các luồng xử lý. Bạn gửi yêu cầu thực thi vào pool. Bạn không tạo mới luồng cho từng công việc riêng biệt.

## Vấn đề: vì sao không tạo thread cho từng việc

Thread là thứ do hệ điều hành cấp. Tạo một thread không miễn phí: hệ điều hành phải xin bộ nhớ cho
stack, ghi sổ, rồi khi thread chết lại phải dọn. Chi phí đó cỡ vài chục tới vài trăm micro giây.

Nghe thì nhỏ. Nhưng một game chạy 60 khung hình mỗi giây chỉ có khoảng 16 mili giây cho mỗi khung
hình, và trong khung hình đó có thể có hàng nghìn mẩu việc nhỏ (cập nhật vật lý cho từng nhóm
entity, gom draw call, tính animation). Nếu mỗi mẩu việc là một thread mới thì thời gian tạo và dọn
thread còn nhiều hơn thời gian làm việc thật.

Chưa kể một chuyện tệ hơn: tạo 5000 thread trên máy 8 core không làm máy nhanh hơn. Hệ điều hành sẽ
liên tục lấy core của thread này đưa cho thread kia (gọi là **context switch**, chuyển ngữ cảnh), và
mỗi lần chuyển như vậy cache của CPU lại nguội đi, phải nạp lại từ RAM.

Kết luận: số thread nên xấp xỉ số core, tạo một lần, và giữ chúng sống suốt đời chương trình. Đó
chính xác là `ThreadPool`.

## Ai tham gia vào pool

Có một chi tiết dễ hiểu nhầm ngay từ đầu, nên nói luôn.

Khi bạn cấu hình `threads: 7`, pool **spawn** 7 thread worker. Nhưng thread đã tạo ra pool (gọi là
**host**, thường là thread `main`) cũng chạy job cùng chúng chứ không đứng nhìn. Vậy tổng số thứ
đang chạy job là 8, không phải 7.

Đó là lý do mặc định là `số core trừ 1`: trừ đi một chỗ để dành cho chính host.

Cũng vì thế, `threads: 0` là hợp lệ và rất hữu ích. Không thread nào được tạo, mọi job chạy ngay
trên thread gọi, theo đúng thứ tự bạn gửi vào. Khi gặp một con bug lạ, đặt biến môi trường
`XYNOK_LANE_THREADS=1` là bạn trả lời được câu "cái này có phải do đa luồng không" trong một lần
chạy, không phải sửa dòng code nào.

## Ba chỗ chứa việc

Đây là phần cốt lõi. Mỗi người tham gia (mỗi worker, và cả host) có hai chỗ chứa việc riêng, còn cả
pool dùng chung một chỗ thứ ba.

**1. Ô LIFO, chứa đúng một job.** Đây là job vừa mới được sinh ra. Vì sao nó được ưu ái có chỗ
riêng? Vì job vừa sinh ra thường dùng chung dữ liệu với job đang chạy, mà dữ liệu đó lúc này còn nằm
trong cache của CPU, tức là còn "nóng". Chạy nó ngay là rẻ nhất. Đổi lại, **không ai trộm được ô
LIFO**, nên chủ của nó phải nhớ vét sạch trước khi rời đi, không thì job nằm đó vĩnh viễn.

2. Ring local là hàng đợi riêng của từng người. Nó chứa phần việc còn lại và cho phép người khác trộm; mỗi hàng đợi có 256 ô.

3. Lane queue là hàng đợi dùng chung của cả pool. Hai loại việc được đưa vào đây gồm việc do một thread ngoài pool đẩy vào vì không sở hữu ring nào và việc bị tràn ra từ ring của một thread trong pool.

> Ví von cho dễ hình dung: ô LIFO là cái đĩa ngay trước mặt đầu bếp, ring local là cái thớt của
> riêng người đó, còn lane queue là thanh treo phiếu order chung giữa bếp. Ai cũng với tới thanh
> chung được, còn cái đĩa trước mặt thì chỉ chủ nhân đụng vào.

## Một job đi đường nào

Khi bạn gọi `spawn`, pool nhìn xem **ai đang gọi** để quyết định bỏ job vào đâu:

```text
   spawn từ bên trong một job          spawn từ ngoài pool
             │                                  │
             ▼                                  ▼
       ô LIFO của worker               lane queue (dùng chung)
             │  (đầy thì đẩy xuống)             │
             ▼                                  │
       ring local của worker  ◀─────────────────┘  worker nạp cả cụm khi ngó tới
             │  (đầy thì xả nửa cũ xuống lane queue)
             ▼
       người khác trộm từ đầu ring
```

Chú ý chỗ "spawn từ bên trong một job". Pool lưu trữ thông tin về định danh, vị trí của luồng và trạng thái liệu luồng đó có đang thực thi trong một chu kỳ công việc hay không.

Cờ đó quan trọng vì nó quyết định chỗ tốt nhất và chỗ tệ nhất, và hai chỗ đó là cùng một chỗ:

- Đang ở trong vòng chạy job: ô LIFO là tốt nhất, vì chính thread này sẽ quay lại lấy nó ngay sau
  khi xong job hiện tại.
- Đang ở ngoài vòng (host vừa gọi `spawn` rồi đi làm việc khác): ô LIFO là tệ nhất, vì không ai trộm
  được nó, mà chủ của nó thì đang không quay lại nhìn. Job sẽ nằm im.

## Worker tìm việc như thế nào

Một worker chạy vòng lặp vô tận, mỗi vòng nó tìm việc theo thứ tự từ rẻ tới đắt:

```text
 1. ô LIFO         job vừa sinh, cache nóng nhất
 2. ring local     việc của chính mình
 3. lane queue     việc từ ngoài, và việc bị xả ra
 4. trộm           đắt: phải CAS vào ring người khác
 5. lane queue     ngó lần cuối trước khi ngủ
 6. ngủ
```

Bước 4 đắt vì nó phải dùng lệnh nguyên tử (CAS) ghi vào vùng nhớ mà thread khác đang đọc ghi. Hai
core cùng chạm một cache line thì phần cứng phải đồng bộ với nhau, và việc đó tốn thời gian. Nên
trộm là phương án cuối, không phải phương án đầu.

Một chi tiết nhỏ: khi trộm, worker bắt đầu từ **người ngồi kế bên** chứ không phải từ người số 0.
Nếu ai cũng bắt đầu từ số 0 thì cả đám đói sẽ xếp hàng nhắm vào cùng một nạn nhân.

### Luật 61 vòng

Cứ 61 vòng lặp thì bước 3 được kéo lên trước cả bước 1. Nghĩa là worker phải ngó lane queue trước
khi ngó cái đĩa trước mặt mình.

Vì sao cần luật này? Vì có một cái bẫy: một job có thể sinh ra job con, job con lại sinh job cháu.
Nếu worker luôn ưu tiên ô LIFO, nó sẽ tự nuôi mình bằng đám job con này mãi mãi và không bao giờ ngó
tới lane queue. Trong khi đó, một job do thread ngoài đẩy vào đang nằm chờ trong lane queue có thể
chờ rất lâu, dù nhìn từ ngoài thì pool vẫn "đang bận rộn chạy hết công suất".

Còn vì sao là 61 mà không phải 60? Vì 61 là số nguyên tố. Một con số chia hết cho số worker, hoặc
chia hết cho nhịp sinh job của thuật toán chia đôi, sẽ khiến nhiều worker cùng ngó lane queue đúng
một nhịp rồi cùng giành một cái khoá. Số nguyên tố đủ lớn thì các worker tự rải đều ra.

## Khi ring đầy

Ring có 256 ô. Đầy thì sao?

Đầy **không phải lỗi**, nó là tín hiệu để xả bớt. Chủ ring lấy **nửa cũ** đổ xuống lane queue rồi
push tiếp. Nhờ vậy giới hạn cứng của ring biến thành một ngưỡng xả mềm, và pool không bao giờ phải
từ chối một job.

Vì sao xả nửa cũ mà không phải nửa mới? Vì nửa cũ là phần nguội nhất, cache đã bay từ lâu, ai chạy
cũng như nhau. Nửa mới thì vẫn còn nóng với chủ của nó.

Nếu lần push thứ hai vẫn hỏng (đúng lúc có kẻ trộm đang giữ cả vùng), job đi thẳng xuống lane queue.
Lane queue không bao giờ từ chối, nên đó luôn là lối thoát cuối.

## Chờ mà không ngồi không

Đây là mẹo quan trọng nhất và cũng dễ làm sai nhất.

Giả sử bạn chia một việc lớn thành 100 job rồi phải chờ cả 100 xong mới đi tiếp. Cách ngây thơ là
thread gọi đi ngủ chờ tín hiệu. Cách đó sai vì hai lý do:

- Bạn vừa bỏ phí một core suốt thời gian chờ.
- Tệ hơn: thread đang ngủ đó có thể chính là thread lẽ ra phải chạy job mà nó đang chờ. Nếu các điểm
  chờ lồng vào nhau thì đó là deadlock, chương trình đứng im vĩnh viễn.

Cách của pool gọi là **work-while-waiting** (chờ thì làm việc luôn). Thread đang chờ không nằm im,
nó lấy job từ pool về chạy như một worker thật, và giữa các job thì kiểm tra xem điều kiện chờ đã
xong chưa. Vì điều kiện đó bị hỏi rất nhiều lần nên nó phải cực rẻ, một lệnh đọc atomic là vừa.

Thread không thuộc pool thì cũng chờ được, nhưng nó không giúp được gì, vì nó không sở hữu ring nào
để nạp job vào. Nó quay tại chỗ một lúc rồi ngủ từng nhịp ngắn 50 micro giây, phòng trường hợp thứ
nó chờ hoàn thành mà quên gọi ai dậy.

## Ngủ và đánh thức

Worker hết việc không đi ngủ ngay. Nó quay tại chỗ khoảng 2048 vòng trước đã.

Vì sao phải quay không như vậy thay vì ngủ luôn cho tiết kiệm điện? Vì đánh thức một thread đang ngủ
tốn một cặp syscall cộng một lần chuyển ngữ cảnh. Trên máy có core mạnh và core tiết kiệm điện (kiểu
P-core và E-core) thì còn tệ hơn: thread vừa dậy không chắc quay lại đúng loại core nó vừa rời. Pool
nào ngủ quá nhanh sẽ trả cái giá đó ở mỗi đợt việc mới.

2048 vòng tương đương khoảng ba tới bốn trăm micro giây. Đủ dài để worker còn nóng khi hệ thống kế
tiếp trong cùng một khung hình đẩy việc ra, đủ ngắn để giữa hai khung hình thì core được trả lại cho
máy thay vì quay không tới hết đời tiến trình.

Việc "ngủ mà không bao giờ ngủ quên mất một job vừa tới" là một bài toán riêng, khá tinh vi, và nó
có tài liệu riêng ở [sleep_protocol.md](sleep_protocol.md). Ý tưởng gọn lại là: có một bộ đếm sự
kiện, worker đọc bộ đếm đó **trước** khi đi tìm việc, và chỉ cho phép mình ngủ nếu lúc sắp ngủ bộ
đếm vẫn y nguyên. Job nào xuất hiện trong lúc worker đang tìm đều làm bộ đếm đổi, nên worker sẽ biết
mà không ngủ.

## Tắt pool

Tắt pool có bốn bước, và bỏ bước nào cũng dẫn tới treo:

1. **Dựng cờ shutdown**, để không worker nào bắt đầu vòng mới.
2. **Gọi mọi worker đang ngủ dậy.** Thread đang ngủ không tự biết là đã tới lúc dừng.
3. Cho phép worker xử lý hết các công việc còn trong hàng đợi. Vứt đi thì mọi điểm chờ đang đếm những
   job đó sẽ đếm mãi không về 0, và bạn có một tiến trình không bao giờ thoát.
4. **Join từng thread**, tức là chờ chúng thật sự dừng.

Bước 4 có một ngoại lệ: nếu người gọi `shutdown` lại chính là một worker của pool này thì bước join
bị bỏ qua. Lý do là join chính mình là hành vi không xác định, tệ hơn cả treo. Và tình huống này với
tới được mà không ai cố ý: chỉ cần một job giữ một bản clone của `ThreadPool` rồi tình cờ là kẻ thả
bản clone cuối cùng.

Đường tử tế vẫn là tắt pool từ chính thread đã dựng nó. Ở đó bạn được đảm bảo mọi worker đã dừng khi
hàm trả về.

## Job panic thì sao

Nếu một job panic mà để nó thoát ra ngoài, nó sẽ giết luôn thread worker, và mọi bước ghi sổ sau đó
bị bỏ qua. Hậu quả là mọi điểm chờ đang đếm job đó sẽ chờ mãi.

Nên mỗi job chạy bên trong một lớp bọc bắt panic lại. Job nào có đường báo lỗi về (mọi job đi qua
`scope`) thì đã tự bắt panic của mình và sẽ dựng lại nó ở điểm chờ. Job giao thẳng qua `spawn` thì
không có điểm chờ nào để trao, mà thông báo lỗi và backtrace thì đã được in ra từ trước rồi.

## Ví dụ cụ thể: một khung hình game

Máy 8 core, pool cấu hình mặc định nên có 7 worker cộng host là 8 người chạy job.

Host chạy hệ thống vật lý, chia 1 triệu entity thành 64 mẩu việc rồi gọi `spawn` 64 lần.

1. Host đang ở ngoài vòng chạy job, nên 64 job này đi thẳng xuống **lane queue**.
2. Sau job đầu tiên được đẩy vào, pool gọi một worker đang ngủ dậy. Worker đó tỉnh, ngó lane queue,
   và bốc về **cả một cụm** chứ không phải một job (bốc từng cái thì mỗi job tốn một lần lấy khoá).
3. Worker đó chạy job đầu, và vì nó đã lấy được việc trong lúc là người đi lùng cuối cùng, nó gọi
   thêm một worker nữa dậy. Cứ thế lan ra.
4. Trong lúc chạy, một job vật lý phát hiện có va chạm và `spawn` thêm một job xử lý va chạm. Job
   con này rơi vào **ô LIFO** của chính worker đang chạy, vì worker đó sẽ quay lại lấy ngay.
5. Worker nào hết việc trong ring của mình thì đi trộm của người kế bên.
6. Host gọi `run_until` chờ cả 64 job xong. Nó không nằm im mà cũng lấy job về chạy.
7. Xong hết, host gọi `end_frame` để dọn sạch vùng nhớ nháp của cả pool, rồi vào khung hình sau.
8. Nếu game bị tạm dừng và không có việc gì trong vài trăm micro giây, các worker lần lượt đi ngủ và
   trả core lại cho máy.

## Cần ghi nhớ

- Pool tạo thread một lần rồi dùng lại. Bạn gửi job, không gửi thread.
- Thread tạo ra pool cũng chạy job. Tổng số thứ chạy job là `threads + 1`.
- Ba chỗ chứa việc, xếp theo độ nóng của cache: ô LIFO (1 job, không ai trộm được), ring local (trộm
  được), lane queue (dùng chung, cửa duy nhất cho người ngoài).
- Worker tìm việc từ rẻ tới đắt, và trộm là bước cuối vì nó phải chạm vào cache line của người khác.
- Cứ 61 vòng thì lane queue được ưu tiên, để việc từ ngoài không bị bỏ đói.
- Ring đầy là tín hiệu xả nửa cũ, không phải lỗi.
- Thread đang chờ thì chạy job giúp, chứ không nằm im. Nằm im vừa phí core vừa có thể deadlock.
- `threads: 0` hoặc `XYNOK_LANE_THREADS=1` cho bạn chạy mọi thứ tuần tự để soi bug.

## Đọc tiếp

- [thread_pool.md](thread_pool.md), bản mô tả kỹ thuật ngắn gọn của cùng chủ đề này.
- [sleep_protocol.md](sleep_protocol.md), chuyện ngủ và đánh thức kể chi tiết.
- [ring_buffer.md](ring_buffer.md), cấu trúc của ring local và cách trộm hoạt động.
- [lane_queue.md](lane_queue.md), hàng đợi dùng chung.
- [scope.md](scope.md), cách chia việc rồi chờ mà không rò rỉ job.
- [concurrency_concepts.md](concurrency_concepts.md), từ điển các thuật ngữ dùng ở đây.

## Tham khảo

- https://en.wikipedia.org/wiki/Work_stealing
- https://github.com/rayon-rs/rayon/blob/main/rayon-core/src/registry.rs
- https://github.com/golang/go/blob/master/src/runtime/proc.go
