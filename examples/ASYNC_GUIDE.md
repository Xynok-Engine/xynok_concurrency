# Async/await trong Rust — bộ ví dụ theo use case

Đọc theo thứ tự 01 → 18. Mỗi case có một file chạy được và assertion kiểm tra kết quả.
Đây là bản đồ các nhóm use case cốt lõi, không phải danh sách hữu hạn của mọi ứng dụng async.
Các file 02–07 dùng một ít helper trong `async_support/mod.rs`; đọc helper khi tới bài 03.
Bài 01 được giữ nguyên để nối tiếp ví dụ đã học.

## Chạy

Từ thư mục gốc `xynok_concurrency`:

```bash
cargo run --example async_01_poll
cargo run --example async_06_custom_executor

cargo run --manifest-path examples/async_runtime/Cargo.toml --bin 09_join_vs_spawn
cargo run --manifest-path examples/async_runtime/Cargo.toml --bin 11_tcp_reactor
```

Bài 01–08 dùng std và thư viện xynok hiện có. Bài 09–18 nằm trong project con
`async_runtime`, có Cargo.toml/Cargo.lock riêng và dùng Tokio. Không cần server bên ngoài;
TCP chỉ dùng loopback `127.0.0.1` với port do OS chọn. Môi trường sandbox cần cho phép mở socket.
Bài 18 cố ý panic trong task: panic hook in lỗi, nhưng main kiểm tra JoinError và vẫn exit 0.

## Các bài và thành phần cần có

| Bài / file | Use case và điều cần quan sát | Cần gì? |
|---|---|---|
| [01 — poll](async_01_poll.rs) | Pending → wake → Ready; sau await chỉ chạy ở poll thứ hai | Future thủ công, Context, waker chỉ log; main poll thủ công |
| [02 — lazy](async_02_lazy.rs) | Tạo rồi drop future không chạy body; await tuần tự; Result và `?` | Executor tối thiểu; không cần driver |
| [03 — external wake](async_03_external_wake.rs) | Thread khác trả kết quả; sender bị drop cũng đánh thức receiver | Shared state, đồng bộ, Waker, executor park/unpark |
| [04 — stack](async_04_stack_no_box.rs) | Borrow biến local, pin future trên stack, không Box future | Pin, poll một lần; noop chỉ vì future hoàn tất ngay |
| [05 — xynok job](async_05_xynok_job.rs) | Await kết quả CPU job trên pool hiện có | xynok pool + oneshot tùy biến + executor trên main |
| [06 — executor](async_06_custom_executor.rs) | Hai task độc lập; queue ID và poll lại khi wake | Executor/scheduler/task storage tự viết; không reactor |
| [07 — IntoFuture](async_07_into_future.rs) | Builder riêng dùng được `.await` | Implement IntoFuture chuẩn, trả về Future chuẩn |
| [08 — static waker](async_08_static_waker.rs) | Tùy biến waker không Arc/Box, future trên stack | RawWaker/VTable chuẩn + static atomic + hợp đồng unsafe |
| [09 — join/spawn](async_runtime/src/bin/09_join_vs_spawn.rs) | Tuần tự, concurrent trong một task, spawn task riêng | Executor; ví dụ dùng timer để biểu diễn chờ |
| [10 — timer](async_runtime/src/bin/10_timer_timeout.rs) | Sleep và timeout operation | Timer driver; timeout không preempt code blocking |
| [11 — TCP](async_runtime/src/bin/11_tcp_reactor.rs) | TCP ping/pong thực sự | I/O driver/reactor; timer chỉ để chặn ví dụ treo quá lâu |
| [12 — channel](async_runtime/src/bin/12_channel_backpressure.rs) | Producer chờ khi buffer đầy; consumer kết thúc khi đóng | Channel biết wake + executor; không cần I/O driver |
| [13 — cancellation](async_runtime/src/bin/13_cancellation.rs) | select drop nhánh thua; abort task; Drop giải phóng state | Combinator/runtime quản lý cancellation |
| [14 — blocking/file](async_runtime/src/bin/14_blocking_and_file.rs) | CPU/blocking offload; đọc file async | Blocking worker pool; file async không đồng nghĩa reactor |
| [15 — local/borrow](async_runtime/src/bin/15_local_borrow_send.rs) | Borrow qua await; Rc trong !Send task | Local executor; phân biệt Send và 'static |
| [16 — mutex](async_runtime/src/bin/16_async_mutex.rs) | Chờ lock mà không block executor thread | Async mutex lưu hàng đợi/waker; không reactor |
| [17 — trait/storage](async_runtime/src/bin/17_async_trait_storage.rs) | Async method generic và interface dyn với boxed future | Trait tự định nghĩa; Future chuẩn; Box là lựa chọn API |
| [18 — errors](async_runtime/src/bin/18_join_errors.rs) | Phân biệt lỗi ứng dụng, panic của task, runtime còn hoạt động | JoinHandle/JoinError của runtime |

## Bức tranh thực thi

```text
async fn / async block → giá trị future
                              │
                 trực tiếp await hoặc spawn thành task
                              │
                   executor gọi poll trên thread
                              │
            ┌─────────────────┴─────────────────┐
          Ready                              Pending
            │                                   │
       hoàn thành                 giữ state và thu xếp wake
                                                │
                  timer / reactor / sender / CPU worker / chính future
                                                │
                                           Waker::wake
                                                │
                                  lên lịch poll lại task
```

Một `.await` không tự spawn task, không tự cấp phát heap và không tự tạo thread.
Future con có thể nằm trực tiếp trong state của future cha. `join!` poll các future con
trong cùng task; `spawn` tạo đơn vị được scheduler quản lý độc lập.
Concurrency là các công việc tiến triển xen kẽ; parallelism cần nhiều thread chạy đồng thời.

Reactor theo dõi sự kiện I/O; executor poll future. Chúng có thể chạy cùng thread.
Timer có cơ chế theo dõi deadline riêng. Không phải task nào cũng cần reactor/timer.
`std` cung cấp giao diện Future, không cung cấp runtime async đa dụng với socket/timer.

## Phần bắt buộc và phần có thể tùy biến

| Thành phần | Hợp đồng chuẩn | Phần bạn thiết kế |
|---|---|---|
| `async` / `.await` | Cú pháp ngôn ngữ, không thay thế bằng keyword riêng | Body, cấu trúc chương trình |
| `IntoFuture` | `.await` chuyển awaitable thành future qua trait chuẩn | Có thể implement cho builder riêng như bài 07 |
| `Future` | Future được poll phải dùng trait chuẩn để tương thích hệ sinh thái | State machine, Output, xử lý readiness |
| `Pin<&mut Self>` | Chữ ký poll chuẩn; phải giữ đúng quy tắc pin | Stack, Box, slab, arena, static storage |
| `Poll<T>` | Ready/Pending chuẩn ở biên poll | Khi nào hoàn thành và giá trị gì |
| `Context` | Context chuẩn truyền vào poll | Executor tạo nó từ waker của task |
| `Waker` | Dùng Waker chuẩn trong Context; đúng quy tắc wake/lifetime/thread safety | Wake sẽ enqueue ID, bật cờ, unpark thread… |
| `Wake` | Không bắt buộc; safe adapter dùng Arc | Implement logic wake; hoặc dùng RawWaker |
| `RawWaker/VTable` | Chỉ cần nếu chọn đường low-level; phải đáp ứng hợp đồng unsafe | Representation, clone/wake/drop, quản lý lifetime |
| Task / executor / scheduler | Không có một trait Task/Executor bắt buộc để `.await` hoạt động | Layout, API spawn, queue, fairness, panic/shutdown |
| Reactor / timer | Không có một reactor/timer trait std bắt buộc | Driver riêng hoặc dùng runtime có sẵn |
| `Send`, `'static` | Không phải bound của mọi Future | API spawn quyết định bound; chạy nhiều thread phải bảo đảm an toàn |
| `Box`, `Arc` | Không phải yêu cầu của async | Tùy chiến lược lưu trữ và chia sẻ ownership |
| Channel, mutex, join, select, async I/O traits | Không phải keyword async/await chuẩn | Tự xây hoặc dùng API/crate; phải tôn trọng hợp đồng API đã chọn |

Lưu ý: `Context` của poll không phải "runtime context". Tokio sleep/socket còn cần
runtime/driver Tokio phù hợp; chỉ đưa chúng vào executor bài 06 không tự cung cấp driver đó.
Cùng implement Future không có nghĩa mọi future độc lập với runtime.

## Liên hệ với InlineFn và cấp phát

InlineFn giữ closure chạy một lần; future giữ state qua nhiều lần poll. Bạn có thể
xóa kiểu bằng vtable tương tự, nhưng future !Unpin phải ở vị trí ổn định sau khi pin
cho đến lúc drop. Queue nên chuyển handle/ID, không chuyển bytes của future đã pin.

Bài 04 và 08 không cần heap cho future/waker. Bài 06 dùng Box cho future để dễ học,
không phải yêu cầu ngôn ngữ. Slab cấp phát trước có thể tránh allocation mỗi spawn,
nhưng vẫn dùng heap lúc khởi tạo; static storage có thể tránh heap hoàn toàn.
Buffer cố định cần kiểm tra cả size và alignment, đồng thời chọn xử lý hết slot/không vừa.
Future có thể lớn do giữ local qua await, nên không mặc định ngưỡng 48 byte của InlineFn phù hợp.

Không được giải phóng/tái sử dụng task storage trong khi waker cũ còn có thể truy cập nó.
Task hoàn thành và mọi handle/waker hết hiệu lực là hai mốc khác nhau.
Ví dụ RawWaker dùng static không bị hết lifetime; đừng thay con trỏ null bằng con trỏ
stack rồi giữ nguyên callback clone/drop: điều đó chưa tạo ra một waker an toàn.

## Điều cần kiểm tra khi tự xây runtime

- Không poll cùng future đồng thời; không poll lại sau Ready.
- Trước Pending, thu xếp wake khi có thể tiến triển; không chỉ tự loop poll liên tục.
- Check readiness và đăng ký waker phải tránh race gây mất wake. Helper oneshot dùng
  cùng một mutex cho cả hai việc và wake bên ngoài lock.
- Cập nhật waker theo Context mới nhất. Wake có thể xảy ra ngay trong poll hoặc trước khi thread ngủ.
- Hủy future không rollback I/O/side effect đã xảy ra. Drop JoinHandle Tokio không abort task.
- Async không preempt CPU loop hoặc biến blocking call thành nonblocking.
- Ví dụ executor 06 chưa có bounded queue, gộp wake, panic isolation hoặc shutdown/cancel API.
  Ví dụ static waker 08 chỉ minh họa một root future với hai lần poll có kiểm soát.

## Tự thử để hiểu

1. Bài 01: đổi yielded thành true; dự đoán assertion nào thất bại.
2. Bài 02: đổi "22" thành chuỗi sai; xem `?` kết thúc future thế nào.
3. Bài 03: bỏ delay của producer; chương trình vẫn phải đúng dù send xảy ra trước poll.
4. Bài 06: thêm task C hoặc yield hai lần; quan sát thứ tự queue.
5. Bài 09: so thứ tự start/done của sequential, join và spawn.
6. Bài 12: tăng capacity từ 1 lên 3; xem producer đi được bao xa trước khi chờ.
7. Bài 15: thử thay spawn_local bằng spawn trong một bản sao; đọc lỗi Rc không Send.

## Nguồn chính thức

- [Future: poll, Pending, cập nhật waker](https://doc.rust-lang.org/std/future/trait.Future.html)
- [IntoFuture: chuyển đổi khi await](https://doc.rust-lang.org/std/future/trait.IntoFuture.html)
- [Pin: địa chỉ ổn định và quy tắc drop](https://doc.rust-lang.org/std/pin/)
- [Wake: adapter an toàn qua Arc](https://doc.rust-lang.org/std/task/trait.Wake.html)
- [RawWakerVTable: hợp đồng unsafe](https://doc.rust-lang.org/std/task/struct.RawWakerVTable.html)
- [Tokio spawning: task, Send, static](https://tokio.rs/tokio/tutorial/spawning)
- [Tokio select và cancellation](https://tokio.rs/tokio/tutorial/select)
- [Tokio timer: runtime requirement](https://docs.rs/tokio/latest/tokio/time/fn.sleep.html)
- [Tokio fs: blocking pool](https://docs.rs/tokio/latest/tokio/fs/index.html)
