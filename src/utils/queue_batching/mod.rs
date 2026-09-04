//! ## Hàng đợi gom theo lô
//!
//! Một hàng đợi vào trước ra trước, nhiều thread đẩy vào và nhiều thread rút ra được, sức chứa tự
//! nới theo nhu cầu.
//!
//! Đây là chỗ việc từ ngoài pool rơi vào, và cũng là chỗ worker xả bớt khi ring riêng của nó đã
//! đầy. Ring riêng che được gần hết nhu cầu, nhưng để hở hai lỗ mà nó không tự bịt được: thread
//! không phải worker thì không sở hữu ring nào nên chẳng có chỗ đẩy việc vào, còn ring thì có biên
//! trong khi lượng việc thì không. Cả hai đều đổ về đây.
//!
//! ### Vì sao lại gom theo lô
//!
//! Hàng đợi nào cũng có một điểm nóng ở chỗ đồng bộ. Đẩy từng phần tử một thì mỗi phần tử phải trả
//! trọn chi phí ở điểm nóng đó, và khi nhiều thread cùng làm vậy thì phần lớn thời gian là giành
//! nhau chứ không phải làm việc.
//!
//! Nên ở đây thao tác nào cũng có bản gom lô đi kèm: chiếm quyền một lần rồi chuyển cả cụm phần
//! tử. Chi phí đồng bộ được chia đều cho cả lô thay vì cho từng phần tử.
//!
//! Đường ra quan trọng nhất là [`steal_batch_and_pop`](QueueBatching::steal_batch_and_pop): trả về
//! một phần tử để chạy ngay và nạp phần còn lại thẳng vào ring riêng của người gọi bằng đúng một
//! lần publish. Cụm lấy về không tham lam, nó chia theo số worker đang dùng chung hàng đợi và luôn
//! chừa lại nửa ring trống cho việc con mà chính worker đó sắp đẻ ra.
//!
//! ### Cách hoạt động
//!
//! Bên trong là một hàng đợi hai đầu thường, được bọc bởi một cờ nguyên tử. Ai chiếm được cờ thì
//! nhận về một tấm vé cho mượn thẳng hàng đợi bên trong, làm bao nhiêu việc cũng được trong một
//! lần chiếm, và vé rơi khỏi tầm nhìn thì quyền tự trả lại.
//!
//! Người chờ thì lùi lại theo nhịp tăng dần chứ không quay tít, vì đoạn giữ quyền ở đây luôn ngắn.
//!
//! Bên cạnh cờ còn một bản sao độ dài, được người cầm vé chốt lại ngay trước lúc nhả quyền. Nhờ nó
//! mà `len` và `is_empty` đọc được không cần giành quyền, và những đường như `pop` biết quay đầu
//! sớm khi hàng đợi đang rỗng. Đổi lại, con số đó có thể trễ vài nhịp so với sự thật.
//!
//! ### Vì sao không lock-free
//!
//! Bên dưới là một hàng đợi thường nằm dưới khoá xoay, và đó là lựa chọn có chủ ý: mỗi lane giữ một
//! hàng đợi riêng nên tranh chấp vốn đã thấp, còn một cấu trúc mình hiểu rõ thì sửa được lúc 2 giờ
//! sáng. Lựa chọn đó chỉ đứng vững nhờ mọi đường nóng đều đụng vào khoá theo lô.
//!
//! > [!NOTE]
//! > Vé cho mượn thẳng hàng đợi bên trong, nên còn giữ vé là còn chặn mọi thread khác. Lấy đủ việc
//! > cần rồi thả ra sớm.
//!
//! > [!NOTE]
//! > Chỗ này không cần biết ring riêng của worker thuộc loại nào. Hai loại ring của crate cùng có
//! > một bộ thao tác với cùng ý nghĩa, nên pool chọn loại nào cũng được, hàng đợi cứ thế đổ việc
//! > vào.

pub mod batch_size;
pub mod local_queue;
pub mod queue_batching;
pub mod queue_batching_guard;

pub use local_queue::LocalQueue;
pub use queue_batching::QueueBatching;
pub use queue_batching_guard::QueueBatchingGuard;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, not(loom)))]
#[path = "tests/stress.rs"]
mod stress_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
