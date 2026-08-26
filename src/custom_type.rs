#![allow(unused)]
use crate::utils::inline_fn::InlineFn;

/// Một đơn vị công việc mà pool nhận và chạy đúng một lần.
///
/// Đây là [`InlineFn`] chứ không phải `Box<dyn FnOnce()>`, và khác biệt nằm ở chỗ closure đi đâu.
/// `Box` luôn đẩy closure ra heap: mỗi lần `spawn` là một lần allocate, và lúc chạy thì worker phải
/// đi theo con trỏ tới một vùng nhớ nó chưa từng chạm, tức là gần như chắc chắn một lần cache miss.
///
/// `InlineFn` giữ closure ngay trong chính nó khi closure đủ nhỏ (48 byte, đủ cho vài `Arc` cộng
/// một handle), và cả cấu trúc vừa đúng 64 byte, tức là một cache line. Nhét vào ring thì mỗi job
/// nằm gọn trong một dòng cache, worker đọc job là đọc luôn cả closure. Closure lớn hơn thì nó tự
/// box lại và giấu con trỏ vào cùng chỗ, nên người gọi không phải bận tâm cỡ bao nhiêu là vừa.
///
/// Job vẫn phải `'static`. Với ECS thì đó không phải giới hạn: `World` đi qua `HeapMut` nên bản
/// thân nó không mang lifetime nào. Chỗ cần mượn stack thật (chia chunk trong một system chẳng hạn)
/// sẽ do `scope()` lo, và tới lúc đó `Job` là chỗ để thêm tham số lifetime.
pub type Job = InlineFn;
