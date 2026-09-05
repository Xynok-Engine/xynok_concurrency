use std::sync::OnceLock;

use crate::profile::end_on_drop::EndOnDrop;
use crate::profile::sink::Sink;

/// Sink đang cắm. `OnceLock` chứ không phải một cái khoá: nó được đọc mỗi job một lần và được ghi
/// mỗi tiến trình một lần, mà một profiler đổi được giữa chừng thì chỉ tạo ra một timeline có vết
/// nối.
static SINK: OnceLock<&'static dyn Sink> = OnceLock::new();

/// Cắm sink cho cả tiến trình, trả về có nhận hay không.
///
/// Chỉ lời gọi đầu tiên có tác dụng; lời thứ hai trả `false` và không đổi gì. Cắm nó **trước** khi
/// dựng pool, không thì đám worker đang chạy sẽ báo cáo vào hư không cho tới lần chúng nhìn lại.
pub fn install(sink: &'static dyn Sink) -> bool
{
    SINK.set(sink).is_ok()
}

/// Sink đang cắm, nếu có.
///
/// Toàn bộ đường nóng nằm ở đây: một `OnceLock::get` là một lần load acquire một con trỏ, và nhánh
/// `None` là nhánh mà bộ dự đoán thấy mọi lần trong một bản build không gắn profiler.
#[inline(always)]
pub(crate) fn current_sink() -> Option<&'static dyn Sink>
{
    SINK.get().copied()
}

/// Chạy `f`, kẹp giữa [`Sink::job_begin`] và [`Sink::job_end`] nếu có sink.
///
/// Lời gọi kết thúc xảy ra cả khi `f` unwind, vì một zone mở ra mà không đóng lại làm hỏng mọi phép
/// đo phía sau nó chứ không riêng phép đo của chính nó.
#[inline]
pub(crate) fn job<R>(worker: usize, f: impl FnOnce() -> R) -> R
{
    match current_sink()
    {
        None => f(),
        Some(sink) =>
        {
            sink.job_begin(worker);
            let _end = EndOnDrop(sink, worker);
            f()
        }
    }
}
