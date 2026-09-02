use crate::utils::inline_fn::fn_buffer::FnBuffer;

/// Hai việc duy nhất cần làm được với một closure đã bị xoá kiểu: chạy nó, hoặc thả nó.
///
/// Mỗi kiểu closure có sẵn một bảng tĩnh riêng, nên job chỉ giữ con trỏ tới bảng chứ không phải
/// mang theo thông tin kiểu.
pub struct VTable
{
    pub runner:  fn(*mut FnBuffer),
    pub dropper: fn(*mut FnBuffer),
}
