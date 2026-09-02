/// Số byte mà một job giữ sẵn để chứa closure ngay tại chỗ.
///
/// Cộng với con trỏ bảng hàm và phần đệm canh lề thì vừa đúng một dòng cache 64 byte. Closure nào
/// không lọt vào bấy nhiêu byte thì mới phải đi đường heap.
// TODO: cần thêm test với giá trị 128 byte cho x86_64
pub const INLINE_BYTES: usize = 48;
