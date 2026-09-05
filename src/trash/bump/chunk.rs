use std::cell::UnsafeCell;

/// Đơn vị của vùng nhớ nền, có cỡ và canh lề sao cho cả buffer canh 64 byte. Đó cũng là mức canh lề
/// chặt nhất mà [`Bump::alloc`] đáp ứng được.
///
/// Đám byte nằm trong một `UnsafeCell` vì [`Bump::alloc`] ghi vào chúng qua `&self`. Một con trỏ
/// dẫn xuất từ `&[Chunk]` trần là chỉ đọc dù có ép kiểu kiểu gì, và ghi qua nó là hành vi không xác
/// định, miri chặn thẳng. `UnsafeCell` chính là thứ cho một tham chiếu chia sẻ mang theo quyền ghi.
/// Dùng `std::cell` chứ không phải bản bọc cho loom: một arena thuộc về đúng một thread, nên ở đây
/// không có interleaving nào để loom kiểm.
#[repr(align(64))]
pub struct Chunk(#[allow(dead_code)] pub(crate) UnsafeCell<[u8; 64]>);
