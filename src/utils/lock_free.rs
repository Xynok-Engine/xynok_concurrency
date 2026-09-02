#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32};

/// Hỏi xem `T` có nhỏ và thẳng hàng vừa đủ để CPU đọc ghi nó bằng một lệnh nguyên tử hay không.
///
/// Kiểu nào trả về `false` thì muốn dùng chung giữa nhiều thread sẽ phải mượn thêm khoá, chứ phần
/// cứng không tự lo được.
pub const fn is_lock_free<T>() -> bool
{
    let lock_free = is_zero_sized::<T>() || can_transmute::<T, AtomicU8>() || can_transmute::<T, AtomicU16>() || can_transmute::<T, AtomicU32>();

    #[cfg(target_has_atomic = "64")]
    let lock_free = lock_free || can_transmute::<T, AtomicU64>();

    lock_free
}

const fn can_transmute<A, B>() -> bool
{
    size_of::<A>() == size_of::<B>() && align_of::<A>() >= align_of::<B>()
}

const fn is_zero_sized<T>() -> bool
{
    size_of::<T>() == 0
}
