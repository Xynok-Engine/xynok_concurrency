#[rustfmt::skip] #[cfg(not(any(loom, miri)))] pub const SPIN_LIMIT: u32 = 6;
#[rustfmt::skip] #[cfg(not(any(loom, miri)))] pub const YIELD_LIMIT: u32 = 10;

/// Dưới loom mỗi vòng quay là một nhánh mới trong mô hình; dưới miri mỗi vòng quay là hàng chục
/// lệnh được diễn giải. Cả hai đều muốn nhường CPU ngay thay vì quay tại chỗ — `spin_loop` là gợi ý
/// hiệu năng, cắt đi không đổi tính đúng đắn của bất cứ thứ gì.
#[rustfmt::skip] #[cfg(any(loom, miri))] pub const SPIN_LIMIT: u32 = 0;
#[rustfmt::skip] #[cfg(any(loom, miri))] pub const YIELD_LIMIT: u32 = 1;
