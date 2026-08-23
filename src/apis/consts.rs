#[rustfmt::skip] #[cfg(not(loom))] pub const SPIN_LIMIT: u32 = 6;
#[rustfmt::skip] #[cfg(not(loom))] pub const YIELD_LIMIT: u32 = 10;

#[rustfmt::skip] #[cfg(loom)] pub const SPIN_LIMIT: u32 = 0;
#[rustfmt::skip] #[cfg(loom)] pub const YIELD_LIMIT: u32 = 1;

/// Số slot thật trong một block của [`QueueBatching`](crate::utils::queue_batching::QueueBatching).
///
/// 63 slot + 1 chỉ số đánh dấu = 64, một lũy thừa 2 nên `% LAP` là một phép `and`. Với `T` cỡ một
/// cache line thì một block ~4 KB — đúng một page, và một lần cấp phát cho 63 job.
///
/// Dưới loom con số phải bé: mỗi lần vượt biên block là một nhánh mới trong mô hình, nên 63 nghĩa là
/// không bao giờ test được đường thay block.
#[rustfmt::skip] #[cfg(not(loom))] pub const QUEUE_BLOCK_CAP: usize = 63;
#[rustfmt::skip] #[cfg(loom)] pub const QUEUE_BLOCK_CAP: usize = 3;
