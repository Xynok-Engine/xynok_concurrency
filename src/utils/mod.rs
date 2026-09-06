pub mod backoff;
pub mod bits;
pub mod cache_padded;
pub mod cursors;
pub mod fixed_ring_buffer;
pub mod fixed_buffer;
pub mod inline_fn;
pub mod lock_free;
pub mod packed;
pub mod queue_batching;
pub mod slots;
pub mod spinlock;
pub mod steal;
pub mod random;
pub mod latch;

#[inline]
pub fn ignore_poison<G>(result: std::sync::LockResult<G>) -> G
{
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[inline]
pub fn available_cores() -> usize
{
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}
