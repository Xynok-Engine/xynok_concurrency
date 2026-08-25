#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32};

pub mod spinlock;
pub mod backoff;
pub mod waker;
pub mod cache_padded;
pub mod queue_batching;
pub mod inline_fn;
pub mod slots;
pub mod steal;

#[inline]
pub const fn pack(a: u32, b: u32) -> u64
{
    (a as u64) << 32 | (b as u64)
}
#[inline]
pub const fn unpack(val: u64) -> (u32, u32)
{
    ((val >> 32) as u32, val as u32)
}

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
#[cfg(test)]
mod test
{
    use super::*;
    #[test]
    fn test_packing()
    {
        let a: u32 = 23;
        let b: u32 = 1;
        println!("a:       {:08b}", a);
        println!("b:       {:08b}", b);
        let packed = pack(a, b);
        println!("packed:  {:08b}", packed);
        let (left, right) = unpack(packed);
        println!("left:    {:08b}", left);
        println!("right:   {:08b}", right);
        assert!(left == a);
        assert!(right == b);
    }
}
