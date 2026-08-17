#[inline]
pub(crate) fn ignore_poison<G>(result: std::sync::LockResult<G>) -> G
{
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}
#[cfg(test)]
mod test
{
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::thread;

    static VALUE1: AtomicU64 = AtomicU64::new(0);
    static VALUE2: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn main()
    {
        thread::spawn(|| {
            VALUE1.store(1, Relaxed);
            VALUE2.store(42, Release);
        });

        println!("{}", VALUE2.load(Acquire));
        println!("{}", VALUE1.load(Relaxed));
    }
}
