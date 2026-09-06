use crate::sync::{AtomicU64, Ordering};
use std::cell::Cell;

const NO_POOL: u64 = 0;

#[cfg(not(loom))]
pub fn next_pool_id() -> u64
{
    static NEXT: AtomicU64 = AtomicU64::new(NO_POOL + 1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[cfg(loom)]
loom::lazy_static! {
    static ref NEXT_POOL_ID: AtomicU64 = AtomicU64::new(NO_POOL + 1);
}

#[cfg(loom)]
pub fn next_pool_id() -> u64
{
    NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Context
{
    pub pool:  u64,
    pub index: usize,
}

impl Context
{
    pub const NONE: Self = Self {
        pool:  NO_POOL,
        index: usize::MAX,
    };
}

thread_local! {

    pub static THREAD_LOCAL_CTX: Cell<Context> = const { Cell::new(Context::NONE) };
}
