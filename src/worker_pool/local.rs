use crate::sync::thread::{self, ThreadId};
use std::cell::Cell;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerCtx
{
    pool:    u64,
    index:   usize,
    in_loop: bool,
}
impl WorkerCtx
{
    const NONE: Self = Self {
        pool:    0,
        index:   usize::MAX,
        in_loop: false,
    };
}
thread_local! {

    static SELF_ID: ThreadId = thread::current().id();
    static CONTEXT: Cell<WorkerCtx> = const { Cell::new(WorkerCtx::NONE) };

}
