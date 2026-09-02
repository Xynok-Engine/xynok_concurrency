use crate::sync::thread::{JoinHandle, Thread};
pub struct WorkerHandle
{
    pub host:   Thread,
    pub handle: JoinHandle<()>,
}

impl WorkerHandle
{
    pub fn new(handle: JoinHandle<()>) -> Self
    {
        let host = handle.thread().clone();

        Self { host, handle }
    }
}
