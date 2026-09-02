use crate::sync::thread::{JoinHandle, Thread};
pub struct WorkerData
{
    pub host:   Thread,
    pub handle: JoinHandle<()>,
}

impl WorkerData
{
    pub fn new(handle: JoinHandle<()>) -> Self
    {
        let host = handle.thread().clone();

        Self { host, handle }
    }
}
