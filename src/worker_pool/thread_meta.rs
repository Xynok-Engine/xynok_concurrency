use crate::sync::thread::{JoinHandle, Thread};
pub struct ThreadData
{
    pub host:   Thread,
    pub handle: JoinHandle<()>,
}

impl ThreadData
{
    pub fn new(handle: JoinHandle<()>) -> Self
    {
        let host = handle.thread().clone();
        Self { host, handle }
    }
}
