#[cfg(not(loom))]
pub(crate) use std::thread::{JoinHandle, Thread, ThreadId, current, park, park_timeout, yield_now};

#[cfg(loom)]
pub(crate) use loom::thread::{JoinHandle, Thread, ThreadId, current, park, yield_now};

/// Spawns a worker, giving it a name so it shows up as itself in a debugger or profiler
/// rather than as `Thread-<a number>`.
#[cfg(not(loom))]
pub(crate) fn spawn_named<F>(name: String, f: F) -> std::io::Result<JoinHandle<()>>
where F: FnOnce() + Send + 'static
{
    std::thread::Builder::new().name(name).spawn(f)
}

/// Loom's threads are cooperatively scheduled coroutines and carry no OS-level name, so the
/// name is dropped here. Nothing in a loom model builds a real pool, see [`crate::pool`].
#[cfg(loom)]
pub(crate) fn spawn_named<F>(_name: String, f: F) -> std::io::Result<JoinHandle<()>>
where F: FnOnce() + Send + 'static
{
    Ok(loom::thread::spawn(f))
}

/// Loom không mô hình hoá đồng hồ, nên ngủ có hạn giờ thành nhường lượt. Chỗ duy nhất dùng nó
/// là vòng chờ có việc để chạy, và ở đó "tỉnh sớm" luôn hợp lệ.
#[cfg(loom)]
pub(crate) fn park_timeout(_dur: std::time::Duration)
{
    loom::thread::yield_now();
}
