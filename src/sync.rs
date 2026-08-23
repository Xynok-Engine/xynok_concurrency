#![allow(unused)]
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{
    AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence,
};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Condvar, Mutex};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{
    AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence,
};
#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Condvar, Mutex};

#[cfg(not(loom))]
pub(crate) mod thread
{
    pub(crate) use std::thread::{JoinHandle, Thread, current, park, yield_now};

    /// Spawns a worker, giving it a name so it shows up as itself in a debugger or profiler
    /// rather than as `Thread-<a number>`.
    pub(crate) fn spawn_named<F>(name: String, f: F) -> std::io::Result<JoinHandle<()>>
    where F: FnOnce() + Send + 'static
    {
        std::thread::Builder::new().name(name).spawn(f)
    }
}

#[cfg(loom)]
pub(crate) mod thread
{
    pub(crate) use loom::thread::{JoinHandle, Thread, current, park, yield_now};

    /// Loom's threads are cooperatively scheduled coroutines and carry no OS-level name, so the
    /// name is dropped here. Nothing in a loom model builds a real pool - see [`crate::pool`].
    pub(crate) fn spawn_named<F>(_name: String, f: F) -> std::io::Result<JoinHandle<()>>
    where F: FnOnce() + Send + 'static
    {
        Ok(loom::thread::spawn(f))
    }
}

#[cfg(not(loom))]
pub(crate) mod cell
{
    #[derive(Debug)]
    pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T>
    {
        pub(crate) const fn new(value: T) -> Self
        {
            Self(std::cell::UnsafeCell::new(value))
        }

        #[inline]
        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R
        {
            f(self.0.get())
        }

        #[inline]
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R
        {
            f(self.0.get())
        }
    }
}

#[cfg(loom)]
pub(crate) mod cell
{
    pub(crate) use loom::cell::UnsafeCell;
}

#[inline]
pub(crate) fn spin_loop()
{
    #[cfg(not(loom))]
    std::hint::spin_loop();

    #[cfg(loom)]
    loom::thread::yield_now();
}
#[inline]
pub(crate) fn yield_now()
{
    std::thread::yield_now();
}
