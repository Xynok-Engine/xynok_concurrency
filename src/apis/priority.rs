//!
//! TODO: This currently only supports macOS. I need to implement support for Windows and Linux to ensure accurate measurements.
//! ## References
//!
//! - <https://developer.apple.com/news/?id=vk3m204o>
//! - <https://developer.apple.com/library/archive/documentation/Performance/Conceptual/power_efficiency_guidelines_osx/PrioritizeWorkAtTheTaskLevel.html>
//! - <https://developer.apple.com/documentation/os/workgroups>
//! - `$(xcrun --show-sdk-path)/usr/include/sys/qos.h`
//! - `$(xcrun --show-sdk-path)/usr/include/pthread/qos.h`
//! - <https://www.man7.org/linux/man-pages/man2/setpriority.2.html>
//! - <https://github.com/torvalds/linux/blob/master/Documentation/scheduler/sched-energy.rst>
//! - <https://docs.kernel.org/scheduler/sched-util-clamp.html>
//! - <https://www.man7.org/linux/man-pages/man1/uclampset.1.html>
//! - <https://lwn.net/Articles/762043/>
//! - <https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-setthreadinformation>
//! - <https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/ns-processthreadsapi-thread_power_throttling_state>
//! - <https://devblogs.microsoft.com/performance-diagnostics/introducing-ecoqos/>
//! - <https://chromium.googlesource.com/chromium/chromium/+/refs/heads/main/base/threading/platform_thread_mac.mm>
//! - <https://github.com/clangd/clangd/issues/1119>

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Priority
{
    #[rustfmt::skip]#[default] Frame,
    Background,
    Io,
}

#[derive(Clone, Copy)]
enum Level
{
    Interactive,
    Low,
}

impl Priority
{
    pub fn apply_to_current_thread(self)
    {
        if cfg!(miri)
        {
            return;
        }

        match self
        {
            Self::Frame => apply(Level::Interactive),
            Self::Background | Self::Io => apply(Level::Low),
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn apply(level: Level)
{
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    const QOS_CLASS_UTILITY: u32 = 0x11;

    unsafe extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }

    let class = match level
    {
        Level::Interactive => QOS_CLASS_USER_INTERACTIVE,
        Level::Low => QOS_CLASS_UTILITY,
    };

    unsafe {
        let _ = pthread_set_qos_class_self_np(class, 0);
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd"))]
fn apply(level: Level)
{
    unsafe extern "C" {
        fn nice(increment: i32) -> i32;
    }

    let increment = match level
    {
        Level::Interactive => return,
        Level::Low => 10,
    };

    unsafe {
        let _ = nice(increment);
    }
}

#[cfg(target_os = "windows")]
fn apply(level: Level)
{
    const THREAD_PRIORITY_ABOVE_NORMAL: i32 = 1;
    const THREAD_PRIORITY_BELOW_NORMAL: i32 = -1;

    unsafe extern "system" {
        fn GetCurrentThread() -> isize;
        fn SetThreadPriority(thread: isize, priority: i32) -> i32;
    }

    let priority = match level
    {
        Level::Interactive => THREAD_PRIORITY_ABOVE_NORMAL,
        Level::Low => THREAD_PRIORITY_BELOW_NORMAL,
    };

    unsafe {
        let _ = SetThreadPriority(GetCurrentThread(), priority);
    }
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "windows"
)))]
fn apply(_level: Level) {}
