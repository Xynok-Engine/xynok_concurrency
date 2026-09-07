#[derive(Debug, Clone, Copy, Hash, Default, PartialEq, Eq)]
pub enum Priority
{
    #[default]
    Frame,
    Io,
    Background,
}
#[derive(Clone, Copy)]
pub enum Level
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
pub fn apply(level: Level)
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
pub fn apply(level: Level)
{
    const THREAD_PRIORITY_ABOVE_NORMAL: i32 = 1;
    const THREAD_PRIORITY_BELOW_NORMAL: i32 = -1;

    /// `ThreadPowerThrottling` in `THREAD_INFORMATION_CLASS`.
    const THREAD_POWER_THROTTLING: i32 = 4;
    const THREAD_POWER_THROTTLING_CURRENT_VERSION: u32 = 1;
    /// The only bit there is right now: lets the OS throttle how fast this thread runs.
    const THREAD_POWER_THROTTLING_EXECUTION_SPEED: u32 = 0x1;

    #[repr(C)]
    struct ThreadPowerThrottlingState
    {
        version:      u32,
        /// Indicates which bits in `state_mask` are significant.
        control_mask: u32,
        /// Toggle each of those bits on or off.
        state_mask:   u32,
    }

    unsafe extern "system" {
        fn GetCurrentThread() -> isize;
        fn SetThreadPriority(thread: isize, priority: i32) -> i32;
        fn SetThreadInformation(thread: isize, information_class: i32, information: *const core::ffi::c_void, size: u32) -> i32;
    }

    let (priority, throttling) = match level
    {
        // Thread frame: prioritizes execution and disables EcoQoS. Without this, on systems with
        // performance and efficiency cores, Windows might migrate the entire pool to the
        // slower cores. A 16 ms frame budget just cannot handle that.
        Level::Interactive => (THREAD_PRIORITY_ABOVE_NORMAL, 0),
        // Thread IO: yields priority and enables EcoQoS. Since most of its time is spent in
        // syscalls, running on efficiency cores rarely impacts performance, and it significantly
        // improves battery life.
        Level::Low => (THREAD_PRIORITY_BELOW_NORMAL, THREAD_POWER_THROTTLING_EXECUTION_SPEED),
    };

    let state = ThreadPowerThrottlingState {
        version:      THREAD_POWER_THROTTLING_CURRENT_VERSION,
        control_mask: THREAD_POWER_THROTTLING_EXECUTION_SPEED,
        state_mask:   throttling,
    };

    unsafe {
        let thread = GetCurrentThread();
        let _ = SetThreadPriority(thread, priority);
        // This requires Windows 10 1809 or later. Older versions return an error, but it's fine to just ignore it. Losing out on a minor power optimization isn't worth preventing the app from running.
        let _ = SetThreadInformation(
            thread,
            THREAD_POWER_THROTTLING,
            (&raw const state).cast::<core::ffi::c_void>(),
            size_of::<ThreadPowerThrottlingState>() as u32,
        );
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
pub fn apply(_level: Level) {}
