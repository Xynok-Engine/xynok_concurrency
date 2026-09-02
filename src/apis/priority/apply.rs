use crate::apis::priority::level::Level;

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub fn apply(level: Level)
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

    /// `ThreadPowerThrottling` trong `THREAD_INFORMATION_CLASS`.
    const THREAD_POWER_THROTTLING: i32 = 4;
    const THREAD_POWER_THROTTLING_CURRENT_VERSION: u32 = 1;
    /// Bit duy nhất hiện có: cho phép hệ điều hành hạ tốc độ thực thi của thread này.
    const THREAD_POWER_THROTTLING_EXECUTION_SPEED: u32 = 0x1;

    #[repr(C)]
    struct ThreadPowerThrottlingState
    {
        version:      u32,
        /// Bit nào trong `state_mask` là có ý nghĩa.
        control_mask: u32,
        /// Bật hay tắt từng bit đó.
        state_mask:   u32,
    }

    unsafe extern "system" {
        fn GetCurrentThread() -> isize;
        fn SetThreadPriority(thread: isize, priority: i32) -> i32;
        fn SetThreadInformation(thread: isize, information_class: i32, information: *const core::ffi::c_void, size: u32) -> i32;
    }

    let (priority, throttling) = match level
    {
        // Thread frame: xin chạy trước, và **tắt** EcoQoS. Không tắt thì trên máy có core hiệu năng
        // và core tiết kiệm điện, Windows có thể dọn cả pool xuống nhóm core chậm, và một frame
        // budget 16 ms thì không chịu nổi chuyện đó.
        Level::Interactive => (THREAD_PRIORITY_ABOVE_NORMAL, 0),
        // Thread IO: nhường đường, và bật EcoQoS. Thời gian của nó nằm trong syscall, nên chạy trên
        // core chậm gần như không đổi gì, mà pin thì đỡ hẳn.
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
        // Windows 10 1809 trở lên mới có. Bản cũ hơn trả lỗi, và bỏ qua là đúng: mất một tinh chỉnh
        // về điện năng thì không đáng để từ chối chạy.
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
