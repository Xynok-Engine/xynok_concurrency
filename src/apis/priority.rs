//! Standing mà một thread xin OS: thread của frame cần được chạy trước, thread IO thì nên nhường.
//!
//! Ba nền tảng, ba API hoàn toàn khác nhau, và không cái nào nhận một thread khác làm đối tượng:
//! tất cả đều đặt cho **thread đang gọi**. Nên [`Priority::apply_to_current_thread`] phải được gọi
//! từ chính worker, không phải từ thread đã spawn nó ra.
//!
//! | nền tảng | dùng gì | lý do |
//! |---|---|---|
//! | macOS, iOS | QoS class | nền tảng duy nhất mà lịch trình thật sự nhìn vào nó, và trên máy có core P/E thì đây là thứ quyết định thread chạy trên loại core nào |
//! | Linux, BSD | `nice` | tác động lên chính task đang gọi, vì trên Linux thread là task |
//! | Windows | `SetThreadPriority`, cộng `THREAD_POWER_THROTTLING` | riêng cái sau mới là thứ bật hoặc tắt EcoQoS, tức là dọn thread xuống core tiết kiệm điện |
//!
//! Còn thiếu, và cố ý để sau: util clamp trên Linux (`sched_setattr`) để nói với governor rằng
//! thread frame cần tần số cao. Nó là một syscall thô, số hiệu khác nhau theo kiến trúc, và nó chỉ
//! đáng làm khi đã có số đo cho thấy governor đang hạ tần số nhầm chỗ.
//!
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
fn apply(_level: Level) {}
