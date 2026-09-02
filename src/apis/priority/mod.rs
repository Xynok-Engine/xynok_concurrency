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

pub mod apply;
pub mod level;
pub mod priority;

pub use priority::Priority;
