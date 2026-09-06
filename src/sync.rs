//! ## Một lớp áo chung cho std và loom
//!
//! Crate này được kiểm bằng loom, và loom chỉ soi được những thao tác đồng thời do chính nó cài
//! đặt. Nghĩa là mọi biến nguyên tử, mọi ô nhớ chia sẻ, mọi thao tác thread đều phải đổi sang bản
//! của loom khi chạy kiểm, rồi đổi ngược lại khi build thật.
//!
//! Rải `#[cfg]` khắp nơi để làm việc đó thì code chính sẽ đầy nhiễu, và chỉ cần sót một chỗ là
//! loom nhìn không thấy, kiểm xong vẫn tưởng là sạch.
//!
//! ### Cách hoạt động
//!
//! Cả crate chỉ mượn kiểu từ đây, không mượn thẳng từ std. Chỗ này quyết định một lần duy nhất là
//! lấy bản của ai, phần còn lại không cần biết.
//!
//! Chỉ những thứ thật sự cần đổi bản mới có mặt ở đây. Cái gì loom không mô hình hoá, hoặc chỉ
//! dùng để đo kích thước kiểu chứ không chạy đồng thời, thì cứ gọi thẳng std cho gọn.
//!
//! > [!IMPORTANT]
//! > Đừng mượn thẳng std cho những thứ có trong danh sách dưới đây. Một chỗ lách thôi là loom mất
//! > dấu đúng cái đoạn cần soi nhất.

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;

/// Ô nhớ cho phép nhiều nơi cùng nắm tham chiếu mà vẫn ghi vào được.
///
/// Bản của std không ghi lại gì cả, còn bản của loom ghi nhận từng lần chạm để phát hiện hai thread
/// cùng vào một ô. Chỗ này bọc bản std lại theo đúng hình dáng của bản loom, để code gọi chỉ cần
/// viết một kiểu duy nhất.
#[cfg(not(loom))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
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

/// Gợi ý cho CPU rằng đây là một vòng chờ, để nó bớt ăn tài nguyên của core anh em.
///
/// Dưới loom thì thành nhường lượt, vì loom cần một điểm cắt thật để thử các thứ tự khác nhau chứ
/// không hiểu gợi ý của phần cứng.
#[inline]
pub(crate) fn spin_loop()
{
    #[cfg(not(loom))]
    std::hint::spin_loop();

    #[cfg(loom)]
    loom::thread::yield_now();
}

pub(crate) mod thread
{
    #[cfg(not(loom))]
    pub(crate) use std::thread::{JoinHandle, Thread, current, park, park_timeout, yield_now};

    #[cfg(loom)]
    pub(crate) use loom::thread::{JoinHandle, Thread, current, park, yield_now};

    /// Dựng một worker và đặt tên cho nó, để lúc soi bằng debugger hay profiler còn biết ai là ai
    /// thay vì nhìn thấy một dãy `Thread-<số>`.
    #[cfg(not(loom))]
    pub(crate) fn spawn_named<F>(name: String, f: F) -> std::io::Result<JoinHandle<()>>
    where F: FnOnce() + Send + 'static
    {
        std::thread::Builder::new().name(name).spawn(f)
    }

    /// Thread của loom là coroutine do nó tự xếp lượt, không có tên ở tầng hệ điều hành, nên tên bị
    /// bỏ đi. Chạy loom thì cũng chẳng có pool thật nào được dựng lên.
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
}
