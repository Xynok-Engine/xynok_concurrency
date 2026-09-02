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
