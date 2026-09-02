/// Lane nào chạy job này.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum LaneId
{
    /// Việc CPU-bound của frame. Mặc định, và là chỗ phần lớn job sống.
    #[default]
    Compute,
    /// Việc mà phần lớn thời gian là chờ: nạp asset, decode, compile shader. Lane này chạy future.
    Async,
    /// Việc buộc phải chạy trên main thread.
    Main,
}
