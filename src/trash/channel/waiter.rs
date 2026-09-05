use crate::sync::thread::Thread;

/// Ai đang chờ ở đầu nhận, và gọi họ dậy bằng cách nào.
pub enum Waiter
{
    /// Một thread đã ghi tên rồi `park`.
    Thread(Thread),
    /// Một task đã trả `Pending`. Waker của nó lo việc xếp task lại vào lane, nên ở đây không cần
    /// biết task ấy sống ở lane nào.
    Task(std::task::Waker),
}

impl Waiter
{
    pub(crate) fn wake(self)
    {
        match self
        {
            Self::Thread(thread) => thread.unpark(),
            Self::Task(waker) => waker.wake(),
        }
    }
}
