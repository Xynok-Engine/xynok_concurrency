use crate::sync::Ordering;
use crate::sync::thread::Thread;
use crate::utils::waker::waker::Waker;

/// Tấm vé đại diện cho một việc chưa xong.
///
/// Vé rơi khỏi tầm nhìn thì con số việc còn lại giảm một, và ai làm nó về không sẽ gọi người đang
/// chờ dậy. Vì việc giảm gắn vào lúc vé bị thả nên một việc panic giữa chừng cũng không làm treo
/// người chờ.
pub struct WakerSignal<'a>
{
    pub(super) waker:   &'a Waker,
    pub(super) sleeper: Thread,
}

impl Drop for WakerSignal<'_>
{
    fn drop(&mut self)
    {
        if self.waker.remaining.fetch_sub(1, Ordering::Release) == 1
        {
            self.sleeper.unpark();
        }
    }
}
