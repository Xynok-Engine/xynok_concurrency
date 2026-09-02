//! Nửa của pool giữ join handle, và dừng thread khi bị thả.

use crate::sync::thread::JoinHandle;
use crate::sync::{Arc, Mutex, Ordering};
use crate::utils::poison::ignore_poison;

use super::shared::Shared;

/// Nửa của pool mà destructor của nó dừng các thread.
///
/// Tồn tại chỉ để `Drop` nổ đúng lúc. Worker giữ `Arc<Shared>`, nên nếu join handle nằm trong
/// `Shared` thì worker tự giữ sống cái cờ shutdown của chính nó và pool không bao giờ dừng được.
pub(super) struct Owner
{
    pub(super) shared:  Arc<Shared>,
    pub(super) handles: Mutex<Vec<JoinHandle<()>>>,
}

impl Owner
{
    pub(super) fn stop(&self)
    {
        if self.shared.shutdown.swap(true, Ordering::SeqCst)
        {
            return;
        }

        self.shared.sleep.notify_all();

        let mut handles = ignore_poison(self.handles.lock());
        match self.shared.on_own_worker()
        {
            // Đang đứng trên chính một worker của pool này: join sẽ là join chính mình. Thả handle
            // ra, thread thấy cờ shutdown thì tự thoát. Xem [`ThreadPool::shutdown`].
            true => handles.clear(),
            false =>
            {
                for handle in handles.drain(..)
                {
                    // Worker nào panic thì panic hook đã in ra rồi, và ở đây không có ai để trao
                    // payload.
                    let _ = handle.join();
                }
            }
        }
        drop(handles);

        // Worker vét được phần lớn trước khi thoát, nhưng thứ được đẩy vào sau lần ngó cuối của
        // worker cuối cùng thì vẫn nằm đó, không còn thread nào với tới. Chạy nốt ở đây thay vì thả
        // trôi: một `scope` đang đếm chúng sẽ không bao giờ về 0 nếu chúng biến mất.
        self.shared.drain_all();
    }
}

impl Drop for Owner
{
    fn drop(&mut self)
    {
        self.stop();
    }
}
