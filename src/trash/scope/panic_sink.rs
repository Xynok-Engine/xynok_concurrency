use std::any::Any;

use crate::sync::Mutex;

/// Chỗ giữ cái panic đầu tiên mà một job trong scope ném ra.
pub type PanicSlot = Mutex<Option<Box<dyn Any + Send + 'static>>>;

/// Con trỏ tới ô panic, đưa được vào job.
///
/// Ô đó nằm trong scope, mà scope thì sống chừng nào còn vé latch chưa thả, nên con trỏ này luôn
/// hợp lệ ở mọi chỗ nó được dùng.
pub struct PanicSink(pub(crate) *const PanicSlot);
unsafe impl Send for PanicSink {}
