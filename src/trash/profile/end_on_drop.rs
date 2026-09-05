use crate::profile::sink::Sink;

/// Đóng khoảng đo của một job khi nó rời khỏi tầm nhìn, kể cả lúc job đang panic.
pub struct EndOnDrop(pub(crate) &'static dyn Sink, pub(crate) usize);

impl Drop for EndOnDrop
{
    fn drop(&mut self)
    {
        self.0.job_end(self.1);
    }
}
