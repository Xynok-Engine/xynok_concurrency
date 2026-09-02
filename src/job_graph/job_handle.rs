use std::marker::PhantomData;

use crate::job_graph::node::Node;
use crate::sync::{Arc, Ordering};

/// Job được spawn qua [`Scope::spawn_after`] hoặc [`Scope::spawn_with_handle`], dùng làm điều kiện
/// cho những job sau.
///
/// # Vì sao chỉ scope thôi thì chưa đủ
///
/// [`ThreadPool::scope`] kết thúc bằng "chờ tất cả", tức là một frame bị cắt thành từng chặng ngăn
/// bởi barrier. Mà một barrier thì tốn đúng bằng job **chậm nhất** trong chặng đó, còn mọi thread
/// khác ngồi không cho hết phần chênh lệch:
///
/// ```text
/// chỉ có scope  [== physics ==][=====]│[= anim =][==========]│[== cull ==]
///                                     ▲ cả pool chờ một anh đi sau
///
/// có handle     [== physics ==][= anim =][== cull ==]
///               [=====][==========][··· bốc luôn việc của chặng sau ···]
/// ```
///
/// Một ECS scheduler vốn đã biết system nào đọc gì ghi gì, tức là nó đã cầm sẵn đồ thị phụ thuộc.
/// Thứ nó thiếu là một cách **nói ra** điều đó. Một cái handle cộng [`Scope::spawn_after`] là bề mặt
/// nhỏ nhất đủ để nói, và sau này có muốn dựng hẳn một đồ thị khai báo thì cũng dựng trên đúng nền
/// này mà không phải sửa gì phía trên.
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
///
/// use xynok_concurrency::pool::{Config, ThreadPool};
///
/// let pool = ThreadPool::new(Config {
///     threads: 3,
///     ..Default::default()
/// });
///
/// let order = AtomicUsize::new(0);
/// let physics_done = AtomicUsize::new(usize::MAX);
/// let render_done = AtomicUsize::new(usize::MAX);
///
/// pool.scope(|s| {
///     let physics = s.spawn_with_handle(|| {
///         physics_done.store(order.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
///     });
///
///     // Chưa chạy khi physics chưa xong, nhưng dòng này không chờ gì cả.
///     s.spawn_after(&[&physics], || {
///         render_done.store(order.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
///     });
/// });
///
/// assert!(physics_done.load(Ordering::SeqCst) < render_done.load(Ordering::SeqCst));
/// ```
///
/// # Handle không có hàm `wait`
///
/// Cố ý. Chờ một job thì [`Scope::spawn`] cộng cái join sẵn có của scope đã làm rồi, và bày thêm
/// `wait` ở đây là mời gọi đúng cái lối mà kiểu này sinh ra để thay: spawn, chờ, spawn, chờ. Hãy nói
/// thứ tự ra thành một quan hệ phụ thuộc, và để một điểm join duy nhất ở cuối scope là chỗ duy nhất
/// có ai đó phải dừng lại.
pub struct JobHandle<'scope>
{
    pub(crate) node:   Arc<Node>,
    /// Buộc handle vào scope của nó, để nó không bị giữ qua khỏi cái join vốn là thứ bảo đảm tham
    /// chiếu mà job bắt được vẫn còn sống.
    pub(crate) marker: PhantomData<&'scope ()>,
}

impl std::fmt::Debug for JobHandle<'_>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("JobHandle")
            .field("pending", &self.node.pending.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}
