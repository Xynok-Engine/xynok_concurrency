use crate::custom_type::Job;
use crate::pool::Shared;
use crate::sync::{Arc, AtomicUsize, Mutex, Ordering};
use crate::utils::poison::ignore_poison;

/// Một nút của đồ thị: phần việc, thứ nó còn đang chờ, và thứ đang chờ nó.
pub struct Node
{
    /// Số phụ thuộc chưa xong, cộng một cho cái chốt "đang còn dựng", xem [`Scope::spawn_after`].
    /// Ai kéo con số này về 0 thì người đó đẩy job đi.
    pub(crate) pending:    AtomicUsize,
    /// Lấy ra đúng một lần, bởi thread chạy nút này.
    pub(crate) work:       Mutex<Option<Job>>,
    /// Những nút cần được thả ra khi nút này xong.
    ///
    /// `None` nghĩa là nút này đã xong rồi, và đó chính là trạng thái mà một kẻ phụ thuộc tới muộn
    /// phải nhìn thấy được, xem [`Scope::spawn_after`].
    pub(crate) successors: Mutex<Option<Vec<Arc<Node>>>>,
    /// Phần dùng chung của pool, **không** kèm join handle: một nút sống lâu hơn tay cầm cuối cùng
    /// của pool thì cũng không được phép giữ pool sống, và càng không được phép làm worker đang
    /// chạy nó đi join chính nó. Xem [`crate::scope::Scope`].
    pub(crate) shared:     Arc<Shared>,
}

impl Node
{
    /// Thêm `dependent` vào danh sách kế nhiệm của nút này, hoặc không làm gì nếu nút này đã xong.
    ///
    /// Bộ đếm được cộng **dưới cùng cái khoá** mà phía kết thúc phải lấy để phát danh sách ra, và đó
    /// là thứ phân biệt được "đã xong" với "sắp xong". Cộng ở ngoài khoá thì còn một khe: nút này
    /// đọc ra là chưa xong, một nhịp sau nó phát danh sách kế nhiệm mà trong đó không có
    /// `dependent`, và thế là một job không bao giờ có ai thả nó ra.
    pub(crate) fn register(&self, dependent: &Arc<Self>)
    {
        let mut successors = ignore_poison(self.successors.lock());

        if let Some(list) = successors.as_mut()
        {
            dependent.pending.fetch_add(1, Ordering::Relaxed);
            list.push(Arc::clone(dependent));
        }
    }

    /// Báo một phụ thuộc đã xong, và đẩy job đi nếu đó là cái cuối cùng.
    ///
    /// Là hàm liên kết chứ không phải phương thức: receiver sẽ phải là `Arc<Self>`, mà dưới
    /// `--cfg loom` thì đó là `loom::sync::Arc`, một kiểu mà Rust không nhận làm receiver.
    pub(crate) fn release(node: &Arc<Self>)
    {
        // `AcqRel` để job nhìn thấy mọi thứ các phụ thuộc của nó đã ghi, và theo đúng lập luận
        // release sequence như ở latch, nó thấy **tất cả** chứ không riêng cái cuối cùng.
        if node.pending.fetch_sub(1, Ordering::AcqRel) != 1
        {
            return;
        }

        let shared = Arc::clone(&node.shared);
        let node = Arc::clone(node);
        shared.inject(crate::custom_type::Job::new(move || Node::run(node)));
    }

    /// Chạy phần việc rồi thả những nút đang chờ nó. Xem [`Self::release`] về chuyện không phải
    /// phương thức.
    pub(crate) fn run(node: Arc<Self>)
    {
        // `Scope::job` đã bọc closure trong `catch_unwind` của riêng nó, nên chỗ này không unwind
        // được và danh sách kế nhiệm bên dưới không thể bị một phụ thuộc panic bỏ qua. Một kẻ kế
        // nhiệm bị bỏ rơi là một scope không bao giờ về 0, tức là treo chứ không phải một thông báo
        // lỗi.
        if let Some(work) = ignore_poison(node.work.lock()).take()
        {
            work.run_once();
        }

        // `take` chứ không phải đọc: nó công bố "nút này xong rồi" cho mọi kẻ phụ thuộc chưa kịp
        // đăng ký, để chúng biết là khỏi chờ.
        let successors = ignore_poison(node.successors.lock()).take().unwrap_or_default();

        for successor in successors
        {
            Node::release(&successor);
        }
    }
}
