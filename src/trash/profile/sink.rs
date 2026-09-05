/// Những lời gọi lại mà pool phát ra ở các điểm chỉ nó nhìn thấy.
///
/// # Nó để làm gì
///
/// Profiler vốn đã đo được **bên trong** một job: đó là code ứng dụng bình thường, một `span!` của
/// Tracy đặt trong thân job là đủ. Thứ nó không nhìn thấy từ ngoài là mọi thứ **giữa** các job:
/// worker nằm park bao lâu, bao nhiêu phần của frame đi vào việc lập lịch thay vì việc thật, và pool
/// có ngủ giữa frame vì một anh đi sau giữ cái join hay không. Đó mới là những câu quyết định một
/// frame budget đang được tiêu hay đang bị phí, và chúng chỉ trả lời được từ trong này.
///
/// Mọi phương thức đều có bản mặc định, nên ai cài chỉ cần nhận đúng thứ mình quan tâm.
///
/// # Giá khi không cắm gì
///
/// Một lần load relaxed và một nhánh rẽ mà bộ dự đoán đoán đúng mọi lần, tính trên mỗi job. So với
/// chi phí mỗi job của chính pool thì nó không hiện lên.
///
/// ```
/// use xynok_concurrency::profile;
///
/// struct Counting;
///
/// impl profile::Sink for Counting
/// {
///     fn job_begin(&self, worker: usize)
///     {
///         let _ = worker; // chỗ mà một bản thật sẽ mở một zone Tracy
///     }
/// }
///
/// static SINK: Counting = Counting;
/// profile::install(&SINK);
/// ```
pub trait Sink: Send + Sync + 'static
{
    /// Một job sắp chạy trên `worker`. Luôn đi kèm đúng một [`Self::job_end`].
    fn job_begin(&self, worker: usize)
    {
        let _ = worker;
    }

    /// Một job vừa xong trên `worker`, dù nó trả về bình thường hay đang unwind.
    fn job_end(&self, worker: usize)
    {
        let _ = worker;
    }

    /// `worker` hết việc và sắp park.
    ///
    /// Khoảng cách tới [`Self::worker_unpark`] tương ứng là thời gian rảnh, và một khoảng lớn nằm
    /// giữa frame nghĩa là frame chưa được chia đủ nhỏ để nuôi hết pool.
    fn worker_park(&self, worker: usize)
    {
        let _ = worker;
    }

    /// `worker` vừa dậy.
    fn worker_unpark(&self, worker: usize)
    {
        let _ = worker;
    }

    /// Một lần [`ThreadPool::end_frame`](crate::pool::ThreadPool::end_frame).
    fn frame_end(&self) {}
}
