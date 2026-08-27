//! Chỗ profiler cắm vào, để hành vi của chính pool nhìn thấy được thay vì phải đoán.

use std::sync::OnceLock;

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

/// Sink đang cắm. `OnceLock` chứ không phải một cái khoá: nó được đọc mỗi job một lần và được ghi
/// mỗi tiến trình một lần, mà một profiler đổi được giữa chừng thì chỉ tạo ra một timeline có vết
/// nối.
static SINK: OnceLock<&'static dyn Sink> = OnceLock::new();

/// Cắm sink cho cả tiến trình, trả về có nhận hay không.
///
/// Chỉ lời gọi đầu tiên có tác dụng; lời thứ hai trả `false` và không đổi gì. Cắm nó **trước** khi
/// dựng pool, không thì đám worker đang chạy sẽ báo cáo vào hư không cho tới lần chúng nhìn lại.
pub fn install(sink: &'static dyn Sink) -> bool
{
    SINK.set(sink).is_ok()
}

/// Sink đang cắm, nếu có.
///
/// Toàn bộ đường nóng nằm ở đây: một `OnceLock::get` là một lần load acquire một con trỏ, và nhánh
/// `None` là nhánh mà bộ dự đoán thấy mọi lần trong một bản build không gắn profiler.
#[inline(always)]
pub(crate) fn sink() -> Option<&'static dyn Sink>
{
    SINK.get().copied()
}

/// Chạy `f`, kẹp giữa [`Sink::job_begin`] và [`Sink::job_end`] nếu có sink.
///
/// Lời gọi kết thúc xảy ra cả khi `f` unwind, vì một zone mở ra mà không đóng lại làm hỏng mọi phép
/// đo phía sau nó chứ không riêng phép đo của chính nó.
#[inline]
pub(crate) fn job<R>(worker: usize, f: impl FnOnce() -> R) -> R
{
    match sink()
    {
        None => f(),
        Some(sink) =>
        {
            sink.job_begin(worker);
            let _end = EndOnDrop(sink, worker);
            f()
        }
    }
}

struct EndOnDrop(&'static dyn Sink, usize);

impl Drop for EndOnDrop
{
    fn drop(&mut self)
    {
        self.0.job_end(self.1);
    }
}

#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct Counting
    {
        begun: AtomicUsize,
        ended: AtomicUsize,
    }

    impl Sink for Counting
    {
        fn job_begin(&self, _worker: usize)
        {
            self.begun.fetch_add(1, Ordering::Relaxed);
        }

        fn job_end(&self, _worker: usize)
        {
            self.ended.fetch_add(1, Ordering::Relaxed);
        }
    }

    static SINK: Counting = Counting {
        begun: AtomicUsize::new(0),
        ended: AtomicUsize::new(0),
    };

    /// Một test duy nhất, vì sink là toàn cục và chỉ cắm được một lần: tách ra nhiều test thì cái
    /// nào chạy trước sẽ giành mất chỗ của cái sau.
    ///
    /// Đếm theo **độ chênh** chứ không theo số tuyệt đối: mọi pool trong cùng lần chạy test đều báo
    /// cáo vào chính cái sink này, nên con số tuyệt đối phụ thuộc vào test nào đang chạy song song.
    #[test]
    fn sink_thay_moi_job_va_dong_zone_ca_khi_job_panic()
    {
        assert!(install(&SINK), "không cắm được sink");
        assert!(!install(&SINK), "cắm lần hai mà vẫn nhận");

        let begun = SINK.begun.load(Ordering::Acquire);
        let ended = SINK.ended.load(Ordering::Acquire);

        job(0, || {});
        assert!(SINK.begun.load(Ordering::Acquire) > begun, "job chạy mà sink không thấy");
        assert!(SINK.ended.load(Ordering::Acquire) > ended, "job xong mà zone không đóng");

        let begun = SINK.begun.load(Ordering::Acquire);
        let ended = SINK.ended.load(Ordering::Acquire);

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = std::panic::catch_unwind(|| job(1, || panic!("job chết giữa zone")));
        std::panic::set_hook(previous);

        assert!(outcome.is_err());
        assert!(SINK.begun.load(Ordering::Acquire) > begun);
        assert!(SINK.ended.load(Ordering::Acquire) > ended, "zone mở ra mà không đóng lại khi job panic");
    }
}
