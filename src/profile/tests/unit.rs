use crate::profile::registry::{install, job};
use crate::profile::sink::Sink;

use std::sync::atomic::{AtomicUsize, Ordering};

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
fn t0_sink_thay_moi_job_va_dong_zone_ca_khi_job_panic()
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
