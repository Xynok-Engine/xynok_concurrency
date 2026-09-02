//! Vòng chạy của một thread worker, và hai cách gọi một job.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::custom_type::Job;
use crate::sync::{Arc, Ordering};
use crate::utils::backoff::Backoff;

use super::context::{CONTEXT, Context};
use super::shared::Shared;
use super::sleep::Wake;

#[cfg(doc)] use super::ThreadPool;

/// Chạy một job, giữ panic lại thay vì để nó giết thread worker.
///
/// Payload bị thả ở đây có chủ đích. Job nào có đường báo lỗi về (mọi job đi qua `scope`) thì đã tự
/// bắt panic và sẽ dựng lại nó ở điểm join. Job được giao thẳng qua [`ThreadPool::spawn`] thì không
/// có điểm join nào, tức là không có ai để trao payload, và panic hook thì đã in message với
/// backtrace từ trước khi tới đây.
pub(super) fn run_job(job: Job)
{
    let _ = catch_unwind(AssertUnwindSafe(move || job.run_once()));
}

/// Chạy một job với `in_loop` bật, để job con của nó biết đường vào ô LIFO.
pub(super) fn run_in_loop(shared: &Shared, index: usize, job: Job)
{
    let previous = CONTEXT.get();
    CONTEXT.set(Context {
        pool:    shared.id,
        index:   index,
        in_loop: true,
    });

    shared.counters.of(index).job_run();
    crate::profile::job(index, || run_job(job));

    CONTEXT.set(previous);
}

pub(super) fn worker_loop(shared: Arc<Shared>, index: usize)
{
    CONTEXT.set(Context {
        pool:    shared.id,
        index:   index,
        in_loop: false,
    });

    // Từ chính worker chứ không phải từ thread đã spawn nó: API của mọi nền tảng ở đây đều đặt
    // priority cho thread **đang gọi**, không nền nào nhận một thread khác làm đối tượng.
    shared.priority.apply_to_current_thread();

    let mut tick = 0u32;
    let mut is_searching = false;
    let mut backoff = Backoff::new();
    // Đếm riêng, không dùng bậc của `backoff`: bậc đó bão hoà, còn cái này thì không. Xem
    // [`Config::spin_rounds`].
    let mut idle_rounds = 0u32;

    loop
    {
        tick = tick.wrapping_add(1);

        // Đọc bộ đếm sự kiện **trước** khi đi tìm. Job nào xuất hiện sau lời gọi này đều làm nó
        // đổi, nên lát nữa nếu tìm không ra gì mà bộ đếm vẫn thế thì chắc chắn không có job nào lọt
        // qua dưới mũi mình. Xem `sleep::Sleep`.
        let seen = shared.sleep.events();

        if let Some(job) = shared.next_local_job(index, tick)
        {
            if is_searching
            {
                is_searching = false;
                // Người lùng cuối cùng vừa vớ được việc thì chỗ nó lấy có thể còn nữa, mà từ giờ
                // không còn ai đi tìm. Gọi thêm một người dậy.
                if shared.sleep.end_searching()
                {
                    shared.sleep.notify();
                }
            }
            backoff.reset();
            idle_rounds = 0;
            run_in_loop(&shared, index, job);
            continue;
        }

        // Kiểm sau khi ngó hàng đợi, không phải trước, để shutdown vét nốt phần còn lại thay vì bỏ
        // chúng lại.
        if shared.shutdown.load(Ordering::Acquire)
        {
            break;
        }

        if !is_searching
        {
            is_searching = shared.sleep.try_start_searching();
        }

        if is_searching && let Some(job) = shared.search_job(index)
        {
            is_searching = false;
            if shared.sleep.end_searching()
            {
                shared.sleep.notify();
            }
            backoff.reset();
            idle_rounds = 0;
            run_in_loop(&shared, index, job);
            continue;
        }

        idle_rounds = idle_rounds.saturating_add(1);
        if idle_rounds < shared.spin_rounds
        {
            backoff.snooze();
            continue;
        }

        if shared.shutdown.load(Ordering::Acquire)
        {
            break;
        }

        shared.counters.of(index).park();
        if let Some(sink) = crate::profile::sink()
        {
            sink.worker_park(index);
        }

        let wake = shared.sleep.park(index, is_searching, seen, || shared.has_work());

        if let Some(sink) = crate::profile::sink()
        {
            sink.worker_unpark(index);
        }

        match wake
        {
            // Người đánh thức đã tính mình vào `searching` rồi, nên đừng xin thêm một lần nữa.
            Wake::Notified => is_searching = true,
            Wake::Cancelled => is_searching = false,
        }
        backoff.reset();
        idle_rounds = 0;
    }

    // Không ai trộm được ô LIFO, nên thread này không được mang nó xuống mồ.
    shared.flush_lifo(index);
    CONTEXT.set(Context::NONE);
}
