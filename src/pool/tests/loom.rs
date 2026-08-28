//! Loom cho giao thức ngủ.
//!
//! Đây là chỗ duy nhất trong pool mà một lỗi không lộ ra thành crash hay kết quả sai: nó lộ ra
//! thành một tiến trình đứng im. Test thường không bắt được, vì cái interleaving làm mất lời đánh
//! thức có thể chỉ xảy ra một lần trong hàng triệu lần chạy. Loom thì chạy **mọi** interleaving của
//! mô hình, nên nếu có một đường dẫn tới "job còn đó mà worker vẫn ngủ", nó tìm ra.
//!
//! Nó đã tìm ra một lần rồi: bản đầu của [`Sleep::notify`] đọc ô `state` bằng một `load` thường,
//! và loom dựng lại đúng cảnh worker đọc phải hàng đợi cũ trong khi người đẩy job đọc phải "chưa ai
//! ngủ". Cả hai phía giờ đều RMW cùng một ô, và ba test dưới đây là thứ giữ cho nó không quay lại.
//!
//! # Vì sao mô hình nhỏ đến thế
//!
//! Mỗi lời gọi atomic là một nhánh mới, nên số interleaving lớn theo hàm mũ. Dựng cả `ThreadPool`
//! dưới loom thì nó chạy tới sáng cũng không xong, và bản thân vòng lặp worker cũng phải có **trần
//! cứng** chứ không được lặp tự do vì cùng lý do đó. Phần đáng ngờ thì vẫn nằm trọn trong `Sleep`.

use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;

use crate::pool::sleep::{Sleep, Wake};

/// Hàng đợi job rút gọn còn đúng cái mà giao thức ngủ nhìn thấy: có việc, hay không có việc.
struct Work
{
    pending: AtomicUsize,
    taken:   AtomicUsize,
}

impl Work
{
    fn new() -> Self
    {
        Self {
            pending: AtomicUsize::new(0),
            taken:   AtomicUsize::new(0),
        }
    }

    fn push(&self)
    {
        self.pending.fetch_add(1, Ordering::AcqRel);
    }

    fn has_work(&self) -> bool
    {
        self.pending.load(Ordering::Acquire) > 0
    }

    /// Lấy một job nếu còn. Là một RMW, y như mọi cách lấy job thật.
    fn take(&self) -> bool
    {
        let mut pending = self.pending.load(Ordering::Acquire);
        loop
        {
            if pending == 0
            {
                return false;
            }
            match self.pending.compare_exchange_weak(pending, pending - 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) =>
                {
                    self.taken.fetch_add(1, Ordering::AcqRel);
                    return true;
                }
                Err(actual) => pending = actual,
            }
        }
    }
}

/// Số lượt ngủ tối đa một worker mô hình được phép, để không gian trạng thái còn hữu hạn.
///
/// Mọi kịch bản ở đây chỉ cần một hoặc hai lượt. Chạm trần nghĩa là worker cứ ngủ rồi dậy mà không
/// tiến được, và đó cũng là một cái sai đáng biết, nên nó là `panic` chứ không phải `break`.
const MAX_PARKS: usize = 3;

/// Vòng lặp worker rút gọn: tìm việc, không có thì ngủ, tỉnh dậy thì tìm lại. Trả về đã lấy được
/// job hay chưa.
///
/// Không có phần spin đệm nào ở đây: bỏ hết đi thì mọi lần "không thấy việc" đều dẫn thẳng tới
/// `park`, tức là loom luôn khám phá đúng cái nhánh đáng ngờ thay vì đi vòng qua nó.
fn worker(sleep: &Sleep, work: &Arc<Work>, index: usize) -> bool
{
    let mut is_searching = false;
    let mut parks = 0;

    loop
    {
        // Đọc bộ đếm **trước** khi tìm, y như worker thật.
        let seen = sleep.events();

        if work.take()
        {
            if is_searching && sleep.end_searching()
            {
                sleep.notify();
            }
            return true;
        }

        if !is_searching
        {
            is_searching = sleep.try_start_searching();
            if is_searching
            {
                continue;
            }
        }

        assert!(parks < MAX_PARKS, "worker {index} ngủ {parks} lượt mà không tiến được bước nào");
        parks += 1;

        let work_for_recheck = Arc::clone(work);
        match sleep.park(index, is_searching, seen, move || work_for_recheck.has_work())
        {
            Wake::Notified => is_searching = true,
            Wake::Cancelled => is_searching = false,
        }
    }
}

/// Một người đẩy việc, một worker. Job phải tới tay worker trong mọi interleaving.
///
/// Đây là hình dạng nhỏ nhất của lỗi mất wake-up: worker quyết định ngủ dựa trên thứ nó thấy trước
/// đó, còn job thì tới ngay sau cái nhìn ấy. Cũng chính là mô hình đã bắt được lỗi thật.
#[test]
fn mot_job_khong_bao_gio_ngu_quen_tren_mot_worker()
{
    loom::model(|| {
        let sleep = Arc::new(Sleep::new(1));
        let work = Arc::new(Work::new());

        let worker_sleep = Arc::clone(&sleep);
        let worker_work = Arc::clone(&work);
        let handle = thread::spawn(move || {
            assert!(worker(&worker_sleep, &worker_work, 0), "worker về tay không dù có đúng một job");
        });

        work.push();
        sleep.notify();

        handle.join().unwrap();
        assert_eq!(work.taken.load(Ordering::Acquire), 1);
    });
}

/// Hai job, hai worker, mỗi worker lấy đúng một cái. Không ai được phép ngủ quên.
///
/// Hai job cho hai worker nên ai cũng có phần: worker nào ngủ mà không dậy thì đó là lỗi thật, chứ
/// không phải chuyện "hết việc rồi thì ngủ là đúng".
#[test]
fn hai_job_hai_worker_khong_ai_ngu_quen()
{
    loom::model(|| {
        let sleep = Arc::new(Sleep::new(2));
        let work = Arc::new(Work::new());

        for _ in 0..2
        {
            work.push();
            sleep.notify();
        }

        let handles: Vec<_> = (0..2)
            .map(|index| {
                let sleep = Arc::clone(&sleep);
                let work = Arc::clone(&work);
                thread::spawn(move || {
                    assert!(worker(&sleep, &work, index), "worker {index} về tay không dù còn job");
                })
            })
            .collect();

        for handle in handles
        {
            handle.join().unwrap();
        }
        assert_eq!(work.taken.load(Ordering::Acquire), 2);
    });
}

/// Job tới trong lúc worker đang trên đường đi ngủ.
///
/// Khác test đầu ở chỗ worker được thả ra trước, nên người đẩy job rơi vào đủ mọi vị trí trong
/// trình tự "ghi tên, rời hàng ngũ, ngó lại, ngủ".
#[test]
fn job_toi_giua_luc_worker_dang_di_ngu()
{
    loom::model(|| {
        let sleep = Arc::new(Sleep::new(1));
        let work = Arc::new(Work::new());

        let worker_sleep = Arc::clone(&sleep);
        let worker_work = Arc::clone(&work);
        let handle = thread::spawn(move || {
            assert!(worker(&worker_sleep, &worker_work, 0));
        });

        thread::yield_now();
        work.push();
        sleep.notify();

        handle.join().unwrap();
        assert_eq!(work.taken.load(Ordering::Acquire), 1);
    });
}

/// `notify_all` phải gọi được mọi người đang ngủ ra khỏi `park`, kể cả người vừa mới ghi tên vào
/// danh sách.
///
/// Đây là đường mà shutdown đi: worker nào ngủ quên ở đây thì `join` của nó không bao giờ về. Ra
/// bằng đường nào thì không quan trọng, và cả hai đường đều hợp lệ: `Notified` là bị bốc khỏi danh
/// sách rồi `unpark`, còn `Cancelled` là thấy bộ đếm sự kiện đã nhích nên tự rút tên ra trước khi
/// kịp ngủ. Cái sau còn rẻ hơn, vì không tốn một lần park rồi dậy ngay.
#[test]
fn notify_all_khong_bo_sot_ai()
{
    loom::model(|| {
        let sleep = Arc::new(Sleep::new(1));
        let woken = Arc::new(AtomicUsize::new(0));

        let worker_sleep = Arc::clone(&sleep);
        let worker_woken = Arc::clone(&woken);
        let handle = thread::spawn(move || {
            let seen = worker_sleep.events();
            worker_sleep.park(0, false, seen, || false);
            worker_woken.fetch_add(1, Ordering::AcqRel);
        });

        // Lặp cho tới khi thật sự gọi được người đó dậy: `notify_all` chỉ với tới ai đã ghi tên vào
        // danh sách, nên người gọi shutdown ngoài đời cũng dựng cờ trước rồi mới gọi.
        while woken.load(Ordering::Acquire) == 0
        {
            sleep.notify_all();
            thread::yield_now();
        }

        handle.join().unwrap();
    });
}

/// `notify_many` gọi cả một đợt dậy, không chỉ một người.
///
/// Hai job đẩy ra cùng một lúc, hai worker đang trên đường đi ngủ. Với [`Sleep::notify`] thì người
/// thứ hai phải chờ người thứ nhất tỉnh rồi gọi hộ, và cái dây chuyền đó là thứ `notify_many` sinh
/// ra để cắt. Nhưng nó chạm cùng ô `state` như mọi phía khác, nên thứ đáng kiểm ở đây không phải là
/// nó nhanh hơn: là nó không mở lại kẽ hở mất wake-up mà `notify` đã bịt.
#[test]
fn notify_many_khong_bo_sot_ai()
{
    // Hai worker cùng ngủ rồi cùng được gọi dậy là mô hình lớn nhất trong file này, và để loom chạy
    // hết mọi interleaving của nó thì phải tính bằng giờ. Chặn số lần cắt ngang lại là cách loom
    // khuyến nghị cho đúng cảnh này: lỗi đồng bộ thật gần như luôn hiện ra trong vài lần cắt đầu
    // tiên, còn phần đuôi chỉ là những hoán vị dài mà không thêm hình dạng mới nào.
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(|| {
        let sleep = Arc::new(Sleep::new(2));
        let work = Arc::new(Work::new());

        let handles: Vec<_> = (0..2)
            .map(|index| {
                let sleep = Arc::clone(&sleep);
                let work = Arc::clone(&work);
                thread::spawn(move || {
                    assert!(worker(&sleep, &work, index), "worker {index} về tay không dù còn job");
                })
            })
            .collect();

        // Đẩy cả đợt rồi mới gọi, đúng thứ tự của `Scope::parallel_for`: job phải có mặt trước, nếu
        // không thì người được gọi dậy sẽ tìm hụt rồi ngủ lại.
        work.push();
        work.push();
        sleep.notify_many(2);

        for handle in handles
        {
            handle.join().unwrap();
        }
        assert_eq!(work.taken.load(Ordering::Acquire), 2);
    });
}
