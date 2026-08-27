//! "Chạy n job xong rồi gọi tôi dậy." Nguyên liệu mà mọi điểm join trong crate này dựng lên từ đó.
//!
//! Ý tưởng thì đúng một câu: có một con số trên bảng. Ai làm xong phần mình thì trừ đi một. Người
//! trừ tới 0 có nhiệm vụ gọi người đang chờ dậy.
//!
//! # Khác gì [`Waker`](crate::utils::waker::Waker)
//!
//! `Waker` cũng là một cái latch, nhưng vé của nó ([`WakerSignal`](crate::utils::waker::WakerSignal))
//! mượn `&'a Waker`, nên nó không nhét vào một [`Job`](crate::custom_type::Job) được: job phải
//! `'static`. Cái vé ở đây mang một con trỏ thô thay cho tham chiếu, nên nó đi được vào job.
//!
//! Con trỏ thô ấy an toàn nhờ đúng một điều, và điều đó phải luôn đúng: **[`Latch::wait_in`] hoặc
//! [`Latch::wait`] không trả về cho tới khi mọi vé đã bị thả**. Latch nằm trên stack của người chờ,
//! và người chờ không rời khung stack đó chừng nào còn vé sống. Ai dựng latch mà không chờ hết vé
//! thì đang tạo ra một use-after-free, chứ không phải một cái bug nhỏ.

use crate::pool::ThreadPool;
use crate::sync::thread::{self, Thread};
use crate::sync::{AtomicUsize, Ordering};
use crate::utils::cache_padded::CachePadded;

/// Một bộ đếm ngược, gọi dậy đúng một người khi về 0.
pub struct Latch
{
    /// Job còn chưa báo về. 0 nghĩa là xong hết.
    remaining: CachePadded<AtomicUsize>,
    /// Thread cần gọi dậy, chụp lại lúc dựng latch.
    ///
    /// Người chờ thường không ngủ mà đi chạy job giúp pool ([`Self::wait_in`]), nên handle này chỉ
    /// dùng tới ở đường [`Self::wait`]: thread không thuộc pool nào thì không có việc để chạy giúp,
    /// ngủ hẳn còn hơn quay tại chỗ đốt một core.
    waiter:    Thread,
}

unsafe impl Send for Latch {}
unsafe impl Sync for Latch {}

/// Vé báo "một job đã xong", mang được vào trong một job.
///
/// Trừ bộ đếm khi bị thả, kể cả khi job panic, nên không có đường nào để một job biến mất mà không
/// báo về. Đó cũng là lý do nó dùng `Drop` chứ không phải một hàm `done()` phải nhớ gọi.
pub struct LatchTicket
{
    /// Con trỏ tới latch trên stack của người chờ. Xem phần đầu file về vì sao nó an toàn.
    latch:  *const Latch,
    /// Bản sao riêng của handle thread cần gọi dậy.
    ///
    /// Cố ý sao ở đây chứ không đọc từ latch lúc trừ về 0: ngay khi bộ đếm chạm 0, người chờ có
    /// quyền trả về và cái latch trên stack của nó biến mất. Đọc `latch.waiter` sau thời điểm đó là
    /// đọc một khung stack đã bị thu hồi.
    waiter: Thread,
}

unsafe impl Send for LatchTicket {}

impl Latch
{
    /// Latch chờ `count` lần báo về. `0` là hợp lệ và làm mọi lần chờ trả về ngay.
    ///
    /// Phải dựng trên chính thread sẽ chờ, vì đó là thread duy nhất mà vé biết cách gọi dậy.
    pub fn new(count: usize) -> Self
    {
        Self {
            remaining: CachePadded::new(AtomicUsize::new(count)),
            waiter:    thread::current(),
        }
    }

    /// Ghi thêm một job vào sổ và trả về vé của nó.
    ///
    /// Cộng **trước** khi job có cơ hội chạy. Cộng sau thì có một khoảnh khắc bộ đếm bằng 0 trong
    /// khi job vẫn đang bay, và người chờ đọc đúng lúc đó sẽ bỏ đi trong lúc job còn cầm tham chiếu
    /// tới stack của nó.
    #[inline]
    pub fn ticket(&self) -> LatchTicket
    {
        self.remaining.fetch_add(1, Ordering::Relaxed);
        LatchTicket {
            latch:  self as *const Latch,
            waiter: self.waiter.clone(),
        }
    }

    /// Số job còn chưa báo về. Ảnh chụp, dùng cho counter và log.
    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.remaining.load(Ordering::Acquire)
    }

    /// Đã xong hết chưa. Đây là thứ để đưa cho [`ThreadPool::run_until`].
    #[inline]
    pub fn is_done(&self) -> bool
    {
        self.remaining() == 0
    }

    /// Chờ bằng cách chạy job giúp pool, thay vì nằm không.
    ///
    /// Đây là đường mà mọi điểm join bên trong pool đi. Thread đang chờ vẫn là một người tham gia
    /// pool, nên nếu nó ngủ thì pool mất một core đúng lúc đang cần nhất, và với join lồng nhau thì
    /// còn tệ hơn: thread ngủ có thể chính là thread lẽ ra phải chạy cái job mà nó đang chờ.
    #[inline]
    pub fn wait_in(&self, pool: &ThreadPool)
    {
        pool.run_until(|| self.is_done());
    }

    /// Chờ bằng cách ngủ. Dành cho thread không thuộc pool nào.
    ///
    /// # Panics
    ///
    /// Nếu gọi từ thread khác với thread đã dựng latch. Đó là thread duy nhất mà vé biết cách gọi
    /// dậy, nên chờ ở chỗ khác là park một thread không ai đánh thức. Kiểm cả trong bản release:
    /// đổi một tiến trình đứng im lấy một stack trace là món hời.
    pub fn wait(&self)
    {
        assert_eq!(
            thread::current().id(),
            self.waiter.id(),
            "Latch::wait phải chạy trên chính thread đã dựng latch, đó là thread duy nhất mà vé biết cách gọi dậy"
        );

        // `park` được phép trả về vu vơ, và một `unpark` tới sớm cũng làm nó trả về ngay. Cả hai
        // đều nói rằng chỉ có bộ đếm mới đáng tin, nên đọc lại nó mỗi vòng.
        while !self.is_done()
        {
            thread::park();
        }
    }
}

impl Drop for LatchTicket
{
    #[inline]
    fn drop(&mut self)
    {
        // Safety: latch còn sống chừng nào còn vé, xem phần đầu file.
        let latch = unsafe { &*self.latch };
        let previous = latch.remaining.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "latch bị trừ nhiều hơn số vé đã phát");

        if previous == 1
        {
            // Từ đây trở đi cái latch có thể đã biến mất: người chờ được phép trả về ngay khi nó
            // thấy số 0 vừa được ghi. `self.waiter` là bản sao của chính vé này nên vẫn sống.
            // Không được chạm vào `self.latch` sau dòng trên nữa.
            self.waiter.unpark();
        }
    }
}

impl std::fmt::Debug for Latch
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("Latch").field("remaining", &self.remaining()).finish()
    }
}

#[cfg(all(test, loom))]
mod loom_test
{
    use loom::sync::Arc;
    use loom::thread;

    use super::Latch;

    /// Mọi interleaving của hai job và một người chờ đều phải kết thúc bằng việc `wait` trả về.
    ///
    /// Đây là test bắt được lỗi mất wake-up nếu vé và người chờ không thống nhất được ai chịu trách
    /// nhiệm đọc lại bộ đếm. Nó cũng là chỗ dựa cho `Scope`, vốn chỉ là latch này cộng một cách phát
    /// closure ra pool.
    #[test]
    fn cho_luon_ket_thuc_du_thu_tu_the_nao()
    {
        loom::model(|| {
            let latch = Arc::new(Latch::new(0));

            let tickets: Vec<_> = (0..2).map(|_| latch.ticket()).collect();
            let handles: Vec<_> = tickets
                .into_iter()
                .map(|ticket| {
                    thread::spawn(move || {
                        drop(ticket);
                    })
                })
                .collect();

            latch.wait();
            assert_eq!(latch.remaining(), 0);

            for handle in handles
            {
                handle.join().unwrap();
            }
        });
    }

    /// Vé cuối cùng phải gọi được người chờ dậy kể cả khi nó thả **trước** lúc người kia kịp ngủ.
    #[test]
    fn ve_tha_truoc_khi_nguoi_cho_kip_ngu_thi_khong_mat()
    {
        loom::model(|| {
            let latch = Arc::new(Latch::new(0));
            let ticket = latch.ticket();

            let worker = thread::spawn(move || {
                drop(ticket);
            });

            // Nhường lượt trước khi chờ, để loom thử cả cảnh vé về đích trước lẫn cảnh nó về sau.
            thread::yield_now();
            latch.wait();

            worker.join().unwrap();
            assert!(latch.is_done());
        });
    }
}

#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::pool::Config;

    #[test]
    fn cho_toi_khi_moi_ve_deu_da_duoc_tha()
    {
        const JOBS: usize = 1_000;

        let pool = ThreadPool::new(Config {
            threads: 3,
            ..Config::default()
        });
        let latch = Latch::new(0);
        let done = Arc::new(AtomicUsize::new(0));

        for _ in 0..JOBS
        {
            let ticket = latch.ticket();
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Relaxed);
                drop(ticket);
            });
        }

        latch.wait_in(&pool);
        assert_eq!(latch.remaining(), 0);
        assert_eq!(done.load(Ordering::Acquire), JOBS);
    }

    #[test]
    fn ve_van_bao_ve_khi_job_panic()
    {
        let pool = ThreadPool::new(Config {
            threads: 2,
            ..Config::default()
        });
        let latch = Latch::new(0);

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        for _ in 0..16
        {
            let ticket = latch.ticket();
            pool.spawn(move || {
                let _ticket = ticket;
                panic!("job này chết giữa chừng");
            });
        }

        latch.wait_in(&pool);
        std::panic::set_hook(previous);
        assert_eq!(latch.remaining(), 0, "một job panic đã mang cái vé của nó xuống mồ");
    }

    #[test]
    fn latch_rong_thi_khong_cho_gi_ca()
    {
        let latch = Latch::new(0);
        assert!(latch.is_done());
        latch.wait();
    }

    #[test]
    fn thread_ngoai_pool_thi_ngu_cho()
    {
        let pool = ThreadPool::new(Config {
            threads: 2,
            ..Config::default()
        });
        let latch = Latch::new(0);
        let done = Arc::new(AtomicUsize::new(0));

        for _ in 0..64
        {
            let ticket = latch.ticket();
            let done = Arc::clone(&done);
            pool.spawn(move || {
                done.fetch_add(1, Ordering::Relaxed);
                drop(ticket);
            });
        }

        // Không chạy giúp ai cả, chỉ nằm chờ. Vé cuối cùng phải gọi được thread này dậy.
        latch.wait();
        assert_eq!(done.load(Ordering::Acquire), 64);
    }

    #[test]
    fn cho_nham_thread_thi_panic()
    {
        let latch = Arc::new(Latch::new(1));
        let foreign = Arc::clone(&latch);
        let outcome = std::thread::spawn(move || foreign.wait()).join();

        assert!(outcome.is_err(), "chờ trên thread lạ mà vẫn trả về");
        drop(latch.ticket());
        // Bù lại cái vé vừa thả, để latch về đúng 0 rồi mới bỏ đi.
        assert_eq!(latch.remaining(), 1);
    }
}
