//! Job chờ job khác, để một frame là một đồ thị chứ không phải một chuỗi barrier.

use std::marker::PhantomData;

use crate::custom_type::Job;
use crate::pool::Shared;
use crate::scope::Scope;
use crate::sync::{Arc, AtomicUsize, Mutex, Ordering};
use crate::utils::ignore_poison;

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
    node:   Arc<Node>,
    /// Buộc handle vào scope của nó, để nó không bị giữ qua khỏi cái join vốn là thứ bảo đảm tham
    /// chiếu mà job bắt được vẫn còn sống.
    marker: PhantomData<&'scope ()>,
}

/// Một nút của đồ thị: phần việc, thứ nó còn đang chờ, và thứ đang chờ nó.
struct Node
{
    /// Số phụ thuộc chưa xong, cộng một cho cái chốt "đang còn dựng", xem [`Scope::spawn_after`].
    /// Ai kéo con số này về 0 thì người đó đẩy job đi.
    pending:    AtomicUsize,
    /// Lấy ra đúng một lần, bởi thread chạy nút này.
    work:       Mutex<Option<Job>>,
    /// Những nút cần được thả ra khi nút này xong.
    ///
    /// `None` nghĩa là nút này đã xong rồi, và đó chính là trạng thái mà một kẻ phụ thuộc tới muộn
    /// phải nhìn thấy được, xem [`Scope::spawn_after`].
    successors: Mutex<Option<Vec<Arc<Node>>>>,
    /// Phần dùng chung của pool, **không** kèm join handle: một nút sống lâu hơn tay cầm cuối cùng
    /// của pool thì cũng không được phép giữ pool sống, và càng không được phép làm worker đang
    /// chạy nó đi join chính nó. Xem [`crate::scope::Scope`].
    shared:     Arc<Shared>,
}

impl<'scope> Scope<'scope>
{
    /// Xếp `f` vào pool và trả về một handle để job khác bám vào.
    ///
    /// Giống hệt [`Scope::spawn`] về thời điểm chạy, tức là chạy ngay, không chờ gì, chỉ khác ở chỗ
    /// nó đưa lại cho bạn một thứ để treo việc sau lên.
    pub fn spawn_with_handle<F>(&self, f: F) -> JobHandle<'scope>
    where F: FnOnce() + Send + 'scope
    {
        self.spawn_after(&[], f)
    }

    /// Xếp `f` để chạy sau khi mọi job trong `deps` đã xong.
    ///
    /// Trả về ngay, đây không phải một điểm join. Không có gì bị chặn cho tới khi
    /// [`ThreadPool::scope`] bao ngoài kết thúc, và đó chính là ý: thread đang mô tả đồ thị thì đi
    /// mô tả tiếp, hoặc đi chạy job, chứ không ngồi lên một cái barrier.
    ///
    /// Phụ thuộc nào **đã** xong rồi thì không phải chờ. `deps` rỗng là hợp lệ và đúng bằng
    /// [`Self::spawn_with_handle`].
    ///
    /// # Không dựng được chu trình
    ///
    /// `deps` là các handle, mà handle chỉ tồn tại sau khi job của nó đã được tạo, nên mọi cạnh đều
    /// chỉ ngược về quá khứ. Không có cách nào gọi tên một job chưa tồn tại, nên cũng không có cách
    /// nào viết ra một chu trình. Điều đó quan trọng, vì một chu trình ở đây là một scope không bao
    /// giờ về 0, tức là một tiến trình không bao giờ ra khỏi điểm join.
    pub fn spawn_after<F>(&self, deps: &[&JobHandle<'scope>], f: F) -> JobHandle<'scope>
    where F: FnOnce() + Send + 'scope
    {
        let node = Arc::new(Node {
            // Bắt đầu từ một, cho cái chốt được thả ở cuối hàm này. Không có nó thì một phụ thuộc
            // vừa xong ở giữa hai lời `register` có thể kéo bộ đếm về 0 và đẩy job đi trong khi
            // thread này vẫn đang nối nốt các cạnh còn lại.
            pending:    AtomicUsize::new(1),
            work:       Mutex::new(Some(self.job(f))),
            successors: Mutex::new(Some(Vec::new())),
            shared:     Arc::clone(self.shared()),
        });

        for dep in deps
        {
            dep.node.register(&node);
        }

        // Thả cái chốt chính là thứ đẩy đi một nút không còn phụ thuộc nào, nên `deps` rỗng thì job
        // chạy ngay.
        Node::release(&node);

        JobHandle {
            node:   node,
            marker: PhantomData,
        }
    }
}

impl Node
{
    /// Thêm `dependent` vào danh sách kế nhiệm của nút này, hoặc không làm gì nếu nút này đã xong.
    ///
    /// Bộ đếm được cộng **dưới cùng cái khoá** mà phía kết thúc phải lấy để phát danh sách ra, và đó
    /// là thứ phân biệt được "đã xong" với "sắp xong". Cộng ở ngoài khoá thì còn một khe: nút này
    /// đọc ra là chưa xong, một nhịp sau nó phát danh sách kế nhiệm mà trong đó không có
    /// `dependent`, và thế là một job không bao giờ có ai thả nó ra.
    fn register(&self, dependent: &Arc<Self>)
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
    fn release(node: &Arc<Self>)
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
    fn run(node: Arc<Self>)
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

impl std::fmt::Debug for JobHandle<'_>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("JobHandle")
            .field("pending", &self.node.pending.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::pool::{Config, ThreadPool};

    /// Miri diễn giải từng lệnh một, nên số vòng lặp ở đây phải nhỏ hẳn khi chạy dưới nó.
    const fn rounds() -> usize
    {
        match cfg!(miri)
        {
            true => 2,
            false => 100,
        }
    }

    fn pool_with(threads: usize) -> ThreadPool
    {
        ThreadPool::new(Config {
            threads: threads,
            ring_capacity: 16,
            thread_name: "graph-test".to_string(),
            ..Config::default()
        })
    }

    #[test]
    fn ke_phu_thuoc_chay_sau_thu_no_phu_thuoc()
    {
        for threads in [0usize, 1, 3]
        {
            let pool = pool_with(threads);
            let ticket = AtomicUsize::new(0);

            for round in 0..rounds()
            {
                let first = AtomicUsize::new(usize::MAX);
                let second = AtomicUsize::new(usize::MAX);

                pool.scope(|s| {
                    let a = s.spawn_with_handle(|| {
                        first.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                    });
                    s.spawn_after(&[&a], || {
                        second.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                    });
                });

                assert!(
                    first.load(Ordering::SeqCst) < second.load(Ordering::SeqCst),
                    "{threads} thread, vòng {round}: kẻ phụ thuộc chạy trước"
                );
            }
        }
    }

    #[test]
    fn cho_du_moi_phu_thuoc_chu_khong_phai_cai_dau_tien()
    {
        let pool = pool_with(4);

        for _ in 0..rounds()
        {
            let done = AtomicUsize::new(0);
            let seen_by_last = AtomicUsize::new(usize::MAX);

            pool.scope(|s| {
                let deps: Vec<_> = (0..8)
                    .map(|_| {
                        s.spawn_with_handle(|| {
                            done.fetch_add(1, Ordering::SeqCst);
                        })
                    })
                    .collect();

                let borrowed: Vec<&_> = deps.iter().collect();
                s.spawn_after(&borrowed, || {
                    seen_by_last.store(done.load(Ordering::SeqCst), Ordering::SeqCst);
                });
            });

            assert_eq!(seen_by_last.load(Ordering::SeqCst), 8, "job cuối chạy khi chưa đủ 8 phụ thuộc");
        }
    }

    #[test]
    fn phu_thuoc_da_xong_thi_khong_phai_cho()
    {
        let pool = pool_with(2);
        let done = AtomicUsize::new(0);

        pool.scope(|s| {
            let first = s.spawn_with_handle(|| {
                done.fetch_add(1, Ordering::SeqCst);
            });

            // Cố tình để phụ thuộc kịp xong hẳn trước khi có ai bám vào nó.
            std::thread::sleep(std::time::Duration::from_millis(20));

            s.spawn_after(&[&first], || {
                done.fetch_add(1, Ordering::SeqCst);
            });
        });

        assert_eq!(done.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn mot_chuoi_dai_van_giu_dung_thu_tu()
    {
        let length = match cfg!(miri)
        {
            true => 4,
            false => 64,
        };

        let pool = pool_with(4);
        let order: Vec<AtomicUsize> = (0..length).map(|_| AtomicUsize::new(usize::MAX)).collect();
        let ticket = AtomicUsize::new(0);

        pool.scope(|s| {
            let mut previous = s.spawn_with_handle(|| {
                order[0].store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
            });

            for step in 1..length
            {
                let order = &order;
                let ticket = &ticket;
                previous = s.spawn_after(&[&previous], move || {
                    order[step].store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                });
            }
        });

        let stamps: Vec<usize> = order.iter().map(|slot| slot.load(Ordering::SeqCst)).collect();
        assert_eq!(stamps, (0..length).collect::<Vec<_>>(), "chuỗi phụ thuộc chạy sai thứ tự");
    }

    #[test]
    fn hinh_kim_cuong_chay_dung()
    {
        let pool = pool_with(4);

        for _ in 0..rounds()
        {
            let ticket = AtomicUsize::new(0);
            let root = AtomicUsize::new(usize::MAX);
            let left = AtomicUsize::new(usize::MAX);
            let right = AtomicUsize::new(usize::MAX);
            let leaf = AtomicUsize::new(usize::MAX);

            pool.scope(|s| {
                let a = s.spawn_with_handle(|| {
                    root.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                });
                let b = s.spawn_after(&[&a], || {
                    left.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                });
                let c = s.spawn_after(&[&a], || {
                    right.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                });
                s.spawn_after(&[&b, &c], || {
                    leaf.store(ticket.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
                });
            });

            let (a, b, c, d) = (
                root.load(Ordering::SeqCst),
                left.load(Ordering::SeqCst),
                right.load(Ordering::SeqCst),
                leaf.load(Ordering::SeqCst),
            );
            assert!(a < b && a < c, "hai nhánh giữa chạy trước gốc");
            assert!(b < d && c < d, "nút cuối chạy trước một nhánh giữa");
        }
    }

    /// Thả tay cầm cuối cùng của pool ngay sau khi scope trả về.
    ///
    /// Đây là kịch bản mà miri bắt được với đúng bốn chữ "trying to join itself". Trình tự: job của
    /// một nút chạy xong và thả cái vé latch của nó, scope thấy bộ đếm về 0 nên trả về, thread gọi
    /// thả nốt tay cầm pool, và worker thì vẫn đang ở trong `Node::run` thêm một nhịp nữa để dọn
    /// danh sách kế nhiệm. Nếu nút giữ một `ThreadPool` thay vì phần dùng chung của nó, cái `Arc`
    /// mà worker thả ở nhịp ấy là tay cầm cuối cùng, và `Drop` của nó đi join chính worker đang
    /// chạy dòng đó.
    #[test]
    fn tha_pool_ngay_sau_scope_khong_lam_worker_join_chinh_no()
    {
        for _ in 0..rounds()
        {
            let pool = pool_with(2);
            let done = AtomicUsize::new(0);

            pool.scope(|s| {
                let first = s.spawn_with_handle(|| {});
                let second = s.spawn_after(&[&first], || {});
                s.spawn_after(&[&second], || {
                    done.fetch_add(1, Ordering::SeqCst);
                });
            });

            assert_eq!(done.load(Ordering::SeqCst), 1);
            drop(pool);
        }
    }

    #[test]
    fn phu_thuoc_panic_thi_ke_ke_nhiem_van_duoc_tha_ra()
    {
        let pool = pool_with(2);
        let ran = AtomicUsize::new(0);

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pool.scope(|s| {
                let broken = s.spawn_with_handle(|| panic!("phụ thuộc chết"));
                s.spawn_after(&[&broken], || {
                    ran.fetch_add(1, Ordering::SeqCst);
                });
            });
        }));

        std::panic::set_hook(previous);
        assert!(outcome.is_err(), "panic của phụ thuộc không tới được điểm join");
        assert_eq!(ran.load(Ordering::SeqCst), 1, "kẻ kế nhiệm bị bỏ rơi, scope này lẽ ra đã treo");
    }
}
