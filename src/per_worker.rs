//! Mỗi thread tham gia một ô riêng, để không phải chia sẻ thứ gì mới ghi được.

use crate::pool::ThreadPool;
use crate::sync::cell::UnsafeCell;
use crate::sync::{AtomicBool, Ordering};
use crate::utils::cache_padded::CachePadded;

/// Một mảng có đúng một ô cho mỗi [`ThreadPool::worker_index`], ghi được mà không cần đồng bộ gì.
///
/// # Vì sao chỗ nào dùng thật cũng cần tới nó
///
/// Một `parallel_for` chỉ đọc thì dễ. Mọi chỗ dùng thật đều **ghi**, và không chỗ nào chia sẻ được
/// cái đích ghi:
///
/// | nơi dùng | ghi cái gì | vì sao không chia sẻ được |
/// | --- | --- | --- |
/// | ECS | command buffer: tạo, huỷ, thêm component | một buffer dùng chung là điểm tranh chấp giữa mọi worker |
/// | Hạt | hạt vừa sinh ra | y như trên, với tần suất cao hơn nhiều |
/// | Render | command buffer đồ hoạ | Vulkan và D3D12 đòi một command pool thuộc về đúng một thread |
/// | Profiler | số lần trộm trúng, thời gian rảnh | đây chính là lý do [`CachePadded`] tồn tại |
///
/// Nên câu trả lời không phải là một cái khoá, mà là không chia sẻ ngay từ đầu. Mỗi thread ghi vào ô
/// của mình, mỗi ô nằm trên một cache line riêng, và kết quả được gộp lại sau.
///
/// # Gộp kết quả là chỗ quyết định tính lặp lại được
///
/// Work-stealing làm **thứ tự** các thread kết thúc trở nên không đoán trước, chuyện đó vô hại với
/// rendering và chí mạng với replay, netcode lockstep, hay việc dựng lại một con bug.
/// [`Self::iter_mut`] đi theo chỉ số ô chứ không theo thứ tự ai xong trước, nên một phép gộp dựng
/// trên nó cho ra cùng một đáp án ở mọi lần chạy. Gộp theo thứ tự kết quả bay về là đúng cái sai mà
/// kiểu dữ liệu này sinh ra để chặn.
pub struct PerWorker<T>
{
    slots: Box<[CachePadded<Slot<T>>]>,
}

/// Ô của một thread, cộng cái cờ bắt lỗi mượn hai lần.
struct Slot<T>
{
    /// Chủ của ô này có đang ở trong [`PerWorker::with`] hay không.
    ///
    /// Chỉ đúng một thread ghi nó, nên hai lần chạm ở đây không cần gộp thành một thao tác atomic.
    /// Nó là [`AtomicBool`] là vì **người đọc**: [`PerWorker::for_each_unchecked`] soi cờ của mọi ô
    /// từ thread host, mà đọc một `Cell` do thread khác ghi là data race bất kể giá trị đọc ra là
    /// gì.
    borrowed: AtomicBool,
    value:    UnsafeCell<T>,
}

/// Xoá cờ mượn của một ô, kể cả khi closure nó canh đang unwind.
struct BorrowGuard<'a>(&'a AtomicBool);

impl Drop for BorrowGuard<'_>
{
    #[inline]
    fn drop(&mut self)
    {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Chia sẻ một `PerWorker` qua nhiều thread thì mỗi thread chỉ với tới ô của chính nó, tức là `T`
/// **di chuyển** giữa các thread chứ không bị dùng chung. Đó cũng là điều kiện của một channel, và
/// là lý do ở đây không đòi `T: Sync`.
unsafe impl<T: Send> Sync for PerWorker<T> {}
unsafe impl<T: Send> Send for PerWorker<T> {}

impl<T> PerWorker<T>
{
    /// Một ô cho mỗi người tham gia `pool`.
    ///
    /// Đi cùng với [`Self::with_mut`], và phải truyền lại đúng cái pool đó: đánh chỉ số mảng của
    /// pool này bằng cách đánh số của pool khác là hai bộ số khác nhau cho hai nhóm thread khác
    /// nhau.
    pub fn for_pool(pool: &ThreadPool, init: impl FnMut(usize) -> T) -> Self
    {
        Self::with_len(pool.worker_count(), init)
    }

    /// Một ô cho mỗi chỉ số trong `0..len`.
    pub fn with_len(len: usize, mut init: impl FnMut(usize) -> T) -> Self
    {
        Self {
            slots: (0..len)
                .map(|i| {
                    CachePadded::new(Slot {
                        borrowed: AtomicBool::new(false),
                        value:    UnsafeCell::new(init(i)),
                    })
                })
                .collect(),
        }
    }

    /// Số ô.
    #[inline]
    pub fn len(&self) -> usize
    {
        self.slots.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.slots.is_empty()
    }

    /// Mượn ô của thread hiện tại để ghi.
    ///
    /// # Panics
    ///
    /// Nếu thread hiện tại không có chỗ trong `pool`, hoặc mảng ngắn hơn pool nó được dựng cho,
    /// hoặc thread này đã đang ở trong chính ô đó. Xem [`Self::with`].
    #[inline]
    pub fn with_mut<R>(&self, pool: &ThreadPool, f: impl FnOnce(&mut T) -> R) -> R
    {
        self.with(pool.worker_index(), f)
    }

    /// Mượn ô số `index`.
    ///
    /// # Hợp đồng an toàn
    ///
    /// Người gọi từ ngoài đi qua [`Self::with_mut`], và hàm đó truyền vào chỉ số của chính thread
    /// đang gọi. Không hai thread nào dùng chung một chỉ số, nên không bao giờ có hai `&mut` cùng
    /// lúc.
    ///
    /// # Panics
    ///
    /// Nếu thread này đã đang ở trong chính ô đó.
    ///
    /// Một dòng tài liệu không đủ để loại trừ chuyện này, vì chính crate này mở đường cho nó: thread
    /// đang chờ ở một điểm join sẽ chạy job **khác**, nên một job có thể bắt đầu ngay bên dưới một
    /// job đang giữ borrow, trên cùng thread, với cùng chỉ số. Không dòng nào của cả hai job trông
    /// sai cả, mà kết quả là hai `&mut` cùng sống trên một giá trị. Nên cái cờ được kiểm cả trong
    /// bản release: một lần load và một nhánh rẽ trên chính cache line mà thread này đang giữ, đổi
    /// lấy một lỗi mà nếu không có nó thì vô hình cho tới lúc nó làm hỏng thứ khác.
    #[inline]
    pub fn with<R>(&self, index: usize, f: impl FnOnce(&mut T) -> R) -> R
    {
        let slot = self
            .slots
            .get(index)
            .unwrap_or_else(|| panic!("chỉ số worker {index} nằm ngoài một PerWorker có {} ô", self.slots.len()));

        assert!(
            !slot.borrowed.load(Ordering::Relaxed),
            "ô {index} của PerWorker này đang được chính thread này mượn: một job chạy bên dưới chỗ mượn không được \
             với vào cùng ô"
        );
        // `store` chứ không phải `swap`: cờ của ô này chỉ có chủ của nó ghi, nên không có ai để mà
        // giành với lần `load` ngay trên.
        slot.borrowed.store(true, Ordering::Relaxed);
        let _release = BorrowGuard(&slot.borrowed);

        // Safety: `index` là chỉ số của chính thread này, mà mỗi thread chỉ có một, nên không thread
        // nào khác đang ở trong ô này; cái cờ ở trên loại nốt khả năng chính thread này vào hai lần.
        slot.value.with_mut(|pointer| f(unsafe { &mut *pointer }))
    }

    /// Đi qua mọi ô theo thứ tự chỉ số, để gộp kết quả.
    ///
    /// `&mut self` chính là bằng chứng rằng không job nào còn đang ghi, và cũng là bằng chứng không
    /// cờ mượn nào còn dựng: một [`BorrowGuard`] giữ một borrow `&self` suốt thời gian nó sống.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T>
    {
        self.slots.iter_mut().map(|slot| slot.value.with_mut(|pointer| unsafe { &mut *pointer }))
    }

    /// Áp `f` lên mọi ô mà không có bằng chứng độc quyền.
    ///
    /// # Safety
    ///
    /// Người gọi phải bảo đảm không thread nào đang ở trong [`Self::with`]. Dùng cho ranh giới
    /// frame, chỗ mà pool chắc chắn đang rảnh.
    ///
    /// # Panics
    ///
    /// Nếu có ô nào đang được mượn. Đó là một phần kiểm tra của hợp đồng trên chứ không thay thế
    /// được nó, và nó bắt đúng trường hợp hay xảy ra thật: một ranh giới frame được tuyên bố trong
    /// lúc vẫn còn job đang cầm arena của nó.
    pub unsafe fn for_each_unchecked(&self, mut f: impl FnMut(&mut T))
    {
        for (index, slot) in self.slots.iter().enumerate()
        {
            assert!(
                !slot.borrowed.load(Ordering::Relaxed),
                "ô {index} của PerWorker này vẫn đang được mượn, tức là pool chưa rảnh, và reset lúc này sẽ kéo đổ \
                 vùng nhớ mà một job đang dùng"
            );
            slot.value.with_mut(|pointer| f(unsafe { &mut *pointer }));
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for PerWorker<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("PerWorker").field("slots", &self.slots.len()).finish()
    }
}

#[cfg(all(test, not(loom)))]
mod test
{
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::pool::Config;

    fn pool_with(threads: usize) -> ThreadPool
    {
        ThreadPool::new(Config {
            threads: threads,
            ..Config::default()
        })
    }

    #[test]
    fn moi_o_duoc_dung_bang_chi_so_cua_no()
    {
        let mut per = PerWorker::with_len(4, |i| i * 10);
        assert_eq!(per.len(), 4);
        assert_eq!(per.iter_mut().map(|slot| *slot).collect::<Vec<_>>(), vec![0, 10, 20, 30]);
    }

    #[test]
    fn moi_worker_ghi_vao_o_cua_rieng_no()
    {
        const JOBS: usize = 5_000;

        let pool = pool_with(4);
        let mut counts: PerWorker<usize> = PerWorker::for_pool(&pool, |_| 0);
        assert_eq!(counts.len(), pool.worker_count());

        pool.parallel_for(JOBS, 16, |_| {
            counts.with_mut(&pool, |n| *n += 1);
        });

        // Gộp theo thứ tự ô, không theo thứ tự ai xong trước.
        assert_eq!(counts.iter_mut().map(|n| *n).sum::<usize>(), JOBS);
    }

    #[test]
    #[should_panic(expected = "đang được chính thread này mượn")]
    fn muon_hai_lan_cung_mot_o_thi_panic()
    {
        let pool = pool_with(1);
        let per: PerWorker<usize> = PerWorker::for_pool(&pool, |_| 0);

        per.with_mut(&pool, |_| {
            per.with_mut(&pool, |_| {});
        });
    }

    #[test]
    #[should_panic(expected = "nằm ngoài một PerWorker")]
    fn chi_so_ngoai_khoang_thi_panic()
    {
        let per: PerWorker<usize> = PerWorker::with_len(2, |_| 0);
        per.with(5, |_| {});
    }

    #[test]
    fn for_each_unchecked_bat_duoc_o_dang_muon()
    {
        let pool = pool_with(1);
        let per: PerWorker<usize> = PerWorker::for_pool(&pool, |_| 0);

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            per.with_mut(&pool, |_| unsafe {
                per.for_each_unchecked(|_| {});
            });
        }));

        assert!(outcome.is_err(), "reset trong lúc một ô còn đang được mượn mà vẫn trót lọt");
    }

    #[test]
    fn du_lieu_khong_sync_van_qua_duoc()
    {
        // `Cell` là `Send` nhưng không `Sync`: đúng loại dữ liệu mà `PerWorker` phải mang được, vì
        // mỗi ô chỉ có một thread chạm vào.
        let pool = pool_with(2);
        let mut cells: PerWorker<std::cell::Cell<usize>> = PerWorker::for_pool(&pool, |_| std::cell::Cell::new(0));
        let total = AtomicUsize::new(0);

        pool.parallel_for(1_000, 32, |_| {
            cells.with_mut(&pool, |cell| cell.set(cell.get() + 1));
        });

        for cell in cells.iter_mut()
        {
            total.fetch_add(cell.get(), Ordering::Relaxed);
        }
        assert_eq!(total.load(Ordering::Acquire), 1_000);
    }
}
