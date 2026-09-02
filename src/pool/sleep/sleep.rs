use crate::pool::sleep::consts::{AWAKE, NOTIFIED, ONE_EVENT, ONE_SEARCHING, ONE_UNPARKED, PARKED, SLEEPER_BITS};
use crate::pool::sleep::params::ParamsPark;
use crate::pool::sleep::split::split;
use crate::pool::sleep::wake::Wake;
use crate::sync::thread::{self, Thread};
use crate::sync::{AtomicU32, AtomicU64, Mutex, Ordering};
use crate::utils::cache_padded::CachePadded;
use crate::utils::poison::ignore_poison;

pub(crate) struct Sleep
{
    /// Bộ đếm sự kiện ở 32 bit cao, `searching` ở 16 bit giữa, `unparked` ở 16 bit thấp.
    ///
    /// - `events`: mỗi job mới cộng một. Worker đọc nó trước khi đi tìm việc và so lại lúc sắp ngủ.
    /// - `searching`: bao nhiêu worker đang đi lùng việc. Còn người lùng thì người đẩy job không
    ///   cần đánh thức ai: người đang lùng sẽ vấp phải job đó.
    /// - `unparked`: bao nhiêu worker đang thức. Bằng `workers` nghĩa là không ai ngủ, khỏi phải
    ///   đụng tới khoá của danh sách.
    state:    CachePadded<AtomicU64>,
    /// Ai đang ngủ, kèm sẵn handle để đánh thức. Dùng như một ngăn xếp: người ngủ sau cùng được
    /// gọi dậy trước, vì cache của nó còn nóng nhất.
    sleepers: Mutex<Vec<(usize, Thread)>>,
    /// Trạng thái từng worker, để phân biệt "thức dậy vì bị gọi" với "thức dậy vu vơ". `park` được
    /// phép trả về bất cứ lúc nào mà không có ai gọi cả.
    parkers:  Box<[CachePadded<AtomicU32>]>,
    workers:  usize,
}

impl Sleep
{
    pub(crate) fn new(workers: usize) -> Self
    {
        assert!(
            workers < (1 << SLEEPER_BITS),
            "pool {workers} worker vượt quá {} ô của giao thức ngủ",
            1 << SLEEPER_BITS
        );

        Self {
            state:    CachePadded::new(AtomicU64::new(workers as u64)),
            sleepers: Mutex::new(Vec::with_capacity(workers)),
            parkers:  (0..workers).map(|_| CachePadded::new(AtomicU32::new(AWAKE))).collect(),
            workers:  workers,
        }
    }

    /// `(events, searching, unparked)`. Cho counter và test, đừng rẽ nhánh theo nó.
    #[inline]
    pub(crate) fn snapshot(&self) -> (u32, u32, u32)
    {
        split(self.state.load(Ordering::Acquire))
    }

    /// Bộ đếm sự kiện, đọc **trước** khi đi tìm việc.
    ///
    /// Con số này rồi được đưa lại cho [`Self::park`]. Mọi job xuất hiện sau lời gọi này đều làm nó
    /// đổi, nên "tìm không thấy gì mà bộ đếm vẫn thế" là bằng chứng rằng không có job nào lọt qua
    /// dưới mũi mình.
    #[inline]
    pub(crate) fn events(&self) -> u32
    {
        (self.state.load(Ordering::Acquire) >> (2 * SLEEPER_BITS)) as u32
    }

    /// Có job mới ở đâu đó trong lane. Đánh thức đúng một người, nếu thật sự cần.
    ///
    /// Đây là đường nóng: mỗi lần `spawn` đều đi qua đây, nên hai lần đọc đầu là để **không** làm
    /// gì trong đại đa số trường hợp.
    #[inline]
    pub(crate) fn notify(&self)
    {
        if self.workers == 0
        {
            return;
        }

        // Cộng bộ đếm **và** đọc hai con số kia trong đúng một RMW. Đây là chỗ giao thức đứng hay
        // đổ, xem phần đầu file: đổi dòng này thành một `load` là mở lại kẽ hở mất wake-up.
        let previous = self.state.fetch_add(ONE_EVENT, Ordering::AcqRel);
        let (_, searching, unparked) = split(previous);

        // Đã có người đang lùng: job này sẽ rơi vào tay nó. Đánh thức thêm chỉ tổ có hai người
        // tranh một job rồi một người ngủ lại.
        if searching > 0
        {
            return;
        }
        // Không ai ngủ thì không có ai để gọi.
        if unparked as usize >= self.workers
        {
            return;
        }

        self.unpark_one();
    }

    /// Có `n` job mới cùng lúc. Đánh thức tối đa `n` người.
    ///
    /// [`Self::notify`] được viết cho "vừa có **một** job": thấy đã có người đi lùng việc là nó im,
    /// vì một job thì một người tìm là đủ, và gọi thêm chỉ tổ có hai người tranh nhau. Một đợt
    /// fan-out thì ngược hẳn. Ở đó người gọi biết chắc mình vừa đẩy ra `n` phần việc rời nhau, và
    /// im lặng nghĩa là chúng được phát theo dây chuyền: người thứ nhất tỉnh, vớ một job, gọi người
    /// thứ hai, người thứ hai tỉnh, vớ một job, gọi người thứ ba. Mỗi bậc là một cặp syscall cộng
    /// một lần chuyển ngữ cảnh, và cả cái dây chuyền nằm thẳng trên đường tới hạn của lời gọi
    /// `parallel_for` đang chờ.
    ///
    /// Người đang lùng việc vẫn được trừ đi: họ sẽ tự vấp phải một phần của đợt này mà không cần ai
    /// gọi.
    pub(crate) fn notify_many(&self, n: usize)
    {
        if self.workers == 0
        {
            return;
        }
        if n <= 1
        {
            return self.notify();
        }

        // RMW trước rồi mới đọc, đúng như `notify`. Xem đầu file về vì sao chỗ này không được là
        // một `load`.
        let previous = self.state.fetch_add(ONE_EVENT, Ordering::AcqRel);
        let (_, searching, unparked) = split(previous);

        if unparked as usize >= self.workers
        {
            return;
        }

        let want = n.saturating_sub(searching as usize);
        if want == 0
        {
            return;
        }

        // Cấp chỗ trước khi cầm khoá, để phần việc dưới khoá chỉ còn là chuyển mấy phần tử.
        let mut taken: Vec<(usize, Thread)> = Vec::with_capacity(want.min(self.workers));

        {
            let mut sleepers = ignore_poison(self.sleepers.lock());
            let woken = sleepers.len().min(want);
            if woken == 0
            {
                return;
            }

            // Tính cả nhóm vào `searching` và `unparked` ngay tại đây, cùng một RMW, y như
            // `unpark_one` làm với một người. Đợi họ tỉnh mới cộng thì trong khoảng giữa những con
            // số này nói dối, và job đẩy vào lúc đó sẽ gọi dậy thêm người nữa cho một đợt đã đủ
            // người.
            self.state.fetch_add((ONE_SEARCHING + ONE_UNPARKED) * woken as u64, Ordering::AcqRel);

            // Người ngủ sau cùng được gọi trước, cache của họ còn nóng nhất. Đó cũng là thứ tự
            // `unpark_one` lấy ra bằng `pop`.
            let start = sleepers.len() - woken;
            taken.extend(sleepers.drain(start..));
            for (index, _) in &taken
            {
                self.parkers[*index].store(NOTIFIED, Ordering::Release);
            }
        }

        // Thả khoá rồi mới gọi, y như `unpark_one`. Gọi trong lúc còn cầm khoá thì người vừa tỉnh
        // có thể quay lại xin đúng cái khoá đó ở [`Self::unregister`] và chờ chính kẻ đang đánh
        // thức mình. Loom dựng lại được cảnh đó ngay ở mô hình hai worker.
        for (_, thread) in taken
        {
            thread.unpark();
        }
    }

    /// Gọi cả pool dậy. Dùng lúc shutdown, và lúc có thứ mà **mọi** worker phải nhìn lại.
    ///
    /// # Vì sao bộ đếm sự kiện vẫn phải nhích khi không có ai đang ngủ
    ///
    /// Danh sách rỗng **không** có nghĩa là không có ai sắp ngủ. Một worker vừa kiểm cờ shutdown
    /// thấy `false` và đang trên đường tới [`Self::park`] thì chưa có tên trong danh sách, nên nó
    /// không nhận được `unpark` nào; nếu ở đây cũng không nhích bộ đếm thì lát nữa nó so `seen` thấy
    /// y nguyên, và nó ngủ qua luôn cả lần shutdown. Thread đi join nó thì đợi tới hết đời tiến
    /// trình.
    ///
    /// Nhích bộ đếm thì bịt kín khe đó, vì hai phía cùng RMW một ô `state`: hoặc worker đọc `seen`
    /// sau lần nhích này, và khi đó lần kiểm cờ shutdown ngay sau đó thấy cờ đã dựng; hoặc nó đọc
    /// trước, và khi đó `events != seen` nên nó rút tên ra thay vì ngủ. Miri dựng lại đúng cảnh này
    /// khi nhiều test cùng dựng rồi tắt pool trong một tiến trình.
    pub(crate) fn notify_all(&self)
    {
        let mut sleepers = ignore_poison(self.sleepers.lock());
        let woken = sleepers.len() as u64;

        self.state.fetch_add(ONE_EVENT + woken, Ordering::AcqRel);
        for (index, thread) in sleepers.drain(..)
        {
            self.parkers[index].store(NOTIFIED, Ordering::Release);
            thread.unpark();
        }
    }

    fn unpark_one(&self)
    {
        let mut sleepers = ignore_poison(self.sleepers.lock());
        let Some((index, thread)) = sleepers.pop()
        else
        {
            return;
        };

        // Người bị gọi dậy được tính luôn là đang lùng việc, ngay tại đây chứ không phải lúc nó
        // tỉnh. Nếu đợi nó tỉnh mới cộng thì trong khoảng giữa `searching` vẫn là 0, và mọi job
        // được đẩy vào lúc đó sẽ gọi dậy thêm người nữa, đánh thức cả pool cho một nhúm job.
        self.state.fetch_add(ONE_SEARCHING + ONE_UNPARKED, Ordering::AcqRel);
        self.parkers[index].store(NOTIFIED, Ordering::Release);
        drop(sleepers);

        thread.unpark();
    }

    /// Xin phép đi lùng việc. Trả `false` nghĩa là đã đủ người lùng rồi, đi ngủ đi.
    ///
    /// Trần đặt ở 50% số worker. Lùng việc là đi CAS vào ring của người khác, nên một pool mà tất
    /// cả cùng lùng thì các core bận đá cache line của nhau qua lại thay vì chạy job. Nửa pool đủ
    /// để việc mới lan ra nhanh, và nửa còn lại ngủ yên cho tới khi thật sự có phần cho mình.
    ///
    /// Sàn của cái trần này là **một**, và đó không phải chuyện làm tròn cho đẹp. Pool một hoặc hai
    /// worker mà lấy đúng 50% thì trần thành 0, tức là không worker nào được phép trộm, và việc nằm
    /// trong ring của người khác thì nằm đó mãi. Đúng một lần treo như vậy đã đưa cái sàn này vào
    /// đây.
    pub(crate) fn try_start_searching(&self) -> bool
    {
        let limit = (self.workers / 2).max(1);
        let mut state = self.state.load(Ordering::Acquire);

        loop
        {
            let (_, searching, _) = split(state);
            if searching as usize >= limit
            {
                return false;
            }

            match self
                .state
                .compare_exchange_weak(state, state + ONE_SEARCHING, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => state = actual,
            }
        }
    }

    /// Thôi lùng, vì đã tìm được việc. Trả `true` nếu mình là người lùng cuối cùng.
    ///
    /// Người lùng cuối cùng vừa tìm ra việc là một tín hiệu: chỗ nó lấy job ra có thể còn job nữa,
    /// mà từ giờ không còn ai đi tìm. Nên nó gọi thêm một người dậy, xem [`Self::notify`].
    #[inline]
    pub(crate) fn end_searching(&self) -> bool
    {
        let previous = self.state.fetch_sub(ONE_SEARCHING, Ordering::AcqRel);
        let (_, searching, _) = split(previous);
        debug_assert!(searching > 0, "end_searching gọi nhiều hơn số lần try_start_searching");
        searching == 1
    }

    /// Đi ngủ, nếu quả thật không có gì xảy ra kể từ lúc bắt đầu tìm việc.
    ///
    /// `seen` là giá trị [`Self::events`] trả về **trước** khi worker đi tìm. `recheck` trả lời câu
    /// "lúc này lane còn việc nào không", và chỉ được hỏi khi worker này là người lùng cuối cùng.
    ///
    /// Trình tự ở đây là toàn bộ chỗ dựa của giao thức, và nó chỉ đứng vững vì cả hai phía cùng RMW
    /// một ô `state`:
    ///
    /// - Job được đẩy vào **trước** lúc worker rời hàng ngũ: RMW của người đẩy đến trước, nên
    ///   `previous` mà worker đọc được đã mang bộ đếm mới. `seen` lệch, worker không ngủ.
    /// - Job được đẩy vào **sau**: RMW của worker đến trước, nên người đẩy nhìn thấy `searching`,
    ///   `unparked` đã giảm, biết là có người vừa đi ngủ, và gọi dậy.
    ///
    /// Không có khe nào ở giữa hai trường hợp đó, vì hai RMW trên cùng một địa chỉ thì luôn có một
    /// cái đến trước.
    pub(crate) fn park<R>(&self, params: ParamsPark<R>) -> Wake
    where R: Fn() -> bool
    {
        let ParamsPark {
            index,
            is_searching,
            seen,
            recheck,
        } = params;

        {
            let mut sleepers = ignore_poison(self.sleepers.lock());
            self.parkers[index].store(PARKED, Ordering::Release);
            sleepers.push((index, thread::current()));
        }

        // Rời hàng ngũ người thức, và người lùng nếu đang lùng. Ba con số đi chung một RMW để người
        // đẩy job không bao giờ thấy trạng thái nửa vời.
        let leaving = match is_searching
        {
            true => ONE_SEARCHING + ONE_UNPARKED,
            false => ONE_UNPARKED,
        };
        let previous = self.state.fetch_sub(leaving, Ordering::AcqRel);
        let (events, searching, _) = split(previous);

        // Có job tới sau lúc mình bắt đầu tìm: chắc chắn mình đã bỏ sót nó.
        let missed = events != seen;
        // Người lùng cuối cùng thì ngó thêm một lần nữa. Không bắt buộc cho tính đúng đắn, chỉ để
        // khỏi ngủ rồi bị gọi dậy ngay.
        let stale = missed || (is_searching && searching == 1 && recheck());

        if stale && self.unregister(index)
        {
            return Wake::Cancelled;
        }

        // `park` được phép trả về vu vơ, và một `unpark` cũ còn sót lại cũng làm nó trả về ngay.
        // Chỉ `NOTIFIED` mới là lời gọi thật.
        while self.parkers[index].load(Ordering::Acquire) != NOTIFIED
        {
            thread::park();
        }
        self.parkers[index].store(AWAKE, Ordering::Release);
        Wake::Notified
    }

    /// Rút tên khỏi danh sách ngủ. Trả `false` nếu có người đã bốc mình ra rồi, và khi đó lời gọi
    /// đánh thức đang trên đường tới: cứ đi ngủ như thường, nó sẽ thức dậy ngay.
    fn unregister(&self, index: usize) -> bool
    {
        let mut sleepers = ignore_poison(self.sleepers.lock());
        let Some(at) = sleepers.iter().position(|(i, _)| *i == index)
        else
        {
            return false;
        };

        sleepers.remove(at);
        self.parkers[index].store(AWAKE, Ordering::Release);
        self.state.fetch_add(ONE_UNPARKED, Ordering::AcqRel);
        true
    }
}

impl std::fmt::Debug for Sleep
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        let (events, searching, unparked) = self.snapshot();
        f.debug_struct("Sleep")
            .field("workers", &self.workers)
            .field("events", &events)
            .field("searching", &searching)
            .field("unparked", &unparked)
            .finish()
    }
}
