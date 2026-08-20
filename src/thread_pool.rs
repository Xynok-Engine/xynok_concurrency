//! Bản phác thảo thread pool, viết gọn trong một `mod test` để thấy cơ chế trước khi dựng thật.
//!
//! Toàn bộ nằm trong test nên chưa có gì lọt ra API của crate: đọc, chạy
//! `cargo test thread_pool -- --nocapture --test-threads=1`, rồi nâng cấp dần thành module thật.
//!
//! Năm phần, đọc theo thứ tự:
//!
//! 1. `Priority` — nói với OS lane nào quan trọng hơn.
//! 2. `Config` — pool được dựng bằng gì.
//! 3. `ThreadPool` — N worker, một hàng đợi, ngủ/thức bằng [`MutexCondition`](crate::mutex_condition).
//! 4. `lane` — registry giữ *nhiều* pool, mỗi lane một cái.
//! 5. Test — một vòng lặp frame giả, để thấy hai lane sống cạnh nhau.
//!
//! Ba chỗ bản này cố tình làm đơn giản hơn `xynok_workers`, và đều là chỗ để nâng cấp sau:
//!
//! - **Một hàng đợi `VecDeque` dưới một `Mutex`**, không phải ring lock-free mỗi worker một cái. Nên
//!   không có work-stealing, và mọi push/pop đều tranh chấp cùng một lock.
//! - **Thread gọi không làm việc cùng**: nó submit rồi `wait_idle`, tức là ngủ, nên một core ngồi
//!   không. Bản thật cho nó tự lấy job như một worker.
//! - **Không có `scope`**: job phải `'static`, không borrow được biến trên stack của người gọi.

#[cfg(test)]
mod test
{
    use std::collections::VecDeque;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::thread::JoinHandle;

    use crate::custom_type::Job;
    use crate::mutex_condition::MutexCondition;
    use crate::utils::ignore_poison;

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // 1. Priority — lý do "nhiều pool" không chỉ là chia hàng đợi
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /// Hai lane chia cùng một bộ core. Không có gợi ý này thì scheduler coi decode texture ngang với
    /// một bước physics: worker của frame bị preempt giữa frame bởi việc không có deadline nào cả.
    ///
    /// Một lời gọi mỗi worker lúc khởi động là toàn bộ cách chữa, và là thứ duy nhất chữa được từ
    /// trong process.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Priority
    {
        /// Việc có deadline frame — 16,6 ms.
        Frame,
        /// Việc "sớm muộn gì cũng xong", và phải nhường core cho lane trên.
        Background,
    }

    impl Priority
    {
        /// Best-effort: thất bại thì thread giữ nguyên standing mặc định. Đó là chuyện lịch chạy,
        /// không phải chuyện đúng/sai, nên không đáng để pool từ chối khởi động.
        fn apply_to_current_thread(self)
        {
            // Miri thông dịch Rust chứ không chạy mã máy, nên nó dừng chương trình khi gặp lời gọi
            // FFI. Bỏ qua ở đây để `cargo miri test` còn kiểm được phần thật sự có thể UB.
            if cfg!(miri)
            {
                return;
            }

            #[cfg(any(target_os = "macos", target_os = "ios"))]
            {
                // QoS không phải một con số ưu tiên mà là lời khai báo ý định: kernel dùng nó để
                // chọn cả *loại core*, nên trên Apple Silicon thread `UTILITY` bị lùa sang E-core và
                // để P-core lại cho frame. `nice` không diễn đạt được điều đó.
                const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21; // <sys/qos.h>
                const QOS_CLASS_UTILITY: u32 = 0x11;

                unsafe extern "C" {
                    fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
                }

                let class = match self
                {
                    Self::Frame => QOS_CLASS_USER_INTERACTIVE,
                    Self::Background => QOS_CLASS_UTILITY,
                };

                // Safety: chữ ký khớp <pthread/qos.h>, và lời gọi chỉ tác động lên thread gọi.
                unsafe {
                    let _ = pthread_set_qos_class_self_np(class, 0);
                }
            }

            // Linux dùng `nice(10)` cho Background, Windows dùng `SetThreadPriority`. Bỏ ở bản phác
            // thảo: thiếu chúng thì hai lane chạy ngang hàng, chưa phải lỗi đúng/sai.
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // 2. Config
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    #[derive(Debug, Clone)]
    struct Config
    {
        /// Số worker thread. `0` là hợp lệ và có ích: mọi job chạy ngay trên thread submit, tức là
        /// tắt hẳn song song để trả lời câu "bug này có phải do đa luồng không".
        threads:     usize,
        /// Tiền tố tên thread, để nó hiện ra trong debugger/profiler thay vì `Thread-7`.
        thread_name: &'static str,
        priority:    Priority,
    }

    impl Config
    {
        /// Lane frame: một worker mỗi core.
        ///
        /// `xynok_workers` để `cores - 1` vì ở đó thread gọi cũng lấy job về chạy. Bản này thì thread
        /// gọi chỉ ngủ trong `wait_idle`, nên nó không tính là một participant.
        fn frame() -> Self
        {
            Self {
                threads:     cores(),
                thread_name: "frame",
                priority:    Priority::Frame,
            }
        }

        /// Lane nền: ít thread, priority thấp.
        ///
        /// Số thread ở đây *không* bị chặn bởi số core, và cố ý như vậy: những thread này phần lớn
        /// thời gian ngủ trong syscall, tốn một stack và gần như không tốn CPU. Cái chặn nó là thiết
        /// bị lưu trữ chịu được bao nhiêu lượt đọc đồng thời.
        fn background() -> Self
        {
            Self {
                threads:     2,
                thread_name: "background",
                priority:    Priority::Background,
            }
        }
    }

    fn cores() -> usize
    {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // 3. ThreadPool
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /// Mọi thứ dùng chung giữa các worker.
    ///
    /// Toàn bộ trạng thái nằm trong *một* `MutexCondition`, và đó là lý do bản này ngắn: giao thức
    /// ngủ/thức khó nhất của một job system — *lost wakeup*, khi producer đẩy job vào đúng khe hở
    /// giữa lúc worker "không thấy việc" và lúc nó ngủ — biến mất khi cả hai bên đều phải giữ cùng
    /// một lock để đổi hoặc để đọc trạng thái. Worker không thể ngủ *sau khi* job được đẩy vào, vì
    /// nó chỉ ngủ khi đang giữ lock, và `Condvar::wait` nhả lock một cách nguyên tử.
    ///
    /// Cái giá là mọi push/pop đều là một lần lấy lock dùng chung. `xynok_workers` đổi nó lấy ring
    /// lock-free mỗi worker một cái, và *phải* tự dựng lại giao thức ngủ bằng một ô atomic đóng gói
    /// hai bộ đếm — đó chính là phần khó mà bản này đang tránh.
    struct Shared
    {
        state:   MutexCondition<State>,
        /// Phân biệt pool này với mọi pool khác trong process. Xem [`CURRENT_POOL`].
        id:      u64,
        /// Số worker đã spawn. `0` nghĩa là không ai tiêu thụ hàng đợi, nên [`ThreadPool::submit`]
        /// phải chạy job ngay tại chỗ.
        workers: usize,
    }

    struct State
    {
        queue:   VecDeque<Job>,
        /// Số job đang nằm trong tay worker. `queue.is_empty() && running == 0` là định nghĩa "rảnh",
        /// và là điều kiện `wait_idle` chờ.
        running: usize,
        /// Bật một lần, trên đường đi xuống. Worker chạy nốt hàng đợi rồi thoát.
        stopped: bool,
    }

    /// Handle tới một pool. Clone rẻ và trỏ về *cùng* pool.
    #[derive(Clone)]
    struct ThreadPool
    {
        shared: Arc<Shared>,
        /// Giữ join handle. Cố ý *không* nằm trong `Shared`: worker giữ `Arc<Shared>`, nên nếu join
        /// handle ở trong đó thì worker sẽ tự giữ sống tín hiệu shutdown của chính nó và pool không
        /// bao giờ drop được.
        owner:  Arc<Owner>,
    }

    struct Owner
    {
        shared:  Arc<Shared>,
        handles: Mutex<Vec<JoinHandle<()>>>,
    }

    /// "Không thuộc pool nào". Id thật bắt đầu từ 1.
    const NO_POOL: u64 = 0;

    fn next_pool_id() -> u64
    {
        static NEXT: AtomicU64 = AtomicU64::new(NO_POOL + 1);
        NEXT.fetch_add(1, Ordering::Relaxed)
    }

    thread_local! {
        /// Pool mà thread này là worker của. Chỉ dùng để chặn `wait_idle` gọi từ trong một job của
        /// chính pool đó — xem [`ThreadPool::wait_idle`].
        static CURRENT_POOL: std::cell::Cell<u64> = const { std::cell::Cell::new(NO_POOL) };
    }

    impl ThreadPool
    {
        /// Dựng pool tường minh, ngay trong `main`.
        ///
        /// Không dùng `OnceLock` khởi tạo lười: làm vậy thì số thread, tên thread và priority được
        /// quyết định bởi call site nào tình cờ chạy trước — tức là quyết định một cách tình cờ.
        fn new(config: Config) -> Self
        {
            let shared = Arc::new(Shared {
                state:   MutexCondition::new(State {
                    queue:   VecDeque::new(),
                    running: 0,
                    stopped: false,
                }),
                id:      next_pool_id(),
                workers: config.threads,
            });

            let mut handles = Vec::with_capacity(config.threads);
            for index in 0..config.threads
            {
                let shared = Arc::clone(&shared);
                let priority = config.priority;
                let handle = std::thread::Builder::new()
                    .name(format!("{}-{index}", config.thread_name))
                    .spawn(move || worker_loop(shared, priority))
                    .expect("không spawn được worker");
                handles.push(handle);
            }

            Self {
                shared: Arc::clone(&shared),
                owner:  Arc::new(Owner {
                    shared:  shared,
                    handles: Mutex::new(handles),
                }),
            }
        }

        /// Đưa job cho pool.
        ///
        /// Hai trường hợp job chạy *ngay trên thread gọi* thay vì được xếp hàng, và cả hai đều phải
        /// ghi ra vì người gọi cần biết: pool không có worker (`threads: 0`), và pool đã shutdown.
        /// Lặng lẽ bỏ job thì tệ hơn — một người đang đợi job đó sẽ đợi mãi.
        fn submit(&self, job: Job)
        {
            let mut state = self.shared.state.get();

            if state.stopped || self.shared.workers == 0
            {
                drop(state);
                run_job(job);
                return;
            }

            state.queue.push_back(job);
            drop(state);

            // `notify_all`, không phải `notify_one`, và đây là một cái bẫy thật: một condvar duy nhất
            // phục vụ *hai* điều kiện chờ khác nhau — worker chờ "có job", `wait_idle` chờ "rảnh".
            // `notify_one` có thể đánh thức đúng thread đang ở trong `wait_idle`; nó kiểm lại thấy
            // vẫn chưa rảnh, ngủ tiếp, và job vừa đẩy vào nằm đó không ai nhận.
            //
            // Bản thật tách thành hai condvar (hoặc hai Sleep) để khỏi phải đánh thức cả đám.
            self.shared.state.notify_all();
        }

        /// Đợi tới khi pool không còn job nào trong hàng đợi và không còn job nào đang chạy.
        ///
        /// Đây là "barrier cuối frame" của bản phác thảo này: chỗ mà bản thật dùng `scope()` —
        /// fork-join có bảo hộ lifetime, đóng lại đúng nhóm job của nó chứ không phải toàn bộ pool.
        ///
        /// # Panics
        ///
        /// Nếu gọi từ trong một job đang chạy trên chính pool này. Nó sẽ đợi chính mình xong: worker
        /// này đang chiếm một suất `running`, nên điều kiện "rảnh" không bao giờ đúng, và nếu mọi
        /// worker đều làm vậy thì pool treo vĩnh viễn. Panic ồn ào tốt hơn một cái treo im lặng.
        ///
        /// Cách chữa thật (`xynok_workers` gọi là work-while-waiting): người đợi không ngủ mà đi lấy
        /// job khác về chạy, hết việc mới cho phép mình park.
        fn wait_idle(&self)
        {
            assert!(
                CURRENT_POOL.get() != self.shared.id,
                "wait_idle() được gọi từ trong một job của chính pool này - nó sẽ đợi chính mình"
            );

            let guard = self.shared.state.get();
            drop(self.shared.state.wait_while(guard, |state| !state.queue.is_empty() || state.running > 0));
        }

        /// Dừng worker và join. Idempotent, và cũng tự chạy khi handle cuối cùng bị drop.
        ///
        /// Đừng gọi từ trong một job: bước join sẽ đợi chính thread đang chạy job đó.
        fn shutdown(&self)
        {
            self.owner.stop();
        }
    }

    impl Owner
    {
        /// Tắt có thứ tự. Bỏ bước nào cũng là một cái treo:
        ///
        /// 1. Bật cờ `stopped`, để không worker nào bắt đầu vòng mới.
        /// 2. Đánh thức mọi worker đang ngủ — thread đang chờ condvar không tự biết đã tới lúc chết.
        /// 3. Worker chạy nốt hàng đợi rồi thoát (chứ không bỏ), vì một job bị bỏ có thể là job mà ai
        ///    đó đang đợi.
        /// 4. Join từng thread.
        /// 5. Chạy nốt thứ được đẩy vào sau khi worker cuối đã kiểm — lúc đó không còn thread nào.
        fn stop(&self)
        {
            {
                let mut state = self.shared.state.get();
                if state.stopped
                {
                    return;
                }
                state.stopped = true;
            }
            self.shared.state.notify_all();

            // Rút handle ra rồi *nhả lock* trước khi join. Giữ nó trong lúc join là deadlock: worker
            // đang chờ lock của `state`, một thread khác giữ `state` và gọi `submit`... mỗi lock nhìn
            // ra một hướng khác nhau là đủ để khoá chặt.
            let handles: Vec<JoinHandle<()>> = ignore_poison(self.handles.lock()).drain(..).collect();
            for handle in handles
            {
                // Worker panic đã tự báo qua panic hook, và ở đây không có ai để trao payload cho.
                let _ = handle.join();
            }

            loop
            {
                // Lấy job trong một block riêng để guard bị drop *trước* khi job chạy. Viết thành
                // `while let Some(job) = ...get().queue.pop_front()` thì guard sống hết thân vòng
                // lặp, và một job nào gọi lại `submit` sẽ khoá chết ngay tại đó.
                let job = {
                    let mut state = self.shared.state.get();
                    state.queue.pop_front()
                };

                match job
                {
                    Some(job) => run_job(job),
                    None => break,
                }
            }
        }
    }

    impl Drop for Owner
    {
        fn drop(&mut self)
        {
            self.stop();
        }
    }

    impl Shared
    {
        /// Lấy một job, ngủ nếu chưa có, trả `None` khi đã shutdown và hàng đợi đã cạn.
        ///
        /// `queue.pop_front()` đứng *trước* khi xét `stopped`, nên shutdown làm cạn hàng đợi chứ
        /// không bỏ nó.
        fn next_job(&self) -> Option<Job>
        {
            let guard = self.state.get();
            let mut state = self.state.wait_while(guard, |state| state.queue.is_empty() && !state.stopped);

            let job = state.queue.pop_front()?;
            state.running += 1;
            Some(job)
        }

        /// Báo một job đã xong, và đánh thức người đang ở trong `wait_idle` nếu đây là job cuối.
        fn finish_job(&self)
        {
            let mut state = self.state.get();
            state.running -= 1;
            let idle = state.running == 0 && state.queue.is_empty();
            drop(state);

            if idle
            {
                self.state.notify_all();
            }
        }
    }

    fn worker_loop(shared: Arc<Shared>, priority: Priority)
    {
        CURRENT_POOL.set(shared.id);

        // Từ chính worker, không phải từ thread đã spawn nó: API của mọi nền tảng ở đây đều đặt
        // priority cho thread *đang gọi* và không nhận tham số "thread nào".
        priority.apply_to_current_thread();

        while let Some(job) = shared.next_job()
        {
            run_job(job);
            shared.finish_job();
        }
    }

    /// Chạy job, giữ lại panic thay vì để nó giết worker.
    ///
    /// Một system ECS panic không được kéo cả engine đi theo. Payload bị bỏ ở đây vì job kiểu này
    /// không có điểm join nào để trao lại; bản thật có `scope` thì bắt panic ở đó và ném lại tại
    /// điểm join.
    fn run_job(job: Job)
    {
        let _ = catch_unwind(AssertUnwindSafe(job));
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // 4. Registry — chỗ "nhiều pool" thật sự được quản lý
    // ─────────────────────────────────────────────────────────────────────────────────────────────
    //
    // Phần này trả lời câu "làm sao quản nhiều pool". Câu trả lời nhạt hơn vẻ ngoài của nó: *không
    // có* cỗ máy nào điều phối giữa các pool. Mỗi lane là một `ThreadPool` độc lập, hàng đợi riêng,
    // thread riêng, priority riêng, vòng đời riêng; thứ duy nhất chung là cái bảng tra dưới đây.
    //
    // Việc "đảm bảo frame logic không bị việc nền giành core" không nằm ở code Rust nào cả — nó nằm ở
    // hai chỗ:
    //
    //   * `Priority`, tức là OS scheduler, ở mục 1.
    //   * *Kỷ luật của người gọi*: `File::read` phải được gửi vào `Lane::Background`. Một `read` lọt
    //     vào lane frame sẽ đóng băng một worker vài ms, và không có cấu hình nào của pool chữa được
    //     — vấn đề không phải nó có bao nhiêu thread, mà là một thread đang block thì không làm cái
    //     việc nó tồn tại để làm.
    //
    // `xynok_workers` dùng hai `static Mutex<Option<ThreadPool>>` riêng (`GLOBAL`, `BACKGROUND`) kèm
    // hai bộ `init`/`shutdown`. Ở đây gộp thành một mảng đánh chỉ số bằng enum, để thấy rõ là chúng
    // hoàn toàn đối xứng — và thêm lane thứ ba (io, audio) chỉ là thêm một phần tử.

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Lane
    {
        Frame,
        Background,
    }

    const LANE_COUNT: usize = 2;

    /// Mutex chứ không phải `OnceLock`: `shutdown` phải *rút pool ra được*, vì join thread nghĩa là
    /// drop pool, và một `OnceLock` thì không đổ lại được.
    static LANES: [Mutex<Option<ThreadPool>>; LANE_COUNT] = [Mutex::new(None), Mutex::new(None)];

    fn slot(lane: Lane) -> MutexGuard<'static, Option<ThreadPool>>
    {
        // Poisoning là tín hiệu sai ở đây: thứ duy nhất cái lock này bảo vệ là một `Option`, mà panic
        // không thể để nó ở trạng thái nửa vời. Cái nó *sẽ* làm là biến một lần panic thành process
        // mà mọi lời gọi sau đều panic, kể cả `shutdown` - đúng lời gọi cần để dọn dẹp.
        ignore_poison(LANES[lane as usize].lock())
    }

    fn init(lane: Lane, config: Config)
    {
        let mut slot = slot(lane);
        assert!(slot.is_none(), "lane {lane:?} đã chạy rồi - gọi shutdown() trước nếu muốn cấu hình lại");
        *slot = Some(ThreadPool::new(config));
    }

    /// Clone handle ra *ngoài* lock rồi mới dùng. Giữ lock của registry trong lúc chạy job sẽ chặn
    /// mọi `spawn` khác vào lane đó.
    fn pool(lane: Lane) -> ThreadPool
    {
        slot(lane).clone().unwrap_or_else(|| panic!("lane {lane:?} chưa được init"))
    }

    fn spawn(lane: Lane, f: impl FnOnce() + Send + 'static)
    {
        pool(lane).submit(Box::new(f));
    }

    fn shutdown(lane: Lane)
    {
        // Rút ra rồi mới shutdown ngoài lock: bước drain có thể chạy một job mà job đó lại gọi vào
        // registry.
        let pool = slot(lane).take();
        if let Some(pool) = pool
        {
            pool.shutdown();
        }
    }

    /// Hai lane có vòng đời khác nhau và tắt theo thứ tự khác nhau: frame trước, để không còn ai sinh
    /// việc nền mới, rồi mới đợi việc nền đang dở.
    fn shutdown_all()
    {
        shutdown(Lane::Frame);
        shutdown(Lane::Background);
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────────
    // 5. Test
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /// Registry là process-wide, nên các test chạm vào nó phải xếp hàng. Test dùng `ThreadPool` cục
    /// bộ thì chạy song song thoải mái.
    static SERIAL: Mutex<()> = Mutex::new(());

    /// Ba frame, mỗi frame bốn job có deadline, và một lượt tải asset chạy xuyên qua các frame.
    ///
    /// Đây là hình mà cả bản phác thảo này tồn tại để cho thấy: `wait_idle(Frame)` đóng frame lại,
    /// còn job nền *không* bị nó đợi — nó vắt qua ranh giới frame, đúng như thiết kế.
    #[test]
    fn hai_lane_song_song_trong_mot_vong_lap_frame()
    {
        println!("hai_lane_song_song_trong_mot_vong_lap_frame");
        let _serial = ignore_poison(SERIAL.lock());
        shutdown_all(); // dọn nếu một test trước panic giữa đường

        init(Lane::Frame, Config { threads: 3, ..Config::frame() });
        init(Lane::Background, Config::background());

        let frame_jobs = Arc::new(AtomicUsize::new(0));
        let assets = Arc::new(AtomicUsize::new(0));

        for frame in 0..3
        {
            for system in 0..4
            {
                let frame_jobs = Arc::clone(&frame_jobs);
                spawn(Lane::Frame, move || {
                    frame_jobs.fetch_add(1, Ordering::Relaxed);
                    println!("frame {frame} · system {system} · trên thread `{}`", thread_name());
                });
            }

            // Một lượt tải asset: được block thoải mái, và được phép chưa xong khi frame đóng lại.
            let assets = Arc::clone(&assets);
            spawn(Lane::Background, move || {
                std::thread::sleep(std::time::Duration::from_millis(5)); // giả một `File::read`
                assets.fetch_add(1, Ordering::Relaxed);
                println!("            asset {frame} xong · trên thread `{}`", thread_name());
            });

            // Barrier cuối frame. Chỉ đợi lane frame - đây là điểm chính.
            pool(Lane::Frame).wait_idle();
            assert_eq!(frame_jobs.load(Ordering::Relaxed), (frame + 1) * 4, "frame đóng lại khi còn job chưa xong");
        }

        // Việc nền thường vẫn đang dở ở đây, và đó là điều đúng.
        shutdown_all();
        assert_eq!(assets.load(Ordering::Relaxed), 3, "shutdown làm mất việc nền đang xếp hàng");
    }

    /// Worker phải sống qua một job unwind, không thì một system lỗi kéo cả engine theo.
    #[test]
    fn mot_job_panic_khong_giet_worker()
    {
        let pool = ThreadPool::new(Config {
            threads:     1,
            thread_name: "test-panic",
            priority:    Priority::Frame,
        });

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        pool.submit(Box::new(|| panic!("boom")));
        pool.wait_idle();
        std::panic::set_hook(previous);

        let done = Arc::new(AtomicUsize::new(0));
        let done_in_job = Arc::clone(&done);
        pool.submit(Box::new(move || {
            done_in_job.fetch_add(1, Ordering::Relaxed);
        }));
        pool.wait_idle();

        assert_eq!(done.load(Ordering::Relaxed), 1, "worker chết cùng job panic");
    }

    /// `threads: 0` là chế độ tắt song song: mọi job chạy ngay trên thread submit.
    #[test]
    fn pool_khong_co_worker_chay_inline()
    {
        let pool = ThreadPool::new(Config {
            threads:     0,
            thread_name: "test-inline",
            priority:    Priority::Frame,
        });

        let here = std::thread::current().id();
        let ran_here = Arc::new(AtomicUsize::new(0));
        let ran_here_in_job = Arc::clone(&ran_here);

        pool.submit(Box::new(move || {
            if std::thread::current().id() == here
            {
                ran_here_in_job.fetch_add(1, Ordering::Relaxed);
            }
        }));

        assert_eq!(ran_here.load(Ordering::Relaxed), 1, "job không chạy trên thread submit");
    }

    /// Job xếp hàng lúc shutdown vẫn phải chạy: một job bị bỏ có thể là job ai đó đang đợi.
    #[test]
    fn shutdown_chay_not_hang_doi()
    {
        let done = Arc::new(AtomicUsize::new(0));
        {
            let pool = ThreadPool::new(Config {
                threads:     2,
                thread_name: "test-drain",
                priority:    Priority::Frame,
            });

            for _ in 0..8
            {
                let done = Arc::clone(&done);
                pool.submit(Box::new(move || {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    done.fetch_add(1, Ordering::Relaxed);
                }));
            }
        } // drop ở đây, tức là shutdown và join

        assert_eq!(done.load(Ordering::Relaxed), 8, "job đang xếp hàng lúc shutdown bị bỏ");
    }

    /// Đợi từ trong một job của chính pool đó là tự đợi mình - phải panic, không được treo.
    #[test]
    fn wait_idle_tu_trong_job_thi_panic()
    {
        let pool = ThreadPool::new(Config {
            threads:     1,
            thread_name: "test-nested",
            priority:    Priority::Frame,
        });

        let inner = pool.clone();
        let outcome = Arc::new(Mutex::new(None));
        let outcome_in_job = Arc::clone(&outcome);

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        pool.submit(Box::new(move || {
            let result = catch_unwind(AssertUnwindSafe(|| inner.wait_idle()));
            *ignore_poison(outcome_in_job.lock()) = Some(result.is_err());
        }));
        pool.wait_idle();
        std::panic::set_hook(previous);

        assert_eq!(
            *ignore_poison(outcome.lock()),
            Some(true),
            "wait_idle lồng nhau không panic - nó đang đợi chính mình"
        );
    }

    fn thread_name() -> String
    {
        std::thread::current().name().unwrap_or("main").to_string()
    }
}
