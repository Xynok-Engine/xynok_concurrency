//! Phần dùng chung của pool: hàng đợi, ring của từng người, và cách tìm việc.

use crate::apis::priority::Priority;
use crate::bump::Bump;
use crate::custom_type::Job;
use crate::lane_queue::LaneQueue;
use crate::per_worker::PerWorker;
use crate::sync::AtomicBool;
use crate::sync::thread::{self, ThreadId};
use crate::utils::backoff::Backoff;
use crate::utils::cache_padded::CachePadded;

use super::context::{CONTEXT, Context, SELF_ID};
use super::counters::PoolCounters;
use super::local::Local;
use super::sleep::Sleep;
use super::worker::{run_in_loop, run_job};
use super::{IDLE_NAP, LANE_QUEUE_TICK};

#[cfg(doc)] use super::ThreadPool;

/// Mọi thứ worker cần, và không có gì giữ cho pool sống.
pub(crate) struct Shared
{
    /// Chỗ job từ ngoài pool rơi vào, và chỗ hứng phần bị xả ra khỏi ring.
    pub(super) lane_queue:  LaneQueue<Job>,
    /// Một ô cho mỗi người tham gia: `workers` worker, cộng host ở chỉ số cuối.
    pub(super) locals:      Box<[CachePadded<Local>]>,
    pub(super) sleep:       Sleep,
    /// Phân biệt pool này với mọi pool khác trong tiến trình. Xem [`worker_index_in`].
    pub(super) id:          u64,
    /// Dựng một lần, trên đường đi xuống. Worker vét nốt hàng đợi rồi thoát.
    pub(super) shutdown:    AtomicBool,
    pub(super) workers:     usize,
    /// Thread đã dựng pool, và là người ngoài duy nhất có một chỗ trong `locals`.
    pub(super) host:        thread::Thread,
    /// Id của [`Self::host`], để so mà không phải clone một `Arc`.
    pub(super) host_id:     ThreadId,
    pub(super) spin_rounds: u32,
    pub(super) priority:    Priority,
    /// Một arena nháp cho mỗi người tham gia. Xem [`ThreadPool::scratch`].
    pub(super) scratch:     PerWorker<Bump>,
    pub(super) counters:    PoolCounters,
}

impl Shared
{
    /// Số thread worker đã spawn.
    #[inline]
    pub(crate) fn worker_threads(&self) -> usize
    {
        self.workers
    }

    /// Số chỗ đứng khác nhau, tức là số worker cộng host.
    #[inline]
    pub(crate) fn worker_count(&self) -> usize
    {
        self.workers + 1
    }

    /// Thread hiện tại có phải một worker của chính pool này không.
    ///
    /// Chỉ worker, không tính host: host thì join được cả pool mà không join chính nó.
    #[inline]
    pub(crate) fn on_own_worker(&self) -> bool
    {
        let context = CONTEXT.get();
        context.pool == self.id && context.index < self.workers
    }

    /// Chỉ số của thread hiện tại nếu nó là người tham gia pool này.
    ///
    /// Cố ý **không** phải [`worker_index_in`] đầy đủ: hàm đó có bước so handle thread, mà so handle
    /// thì phải clone một `Arc`, quá đắt cho một thứ nằm trên đường push. Host bị pool dựng sau ghi
    /// đè chỗ đứng thì ở đây đọc ra "người lạ", và nó chỉ mất cái ring của mình chứ không sai.
    #[inline]
    pub(super) fn context(&self) -> Option<Context>
    {
        let context = CONTEXT.get();
        if context.pool == self.id
        {
            return Some(context);
        }

        // Không khớp, mà vẫn có thể là host của chính pool này: ô `CONTEXT` chỉ giữ được **một** chỗ
        // đứng, nên một thread dựng hai pool sẽ bị pool sau ghi đè chỗ của pool trước. Bỏ qua
        // trường hợp đó thì host mất cái ring của mình và mọi job nó spawn phải đi vòng qua lane
        // queue, tức là qua một cái khoá, cho từng job một. Bộ đếm `lane_pops` trong một ví dụ thật
        // là chỗ chuyện này lộ ra.
        let is_host = SELF_ID.with(|id| *id == self.host_id);
        is_host.then_some(Context {
            pool:    self.id,
            index:   self.workers,
            in_loop: false,
        })
    }

    #[inline]
    pub(super) fn participants(&self) -> usize
    {
        self.locals.len()
    }

    /// Giao một job. Xem [`ThreadPool::inject`].
    pub(crate) fn inject(&self, job: Job)
    {
        // Pool không có worker nào: không có ai để giao, nên chạy luôn. Điều này giữ cho
        // `threads: 0` không phải là một nhánh riêng ở mọi call site phía trên.
        if self.workers == 0 && !matches!(self.context(), Some(context) if context.in_loop)
        {
            run_job(job);
            return;
        }

        match self.context()
        {
            Some(context) if context.in_loop =>
            {
                // Safety: `in_loop` chỉ được bật bởi chính thread này, trong vòng chạy job của pool
                // này, với đúng `context.index` đó.
                let displaced = unsafe { self.locals[context.index].swap_lifo(job) };
                if let Some(older) = displaced
                {
                    self.push_local(context.index, older);
                }
            }
            Some(context) => self.push_local(context.index, job),
            None => self.lane_queue.push(job),
        }

        self.sleep.notify();
    }

    /// Đánh thức tối đa `n` người đang ngủ, cho một đợt spawn biết trước mình có bao nhiêu phần
    /// việc. Xem [`Sleep::notify_many`].
    #[inline]
    pub(crate) fn wake_many(&self, n: usize)
    {
        self.sleep.notify_many(n);
    }

    /// Đẩy vào ring của người tham gia `index`, xả nửa cũ xuống lane queue nếu ring đã đầy.
    ///
    /// Chỉ gọi từ chính chủ của `index`.
    pub(super) fn push_local(&self, index: usize, job: Job)
    {
        // Safety: chỉ chủ của ring mới tạo `Producer`, và chủ ở đây là thread đang gọi.
        let mut producer = unsafe { self.locals[index].ring.producer() };

        let Err(job) = producer.push(job)
        else
        {
            return;
        };

        // Ring đầy. Xả nửa cũ xuống lane queue rồi push lại: biên cứng của ring thành ngưỡng xả,
        // và phần bị xả là phần nguội nhất, ai chạy cũng như nhau.
        let mut spilled = Vec::new();
        producer.spill_half(&mut spilled);
        self.lane_queue.push_batch(spilled);
        self.counters.of(index).spill();

        if let Err(job) = producer.push(job)
        {
            // Không xả được ô nào (kẻ trộm đang giữ cả vùng): job vẫn phải có chỗ, và lane queue thì
            // không bao giờ từ chối.
            self.lane_queue.push(job);
        }
    }

    /// Bước 1 tới 3: ô LIFO, ring của mình, rồi lane queue.
    ///
    /// `tick` là số vòng worker đã chạy. Cứ [`LANE_QUEUE_TICK`] vòng thì lane queue được ngó trước
    /// cả ô LIFO, xem tài liệu module.
    pub(super) fn next_local_job(&self, index: usize, tick: u32) -> Option<Job>
    {
        // Safety: chỉ chủ tạo `Producer`.
        let mut producer = unsafe { self.locals[index].ring.producer() };

        if tick.is_multiple_of(LANE_QUEUE_TICK)
            && let Some(job) = self.lane_queue.steal_batch_and_pop(&mut producer, self.participants())
        {
            self.counters.of(index).lane_pop();
            return Some(job);
        }

        // Safety: chỉ chủ chạm ô LIFO.
        if let Some(job) = unsafe { self.locals[index].take_lifo() }
        {
            return Some(job);
        }

        if let Some(job) = producer.pop()
        {
            return Some(job);
        }

        let job = self.lane_queue.steal_batch_and_pop(&mut producer, self.participants());
        if job.is_some()
        {
            self.counters.of(index).lane_pop();
        }
        job
    }

    /// Bước 4 và 5: trộm của người khác, rồi ngó lane queue lần cuối.
    ///
    /// Bắt đầu từ `index + 1` chứ không từ 0, để những kẻ đói không xếp hàng cùng nhắm vào người
    /// thứ nhất.
    pub(super) fn search_job(&self, index: usize) -> Option<Job>
    {
        let participants = self.participants();
        // Safety: chỉ chủ tạo `Producer`.
        let mut producer = unsafe { self.locals[index].ring.producer() };

        let counters = self.counters.of(index);

        for offset in 1..participants
        {
            let victim = (index + offset) % participants;
            let consumer = self.locals[victim].ring.consumer();

            if consumer.steal_into(&mut producer) > 0
                && let Some(job) = producer.pop()
            {
                counters.steal_hit();
                return Some(job);
            }
            counters.steal_miss();
        }

        let job = self.lane_queue.steal_batch_and_pop(&mut producer, participants);
        if job.is_some()
        {
            counters.lane_pop();
        }
        job
    }

    /// Còn việc ở đâu đó trong lane không. Dùng cho lần ngó lại trước khi ngủ, nên nó chỉ được
    /// nhìn, không được lấy.
    pub(super) fn has_work(&self) -> bool
    {
        if !self.lane_queue.is_empty()
        {
            return true;
        }
        self.locals.iter().any(|local| !local.ring.is_empty())
    }

    /// Lấy một job bất kỳ và chạy, cho thread đang chờ chứ không phải worker. Trả về có tìm thấy
    /// gì không.
    pub(super) fn run_one(&self, index: usize, tick: u32) -> bool
    {
        let job = self.next_local_job(index, tick).or_else(|| self.search_job(index));

        match job
        {
            Some(job) =>
            {
                run_in_loop(self, index, job);
                true
            }
            None => false,
        }
    }

    /// Xem [`ThreadPool::run_until`].
    pub(crate) fn run_until(&self, done: impl Fn() -> bool)
    {
        let Some(context) = self.context()
        else
        {
            // Người ngoài: không có ring để nạp job vào, nên chỉ còn nước chờ. Quay tại chỗ một
            // lúc rồi ngủ có hạn giờ, chứ quay mãi thì đốt một core cho không.
            let mut backoff = Backoff::new();
            while !done()
            {
                match backoff.is_completed()
                {
                    true => thread::park_timeout(IDLE_NAP),
                    false => backoff.snooze(),
                }
            }
            return;
        };

        let mut tick = 0u32;
        let mut backoff = Backoff::new();

        while !done()
        {
            tick = tick.wrapping_add(1);

            if self.run_one(context.index, tick)
            {
                backoff.reset();
                continue;
            }

            // Không có việc mà điều kiện cũng chưa xong: thứ mình chờ đang chạy trên thread khác.
            if !backoff.is_completed()
            {
                backoff.snooze();
                continue;
            }

            // Quay mãi thì đốt một core cho không. Ngủ có hạn giờ là đường ở giữa: [`Latch`] gọi
            // dậy ngay khi vé cuối cùng được thả, còn điều kiện nào không biết cách gọi ai thì hạn
            // giờ tự lo. Nó ngắn, vì lỡ một nhịp ở đây là lỡ luôn cả phần việc còn lại của frame.
            thread::park_timeout(IDLE_NAP);
        }

        // Ô LIFO không ai trộm được, nên nó không được phép còn gì khi mình rời vòng.
        self.flush_lifo(context.index);
    }

    /// Đẩy job còn kẹt trong ô LIFO xuống ring, để người khác với tới được.
    pub(super) fn flush_lifo(&self, index: usize)
    {
        // Safety: chỉ chủ chạm ô LIFO.
        if let Some(job) = unsafe { self.locals[index].take_lifo() }
        {
            self.push_local(index, job);
            self.sleep.notify();
        }
    }

    /// Chạy sạch mọi thứ còn xếp hàng, kể cả job sinh ra trong lúc đang vét.
    ///
    /// Dành cho cuối [`Owner::stop`], khi worker đã đi hết: thứ chúng để lại không còn thread nào
    /// chạy, mà thả trôi thì một scope đang đếm chúng sẽ treo.
    ///
    /// # Vì sao chỗ này chỉ trộm chứ không dùng ring của ai
    ///
    /// `shutdown` chạy trên thread nào là chuyện của người gọi, và `Drop` của tay cầm cuối cùng thì
    /// còn tuỳ tiện hơn nữa: nó chạy trên bất cứ thread nào tình cờ thả cái tay cầm ấy. Nếu ở đây
    /// dựng một `Producer` cho ô của host, mà host lúc đó lại đang ở trong `run_until` với
    /// `Producer` của chính nó, thì có hai `Producer` trên cùng một ring, mà đó là đúng cái mà
    /// `Producer` không cho phép.
    ///
    /// Đường trộm thì an toàn với mọi thread: `Consumer` là `Copy` và `Sync`, sinh ra để nhiều
    /// thread cùng dùng.
    pub(super) fn drain_all(&self)
    {
        // Ô LIFO chỉ chủ mới được chạm. Nếu thread đang vét chính là chủ của một ô thì vét nốt ô
        // đó, còn không thì để yên: chủ của nó đã tự vét trước khi thoát.
        if let Some(context) = self.context()
        {
            self.flush_lifo(context.index);
        }

        let mut leftovers = Vec::new();

        loop
        {
            self.lane_queue.drain_into(&mut leftovers);

            for local in &self.locals
            {
                let consumer = local.ring.consumer();
                while consumer.steal_batch(&mut leftovers, usize::MAX) > 0
                {}
            }

            if leftovers.is_empty()
            {
                return;
            }

            // Job chạy ở đây có thể đẻ ra job mới, và chúng rơi vào lane queue hoặc vào ring của
            // thread này, nên vòng ngoài phải quay lại vét tiếp.
            for job in leftovers.drain(..)
            {
                run_job(job);
            }
        }
    }
}
