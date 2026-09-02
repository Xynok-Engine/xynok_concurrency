use crate::utils::cores::available_cores;
use crate::utils::inline_fn::InlineFn;
use crate::utils::queue_batching::QueueBatching;
use crate::worker_pool::cfg::CfgWorkerPool;
use crate::worker_pool::worker::Worker;

/// Pool giữ đám worker cộng hàng đợi chung cho việc tới từ bên ngoài.
///
/// Vài con số trong cấu hình được nắn lại ngay lúc dựng thay vì báo lỗi: số worker kẹp xuống theo
/// số core thật, sức chứa mỗi worker nâng lên luỹ thừa hai gần nhất.
pub struct WorkerPool
{
    cfg:     CfgWorkerPool,
    workers: Vec<Worker>,
    tasks:   QueueBatching<InlineFn>,
}

impl WorkerPool
{
    pub fn new(mut cfg: CfgWorkerPool) -> Self
    {
        cfg.worker_capacity = cfg.worker_capacity.min(available_cores());

        if cfg.per_worker_task_capacity < 1 || !cfg.per_worker_task_capacity.is_power_of_two()
        {
            cfg.per_worker_task_capacity = cfg.per_worker_task_capacity.next_power_of_two().max(2);
        }

        let tasks = QueueBatching::with_capacity(cfg.task_capacity);
        // Thread gọi cũng là một người tham gia, nên chỉ cần spawn ít hơn một thread.
        let workers_count = cfg.worker_capacity - 1;
        let mut workers = Vec::with_capacity(workers_count);
        for i in 0..workers_count
        {
            workers.push(Self::spawn_worker(format!("{}.workers[{}]", cfg.name, i).as_str(), &cfg));
        }
        Self {
            cfg:     cfg,
            tasks:   tasks,
            workers: workers,
        }
    }

    #[inline]
    pub fn push(&mut self, task: InlineFn)
    {
        self.tasks.push(task);
    }

    #[track_caller]
    fn spawn_worker(name: &str, cfg: &CfgWorkerPool) -> Worker
    {
        let handle = match crate::sync::thread::spawn_named(name.to_string(), move || {
            Worker::update();
        })
        {
            Ok(r) => r,

            Err(e) => panic!("Failed to create worker `{}`: {}", name, e),
        };
        Worker::new(handle, cfg.per_worker_task_capacity)
    }
}
