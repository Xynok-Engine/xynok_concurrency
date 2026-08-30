use crate::sync::thread;
use crate::utils::available_cores;
use crate::utils::inline_fn::InlineFn;
use crate::worker_pool::cfg::CfgWorkerPool;
use crate::worker_pool::worker::Worker;
use xynok_std::collection::Queue;

pub mod cfg;
pub mod identifies;

pub(crate) mod worker;
pub(crate) mod leader;
pub(crate) mod thread_meta;

pub struct WorkerPool
{
    cfg:     CfgWorkerPool,
    workers: Vec<Worker>,
    tasks:   Queue<InlineFn>,
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

        let tasks = Queue::with_capacity(cfg.task_capacity);
        let workers_count = cfg.worker_capacity - 1;
        let mut workers = Vec::with_capacity(workers_count);
        for i in 0..workers_count
        {
            workers.push(create_a_worker(format!("{}.workers[{}]", cfg.name, i).as_str(), &cfg));
        }
        Self { cfg: cfg, tasks, workers }
    }

    pub fn push(&mut self, task: InlineFn)
    {
        self.tasks.enqueue(task);
    }
}

fn create_a_worker(name: &str, cfg: &CfgWorkerPool) -> Worker
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

#[cfg(test)]
mod test
{
    use crate::worker_pool::cfg::CfgWorkerPool;
    use crate::worker_pool::WorkerPool;

    #[test]
    fn test_init()
    {
        let _worker_pool = WorkerPool::new(CfgWorkerPool {
            name:                     "test_pool".to_string(),
            priority:                 crate::apis::priority::Priority::Frame,
            worker_capacity:          8,
            task_capacity:            256,
            per_worker_task_capacity: 64,
        });

        std::thread::sleep(std::time::Duration::from_secs(3));
    }
}
