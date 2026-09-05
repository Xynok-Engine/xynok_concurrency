use crate::apis::priority::Priority;

pub struct CfgThreadPool
{
    pub name:                     String,
    pub priority:                 Priority,
    pub per_worker_task_capacity: usize,
    pub task_capacity:            usize,
    pub worker_count:             usize,
}
impl CfgThreadPool
{
    pub fn new(name: impl Into<String>, worker_count: usize) -> Self
    {
        Self {
            name:                     name.into(),
            priority:                 Priority::default(),
            per_worker_task_capacity: 256,
            task_capacity:            1024,
            worker_count:             worker_count,
        }
    }

    pub fn with_capacity(mut self, per_worker: usize, shared: usize) -> Self
    {
        self.per_worker_task_capacity = per_worker;
        self.task_capacity = shared;
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self
    {
        self.priority = priority;
        self
    }
}
