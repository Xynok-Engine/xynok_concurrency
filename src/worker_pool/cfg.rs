use crate::apis::priority::Priority;

pub struct CfgWorkerPool
{
    pub name:     String,
    pub priority: Priority,

    /// The estimated maximum number of tasks for the pool. This pool will grow if this limit is exceeded.
    pub task_capacity: usize,

    /// The maximum number of workers in this pool.
    /// > [!IMPORTANT]
    /// > This value can be updated after being passed to a [`WorkerPool`] to ensure it doesn't exceed the total number of available cores on the device.
    pub worker_capacity: usize,

    /// The maximum number of tasks per worker.
    /// > [!IMPORTANT]
    /// > This value can be updated after it is passed to a [`WorkerPool`] to ensure it is a power of 2.
    pub per_worker_task_capacity: usize,
}
