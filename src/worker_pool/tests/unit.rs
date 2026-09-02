use crate::apis::priority::Priority;
use crate::worker_pool::WorkerPool;
use crate::worker_pool::cfg::CfgWorkerPool;

#[test]
fn t0_dung_duoc_pool_voi_cau_hinh_co_ban()
{
    let _worker_pool = WorkerPool::new(CfgWorkerPool {
        name:                     "test_pool".to_string(),
        priority:                 Priority::Frame,
        worker_capacity:          8,
        task_capacity:            256,
        per_worker_task_capacity: 64,
    });

    std::thread::sleep(std::time::Duration::from_secs(3));
}
