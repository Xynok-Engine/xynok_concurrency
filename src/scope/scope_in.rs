use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

use crate::latch::Latch;
use crate::pool::Shared;
use crate::scope::scope::Scope;
use crate::sync::{Arc, Mutex};
use crate::utils::ignore_poison;

/// Thân của [`ThreadPool::scope`], gọi được từ bất cứ chỗ nào đang cầm phần dùng chung của pool.
pub(crate) fn scope_in<'scope, R>(shared: &Arc<Shared>, f: impl FnOnce(&Scope<'scope>) -> R) -> R
{
    let scope = Scope {
        shared: Arc::clone(shared),
        latch:  Latch::new(0),
        panic:  Mutex::new(None),
        marker: PhantomData,
    };

    let outcome = catch_unwind(AssertUnwindSafe(|| f(&scope)));
    // Chờ cả khi `f` đã panic: bỏ mặc job chạy tiếp trong lúc khung stack chúng mượn đang bị tháo
    // dỡ thì đó đúng là cái use-after-free mà scope sinh ra để chặn.
    shared.run_until(|| scope.latch.is_done());

    let job_panic = ignore_poison(scope.panic.lock()).take();

    match (outcome, job_panic)
    {
        (Err(payload), _) => resume_unwind(payload),
        (Ok(_), Some(payload)) => resume_unwind(payload),
        (Ok(value), None) => value,
    }
}
