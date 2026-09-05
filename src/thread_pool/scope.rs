use crate::custom_type::Job;

use crate::sync::cell::UnsafeCell;
use crate::thread_pool::ThreadPoolInner;
use crate::utils::latch::Latch;
use std::any::Any;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use xynok_std::unsafe_ptr::HeapMut;

type PanicPayload = Box<dyn Any + Send + 'static>;

pub struct Scope<'scope>
{
    pub root:   HeapMut<ThreadPoolInner>,
    pub latch:  Latch,
    pub panic:  UnsafeCell<Option<PanicPayload>>,
    pub marker: PhantomData<&'scope mut &'scope ()>,
}

unsafe impl Sync for Scope<'_> {}
unsafe impl Send for Scope<'_> {}

#[derive(Clone, Copy)]
struct ScopePtr<'scope>(*const Scope<'scope>);

unsafe impl Send for ScopePtr<'_> {}

impl<'scope> Scope<'scope>
{
    pub fn spawn<F>(&self, f: F)
    where F: FnOnce() + Send + 'scope
    {
        let job = self.job(f);
        self.push_job(job);
    }

    pub fn spawn_with<F>(&self, f: F)
    where F: FnOnce(&Scope<'scope>) + Send + 'scope
    {
        let scope = ScopePtr(self as *const Scope<'scope>);
        self.spawn(move || {
            let scope = scope;
            f(unsafe { &*scope.0 });
        });
    }

    fn push_job(&self, job: Job)
    {
        let Some(handle) = self.root.local_worker()
        else
        {
            self.root.push(job);
            return;
        };

        let mut job = job;
        let mut tick = self.latch.remaining() as u64;
        loop
        {
            match handle.worker.tasks.push(job)
            {
                Ok(()) =>
                {
                    return;
                }
                Err(back) =>
                {
                    tick = tick.wrapping_add(1);

                    // Không tìm được việc nào để chạy bớt thì cũng không chờ được ai dọn chỗ hộ:
                    // người duy nhất lấy việc ra khỏi deque này là chính thread đang đứng đây.
                    // Chạy luôn tại chỗ là đường ra duy nhất không treo.
                    if !handle.worker.run_one(tick)
                    {
                        back.run_once();
                        return;
                    }
                    job = back;
                }
            }
        }
    }

    /// Đóng gói `f` thành một job mà scope này đang đếm, và xoá lifetime của nó đi.
    fn job<F>(&self, f: F) -> Job
    where F: FnOnce() + Send + 'scope
    {
        let ticket = self.latch.ticket();
        let scope = ScopePtr(self as *const Scope<'scope>);

        unsafe {
            Job::new(move || {
                let ticket = ticket;
                let scope = scope;

                if ticket.is_canceled()
                {
                    return;
                }

                if let Err(payload) = catch_unwind(AssertUnwindSafe(f))
                {
                    (*scope.0).record_panic(payload);
                }
            })
        }
    }

    fn record_panic(&self, payload: PanicPayload)
    {
        if !self.latch.try_cancel()
        {
            return;
        }

        self.panic.with_mut(|slot| unsafe { *slot = Some(payload) });
    }
}
