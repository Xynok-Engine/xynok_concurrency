use crate::custom_type::Job;

use crate::thread_pool::ThreadPoolInner;
use crate::utils::latch::Latch;
use crate::utils::spinlock::SpinLock;
use std::any::Any;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use xynok_std::unsafe_ptr::HeapMut;

type PanicPayload = Box<dyn Any + Send + 'static>;

/// Non-owning pointer used only while the scope is draining its jobs.
struct ScopePtr<'a>
{
    ptr:    *const Scope<'a>,
    marker: PhantomData<&'a Scope<'a>>,
}

// SAFETY: Scope is Sync; the scope waits for all jobs before it is dropped.
unsafe impl Send for ScopePtr<'_> {}

impl<'a> ScopePtr<'a>
{
    fn new(scope: &Scope<'a>) -> Self
    {
        Self {
            ptr:    scope,
            marker: PhantomData,
        }
    }

    /// # Safety
    /// The pointed-to scope must still be alive and at its original address.
    unsafe fn get(&self) -> &Scope<'a>
    {
        unsafe { &*self.ptr }
    }
}

pub struct Scope<'a>
{
    root:   HeapMut<ThreadPoolInner>,
    latch:  Latch,
    marker: PhantomData<&'a mut &'a ()>,
    panic:  SpinLock<Option<PanicPayload>>,
}

unsafe impl Sync for Scope<'_> {}
unsafe impl Send for Scope<'_> {}

impl<'a> Scope<'a>
{
    pub(crate) fn new(r: HeapMut<ThreadPoolInner>) -> Self
    {
        Self {
            root:   r,
            latch:  Latch::new(),
            marker: PhantomData,
            panic:  SpinLock::new(None),
        }
    }
    #[inline]
    pub(crate) fn cancel(&self)
    {
        self.latch.cancel();
    }
    #[inline]
    pub(crate) fn is_completed(&self) -> bool
    {
        self.latch.is_completed()
    }
    #[inline]
    pub(crate) fn take_panic(&self) -> Option<PanicPayload>
    {
        self.panic.get().take()
    }
}
impl<'a> Scope<'a>
{
    #[inline]
    pub fn spawn<F: FnOnce() + Send + 'a>(&self, f: F)
    {
        let job = self.new_job(f);
        self.push_job(job);
    }
}

impl<'a> Scope<'a>
{
    #[inline]
    fn push_job(&self, job: Job)
    {
        let Some(handle) = self.root.current_worker()
        else
        {
            self.root.push(job);
            return;
        };

        match handle.worker.tasks.push(job)
        {
            Ok(()) => self.root.wake_one(),
            Err(back) => back.run_once(),
        }
    }

    #[inline]
    fn new_job<F: FnOnce() + Send + 'a>(&self, f: F) -> Job
    {
        // make sure to initialize the ticket before moving it into the closure
        let ticket = self.latch.ticket();

        let scope_ptr = ScopePtr::new(self);
        let scoped = move || {
            let ticket = ticket;

            // If a panic occurs, we should stop all current and subsequent tasks
            if ticket.is_canceled()
            {
                drop(ticket);
                return;
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(f))
            {
                // SAFETY: the ticket keeps scope draining until this job finishes
                unsafe { scope_ptr.get() }.record_panic(payload);
            }
            drop(ticket);
        };
        // SAFETY: the ticket is acquired before enqueueing and released only after
        // the closure finishes (or is dropped). ThreadPool::scope waits for all
        // tickets before returning, including when its callback panics.
        let result = unsafe { Job::new_scoped(scoped) };
        debug_assert!(size_of_val(&result) <= 64, "the job size exceeds 64 bytes");
        result
    }

    /// records only the first panic, subsequent panics are dropped
    #[inline]
    fn record_panic(&self, payload: PanicPayload)
    {
        let mut guard = self.panic.get();
        if guard.is_none()
        {
            *guard = Some(payload);
        }
    }
}
