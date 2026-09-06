use crate::custom_type::Job;

use crate::sync::UnsafeCell;
use crate::thread_pool::ThreadPoolInner;
use crate::utils::latch::Latch;
use std::any::Any;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use xynok_std::unsafe_ptr::HeapMut;

type PanicPayload = Box<dyn Any + Send + 'static>;

pub struct Scope<'a>
{
    root:   HeapMut<ThreadPoolInner>,
    latch:  Latch,
    marker: PhantomData<&'a mut &'a ()>,
    panic:  UnsafeCell<Option<PanicPayload>>,
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
            panic:  UnsafeCell::new(None),
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
        self.panic.with_mut(|p| unsafe { (*p).take() })
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
            Ok(()) =>
            {}
            Err(back) =>
            {
                back.run_once();
            }
        }
    }

    #[inline]
    fn new_job<F: FnOnce() + Send + 'a>(&self, f: F) -> Job
    {
        // make sure to initialize the ticket before moving it into the closure
        let ticket = self.latch.ticket();

        let result = Job::new(move || {
            let ticket = ticket;

            // If a panic occurs, we should stop all current and subsequent tasks
            if ticket.is_canceled()
            {
                drop(ticket);
                return;
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(f))
            {
                self.record_panic(payload);
            }
            drop(ticket);
        });
        debug_assert!(size_of_val(&result) <= 64, "the job size exceeds 64 bytes");
        result
    }

    #[inline]
    fn record_panic(&self, payload: PanicPayload)
    {
        self.panic.with_mut(|slot| unsafe { *slot = Some(payload) });
    }
}
