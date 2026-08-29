//! src: https://github.com/crossbeam-rs/crossbeam/blob/main/crossbeam-utils/src/backoff.rs
use crate::apis::consts::SPIN_LIMIT;
use crate::sync::spin_loop;
use crate::sync::thread::yield_now;
pub struct BackoffManual
{
    step:       u32,
    spin_limit: u32,
}
impl BackoffManual
{
    pub fn new(spin_limit: u32) -> Self
    {
        Self { step: 0, spin_limit }
    }

    #[inline]
    pub fn spin(&mut self)
    {
        for _ in 0..1 << self.step.min(self.spin_limit)
        {
            spin_loop();
        }

        if self.step <= self.spin_limit
        {
            self.step += 1;
        }
    }

    #[inline]
    pub fn snooze(&mut self)
    {
        match self.step <= self.spin_limit
        {
            true =>
            {
                for _ in 0..1 << self.step
                {
                    spin_loop();
                }
            }
            false =>
            {
                yield_now();
            }
        }

        if self.step <= self.spin_limit
        {
            self.step += 1;
        }
    }

    #[inline]
    pub fn is_completed(&self) -> bool
    {
        self.step > self.spin_limit
    }
    #[inline]
    pub fn rounds(&self) -> u32
    {
        self.step
    }
    #[inline]
    pub fn reset(&mut self)
    {
        self.step = 0;
    }
}

impl std::fmt::Debug for BackoffManual
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("Backoff")
            .field("step", &self.step)
            .field("is_completed", &self.is_completed())
            .finish()
    }
}

impl Default for BackoffManual
{
    fn default() -> Self
    {
        Self::new(SPIN_LIMIT)
    }
}
