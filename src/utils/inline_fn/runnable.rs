pub trait Runnable: FnOnce() + Send + 'static {}

impl<F> Runnable for F where F: FnOnce() + Send + 'static {}
