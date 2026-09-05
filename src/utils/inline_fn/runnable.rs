pub trait Runnable: FnOnce() + Send {}

impl<F> Runnable for F where F: FnOnce() + Send {}
