#![allow(unused)]
pub type Job = Box<dyn FnOnce() + Send + 'static>;
