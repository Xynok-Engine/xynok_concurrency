pub mod consumer;
pub mod producer;
pub mod spmc_ring_buffer_fifo;

pub use consumer::Consumer;
pub use producer::Producer;
pub use spmc_ring_buffer_fifo::SpmcRingBufferFifo;

#[cfg(all(test, not(loom)))]
#[path = "tests/unit.rs"]
mod unit_test;

#[cfg(all(test, loom))]
#[path = "tests/loom.rs"]
mod loom_test;
