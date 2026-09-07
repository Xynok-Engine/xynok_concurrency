//! ## Containers shared between threads
//!
//! Data structures that several threads can move in and out of, kept apart from the pool logic so
//! they can be reused elsewhere.
//!
//! ### What lives here
//!
//! So far just the ring buffer family: a fixed size region of memory used as a circle, laid out so
//! the two sides touch different ends and step on each other as little as possible.

pub mod ring_buffer;
