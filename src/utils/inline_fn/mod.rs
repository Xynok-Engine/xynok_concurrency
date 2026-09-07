//! ## Packing up a job without touching the heap
//!
//! A job in this pool is just a closure. The usual way to store a closure and run it later is to
//! box it behind a heap pointer, but then every hand off is another allocation, and running it
//! means hopping through one more pointer before reaching the data.
//!
//! For small jobs that come in bulk, that setup alone costs more than the work itself.
//!
//! ### How it works
//!
//! Here a job has a fixed size, exactly one cache line. A closure small enough sits right inside
//! it, with no allocation and no pointer in between. Only a closure that is too big falls back to
//! the old way, and the call site never has to know which path it took.
//!
//! Because the size is fixed, jobs pack tightly into the queue and the ring buffer, so reading them
//! in order is one continuous read instead of chasing pieces scattered across the heap.
//!
//! ### Borrowing outside data
//!
//! Besides the usual path for closures that live on their own, there is a path for closures that
//! borrow from the caller's stack. That is what scope needs, and it also avoids the allocation that
//! boxing would charge.
//!
//! > [!IMPORTANT]
//! > The borrowing path is `unsafe`, and the caller has to guarantee the job finishes before what
//! > it borrows goes away. Scope takes care of that for you, but doing it by hand is on you.

pub mod fn_buffer;
pub mod inline_fn;
pub mod runnable;
pub mod unbound_v_table;
pub mod v_table;
pub mod v_table_alias;

pub use inline_fn::InlineFn;
pub use runnable::Runnable;
