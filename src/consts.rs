/// How many times a waiter spins before it gives up the CPU and calls `yield_now`.
///
/// The budget is a *duration* in disguise: it should cover roughly how long a critical section
/// runs, so that a waiter rides out the common case in user space instead of paying for a syscall
/// on a lock that was about to be free anyway. Past that point the owner is most likely not
/// running at all - preempted, or sitting on a slower core - and no amount of spinning makes it
/// run sooner.
///
/// Do not raise this much: the cost of getting it wrong is asymmetric. Too low wastes a syscall
/// per handoff; too high burns whole timeslices while the owner waits for a core, and that reads
/// as *faster* in a `ns/op` benchmark because one thread stops sharing the lock and runs to
/// completion while the rest starve. Watch `threads done at` in `examples/bench_spin.rs`.
pub const SPIN_LIMIT: u32 = 64;
