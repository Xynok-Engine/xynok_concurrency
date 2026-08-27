---
title: The Toolbox
excerpt: The small pieces everything else is assembled from: padding, backoff, spin locks, slots, and the loom shim.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/d/d3/Red_toolbox.svg/1280px-Red_toolbox.svg.png
tags:
  - concurrency
  - utilities
---
## Overview

None of these are interesting on their own, which is the point. Each exists because the same few lines kept appearing in the rings, the pool and the queues, and one copy means one place to fix.

## The Pieces

**Cache padding** aligns a value so it occupies a cache line by itself. Two counters sharing a line means every write by one core invalidates the other core's copy, and the two cores end up trading a line instead of doing work. That is false sharing, and it is invisible in a profiler unless you know to look for it. The alignment is per architecture and 64 is not the universal answer: x86_64, aarch64 and powerpc64 use 128 because the spatial prefetcher pulls pairs of lines, s390x uses 256, arm and mips use 32, and m68k uses 16. It wraps every ring index, every counter block, the sleep protocol's state word, and each per-worker slot.

**Backoff** is the standard escalation for a thread waiting on something short: spin with a doubling number of hints, then start yielding, then report that yielding has stopped helping, which is the caller's cue to park instead. Under loom and miri the limits collapse to almost nothing, because under loom every spin is another branch in the model and under miri every spin is dozens of interpreted instructions. The spin hint is only a performance hint, so removing it changes nothing about correctness.

**The spin lock** is a mutex that never sleeps, an atomic flag plus a backoff. It is right when the critical section is a handful of instructions and the wait would be shorter than a syscall pair. It is wrong when the holder can be preempted while holding it, which is worth remembering around the low priority blocking lane.

**The batching queue** is a plain queue behind that same spin lock, with batch entry points, and it is what the [lane queue](lane_queue.md) is built from. Batching is the whole reason it is a separate type: a lock touched once per job would not survive the traffic, while a lock touched once per few dozen jobs spreads its cost thin enough to disappear.

**The slot array** is the raw storage under all three ring buffers, a run of uninitialised cells plus a mask. Capacity is rounded up to a power of two so mapping an index to a slot is a single bitwise AND rather than a division. Reading, writing and dropping are all unsafe, since knowing whether a slot currently holds a value is the ring's job, not the storage's.

**The steal outcome** is three-way: empty, busy, or success. Busy is separate from empty on purpose. Empty means look elsewhere, while busy means someone is mid-operation here and coming back in a moment is reasonable. Collapsing them into an option throws away exactly the difference a thief needs to decide its next move, and chaining preserves it: a busy attempt followed by an empty one still reports busy.

**The waker** is the earlier generation of the [latch](latch.md), the same countdown but with a ticket that borrows the waker instead of carrying a raw pointer. That borrow is why it cannot go inside a job, and why the latch exists. Nothing in the crate calls it today outside its own tests. If you do use it, note that its count is fixed at construction rather than growing as tickets are handed out, and its same-thread check only runs in debug builds.

**The mutex with a condition variable** bundles the two together and forwards the whole wait family, ignoring poisoning throughout. Ignoring poisoning is a deliberate stance here: poisoning means a thread panicked while holding the lock, but every panic in this crate is already caught and routed to a join point, so refusing the lock afterwards would turn one handled panic into a second, unhandled one.

> [!IMPORTANT]
> **The loom shim**
>
> Everything concurrent in the crate imports its atomics, reference counts, mutexes and thread functions from one internal module rather than from the standard library. Building with the loom flag rebuilds the whole crate against loom's instrumented types, and loom then explores the interleavings exhaustively. It is a compile flag rather than a Cargo feature because it has to replace the atomics at compile time. The shim also fills the gaps: timed parking becomes a yield since loom does not model a clock, and thread names are dropped since loom threads are cooperatively scheduled coroutines with no OS identity. Its cell wrapper is how loom tracks who is touching a value and catches an aliasing violation the moment it happens.

## Odds and Ends

* Packing two 32 bit numbers into one 64 bit word and back, for indices that have to move together in one atomic operation.
* Unwrapping a lock result while taking the inner guard even when poisoned.
* Reading the available core count, defaulting to one when the OS will not say.
* Asking at compile time whether a type fits in a natively atomic word, useful for asserting that a chosen representation is not secretly falling back to a lock.

## References
- https://github.com/crossbeam-rs/crossbeam/tree/main/crossbeam-utils
- https://docs.rs/loom/latest/loom/
- https://doc.rust-lang.org/std/hint/fn.spin_loop.html
