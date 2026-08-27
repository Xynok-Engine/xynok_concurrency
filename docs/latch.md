---
title: Latch
excerpt: The countdown every join point in the engine is built from, and the one rule that keeps it sound.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/f/f1/Fork_join.svg/1280px-Fork_join.svg.png
tags:
  - concurrency
  - data_structure
---

## Overview

Splitting work across threads is the easy half. The hard half is knowing when it is finished.

Every parallel API in this crate ends at the same question. `scope` has to hold the stack frame open until the last spawned job has run. `parallel_for` has to return only after every batch is done. A job graph node cannot start until all of its parents report back. Different names, same shape: run `n` pieces of work, then wake me up.

`Latch` is that shape, on its own, in about a hundred lines. Everything else in the crate that waits for jobs is this counter plus a way to hand closures to the pool.

The idea fits in one sentence. There is a number on a whiteboard. Whoever finishes their part subtracts one. Whoever subtracts it to zero has the duty of waking the waiter up.

## The Two Pieces

```text
        waiter's stack frame
   ┌──────────────────────────────┐
   │  Latch                       │
   │    remaining: AtomicUsize ───┼──── 3
   │    waiter:    Thread         │
   └──────────────────────────────┘
        ▲          ▲          ▲
        │          │          │      raw pointer + its own Thread copy
   ┌────┴───┐ ┌────┴───┐ ┌────┴───┐
   │ ticket │ │ ticket │ │ ticket │  each one lives inside a job
   └────────┘ └────────┘ └────────┘
     job A      job B      job C
```

**`Latch`** holds the counter and a handle to the thread that has to be woken. The handle is captured in `new`, which is why a latch must be built on the thread that will wait on it. That is the only thread a ticket knows how to wake.

**`LatchTicket`** is the receipt for one outstanding job. It subtracts one when dropped, and the last one out calls `unpark`. There is no `done()` method to remember to call, and that is deliberate: `Drop` runs on the panic path too, so a job that dies halfway through still reports back instead of leaving the waiter counting forever.

A ticket carries two things, and both choices are load bearing.

**A raw pointer instead of a reference.** A ticket has to travel inside a `Job`, and a job is `'static`. A `&'a Latch` would drag a lifetime along and never fit. This is the one real difference from [`Waker`](../src/utils/waker.rs), which is the same counter with a borrowed ticket. `Waker` is used for pool lifecycle work such as waiting for workers to come up, where nothing needs to be smuggled into a job. `Latch` is used everywhere a job has to hold the receipt.

**Its own copy of the `Thread` handle**, rather than reading `latch.waiter` at the moment the count hits zero. That copy is not redundancy, it is the fix for a real race, and the next section is about why.

## The One Rule

> **`wait_in` or `wait` does not return until every ticket has been dropped.**

The latch lives on the waiter's stack. Tickets point at it with a raw pointer. Those two facts only coexist because the waiter refuses to leave its stack frame while any ticket is still alive. Build a latch, hand out tickets, and walk away without waiting, and you have written a use after free, not a small bug.

Everything else in the file follows from keeping that rule true.

### Count up before the job can run

```rust
pub fn ticket(&self) -> LatchTicket
{
    self.remaining.fetch_add(1, Ordering::Relaxed);
    LatchTicket { latch: self as *const Latch, waiter: self.waiter.clone() }
}
```

The increment happens when the ticket is created, before the job is handed to the pool. Doing it the other way around, pushing the job first and counting afterwards, opens a window where the counter reads zero while a job is already in flight. A waiter that looks in exactly that window returns and tears down the frame that the job is still borrowing.

This is also why `Latch::new(0)` is the normal way to start. The count is not known up front, it grows as jobs are spawned. A latch built at zero is already done and any wait on it returns immediately, which is exactly right for a scope that spawned nothing.

### The last ticket must not touch the latch

```rust
let previous = latch.remaining.fetch_sub(1, Ordering::AcqRel);

if previous == 1
{
    // the latch may already be gone from here on
    self.waiter.unpark();
}
```

The moment the counter is written to zero, the waiter is allowed to observe it, return, and let its stack frame go. The ticket doing the writing may still be a few instructions from finishing. So after that `fetch_sub`, `self.latch` is a pointer to memory that might have been reclaimed already, and reading `latch.waiter` to find out who to wake is reading a dead stack frame.

Hence the copy in the ticket. `self.waiter` belongs to the ticket, and the ticket is still alive because we are inside its `drop`. It stays valid no matter what the waiter is doing.

### Memory ordering

`fetch_add` is `Relaxed`, and that is enough. Every read modify write on one atomic lands in a single total order, so no increment can be lost or reordered against another. `Relaxed` only gives up ordering for *other* memory, and at the moment a ticket is created there is nothing yet to publish.

The ordering that matters is on the way out. The drop side uses `AcqRel` and `remaining()` loads with `Acquire`, so everything a job wrote before releasing its ticket is visible to the waiter once it sees zero. Without that pairing a scope could return while the results of its own jobs were still sitting in another core's store buffer.

## Two Ways to Wait

The counter is shared, but how you wait depends on where you are standing.

### `wait_in`: work while waiting

```rust
pub fn wait_in(&self, pool: &ThreadPool)
{
    pool.run_until(|| self.is_done());
}
```

This is the path every join point inside the pool takes. The waiting thread is itself a pool participant, so putting it to sleep loses a core at the exact moment the pool needs it most.

With nested joins it gets worse than just wasteful. The sleeping thread can be the very thread that was supposed to run the job it is waiting for, and the program hangs. Running jobs while waiting removes that whole class of deadlock: instead of blocking, the thread keeps pulling work, and the job it is waiting on is one of the things it might pull.

`is_done` is called in a tight loop, so it stays a single atomic load and nothing more.

### `wait`: sleep

For a thread that belongs to no pool. There is no ring to load jobs into and nothing useful to run, so spinning would just burn a core. Park instead, and let the last ticket do the waking.

```rust
while !self.is_done()
{
    thread::park();
}
```

The loop is not decoration. `park` is allowed to return for no reason at all, and an `unpark` that arrives early makes the next `park` return immediately. Neither tells you anything, so the counter is re-read every time around. It is the only source of truth.

`wait` also asserts that it is running on the thread that built the latch, in release builds too. Waiting anywhere else parks a thread that nobody will ever wake, and trading a silently frozen process for a stack trace is a good deal.

## What It Looks Like In Use

`Scope` is the clearest example, since it is not much more than this latch plus a place to store a panic:

```rust
let scope = Scope { latch: Latch::new(0), .. };

let outcome = catch_unwind(AssertUnwindSafe(|| f(&scope)));
shared.run_until(|| scope.latch.is_done());
```

Every `scope.spawn` takes a ticket before the closure goes anywhere near the pool. The wait happens even when `f` panicked, because leaving jobs running while the stack frame they borrow from is being unwound is precisely the use after free that scopes exist to prevent.

Ticket drop order inside a job matters for the same reason. The ticket is declared first so it drops last, after the panic has been recorded. Releasing it earlier would let the waiter return and take the panic slot with it while the job was still trying to write into it.

## Testing

Two layers, because they catch different things.

**Loom** explores the interleavings by hand, on two jobs and one waiter. One model checks that the wait always terminates no matter what order things happen in. The other drops a ticket *before* the waiter has had a chance to park, which is the classic lost wakeup, the one that shows up once a week in production and never on your machine.

**Stress tests** run real threads: a thousand jobs through a three thread pool, sixteen jobs that all panic, a wait from outside the pool, and a wait on the wrong thread that has to panic instead of hanging. Loom proves the ordering is sound, real threads prove the thing survives contact with an actual scheduler.

## Related Documentation

- [`src/latch.rs`](../src/latch.rs): the implementation and its tests.
- [`src/scope.rs`](../src/scope.rs): the main consumer, one latch per scope.
- [`src/utils/waker.rs`](../src/utils/waker.rs): the same counter with a borrowed ticket, used for pool lifecycle.
- [`docs/lane_queue.md`](lane_queue.md): where the jobs a latch counts actually run.

External reading:

- [`std::thread::park`](https://doc.rust-lang.org/std/thread/fn.park.html): spurious wakeups and early unparks, straight from the docs.
- [Rayon's latch module](https://github.com/rayon-rs/rayon/blob/main/rayon-core/src/latch.rs): the same idea taken much further, with a latch type per use case.
- [`Ordering` in the nomicon](https://doc.rust-lang.org/nomicon/atomics.html): why `Relaxed` on the way in and `AcqRel` on the way out.
