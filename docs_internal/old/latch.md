---
title: Latch
excerpt: The countdown every join point in the engine is built from, and the one rule that keeps it sound.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/f/f1/Fork_join.svg/1280px-Fork_join.svg.png
tags:
  - concurrency
  - data_structure
---
## Overview

Splitting work across threads is the easy half. Knowing when it is finished is the hard half.

Every parallel API in this crate ends at the same question. A scope has to hold its stack frame open until the last spawned job has run. A graph node cannot start until all of its parents report back. Different names, same shape: run `n` pieces of work, then wake me up.

A latch is that shape on its own. There is a number on a whiteboard, whoever finishes their part subtracts one, and whoever subtracts it to zero has the duty of waking the waiter up. Everything else in the crate that waits for jobs is this counter plus a way to hand closures to the pool.

## Data Structure

The latch itself holds two things:

* **A counter**, the number of jobs that have not reported back yet. Zero means done.
* **A thread handle**, captured when the latch is built. That is why a latch has to be built on the thread that will wait on it: it is the only thread a ticket knows how to wake.

Handing work out produces a **ticket**, one per outstanding job. A ticket subtracts one when it is dropped, and the last one out wakes the waiter. There is no `done()` method to remember, because `Drop` also runs while a panic unwinds, so a job that dies halfway still reports back instead of leaving the waiter counting forever.

A ticket carries a raw pointer to the latch rather than a reference, because it has to travel inside a job and a job is `'static`. It also carries its own copy of the thread handle rather than reading it from the latch, and that copy is not redundancy: the instant the counter hits zero the waiter may return and its stack frame may be gone, so reading the handle out of the latch after that point is reading dead memory.

> [!IMPORTANT]
> **The one rule**
>
> Waiting does not return until every ticket has been dropped. The latch lives on the waiter's stack and tickets point at it with a raw pointer, and those two facts only coexist because the waiter refuses to leave that frame while a ticket is alive. Building a latch, handing out tickets, and walking away without waiting is a use after free, not a small bug.

## Operational Mechanism

### Handing out work
* The counter goes up when the ticket is created, before the job reaches the pool. Counting afterwards leaves a window where the counter reads zero while a job is already in flight, and a waiter looking in that window tears down the frame the job is still borrowing.
* Because the count grows as jobs are spawned, a latch normally starts at zero. A zero latch is already done and any wait on it returns immediately, which is exactly right for a scope that spawned nothing.

### Reporting back
* Dropping a ticket subtracts one and reads the previous value.
* If the previous value was one, this ticket brought the count to zero and it wakes the waiter using its own copy of the handle. From that line onwards it must not touch the latch again.
* The subtraction releases and the counter reads acquire, so everything a job wrote before releasing its ticket is visible to the waiter once it sees zero. Without that pairing a scope could return while its own results were still sitting in another core's store buffer.

### Waiting
* **Inside the pool**, the waiter runs other jobs instead of sleeping. A participant that blocks is a lost core at the worst possible moment, and with nested joins the sleeping thread can be the very one that was supposed to run the job it is waiting for.
* **Outside the pool**, the waiter parks. There is no ring to load jobs into, so spinning would just burn a core.
* Parking sits in a loop that re-reads the counter, because `park` may return for no reason and an early `unpark` makes it return at once. The counter is the only source of truth.
* Waiting on the wrong thread panics, in release builds too, since it would park a thread nobody will ever wake. A stack trace is a better trade than a silently frozen process.

## References
- https://doc.rust-lang.org/std/thread/fn.park.html
- https://github.com/rayon-rs/rayon/blob/main/rayon-core/src/latch.rs
- https://doc.rust-lang.org/nomicon/atomics.html
- https://docs.rs/loom/latest/loom/
