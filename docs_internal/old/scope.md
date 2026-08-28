---
title: Scope
excerpt: Fork-join with borrowed stack data, and the parallel helpers built on top of it.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/8/88/Fork-join_computation.svg/1280px-Fork-join_computation.svg.png
tags:
  - concurrency
  - api
---
## Overview

A job handed to the pool has to be `'static`. That is an honest requirement: the pool has no idea when the job will run, so it cannot hold a reference to something that might go away first.

Inside a scope the requirement lifts. A scope does not return until every job it spawned is finished, including while it is unwinding from a panic, so anything a job borrowed is guaranteed to outlive it. A local array can be split four ways and written in parallel with no `Arc`, no `Mutex`, and no channel to collect the results out of.

This is the layer everything above calls into. An ECS scheduler chunking a system, a frame graph running passes in parallel, or a `for` loop heavy enough to be worth splitting, all of it ends up here.

> [!IMPORTANT]
> **Do not hold a lock while calling into this crate**
>
> A thread waiting on a scope does not sit idle, it runs other jobs from the pool. That is the only way nested parallelism works at all. It also means an unrelated job can run underneath a paused one, on the same thread, and if that job asks for the lock the paused one is holding, the thread deadlocks against itself. Neither job is wrong, and nothing here can detect it for you.

## Data Structure

A scope holds four things:

* **The pool's shared half**, without the worker join handles. Keeping a whole pool handle here would mean a scope opened inside a job drops that handle on a worker thread, and if it is the last one, shutting down joins every worker including the one running that very line.
* **A latch**, one ticket per spawned job. See [latch.md](latch.md).
* **A panic slot**, holding the first panic any job threw so it can be re-raised at the join.
* **An invariant lifetime marker**, so the compiler cannot quietly stretch or shrink the scope's lifetime to make a call typecheck.

Jobs reach back into the scope through raw pointers rather than references, because a reference would borrow a frame that lives on someone else's stack. What keeps them valid is the same promise as always: the scope does not return while a ticket is alive.

## Operational Mechanism

### The lifecycle
1. Build the scope, then run the body inside a `catch_unwind`.
2. Wait for the latch, **on every path**. If the body panicked, the jobs it already spawned still hold references into a frame being torn down, and letting them run on is precisely the use after free a scope exists to prevent.
3. Re-raise a panic if there was one. A panic from the body wins over a panic from a job, and among jobs the first one recorded wins, because picking the last would mean picking by finishing order, which differs every run.

### Spawning
* The latch ticket is taken at spawn time, not when the job starts, or a waiter could read zero while a job is already queued.
* Every job catches its own panic. A panic escaping a worker would skip the ticket drop and hang the join forever.
* The ticket is dropped last, after the panic has been recorded, because releasing it earlier lets the waiter return and take the panic slot with it.
* `spawn_with` hands the scope back to the job so it can spawn into it. Every recursive divide and conquer needs this, otherwise each level opens its own scope and each new scope is another join point where the whole pool waits for the slowest straggler.

### The helpers
* **`join`** runs one closure on the calling thread and gives the other to the pool. If nobody steals it, the calling thread runs it while waiting, so `join` is never worse than sequential by more than the bookkeeping.
* **`parallel_for`** takes an explicit batch size, because spawning costs 1 to 5 microseconds and a job that runs for less than that is a loss dressed up as busy cores. Aim for roughly 20 microseconds of work per job and derive the batch from a measurement. A batch bigger than the range is sequential, which is a legitimate way to switch parallelism off at one call site.
* Batches are not one job each. The job count equals the participant count, and batches are handed out through one atomic counter, so a worker that finishes early just takes the next one. That is a work stealing deque's behaviour for a tenth of the code.
* **`par_reduce`** folds each batch separately and joins the partials **in batch order** after everything is done. Joining as results arrive is slightly faster and far less predictable, and with floating point a bounding box that drifts by a hair every frame is a bug you spend a week finding. Each batch gets its own result slot rather than each thread, so even the intermediate values are reproducible.

## References
- https://docs.rs/rayon/latest/rayon/fn.scope.html
- https://doc.rust-lang.org/std/thread/fn.scope.html
- https://doc.rust-lang.org/nomicon/subtyping.html
