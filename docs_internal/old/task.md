---
title: Async Tasks
excerpt: A future becomes a task, and the task puts itself back in the lane every time it is woken.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/1/1e/Relay_race_%28cropped%29.jpg/1280px-Relay_race_%28cropped%29.jpg
tags:
  - concurrency
  - api
---
## Overview

The async lane does not run threads that sit inside `read()`. It runs tasks, and a task is a future: polling it to `Pending` hands the thread straight back to the lane, and the waker it leaves behind is how anyone else, a job on another lane, a channel that just got its value, one day a reactor, puts it back in the queue.

Nothing new sits underneath. A task is scheduled as an ordinary [job](inline_fn.md) on the lane's own queue, so it competes for the same workers, gets stolen by the same rules, and shows up in the same counters as everything else. The executor is the state machine on top, not a second runtime.

> [!IMPORTANT]
> **There is no reactor yet**
>
> No epoll, no kqueue, no io_uring. A future that calls `File::read` inside `poll` still holds one of the lane's threads until the syscall returns, exactly as before. What changed is the shape: the blocking part is confined to `run_blocking`, which returns a receiver you can await, so a task waiting on I/O is sitting on no thread at all. The day a reactor lands, the thing that changes is `run_blocking`, not the call sites.

## Data Structure

A task is a pinned boxed future, a state cell, and a handle to the lane it belongs to.

The handle is the lane's shared half rather than a pool handle, and that distinction is not cosmetic. A pool handle keeps the pool alive, so a task that happened to drop the last one would make a worker join itself, which is the same trap [scope](scope.md) and [job graph](job_graph.md) hit earlier from a different direction.

The future lives behind a lock that is never contended, because the state cell already guarantees only one worker polls at a time. It is there so the guarantee is stated by the type rather than only in the reader's head.

### Why a flag is not enough

A waker may be called at any moment, any number of times, including while a worker is in the middle of polling. With a single `queued` boolean, that mid-poll call either gets dropped, and the task sleeps forever, or it queues a second copy of a task that is already running, and two workers poll one future.

Five states, and `NOTIFIED` is where a wake that lands mid-poll is remembered until the poll finishes:

```text
   IDLE ──wake──▶ SCHEDULED ──worker picks it up──▶ RUNNING ──Ready──▶ DONE
     ▲                 ▲                               │
     │                 │                            Pending
     └── poll done ────┴── requeue ◀── NOTIFIED ◀──────┴── woken mid-poll
```

Wakes arriving in `SCHEDULED`, `NOTIFIED` or `DONE` are no-ops, which is what a waker contract asks for: spurious wakes have to be harmless.

## Operational Mechanism

### Running a task
* Claim the task by moving `SCHEDULED` to `RUNNING`. Losing that race means the task is already finished, so there is nothing to do.
* Poll once, with the panic caught. A panicking task must not take a worker down with it, and it has nobody to hand the payload to: the panic hook has already printed the message, and whoever awaits the result gets `None` when the sending end drops during the unwind.
* `Ready` drops the future immediately rather than waiting for the last `Arc` to go, so everything it held, including that sending end, is released at once.
* `Pending` tries to move `RUNNING` back to `IDLE`. Failing means somebody woke the task mid-poll, so it goes back into the queue for another round.

### Getting a result out
Spawning returns a [receiver](channel.md), not a bespoke handle, because a receiver is already both things you need: awaitable from inside another task, and drainable with `recv_in` from a thread running compute jobs. A task that panics gives its waiter `None`, the same as a job that panics.

### Waiting from outside
* `block_on` parks between polls. It suits a thread that belongs to no pool, the main thread at startup for instance. Calling it from inside a task is the fastest way to deadlock a two-thread lane; inside a task, `.await` is the only way to wait.
* `block_on_in` waits by running the pool's jobs instead of parking, the same bargain `recv_in` makes: idling wastes a seat in the lane, and with nested joins this thread may be the one that has to run the job the future is waiting on.
* `yield_now` puts the task at the back of the lane's queue and continues. It is not a wait, the waker fires immediately, so the task never sleeps.

## References
- https://doc.rust-lang.org/std/task/trait.Wake.html
- https://rust-lang.github.io/async-book/02_execution/01_chapter.html
- https://tokio.rs/blog/2019-10-scheduler
