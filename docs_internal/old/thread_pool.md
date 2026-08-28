---
title: Thread Pool
excerpt: One lane's work-stealing pool: N workers, one ring each, one shared queue, and the rules that keep work moving.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/a/a5/Multithreaded_process.svg/1280px-Multithreaded_process.svg.png
tags:
  - concurrency
  - scheduling
---
## Overview

The pool is one lane's engine room. It owns N worker threads, gives each of them a local ring, puts one shared queue underneath, and keeps moving jobs between the three until someone shuts it down.

Two facts shape everything else. The thread that built the pool is a participant rather than a spectator, so the configured thread count is how many workers get **spawned** and the number of things running jobs is one more than that, which is why the default is `cores - 1`. And handles are cheap clones pointing at the same pool, so the threads stop and get joined when the last handle drops, or earlier on request.

Configuring a pool with zero threads is not a degenerate mode kept around for completeness. Nothing is spawned and every job runs inline on the caller, which is how you answer "is this bug caused by parallelism" without editing a line of code.

## Data Structure

Each participant owns two places to keep work, and the whole lane shares a third:

* **A LIFO slot** holding exactly one job, the one just spawned. Its cache lines are the warmest thing around, so running it next is the cheapest move available. Nobody can steal from this slot, which is why it has to be flushed before its owner leaves the loop.
* **A local ring**, holding the rest and open to thieves. See [ring_buffer.md](ring_buffer.md).
* **The lane queue**, catching everything that overflows and serving as the only door a non-participant can push through. See [lane_queue.md](lane_queue.md).

Which one a new job goes to is decided by a thread local record of where this thread is standing: which pool it belongs to, which seat it holds, and whether it is currently inside the job loop. That last flag is the interesting one. Inside the loop the LIFO slot is the best destination, since this same thread comes back for it right after the current job. Outside the loop it is the worst, because nobody can steal it and its owner is not looking.

> [!IMPORTANT]
> **The seat number is checked against the pool identity, not just the range**
>
> Building two pools on the same thread is enough to break a range-only check: the second registration overwrites the first, and the host reads back a number that also happens to be a valid seat in the first pool. Two threads pass the test, two threads get the same slot, two threads write to the same place.

## Operational Mechanism

### How a worker looks for work
1. The LIFO slot, the freshest job with the hottest cache.
2. Its own local ring.
3. The lane queue, holding outside work and anything spilled.
4. Stealing, which is expensive because it means a CAS into someone else's ring. It starts at the next seat rather than at zero, so hungry workers do not queue up on the same victim.
5. The lane queue one last time before sleeping.
6. Sleeping. See [sleep_protocol.md](sleep_protocol.md).

Every 61 rounds step 3 is promoted ahead of step 1. Without that rule a worker whose jobs keep spawning child jobs feeds itself forever, and a job pushed from outside can wait a very long time while the pool looks perfectly busy. The number is prime on purpose: a constant that divides evenly into the worker count, or into the spawn rhythm of a divide and conquer algorithm, makes several workers check the queue on the same beat and then contend on the same lock.

### When a ring fills up
* A full ring is not an error, it is the signal to spill. The owner dumps the older half into the lane queue and pushes again, so the hard bound becomes a spill threshold.
* What gets spilled is the coldest half, the jobs where it makes no difference who runs them.
* The second push can still fail if a thief holds the whole region, and the job then goes straight to the lane queue, which never refuses one.

### Waiting without idling
* A participant waiting for something runs pool jobs until the condition holds, because blocking loses a core at the worst moment and, with nested joins, the sleeping thread can be the one that was supposed to run the job it is waiting for.
* The condition is checked constantly, so it has to stay cheap. One atomic load is right.
* A thread belonging to no pool can wait too, it just cannot help, since it owns no ring to load jobs into. It spins, then naps in short slices as a deadline for the worst case where the awaited thing completes without waking anyone.

### Shutting down
1. Raise the shutdown flag, so no worker starts a new round.
2. Wake everyone, since a parked thread cannot know it is time to die.
3. Let workers finish what is still queued rather than throwing it away, because thrown away jobs are counted forever by whatever is waiting on them.
4. Join each thread.

Step 4 is skipped when the caller is itself a worker of this pool, because joining yourself is undefined behaviour rather than a tidy hang, and it is reachable by accident whenever a job holds the last pool handle. The trade is that shutdown then returns before the workers have actually stopped, so the clean path is still to shut down from the thread that built the pool.

Anything pushed after the last worker's final look is drained on the calling thread. That drain only steals, never using anyone's producer handle, because shutdown can run on any thread and building a second producer for a ring whose owner is mid-operation is exactly what a producer forbids.

### Panics
A panic escaping a worker would skip the bookkeeping and hang every join point counting that job, so each job runs inside a `catch_unwind` and the payload is dropped there. Jobs with a way to report back have already caught their own panic and re-raise it at the join, and a job handed straight to the pool has no join point to hand a payload to, while the panic hook has already printed the message and backtrace.

## References
- https://en.wikipedia.org/wiki/Work_stealing
- https://github.com/rayon-rs/rayon/blob/main/rayon-core/src/registry.rs
- https://github.com/golang/go/blob/master/src/runtime/proc.go
