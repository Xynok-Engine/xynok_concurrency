---
title: Lane Queue
excerpt: How the engine splits its threads into lanes, and what each lane is built from.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Thread_pool.svg/960px-Thread_pool.svg.png
tags:
  - concurrency
  - data_structure
---
## Overview

A game engine runs work of very different shapes. Physics has to finish inside this frame. Reading a texture off disk might take twenty frames. An audio callback gets a few milliseconds and cannot wait for anything.

Throwing all of that into one thread pool goes badly, because a thread parked inside a file read still holds a core the frame needed. Giving every subsystem its own pool goes badly in the other direction, because six pools of eight threads on an eight core machine means the threads mostly fight each other. So the engine splits its threads into lanes, grouped by how the work runs rather than by what it is called. See [lanes.md](lanes.md) for that split.

Every lane that owns a pool has one `LaneQueue`. A dedicated lane is a single thread and takes work over a bounded SPSC ring instead, which is what lets the audio thread avoid locks and allocation entirely.

## Data Structure

A pool lane is built from four pieces:

* **The lane queue**, one per lane. An unbounded MPMC queue, so many threads write to it and many read from it, and it never refuses a job. It is the lane's front door for anything that does not own a ring, and it is also where a worker dumps the overflow when its own ring fills up.
* **The workers**, the threads that actually run jobs. Each owns exactly one ring and is the only thread allowed to push into it. A worker is not tied to a kind of work, which is the whole reason one shared pool beats several small ones.
* **The local rings**, one per worker. A fixed size array with one writer and many readers. This is the hot path, and most jobs never travel any further. The owner pushes and pops at one end while other workers steal from the other. Both `RingBufferFifo` and `RingBufferLifo` exist and a lane picks whichever it wants.
* **The LIFO slot**, a single slot in front of the ring holding the job that was just spawned. Its cache lines are still warm, so running it immediately is the cheapest thing a worker can do.

> [!IMPORTANT]
> **The lane queue is the slow tier, and it is supposed to be**
>
> A healthy lane keeps its queue nearly empty, because jobs land there and get pulled up into a ring right away. A queue that is always full means the rings are too small, or the workers are being starved.

## Operational Mechanism

### Three ways in
* **From outside.** Any thread that is not a worker of this lane pushes into the lane queue, one job at a time.
* **From a worker whose ring filled up.** It pulls the older half out of its ring and drops the whole batch into the queue, which guarantees room for the job it was trying to push. The ring's hard limit stops being a failure and becomes a spill threshold.
* **From another lane.** A blocking job that finished decoding a texture and wants the GPU upload done on the compute lane owns no ring over there, so it goes through that lane's queue, the same path as submitting from outside.

### One way out, in batches
The lane queue is the one place every thread in the lane meets, so taking a single job per visit makes every job pay for a round of contention. Taking a cluster shares that cost: one visit, then the rest of the jobs run out of the ring without touching anyone. A worker keeps one job to run now and loads the rest into its ring.

The cluster size is the smallest of three limits:

* `len / n_workers + 1`, this worker's fair share, so the first one to arrive does not take everything.
* `capacity / 2`, leaving half the ring free for the jobs this worker is about to spawn.
* The space actually left in the ring.

### Where a worker looks for work
1. The LIFO slot, the hottest job, just spawned.
2. Its local ring, cheap and uncontended.
3. The lane queue, holding work from outside and work that was spilled.
4. Stealing from a sibling, which is expensive because it has to CAS into another worker's ring.
5. The lane queue one last time before sleeping.
6. Parking.

Step 3 comes before step 4 because asking one queue is cheaper than picking a random victim and fighting for it, and because jobs sitting in the queue are usually older. Step 5 exists because a job can arrive between step 3 and the moment the worker actually sleeps; it narrows that window without closing it, and the rest is the [sleep protocol's](sleep_protocol.md) job.

One more rule keeps the queue from starving. A worker running only its slot and its ring can feed itself forever, since jobs spawn children and the ring never empties, so every 61st pass it checks the lane queue first even with a ring full of work.

## Implementation Notes

`LaneQueue` is currently a plain queue behind a spin lock, not a lock-free structure. Each lane has its own queue so contention is already low, and a structure I fully understand is one I can fix at two in the morning. That choice only holds because every hot path touches the lock in batches, spreading the cost over a few dozen jobs. Touch it once per job and the lock would not survive.

The planned replacement is what crossbeam and Bevy both use: a linked list of blocks, where most pushes are a single atomic bump inside the current block and only a full block allocates. Two things would trigger the switch. One is measurement showing workers really do wait on the lock. The other is priority inversion, since a low priority blocking thread can be preempted while holding a lock a compute worker is spinning on.

## References
- https://docs.rs/crossbeam-deque/latest/crossbeam_deque/struct.Injector.html
- https://github.com/smol-rs/async-executor/blob/master/src/lib.rs
- https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/scheduler/inject.rs
- https://github.com/golang/go/blob/master/src/runtime/proc.go
- https://en.wikipedia.org/wiki/Work_stealing
