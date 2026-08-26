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

Throwing all of that into one thread pool goes badly. A thread parked inside a file read still holds a core the frame needed. Giving every subsystem its own pool goes badly in the other direction: six pools of eight threads on an eight core machine means the threads mostly fight each other.

So the engine splits its threads into **lanes**. A lane is a group of threads that handles work of one shape. The split is by how the work runs, not by what it is called.

## The Lanes

| lane | what runs there | threads | why it is separate |
|---|---|---|---|
| **Compute** | frame logic, ECS systems, physics, character movement, culling, animation, particles, meshing, recording command buffers | one work-stealing pool, `N = cores - 1`, with the main thread joining as the Nth | CPU bound and never blocks, so it wants a hot cache and every core it can get |
| **Blocking** | file reads, texture decode, shader compile, decompression | its own small pool, 2 to 4 threads, low priority | the time is spent inside syscalls, and a thread waiting on the disk must not hold a core the frame needs |
| **Dedicated** | audio callback, the main and window thread, GPU submit when the driver demands it | one thread each, in no pool, never steals | hard constraints, either a realtime deadline or a rule that it must be that exact thread |

Physics, rendering and GPU work are not three separate lanes. They are groups of jobs inside the Compute lane, told apart by where they sit in the job graph. Giving physics its own pool would mean the render cores sit idle while physics runs, and the other way around. Only the parts with a hard thread requirement, such as the actual submit call on drivers that demand it, get pulled out into a dedicated lane.

Only the lanes that own a pool have a `LaneQueue`. A dedicated lane is a single thread, so it takes work over a bounded SPSC ring instead, which is what lets the audio thread avoid locks and allocation entirely.

## What a Lane Is Made Of

Every pool lane is built from the same pieces.

```text
   LANE QUEUE   [j][j][j][j][j][j][j][j][j]    unbounded, anyone can write
        │
        │  steal_batch_and_pop: keep one job, load the rest into a ring
        │
        ├────────────────┬────────────────┐
        ▼                ▼                ▼
   ┌─────────┐      ┌─────────┐      ┌─────────┐
   │  slot   │      │  slot   │      │  slot   │   one hot job
   ├─────────┤      ├─────────┤      ├─────────┤
   │  ring   │◀────▶│  ring   │◀────▶│  ring   │   bounded, one writer
   └─────────┘steal └─────────┘steal └─────────┘
    worker 0         worker 1         worker 2
```

**The lane queue**

One per lane. An unbounded MPMC queue, so many threads write to it and many read from it, and it never refuses a job.

It is the lane's front door. Anything that does not own a ring pushes here: the main thread submitting work, an I/O thread reporting a finished load, a job on another lane handing back a result. It is also where a worker dumps the overflow when its own ring fills up.

It is the **slow tier**, and it is supposed to be. A healthy lane keeps its queue nearly empty, because jobs land there and get pulled up into a ring right away. A queue that is always full means the rings are too small, or the workers are being starved.

**The workers**

The threads that actually run jobs. Each one owns exactly one ring and is the only thread allowed to push into it.

A worker is not tied to a kind of work. It takes whatever job comes next, which is the whole reason one shared pool beats several small ones.

**The local rings**

One per worker. A fixed size array with one writer and many readers. This is the hot path, and most jobs never travel any further than here.

The owner pushes and pops at one end. Other workers in the same lane steal from the other end when they run dry. Both flavours exist, `RingBufferFifo` and `RingBufferLifo`, and a lane picks whichever it wants.

Because the ring is bounded, `push` can fail. That is not an error, it is the signal to spill.

**The LIFO slot**

A single slot sitting in front of the ring, holding the job that was just spawned. Its cache lines are still warm, so running it immediately is the cheapest thing a worker can do.

## How Work Moves

### Three ways in

**From outside.** Any thread that is not a worker of this lane pushes into the lane queue, one job at a time.

```rust
match Worker::current()
{
    Some(worker) => worker.push(job),  // already a worker: straight into its ring
    None => self.lane_queue.push(job), // everyone else: into the lane queue
}
```

**From a worker whose ring filled up.** The worker pulls the older half out of its ring and drops the whole batch into the lane queue, which guarantees room for the job it was trying to push. The ring's hard limit stops being a failure and becomes a spill threshold.

**From another lane.** A job on the Blocking lane finishes decoding a texture and wants the GPU upload done on the Compute lane. It owns no ring over there, so it goes through that lane's queue. Same path as submitting from outside.

### One way out, in batches

```rust
// take a cluster, keep one job to run now, load the rest into the local ring
let job = lane_queue.steal_batch_and_pop(&mut self.ring, n_workers);
```

The lane queue is the one place every thread in the lane meets, so taking a single job per visit makes every job pay for a round of contention. Take a cluster and that cost is shared: one visit, then the rest of the jobs run out of the ring without touching anyone.

The cluster size is the smallest of three limits:

* `len / n_workers + 1`, this worker's fair share, so the first one to arrive does not take everything
* `capacity / 2`, leaving half the ring free for the jobs this worker is about to spawn
* `remaining`, the space actually left in the ring

### Where a worker looks for work

```text
 1. LIFO slot        hottest job, just spawned
 2. local ring       cheap, no contention
 3. lane queue       work from outside, and work that was spilled
 4. steal a sibling  expensive, has to CAS into another worker's ring
 5. lane queue       one last look before sleeping
 6. park
```

Step 3 comes before step 4 because asking one queue is cheaper than picking a random victim and fighting for it, and because jobs sitting in the queue are usually older.

Step 5 exists because a job can arrive between step 3 and the moment the worker actually sleeps. It narrows that window without closing it, and the rest is the sleep protocol's job.

One more rule keeps the queue from starving. A worker running only its slot and its ring can feed itself forever, since jobs spawn children and the ring never empties. So every 61st pass it checks the lane queue first, even with a ring full of work.

## Implementation Notes

`LaneQueue` is currently a plain queue behind a spin lock, not a lock-free structure. Each lane has its own queue so contention is already low, and a structure I fully understand is one I can fix at two in the morning.

That choice only holds because every hot path touches the lock in batches, spreading the cost over a few dozen jobs. Touch it once per job and the lock would not survive.

The planned replacement is what crossbeam and Bevy both use: a linked list of blocks, where most pushes are a single atomic bump inside the current block and only a full block allocates. Two things would trigger the switch. One is measurement showing workers really do wait on the lock. The other is priority inversion, since a low priority Blocking thread can be preempted while holding a lock a Compute worker is spinning on.

## Status

Both rings and the lane queue are written and tested. The worker loop that ties them together does not exist yet, so the search order above describes a design rather than running code. `spill_half` exists on the FIFO ring with no caller, and the LIFO ring still needs its own.

## References
- https://docs.rs/crossbeam-deque/latest/crossbeam_deque/struct.Injector.html
- https://github.com/smol-rs/async-executor/blob/master/src/lib.rs
- https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/scheduler/inject.rs
- https://github.com/golang/go/blob/master/src/runtime/proc.go
- https://supertech.csail.mit.edu/papers/steal.pdf
