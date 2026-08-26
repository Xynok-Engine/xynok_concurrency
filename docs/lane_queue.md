---
title: Lane Queue
excerpt: Where a job goes when there is no local ring to land in, and why it always leaves in batches.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Thread_pool.svg/960px-Thread_pool.svg.png
tags:
  - concurrency
  - data_structure
---
## Overview

The [ring buffer](./ring_buffer.md) handles the hot path well, but it leaves two gaps that a real thread pool runs into immediately.

The first gap is ownership. A ring buffer has exactly one producer, and that producer is the worker thread that owns it. Any thread that is not a worker has nowhere to push. The main thread submitting work at the start of a frame, an I/O thread reporting a finished asset load, a job on another lane handing back a result: none of them own a ring.

The second gap is capacity. A ring is a fixed-size array, so `push` can fail. Refusing the job is a reasonable answer for a data structure and a useless one for a scheduler. Work arrives at whatever rate the game produces it, and "sorry, full" is not something I can hand back to the caller.

`LaneQueue` fills both gaps. It is the queue every non-worker pushes into, and the place a worker spills to when its own ring fills up.

### What a lane is

A lane is a group of threads separated by how they run, not by what they do. Compute work that never blocks lives in one work-stealing pool sized to the machine. Blocking work like file reads and texture decoding gets its own small pool at low priority, so a thread sitting in a syscall never occupies a core the frame needs. Threads with hard constraints, such as the audio callback, stand outside every pool.

Each lane keeps its own `LaneQueue`. Lanes do not share work with each other, so giving them separate queues costs nothing and keeps contention local to the threads that are already cooperating.

## Data Structure

`LaneQueue` is an unbounded MPMC queue shared by one lane, sitting next to the per-worker rings.

```text
                        ┌─────────────────────────────┐
   main thread ────────▶│         LANE QUEUE          │  many writers
   outside thread ─────▶│  [j][j][j][j][j][j][j][j]   │  many readers
   worker spill ───────▶└──────────┬──────────────────┘  unbounded
                                   │
                                   │ steal_batch (take a whole cluster)
                 ┌─────────────────┼─────────────────┐
                 ▼                 ▼                 ▼
            ┌─────────┐       ┌─────────┐       ┌─────────┐
            │ ring W0 │◀─────▶│ ring W1 │◀─────▶│ ring W2 │  one writer
            └─────────┘ steal └─────────┘ steal └─────────┘  bounded
                 ▲                 ▲                 ▲
              worker 0          worker 1          worker 2
```

Alongside the queue itself there is a length counter kept in a separate cache line, readable without touching the queue at all. Every operation updates it while still holding the queue, so it is accurate the moment the queue is released. Readers seeing a slightly stale value is fine, because they only use it to decide whether reaching for the queue is worth it, and a wrong guess costs one wasted attempt.

> [!IMPORTANT]
> **The length counter cannot decide whether a worker sleeps.**
>
> Reading `0` and then parking can miss a job that was pushed in between. Preventing lost wakeups belongs to the pool's sleep protocol, not to this number. This is the one place where a stale read is not harmless.

> [!IMPORTANT]
> **A lane queue does not replace the local rings.**
>
> It is the slow tier, and it has to be. The local ring is where most jobs travel, and every job that detours through the lane queue pays for contention. A healthy pool keeps its lane queue nearly empty, because jobs land there and get pulled up into a local ring right away. If you measure a lane queue that is always full, that is not the queue working well. That is a local ring that is too small, or workers being starved.

## Operational Mechanism

### Three ways in

**Submit from outside the pool.** The obvious one. Main thread, I/O thread, network thread, anyone who is not a worker.

```rust
pub fn spawn(&self, job: Job)
{
    match Worker::current()          // is this thread a worker of the pool?
    {
        Some(worker) => worker.push(job),  // yes: straight into the local ring
        None => self.lane_queue.push(job), // no: into the lane queue
    }
}
```

The `Some` branch is why work-stealing is fast. A job spawned by another job never touches the lane queue at all.

**A worker spilling a full ring.** When a worker's ring fills up, it pulls out the older half and drops it into the lane queue, which guarantees room for the job it was trying to push.

```rust
fn push_local(&mut self, job: Job)
{
    if self.worker.push(job).is_ok()
    {
        return;
    }

    // ring is full: pull out the older half, hand it down, now there is room
    let mut spill = Vec::with_capacity(self.worker.capacity() / 2 + 1);
    self.worker.spill_half(&mut spill);
    spill.push(job);
    self.lane_queue.push_batch(spill);
}
```

This turns the ring's hard limit into a spill threshold. The bound stops being a failure and becomes a scheduling decision.

**Handing work between lanes.** A job on the blocking lane finishes decoding a texture and wants the GPU upload to happen on the compute lane. It owns no ring over there, so it goes through that lane's queue. Same mechanism as submitting from outside.

### One way out, always in batches

```rust
// take several, keep one to run now, load the rest into the local ring
let job = lane_queue.steal_batch_and_pop(&mut self.worker, n_workers);
```

Batching is not an optimization here, it is the whole point. The lane queue is the one place every thread in the lane meets, so taking a single job per visit means every job pays for one round of contention on the same cache line. Take 32 and you pay once, then 31 jobs run out of the local ring without touching anyone.

The cluster size is the smallest of three caps:

* `len / n_workers + 1` is this worker's fair share, plus one so it never comes out as zero while work remains. Without the division, the first worker to arrive takes everything and the rest stay hungry even though there was plenty to go around.
* `capacity / 2` leaves half the ring empty for the jobs this worker is about to spawn. Fill the ring completely and the first child job has to spill straight back down.
* `remaining` is the space actually left. The ring's owner is the only thread allowed to push and that owner is you, so this number can only grow while you work with it.

The job returned to the caller is not counted in any of that, since it never occupies a slot. A single round therefore moves `1 + cluster` jobs out of the queue.

Loading the cluster goes through one `push_iter` call, so the whole batch is written and published once instead of per job. Pulling from the queue stays lazy, which means a job is never taken out only to be pushed back when the ring turns out to be full.

### Where it sits in the worker loop

The lane queue appears exactly twice in the search order, and both appearances have their own reason.

```text
 1. lifo_slot            hottest job, just spawned, cache still warm
 2. local ring           cheap, no contention
 3. LANE QUEUE           ◀── work from outside, and work that was spilled
 4. steal from sibling   expensive: CAS into someone else's head
 5. LANE QUEUE again     ◀── one last check right before sleeping
 6. park
```

**Why does step 3 come before step 4?** Because the lane queue is cheaper than stealing. Step 4 has to pick a random victim, CAS into their `head`, possibly lose and retry, and possibly come back busy because another thief is mid-operation. The lane queue is a single place to ask. Jobs sitting in it also tend to be **older**, and older work should run first.

**Why is there a step 5?** Between the moment a worker finishes step 3 and the moment it actually parks, the main thread can push a job. If the main thread checks "is any worker still searching?" just before this one flips to sleeping, it sees yes and decides not to wake anybody. The job then sits there while the worker sleeps. That is a **lost wakeup**. Step 5 narrows the window but does not close it, and the rest is the job of the sleep protocol. Do not mistake step 5 for a complete fix.

### Fairness

A worker running only `lifo_slot -> local ring` can feed itself **forever**. Jobs spawn children, children spawn grandchildren, the local ring never runs dry, and step 3 never happens. Meanwhile the main thread's work starves in the lane queue. Not "runs late", but starves: there is no upper bound on how long it waits.

The fix is a counter that forces a look:

```rust
self.tick = self.tick.wrapping_add(1);

let job = match self.tick % GLOBAL_QUEUE_INTERVAL == 0
{
    // check the lane queue first, even with a local ring full of work
    true => self.lane_queue.pop().or_else(|| self.worker.pop()),
    false => self.worker.pop().or_else(|| self.lane_queue.pop()),
};
```

61 is the number I plan to start with, borrowed from Tokio. Tokio's multi-thread scheduler actually tunes the interval at runtime, aiming for roughly 61 tasks polled between checks and capping it at 127, while its current-thread scheduler just uses a fixed 31. A prime is the usual choice here so the forced check does not fall into step with some natural period in the workload.

## Why It Cannot Just Be Another Ring

| | local ring | lane queue |
|---|---|---|
| writers | 1, the owner | many |
| bounded | yes, fixed at init | no |
| allocation | once | as needed |
| how often it is touched | very often | rarely |
| what a push costs | one `store(Release)` | a shared write others contend for |

A ring trades flexibility for speed. A lane queue trades speed for never refusing a job. Push one structure to carry both and you get something mediocre at each.

Concretely, using a ring as the lane queue means `push` becomes a CAS loop, which throws away the single-release-store that made it fast in the first place. And the bound comes right back: you have moved the second gap up a layer rather than closing it, because now you have to answer where a full lane queue spills to.

## Current Implementation

The version in [`src/lane_queue.rs`](../src/lane_queue.rs) is built on `QueueBatching`, which is a plain queue behind a spin lock. It is **not** lock-free, and that is deliberate for now. Each lane has its own queue, so contention is already low, and a structure I fully understand is one I can fix at two in the morning.

Keeping the lock tolerable depends entirely on only ever touching it in batches, so the cost is amortized across a few dozen jobs instead of paid per job. That constraint is why every method on the type is batch-shaped.

The planned successor is the approach crossbeam uses: a **linked list of blocks**, each block holding a few dozen slots.

```text
        head                                          tail
         │                                              │
         ▼                                              ▼
   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
   │ [x][x][ ][ ] │ ───▶ │ [ ][ ][ ][ ] │ ───▶ │ [ ][ ][ ][ ] │ ───▶ null
   │  block 0     │      │  block 1     │      │  block 2     │
   └──────────────┘      └──────────────┘      └──────────────┘
      drained             being read            being written
```

Most pushes are then a single `fetch_add` on an index inside the current block, with no allocation. Only a full block triggers a new one, so that is one allocation per few dozen jobs. Drained blocks get reclaimed, so memory does not grow to match the historical peak. The hard part is freeing a block while readers might still be inside it, and crossbeam solves that with a reference count per block: the last thread to leave frees it.

Two things would push me to go do that work. One is measurement, if workers turn out to spend real time waiting on the lock. The other is priority inversion, which is the sharper risk: the blocking lane runs at low priority, so one of its threads can be preempted while holding a lock that a compute worker is spinning on. That failure only shows up once the machine is loaded, which is exactly when the frame can least afford it.

## Status

The queue itself is written and tested. What is not built yet is the worker loop that consumes it, so steps 1 through 6 above describe a design rather than running code. `spill_half` exists on the FIFO ring with no caller yet, and the LIFO ring still needs its counterpart.

## References
- https://docs.rs/crossbeam-deque/latest/crossbeam_deque/struct.Injector.html
- https://github.com/crossbeam-rs/crossbeam/blob/master/crossbeam-deque/src/deque.rs
- https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/scheduler/inject.rs
- https://github.com/golang/go/blob/master/src/runtime/proc.go
- https://supertech.csail.mit.edu/papers/steal.pdf
