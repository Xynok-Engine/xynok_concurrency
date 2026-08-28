---
title: Profiling and Counters
excerpt: What the pool itself is doing, since none of it is visible from the outside.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/3/32/Dashboard.svg/1280px-Dashboard.svg.png
tags:
  - performance
  - observability
---
## Overview

Every constant in this crate is a reasoned guess: ring size, worker count, the 61 of the fairness rule, the 50% searching cap, the 20 microsecond batch target. A reasoned guess is still a guess, and the only way to turn it into a number that is right for this machine and this workload is to measure.

There are two tools for that, answering different questions. **Counters** are cumulative totals, always on, read whenever you want, and they answer whether the work is spread evenly and whether the constants are sane. **The profiler sink** is a set of callbacks fired at moments only the pool can see, and it answers where the frame budget actually went.

A profiler can already measure inside a job, since that is ordinary application code and a span in the job body is enough. What it cannot see from outside is everything **between** jobs: how long a worker sat parked, how much of the frame went into scheduling rather than work, and whether the pool fell asleep mid-frame because one straggler was holding a join.

## Data Structure

Each participant owns its own counter block, on its own cache line. A single shared counter would contend on the one line every worker touches, and measurement that slows down the thing being measured stops describing the real system. Summing only happens when someone asks, and the increments are relaxed on purpose: nothing is published through these numbers, so paying for stronger ordering would buy a guarantee nobody uses.

The numbers a block holds, and what each one means when it looks wrong:

* **Jobs run**, by this participant. The spread between workers is the interesting part, because one worker running several times as many jobs as the others means work is piling up in one place while the pool-wide total still looks healthy.
* **Steal hits and misses.** A high miss ratio means workers are spending time touching other people's rings for nothing, usually because the work is split too finely, or because there are more workers than there is actual work.
* **Lane pops**, jobs taken from the lane queue.
* **Spills**, times a local ring filled and dumped its older half. A large number means the ring is small relative to the spawn rate. That is not wrong, but every spill is a queue lock touch for jobs that could have stayed hot.
* **Parks**, times this worker went to sleep. Frequent parks mid-frame mean the frame is not split finely enough to keep the pool fed, and waking a thread costs a syscall pair.
* **Queued**, jobs waiting in the lane queue at the moment of the snapshot. Consistently positive and not falling means workers are starved or the queue is a bottleneck.

> [!IMPORTANT]
> **A pool-wide total is not an atomic snapshot**
>
> The blocks are read one after another, so a total can mix an early read of one worker with a late read of another. For tuning that skew does not matter. If you need exact numbers, read while the pool is idle.

## Operational Mechanism

### The profiler sink
* An implementation receives a job starting and finishing, a worker parking and unparking, and the end of a frame. Every method has a default, so an implementation only takes the parts it cares about.
* Install it **before** building any pool, because workers already running report into nothing until they next look.
* Installation is process-wide and only the first call wins. It is a write-once cell rather than a lock, since it is read once per job and written once per process, and a profiler swappable mid-run would only produce a timeline with a seam in it.

### What it costs
* With nothing installed, one relaxed load and one branch the predictor gets right every time, per job. Against the pool's own per-job cost it does not show up.
* The end-of-job callback fires even while the job is unwinding, because a zone that opens and never closes ruins every measurement after it, not just its own.

## References
- https://github.com/wolfpld/tracy
- https://doc.rust-lang.org/std/sync/struct.OnceLock.html
