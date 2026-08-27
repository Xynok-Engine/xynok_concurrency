---
title: Lanes
excerpt: Where each kind of work runs, and how a job crosses from one lane to another.
cover img: https://upload.wikimedia.org/wikipedia/commons/6/69/Autobahn.jpg
tags:
  - concurrency
  - architecture
---
## Overview

The easiest mistake after hearing "the engine has a physics lane, a render lane, an audio lane, an I/O lane" is to give each one its own pool. Six lanes of eight threads is 48 threads on an 8 core machine, and work stealing only means something when the worker count is close to the core count. Past that, what you bought is workers fighting each other for CPU and cache lines bouncing between cores.

So lanes are split by how the work behaves, not by what it is called:

* **Compute** runs frame logic, ECS, physics, culling, animation, and command buffer recording. One work stealing pool of `cores - 1` threads, with the calling thread joining as the last participant. It is CPU bound and never blocks, so it wants a hot cache and every core it can get.
* **Blocking** runs file reads, texture decode, shader compile, decompression. Its own small pool of 2 to 4 low priority threads, because that time is spent inside syscalls and a blocked thread must not hold a core the frame needs.
* **Main** runs present, window API calls, and the few driver calls that demand a specific thread. No threads at all, just a queue the main thread drains.

Physics, rendering and compute are not three lanes. They are three groups of jobs inside the compute lane, told apart by where they sit in the dependency graph. Giving physics its own pool means the render cores idle while physics runs, and the other way around.

The audio thread is deliberately absent from that list. It is never a worker in any pool, and it receives commands over a [SPSC ring](ring_buffer_spsc.md) that it only ever reads.

> [!IMPORTANT]
> **Do not send CPU bound work to the blocking lane**
>
> Its threads run at low priority, so a heavy computation misplaced there runs measurably slower for a reason that is completely invisible from the code.

## Data Structure

Building the registry once at startup fixes two things:

* **The calling thread becomes the main thread.** Draining the main queue asserts against it later, in release builds too, because running a main-thread-only job somewhere else breaks the one promise that queue exists to keep.
* **The blocking pool is built first, the compute pool second.** A thread can only remember its seat in one pool, so the pool built last is the one this thread owns a ring in. The main thread works with the compute lane all frame; it only ever hands work to the blocking lane.

The blocking lane deliberately does not scale with core count. It exists to keep a thread sitting in a syscall from holding a core the frame needs, and two to four threads is enough to keep a few large files in flight without making the drive seek. Its workers also sleep almost immediately rather than spinning, since spinning would burn a core the compute lane wants.

Two environment variables tune it without a rebuild. `XYNOK_LANE_THREADS` is the total compute thread count including the caller, so setting it to 1 spawns nothing and runs everything inline, which answers "is this bug caused by parallelism" in one run without touching any code. `XYNOK_BLOCKING_THREADS` sets the blocking pool size.

## Operational Mechanism

### Getting work into a lane
* Compute and blocking work is spawned straight into the matching pool.
* Main thread work goes into a queue from any thread, including from inside a job. It is not run on the spot even when you are already standing on the main thread, because that would make ordering depend on who called from where, and a driver call landing mid-frame instead of at the synchronisation point is a bug that takes a long time to find.
* Draining the main queue runs only what was queued when the drain started, so a job that spawns a job cannot hold the main thread hostage.

### Getting a result back
* Blocking work hands back a [receiver](channel.md), not a value. The caller has never assumed "the call returned, so the result is here", so the day the blocking lane becomes an async reactor, nothing at the call sites changes.
* Waiting on that receiver from inside the compute lane keeps running compute jobs rather than idling.

### Frame boundary and shutdown
* Ending a frame resets every scratch arena at once and fires the profiler's frame callback. Call it when the compute lane is idle; if a job is still holding an arena it panics, which is much better than resetting memory out from under running code.
* `block_in_place` is the escape hatch for a compute job that has to wait on a GPU fence or a third party lock. It wakes one extra worker to cover the seat and runs the closure right there. It is not tokio's version: this thread keeps its seat and no replacement is spawned, so it suits one job waiting on something short, not a substitute for moving the work to the blocking lane.
* Shutdown stops both pools and then drains whatever is left for the main thread. Call it from the main thread and never from inside a job, since stopping a pool joins every worker.

## References
- https://www.gdcvault.com/play/1022186/Parallelizing-the-Naughty-Dog-Engine
- https://doc.rust-lang.org/std/thread/fn.available_parallelism.html
- https://en.wikipedia.org/wiki/Work_stealing
