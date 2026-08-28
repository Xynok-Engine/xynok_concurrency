---
title: Thread Priority
excerpt: OS scheduler, thread priority, and how Xynok keeps frame work ahead of background work.
cover img: https://www.cgdirector.com/wp-content/uploads/media/2021/10/Intel-P-Cores-vs-E-Cores-Twitter-1200x675.jpg
tags:
  - concurrency
  - performance
---
## Overview

A game engine runs tasks that compete for the CPU without sharing the same urgency. Physics, animation and culling have to land inside the current frame. Loading a texture off disk does not. Compiling a shader does not. Neither do the background applications the OS is running outside the engine.

At 60 FPS the engine has about 16.67 ms per frame. A texture arriving slightly late is usually imperceptible, while missing the frame deadline shows up immediately as a stutter. So the engine needs a way to tell the operating system which of its threads are on the critical path and which ones can step aside.

That is what `Priority` is for. It is a thin policy layer between the lane structure at the application level and the scheduler at the OS level, and its goal is not to make every job as fast as possible but to make sure the right work is preferred at the right time.

## Prerequisites

A **process** is a program in execution, and it can contain many **threads**, each an independent stream of execution. A **core** is the hardware that actually runs instructions, and there are usually more threads waiting than there are cores, so a thread existing does not mean a thread running. Modern CPUs also mix fast, power-hungry performance cores with slower, efficient ones.

The **scheduler** decides which thread runs right now, which ones keep waiting, which core each one lands on, and how long a thread may run before it has to yield. A thread cycles between runnable, running and blocked as it waits for CPU time, gets preempted, or waits on I/O.

**Quality of Service**, or QoS, is metadata describing the nature and urgency of work rather than a bare number. It tells the scheduler whether a thread is producing the frame the user is looking at, completing something the user explicitly asked for, making gradual progress on a long task, or doing maintenance nobody can see. The OS uses that to decide how quickly the thread gets CPU time, how easily it can be preempted, whether to prefer a performance or an efficiency core, and how aggressively to trade speed for power. Apple platforms expose this directly, from `USER_INTERACTIVE` down through `USER_INITIATED` and `UTILITY` to `BACKGROUND`.

> [!IMPORTANT]
> **QoS is an input to the scheduler, not a reservation**
>
> Nothing here guarantees that a high priority thread runs first, that a task finishes by a deadline, or that a thread stays on a particular core. The scheduler still weighs system load, thermal state, battery, and process permissions. Setting priorities too high can starve the UI, the audio, or the whole system.

## Data Structure

`Priority` has three values, and picking one is a statement about the job, not about the current mapping:

* **`Frame`** for ECS, physics, animation, culling and render preparation, where the goal is minimum frame latency. This is the default.
* **`Background`** for work off the critical frame path, which should yield resources to keep the application responsive.
* **`Io`** for file reads, asset streaming and anything syscall bound, where the goal is less CPU contention and better power efficiency.

Internally there are only two levels: `Frame` becomes interactive, while `Background` and `Io` both become low. They behave identically today and stay separate because they describe different situations, one being work off the frame path and the other work waiting on a device, so a future backend can treat them differently without anyone revisiting a call site.

A thread waiting on a disk read burns no CPU, but it is still a thread the scheduler has to place, and on a mixed-core machine it may land on a fast one. Telling the OS the work is not urgent lets it park the thread somewhere cheap, and since the wait is dominated by the device rather than by instructions, the slower core costs almost nothing and saves noticeable power. That is why the async lane defaults to `Io`: not to finish the read sooner, but to keep it out of the frame's way.

> This is thread priority, not per-job priority. Once a worker picks up a job, everything it runs inherits that worker's scheduling characteristics. Work that needs different treatment has to go to a different lane.

## Operational Mechanism

### What each platform actually does
None of the three platforms share an API here, and not one of them lets a thread set someone else's priority. Everything applies to the **calling** thread, which is exactly why the call lives inside the worker loop rather than in whoever spawned it.

* **macOS and iOS** use `pthread_set_qos_class_self_np`. This is the only platform whose scheduler genuinely reads the intent, and on Apple silicon it is also what steers a thread toward a performance or an efficiency core.
* **Linux, Android and BSD** use `nice`, which acts on the calling task since a Linux thread is a task.
* **Windows** uses `SetThreadPriority` plus `THREAD_POWER_THROTTLING`, the second being what switches EcoQoS on or off and nudges a thread toward the efficient cores.
* **Anything else** compiles down to an empty function, and Miri skips the whole thing since these are FFI calls it cannot execute.

Two behaviours are worth knowing before reading too much into a profile. On Linux `Frame` does nothing at all, because lowering a nice value needs `CAP_SYS_NICE` which a game normally does not have, so only the low level calls `nice(10)`: frame threads keep the default and I/O threads step aside, which is the half that works without privileges. On Windows frame threads switch EcoQoS **off** on purpose, because leaving it on lets the OS migrate the whole pool onto efficiency cores and a 16 ms budget does not survive that.

### From lanes to the scheduler
* Each lane configures itself from its own default priority, and every worker in that pool carries the same policy whatever jobs it happens to pick up.
* Compute workers ask for the interactive class, so the scheduler tends to keep them on performance cores and away from aggressive throttling. Async lane workers ask for the low class, so it is free to park them on efficiency cores, which suits work that spends its time waiting.
* Neither of those is pinning. The engine never calls an affinity API, and the OS stays free to move any thread anywhere at any time.

### Limitations worth knowing
* **Rejections are not errors.** Native calls can fail from missing permissions or an older OS, and the module ignores that. Priority is best effort, the call returns nothing, and failing to set it is not a reason to refuse to launch.
* **The host thread is untouched.** The pool lets the thread that created it run jobs too, but the priority call only happens inside spawned workers, so the main thread keeps whatever the application or OS gave it. With zero workers configured, nothing calls the module at all.
* **Priority cannot fix a job in the wrong lane.** A heavy CPU bound job on the async lane still runs at low priority, and a synchronous file read on the compute lane still blocks a compute worker. Put frame-deadline CPU work on compute, syscall-bound or non-urgent work on the async lane, main-thread-only work on main, and real-time callbacks such as audio on a dedicated thread of their own.
* **More threads is not more cores.** Oversubscribing just adds contention, cache thrashing and context switches. That is why compute spawns one worker per core minus one, with the calling thread making up the difference, while the async lane stays at two to four regardless of machine size: that count is chosen for how many parallel reads a disk handles well, not for how many cores you have.

## References
- https://www.geeksforgeeks.org/operating-systems/process-schedulers-in-operating-system/
- https://developer.apple.com/library/archive/documentation/Performance/Conceptual/power_efficiency_guidelines_osx/PrioritizeWorkAtTheTaskLevel.html
- https://www.man7.org/linux/man-pages/man2/setpriority.2.html
- https://devblogs.microsoft.com/performance-diagnostics/introducing-ecoqos/
