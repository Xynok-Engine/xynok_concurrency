---
title: Concurrency Concepts
excerpt: Key concepts and keywords to keep in mind when working with multithreaded programming.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/9/9d/Multithreaded_process.svg/1280px-Multithreaded_process.svg.png
tags:
  - addendum
---

## Preface

This document is a high-level introduction to the core concepts and terminology involved in multithreaded programming. My goal is to help you visualize the broader landscape of concurrency, including the tools available and the historical evolution of these techniques.

**Why This Matters**

Understanding the history and the progression of concurrency tools is essential for building confidence when working with multithreading. As I develop the engine, this knowledge has proven to be a fundamental requirement. By grasping the "big picture", you can better navigate the complexities of concurrent systems.

**A Living Document**

Please keep in mind that this is a living document. I will update it regularly as I learn new concepts or refine my understanding. It is not a static reference, so expect additions and revisions as the project progresses.

## How to read this

The sections go from what you can observe from outside the program toward what is happening inside the CPU. If you are starting out, read them in order: each one leans on the one before it. If you are here to look something up, jump straight to the section that owns the term.

Nothing here is a full explanation of its topic. Each entry is the shortest description that still lets you read the rest of the docs in this repo without getting stuck, plus a link to where the crate actually deals with it.

## 1. The machine and who runs what

**Process** is a program in execution, with its own address space. Two processes do not see each other's memory unless they go out of their way to arrange it.

**Thread** is one stream of execution inside a process. Threads of the same process share the address space, which is exactly what makes them useful and exactly what makes them dangerous: a pointer valid on one thread is valid on all of them.

**Core** is the hardware that actually executes instructions. There are almost always more threads alive than cores, so a thread existing is not the same as a thread running.

**Hardware thread**, also called SMT or Hyper-Threading, is a core pretending to be two. It has two sets of registers but one set of execution units, so two hardware threads on the same core help when one of them is stalled on memory and fight each other when both are doing real math. This is why `available_parallelism` on an 8-core machine often reports 16.

**Scheduler** is the part of the OS that decides which thread runs right now, on which core, and for how long before it has to give the core back. A thread cycles between runnable, running, and blocked as it waits for CPU time, gets preempted, or waits on a device.

**Context switch** is the scheduler taking a core away from one thread and giving it to another: save the registers, swap the stack, possibly flush parts of the cache. It costs on the order of a microsecond of direct work plus the cache the new thread has to warm up again, which is the reason a pool of long-lived worker threads beats spawning a thread per job.

**Preemption** is that switch happening whether the thread liked it or not. It is the reason a spin lock is risky: the thread holding it can lose the core mid-critical-section, and everyone spinning on that lock is now burning a core waiting for a thread that is not running.

**P-core and E-core**, performance and efficiency cores, are the two kinds of core on modern mobile-derived CPUs (Apple silicon, Intel since Alder Lake, most ARM designs). They run the same instructions at very different speed and power. The scheduler decides which one a thread lands on, and a frame that budgets for a P-core does not survive being moved to an E-core. See [thread_priority.md](thread_priority.md).

**QoS**, Quality of Service, is how you tell the OS what kind of work a thread is doing (rendering the frame the user is looking at, versus maintenance nobody can see) instead of handing it a bare priority number. The scheduler uses it to pick core type, preemption behaviour, and power tradeoffs. It is an input, never a reservation. See [thread_priority.md](thread_priority.md).

## 2. Concurrency, parallelism, and async

**Concurrency** is structuring a program as several independent tasks that can be in flight at once. It says nothing about hardware: a single core alternating between tasks is concurrent.

**Parallelism** is several things literally executing at the same instant, which needs several cores. Concurrency is about how the program is written, parallelism is about how the machine happens to run it.

**Blocking** is a thread that cannot proceed and hands the core back, usually because it is waiting on a device, a lock, or another thread. The thread stops burning CPU, but it is still a thread the OS has to keep around, and in a fixed-size pool a blocked worker is a lost core.

**async/await** is concurrency without a thread per task. A task that would block instead yields back to a runtime, which parks its state in memory and runs something else on the same thread. It is the right tool when you have thousands of things waiting on I/O, because a waiting task costs a few hundred bytes instead of a whole stack. It is the wrong tool for CPU-bound work: awaiting does not make math faster, and there is nothing to yield to in the middle of a matrix multiply.

That distinction is why this crate has lanes rather than one pool. Frame work is parallelism, wants every core, and never blocks. Loading a file blocks and belongs somewhere it cannot steal a core from the frame. See [lanes.md](lanes.md).

**Latency vs throughput** is the tradeoff underneath most of the tuning here. Throughput is total work per second, latency is how long one specific piece takes. Batching improves throughput and hurts latency. A frame deadline is a latency budget, so several decisions in this crate deliberately give up throughput.

## 3. Sharing data between threads

**Shared mutable state** is the whole problem in one phrase. Multiple readers are fine. Multiple writers, or one writer and one reader, need an agreement about who touches what and when.

**Race condition** is a bug where the outcome depends on timing. Two threads both check a flag, both see it clear, both act on it.

**Data race** is the narrower and nastier subset: two threads access the same memory, at least one writes, and there is no synchronization between them. In C, C++, and Rust this is undefined behaviour, not merely a wrong answer. The compiler is allowed to assume it never happens, so the code it generates can misbehave in ways that make no sense at the source level. Safe Rust prevents data races at compile time; every place this crate uses `unsafe` is a place that guarantee has been handed back to the author.

**Critical section** is a stretch of code only one thread may be inside at a time.

**Mutex** is the standard way to enforce that. Lock it, touch the data, unlock. A thread that finds it locked goes to sleep and the OS wakes it later. Correct, easy to reason about, and expensive when contended: two syscalls and a context switch for a section that might be five instructions.

**Condition variable** covers "wait until something is true". Waiting on one atomically releases the mutex and sleeps, so there is no window where you have checked the condition, released the lock, and missed the signal. It always goes in a loop, because a wait can return without anyone signalling. See [utils.md](utils.md).

**RwLock** allows many readers or one writer. Worth it when reads dominate and the critical section is long enough to pay for the extra bookkeeping, which is less often than people expect.

**Spin lock** is a mutex that never sleeps: it loops until the flag clears. Right when the critical section is a few instructions and sleeping would cost more than waiting. Wrong the moment the holder can be preempted while holding it. See [utils.md](utils.md).

**Contention** is threads wanting the same thing at the same time. It is the number that matters, not the lock itself: an uncontended lock is nearly free, and a contended one can be slower than doing the work on one thread. Most of this crate's structure (a ring per worker, an arena per worker, a slot per worker) exists to keep contention near zero rather than to make locking faster.

## 4. Working without locks

**Atomic operation** is a read, write, or read-modify-write that the hardware guarantees happens all at once. No other core can observe it half done. This is the primitive everything lock-free is built from.

**Read-modify-write** is the family that reads and writes in one indivisible step: `fetch_add`, `swap`, `compare_exchange`. Plain load then store is not one of these, which is why an unsynchronized `x = x + 1` from two threads loses increments.

**CAS**, compare-and-swap, is the workhorse: "if this location still holds the value I read, replace it, otherwise tell me what it holds now". Nearly every lock-free algorithm is a loop around a CAS. `compare_exchange_weak` is the version allowed to fail spuriously on some architectures, which makes it cheaper inside a loop that was going to retry anyway.

**Lock-free** means the system as a whole always makes progress: if you pause any one thread, the others are not stuck. **Wait-free** is stronger, every thread finishes in a bounded number of steps regardless of what the others do. **Blocking** is neither, one stalled thread can hold everyone up. The SPSC ring here is wait-free on both ends, the work-stealing rings are lock-free, and a mutex is blocking. See [ring_buffer_spsc.md](ring_buffer_spsc.md).

**ABA problem** is the classic trap in CAS loops. You read A, someone changes it to B and back to A, your CAS succeeds because the value matches, but the world moved underneath you. The usual fix is a version counter packed next to the value so the pair changes even when the value returns.

**Backoff** is what a thread does while waiting on something short: spin with a CPU hint, then spin more, then yield, then admit that yielding is not helping and park. Escalating matters, because both "sleep immediately" and "spin forever" are wrong for different wait lengths. See [utils.md](utils.md).

## 5. Handing work between threads

**SPSC, MPSC, SPMC, MPMC** describe how many producers and how many consumers a queue supports: single or multi, producer and consumer. Every P and C you add costs synchronization, so pick the weakest one that fits. SPSC is the cheapest thing in this list by a wide margin and it is why the audio thread gets its own dedicated ring instead of sharing the general queue.

**Bounded vs unbounded** is whether the queue has a fixed capacity. Bounded means a full queue has to do something (refuse, block, overwrite), and that is a feature: it puts a hard ceiling on memory and turns a runaway producer into a visible error instead of a slow slide into swap.

**Ring buffer** is a fixed array with indices that wrap around, which is how you get a bounded queue with no allocation in the steady state. With a power-of-two capacity, mapping an index to a slot is one bitwise AND. See [ring_buffer.md](ring_buffer.md).

**Work stealing** is the scheduling strategy this crate is built around. Each worker owns a local queue and pushes to it without synchronizing. A worker that runs dry steals from someone else's queue. Contention only happens when a thread has nothing to do, which is exactly when it can afford to pay for it. See [thread_pool.md](thread_pool.md).

**Fork/join** is the shape of most parallel work: split into `n` pieces, run them, wait for all of them. The waiting half is a **latch**, a counter that jobs decrement and whoever hits zero wakes the waiter. In a pool, the thread waiting at the join should run other jobs rather than sleep, otherwise nested joins can put the very thread that was supposed to do the work to sleep. See [latch.md](latch.md) and [scope.md](scope.md).

**Barrier** is the neighbouring idea: `n` threads all wait until every one of them has arrived, then all continue. A frame built from barriers stalls on the slowest task in each phase, which is why this crate expresses a frame as a dependency graph instead. See [job_graph.md](job_graph.md).

## 6. What the hardware is really doing

**Cache line** is the unit the CPU moves memory in, 64 bytes on x86_64 and aarch64 (with those two prefetching in pairs, so 128 is the safer number to pad to). You never load one byte; you load the line it lives in.

**L1, L2, L3** are the cache levels, ordered small-and-fast to large-and-slow. Rough shape: L1 a few cycles, L2 about a dozen, L3 several dozen, main memory a few hundred. A cache miss chain is why an algorithm with fewer operations can lose badly to one with better locality.

**Cache coherence** is the hardware keeping every core's copy of a line consistent. Cores do not read main memory directly; they read their own copy, and something has to make sure two cores never disagree about what the line contains.

**MESI** is the protocol that does it, named for the four states a line can be in: **M**odified (this core changed it and owns the only valid copy), **E**xclusive (only copy, unchanged), **S**hared (several cores have a clean copy), **I**nvalid (stale, must refetch). Real CPUs run extended versions (MOESI, MESIF) but the idea is the same: writing to a line requires invalidating everyone else's copy first, and that invalidation is a message on the interconnect.

**False sharing** falls straight out of that. Two counters that are logically unrelated but sit in the same cache line will invalidate each other on every write, and the two cores end up trading a line back and forth instead of doing work. Nothing in a profiler points at it. The fix is padding each hot value onto its own line, which is why every ring index and per-worker slot in this crate is padded. See [utils.md](utils.md).

**Store buffer** is a small queue in front of the cache. A store lands there first and drains later, so the writing core sees its own write immediately while other cores do not yet. This is the concrete reason a value can be "written" and still invisible elsewhere, and the reason release/acquire pairs exist at all.

**NUMA** is when memory is physically closer to some cores than others, so where a thread allocated matters as much as where it runs. Mostly a server concern, rarely one on a gaming desktop.

## 7. Memory ordering

**Reordering** is the fact that the order your instructions execute in is not the order you wrote them. The compiler reorders while optimizing, and the CPU reorders while executing. Both preserve what a single thread observes, and neither promises anything about what another thread observes. Every rule below exists to buy back exactly as much of that ordering as you actually need.

**Relaxed** guarantees atomicity and nothing else. No ordering relative to anything around it. Correct for a pure counter whose value nobody uses to decide whether other memory is ready.

**Acquire**, on a load, means nothing after it can be moved before it. Once you see the value, you see everything the writer did beforehand.

**Release**, on a store, means nothing before it can be moved after it. Everything you did is finished and visible before the store lands.

**Acquire and Release only work as a pair.** A release store on one thread and an acquire load of that same location on another set up a happens-before edge between them. One half alone buys nothing. This pairing is the single most common bug in hand-written concurrent code and the one Miri is best at catching.

**AcqRel** is both at once, for a read-modify-write that is simultaneously receiving and publishing.

**SeqCst** adds a single total order that every thread agrees on. It is the easiest to reason about and the most expensive, and it is genuinely required less often than it gets used. A reasonable habit: write SeqCst first if it helps you get it correct, then weaken deliberately with a comment saying what pairs with what.

**Happens-before** is the relation all of this is really about. If A happens-before B, then B sees everything A did. Within one thread, program order gives it to you for free. Across threads, you only get it where you built it, with a release/acquire pair, a lock, or a thread join.

**Fence** orders the operations around it without being tied to a specific location. Occasionally what you want; usually the ordering belongs on the atomic access itself, where a reader can see which access it applies to.

> [!IMPORTANT]
> **Your hardware is hiding your bugs**
>
> x86_64 has a strong memory model and gives you acquire/release on ordinary loads and stores whether or not you asked for it. Code with missing orderings can run correctly on an x86 desktop for years and fall apart on an ARM phone or console. This is why the crate is tested under [Miri](https://github.com/rust-lang/miri), which interprets against the abstract C++ model instead of your CPU, and under [loom](https://github.com/tokio-rs/loom), which enumerates the interleavings a stress test would need luck to find.

## 8. The ways it goes wrong

**Deadlock**: two threads each hold what the other needs, and neither ever moves. The usual fix is a global lock ordering that everyone follows.

**Livelock**: everyone is busy and nothing progresses, like two threads politely backing off into each other forever. Randomized backoff is the standard escape.

**Starvation**: one thread never gets served because others keep winning. Not a hang, just a thread that mysteriously does nothing under load.

**Priority inversion**: a low priority thread holds a lock a high priority thread needs, and a medium priority thread keeps preempting the low one, so the high priority thread waits on work that is not being done. This is a real hazard around any lane running at low priority.

**Lost wakeup**: a thread checks a condition, decides to sleep, and the signal arrives in the window between the two. It sleeps forever. This class of bug does not crash, does not log, and does not show up under a debugger, which is precisely why the sleep protocol is verified under loom instead of by testing. See [sleep_protocol.md](sleep_protocol.md).

**Spurious wakeup**: `park` and condition variable waits may return with nothing having happened. Always re-check the condition in a loop; the shared state is the truth, the wakeup is only a hint.

**Use-after-free across threads**: a job outlives the stack frame it borrowed from. Rust's lifetimes stop this everywhere except where the crate reaches for raw pointers to let a job borrow a caller's stack, and there the guarantee is a rule a human maintains instead of a check the compiler runs. See [scope.md](scope.md).

## 9. What to verify with

**Loom** explores every interleaving of a small model exhaustively. It is the only realistic way to be confident about a sleep protocol, because the failure mode is a thread quietly never waking rather than a crash.

**Miri** interprets the code against the C++ weak memory model and catches missing `Acquire`/`Release` pairs plus the usual undefined behaviour in `unsafe` blocks, including ones that real hardware would have hidden.

**ThreadSanitizer** instruments a real build to catch data races at runtime. Broader reach than loom, but it only sees the interleavings that actually happened.

**Counters and a profiler sink** answer the questions none of the above do: how often a steal succeeded, how long workers slept, where the frame time actually went. See [profiling.md](profiling.md).

None of these replaces the others, and a stress test replaces none of them. A stress test that passes tells you the bug is rare, not that it is absent.

## References
- https://marabos.nl/atomics/ (Rust Atomics and Locks, the best starting point for everything in sections 4 and 7)
- https://doc.rust-lang.org/nomicon/atomics.html
- https://en.cppreference.com/w/cpp/atomic/memory_order
- https://preshing.com/20120612/an-introduction-to-lock-free-programming/
- https://en.wikipedia.org/wiki/MESI_protocol
- https://www.kernel.org/doc/Documentation/memory-barriers.txt
- https://en.wikipedia.org/wiki/Work_stealing
- https://github.com/tokio-rs/loom
- https://github.com/rust-lang/miri
