---
title: Work Stealing for CPU-bound Tasks
excerpt: The mechanics behind work-stealing for CPU-bound task scheduling
cover img: "../images/work_stealing.png"
tags:
  - concurrency
  - data_structure
---

## Overview

Work stealing is a multithreading architecture designed to distribute large workloads across multiple threads. In this model, if a worker thread completes its assigned tasks ahead of others, it proactively visits other workers to "steal" pending tasks. This ensures that CPU resources remain fully utilized rather than sitting idle. This post explores the implementation details of this architecture.

## Scheduling in ECS

An Entity Component System (ECS) typically relies on a scheduler to manage the execution of systems. These systems generally fall into two categories: sequential and parallel.

*   **Sequential Systems:** If you have a sequence of systems (e.g., A, B, and C), the main thread executes them one after another. No complex work stealing is required here.
*   **Parallel System Groups:** When a system contains a group of sub-systems that can run in parallel, we utilize a thread pool. The scheduler distributes these sub-tasks among the available worker threads.

Each parallel group runs inside its own `scope`, and the scheduler blocks on that scope before moving to the next step. That means the shared queue never holds more than one step's worth of work at a time. Systems within a group are checked against each other for conflicting query access at the moment the group is registered (`add_system_parallel`), not at run time, so nothing inside a single group ever needs to wait on another job in the same group.

## The Stealing Priority

When a worker thread finishes its assigned tasks, it must decide where to look for more work. The priority logic is as follows:

1.  **Own local queue and inbox:** the worker first drains its own deque, then its own inbox (jobs a batch publisher or another thief routed to it directly).
2.  **Shared queue:** if both are empty, the worker pulls a batch from the shared queue (see below) into its own local queue.
3.  **Peer stealing:** only if the shared queue is also empty does the worker start visiting other workers, checking their inbox first and then stealing a batch straight out of their local deque.

## Managing Recursive Tasks

A more complex scenario arises with recursive task generation. A system might be parallel by nature, but its execution logic may spawn additional parallel tasks. These sub-tasks can, in turn, spawn their own sub-tasks, creating a hierarchical dependency chain.

### FIFO vs. LIFO Strategies

Choosing the right data structure for task queues is critical when dealing with these recursive dependencies:

*   **FIFO (First-In, First-Out):** This is ideal for the shared queue. Since the scheduler treats system groups as sequential steps, maintaining the entry order ensures that the overall system logic remains predictable.
*   **LIFO (Last-In, First-Out):** This is preferred for individual worker threads. When a task spawns sub-tasks, it is often more efficient to process the newest sub-tasks immediately. Using a LIFO structure (a stack) allows the worker to focus on the most recently generated dependencies.

### Why LIFO Works for Stealing

When another worker thread attempts to steal, it should also pull from the LIFO stack. This ensures that the thief takes the most recently spawned sub-task. This approach maintains the locality of the work and respects the implicit dependency order created by the recursive generation of tasks.
In practice, however, this buffer does not function as a pure LIFO queue. Only the owner thread pops tasks in LIFO order, while other worker threads steal from the FIFO end. This design is intended to reduce contention between threads.
[More details at here](fifo_and_lifo.md).

## Handling Dependencies and Deadlocks

In systems where a task is split into parallel sub-tasks, the worker executing the parent task must wait for these sub-tasks to complete before proceeding to the next step. If every worker in the system is occupied by such dependencies, the system risks a deadlock where no worker is available to steal pending tasks from others.

## The Local Queue Design

Each worker owns a local queue for its own sub-tasks. When a task generates multiple sub-tasks, they are pushed onto this queue, and the owner pops them back off in LIFO order (see [fifo_and_lifo.md](fifo_and_lifo.md)), which is generally more cache-friendly for freshly-spawned work.

The local queue in `xynok_concurrency` uses a fixed-size ring buffer. This is a fixed size for two primary reasons:

*   **Memory Safety:** Avoiding re-allocation prevents dangling pointers. If a buffer were to re-allocate, other workers attempting to read from it would encounter invalid memory addresses, necessitating complex synchronization to ensure thread safety.
*   **Resource Efficiency:** Since tasks migrate between workers across frames, a growable local queue would eventually lead to excessive memory allocation scattered across every worker, resulting in significant memory waste.

## Overflowing to the Shared Queue

Because the local queue has a fixed capacity, pushing a sub-task can fail when a parent task generates more sub-tasks than the local queue has room for. An earlier version of this design tried to work around that by lazily splitting the sub-tasks into smaller and smaller batches until one fit, but that added complexity for a case that the pool already had a simpler answer for.

Today, a sub-task that does not fit in the local queue is simply pushed onto the shared queue instead (the same unbounded, FIFO queue described above, and the same queue `ThreadPool::push` writes to). This works cleanly with the stealing priority: any worker that runs out of local and inbox work already checks the shared queue first, before it starts stealing from peers, so an overflowed sub-task gets picked up naturally without any special-casing.

This only holds because sub-tasks spawned this way, by construction, do not depend on each other's completion order. A system group is already guaranteed conflict-free before it is scheduled, and something like splitting one query's rows into independent chunks (planned, not yet implemented) only ever touches disjoint rows per chunk. If a future kind of sub-task needed to run before another, spilling it to a shared, unordered queue would not be safe, and it would need a different mechanism.


## References
- [fifo & lifo, why it matter ?](fifo_and_lifo.md)

