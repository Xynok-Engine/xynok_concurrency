---
title: Work Stealing for CPU-bound Tasks
excerpt: The mechanics behind work-stealing for CPU-bound task scheduling
cover img: "../images/worker_queue.png"
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

## The Stealing Priority

When a worker thread finishes its assigned tasks, it must decide where to look for more work. The priority logic is as follows:

1.  **Local/Global Pool:** The worker first checks the primary pool provided by the scheduler or the system group to see if any remaining tasks are available.
2.  **Peer Stealing:** If no tasks remain in the primary pool, the worker attempts to steal tasks from other worker threads.

## Managing Recursive Tasks

A more complex scenario arises with recursive task generation. A system might be parallel by nature, but its execution logic may spawn additional parallel tasks. These sub-tasks can, in turn, spawn their own sub-tasks, creating a hierarchical dependency chain.

### FIFO vs. LIFO Strategies

Choosing the right data structure for task queues is critical when dealing with these recursive dependencies:

*   **FIFO (First-In, First-Out):** This is ideal for the global scheduler. Since the scheduler treats system groups as sequential steps, maintaining the entry order ensures that the overall system logic remains predictable.
*   **LIFO (Last-In, First-Out):** This is preferred for individual worker threads. When a task spawns sub-tasks, it is often more efficient to process the newest sub-tasks immediately. Using a LIFO structure (a stack) allows the worker to focus on the most recently generated dependencies.

### Why LIFO Works for Stealing

When another worker thread attempts to steal, it should also pull from the LIFO stack. This ensures that the thief takes the most recently spawned sub-task. This approach maintains the locality of the work and respects the implicit dependency order created by the recursive generation of tasks.
In practice, however, this buffer does not function as a pure LIFO queue. Only the owner thread pops tasks in LIFO order, while other worker threads steal from the FIFO end. This design is intended to reduce contention between threads.
[More details at here](fifo_and_lifo.md).

## Handling Dependencies and Deadlocks

In systems where a task is split into parallel sub-tasks, the worker executing the parent task must wait for these sub-tasks to complete before proceeding to the next step. If every worker in the system is occupied by such dependencies, the system risks a deadlock where no worker is available to steal pending tasks from others.

## The LIFO Stack Design

To address this, my design uses a LIFO (Last-In, First-Out) stack for each worker. When a task generates multiple sub-tasks, they are pushed onto this stack. This allows workers to prioritize the most recently generated sub-tasks, which is generally more cache-friendly.

Currently, the LIFO stack in `xynok_concurrency` uses a fixed-size buffer. I chose a fixed size for two primary reasons:

*   **Memory Safety:** Avoiding re-allocation prevents dangling pointers. If a buffer were to re-allocate, other workers attempting to read from it would encounter invalid memory addresses, necessitating complex synchronization to ensure thread safety.
*   **Resource Efficiency:** Since tasks migrate between workers across frames, a growable stack would eventually lead to excessive memory allocation scattered across every worker, resulting in significant memory waste.

## Managing Stack Overflow with Lazy Allocation

Because the stack has a fixed capacity, it may overflow when a parent task generates a large number of sub-tasks. To handle this without re-allocation, I implement a form of "lazy allocation" (or lazy pushing):

1.  **Check Capacity:** Before pushing sub-tasks, check the available slots in the stack.
2.  **Partial Pushing:** If the number of sub-tasks is less than or equal to the available space, push them all onto the stack.
3.  **Recursive Division:** If the number of sub-tasks exceeds the remaining capacity, divide the sub-tasks into smaller batches. For example, if you have 32 sub-tasks and limited space, divide them into smaller groups (e.g., 16, 8, then 4) until a batch fits into the stack.
4.  **Deferred Processing:** Push only the first batch (e.g., 4 tasks) onto the stack and keep the remaining tasks within the current scope.
5.  **Iterative Filling:** As workers process the tasks in the stack and free up slots, continue pushing the remaining batches until all sub-tasks have been processed.

This approach ensures the stack never overflows and eliminates the need for dynamic resizing, keeping the synchronization logic straightforward and the memory footprint predictable.


## References
- [fifo & lifo, why it matter ?](fifo_and_lifo.md)

