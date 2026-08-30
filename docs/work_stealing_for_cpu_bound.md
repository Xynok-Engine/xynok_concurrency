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

## Handling Dependencies

Sometimes, a system consists of two steps, where step B depends on the results of step A. If step A generates multiple parallel sub-tasks, the worker thread must wait for those tasks to complete before proceeding to step B. While a worker could theoretically perform other work while waiting, this introduces significant complexity. For now, the core focus remains on understanding the interplay between task distribution, LIFO/FIFO selection, and the prevention of execution conflicts.

This is likely a challenge I will need to address in the future. For now, if this waiting period does not lead to calculation errors, it is acceptable. This means if a worker spawns sub-tasks and depends on their parallel completion, it is best for that worker to wait. Theoretically, it is permitted to perform other work in the meantime, but the system only receives a pointer when a task is submitted. Because I do not know how many segments the logic contains, I cannot easily decompose it. I will leave this open for a future post, or perhaps I will update this article later.

## References
- [fifo & lifo, why it matter ?](fifo_and_lifo.md)
