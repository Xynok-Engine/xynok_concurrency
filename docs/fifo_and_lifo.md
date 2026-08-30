---
title: FIFO & LIFO, why it matter ?
excerpt: The problem when using FIFO in a Task Pool
cover img: https://i.ytimg.com/vi/8FHoL9KRrdk/sddefault.jpg
tags:
  - concurrency
  - data_structure
---
## Overview

Most developers are familiar with the basic definitions of FIFO (First-In, First-Out) and LIFO (Last-In, Last-Out). These structures are fundamental for managing collections of elements, and their utility depends entirely on the access pattern required for your specific use case.

## The Case for FIFO in Task Pools

When working with thread pools or task pools, we often maintain a queue of pending work. In many scenarios, sequential execution is critical, making FIFO the natural choice. When tasks are processed in the order they arrive, the system maintains a predictable flow. This is generally the standard approach for managing global task queues.

## The Challenge of Nested Tasks in ECS

In an ECS (Entity Component System) architecture, the situation becomes more complex. Tasks are not always high-level systems. Often, a system contains internal loops or logic that requires parallel execution to achieve maximum performance. For instance, processing 1024 tracks might necessitate multi-threading to remain efficient.

In this scenario, a parent task can spawn multiple child tasks. If we use a standard FIFO queue for these child tasks, they are placed at the back of the global queue. This means they will only be executed after all other pending tasks in the pool, which breaks the logic of the parent task. The parent task cannot complete until its children have finished, but the FIFO structure forces the children to wait behind unrelated work.

## A Two-Layer Scheduling Solution

To solve this, we can implement a two-layer scheduling architecture:

1.  **Global Task Pool (FIFO):** The primary pool maintains a FIFO structure to manage the high-level systems or top-level tasks. This ensures that the overall order of system execution remains consistent.
2.  **Worker Thread Local Queues (LIFO):** Each worker thread should maintain a local LIFO queue for the tasks it is currently processing. 

By using LIFO at the worker level, any child tasks spawned by a parent task are pushed to the front of the worker's local queue. This ensures that child tasks are executed immediately. Once these child tasks complete, the worker can return to the parent task, allowing it to finish its work without being blocked by other unrelated tasks in the global queue.
