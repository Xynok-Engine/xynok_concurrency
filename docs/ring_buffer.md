---
title: Ring Buffer
excerpt: A data structure that helps us minimize contention between threads.
cover img: "../images/ring_buffer.png"
tags:
  - concurrency
  - data_structure
---
## Overview

When building a thread pool for my ECS, the primary challenge was managing concurrent access to a shared job buffer. Processing jobs sequentially on a single thread creates a performance bottleneck. To improve throughput, I need multiple threads to consume these jobs in parallel.

This architecture involves two distinct roles:
* **Producer:** The main thread pushes new jobs into the buffer.
* **Consumers:** Multiple worker threads pull jobs from the buffer for execution.

Because these threads operate in parallel, data contention is inevitable. Relying on standard synchronization primitives like `Mutex` or `RwLock` introduces significant overhead. Frequent locking forces threads to wait, which increases latency and extends the time required to complete a single frame. To maintain high performance, the goal is to minimize contention. A ring buffer is the ideal data structure here, as it decouples production from consumption and enables a more efficient, lock-free approach.

## Data Structure

A ring buffer is a fixed-size array. Beyond the storage array itself, the structure relies on three essential indices that track the state of the buffer:

* **Tail:** Represents the next position where a new job will be pushed.
* **Steal:** Represents the elements that have already been consumed.
* **Real:** Represents the elements currently in the process of being consumed.

These three indices technically increase indefinitely. In practice, they will eventually overflow, but the architecture handles this by wrapping the values around.

> [!IMPORTANT]
> **Index Mapping and Performance**
>
> To convert these abstract indices into physical positions within the buffer, we map them using either a modulo operation or a bitwise mask. This design requires the buffer capacity to be a power of two. Using a power of two allows us to calculate the physical index rapidly using masking, which is significantly faster than the division required for a standard modulo operation.

## Operational Mechanism

The three cursors act as both pointers and synchronization primitives. They allow threads to interact with the buffer without explicit locks.

### The Producer Flow
The producer is responsible for pushing jobs into the buffer. Since there is only one producer, it primarily tracks the `Tail` index. 
* The producer does not need to synchronize with other consumers.
* It must occasionally check the `Steal` cursor to ensure the buffer is not full.
* If the buffer is full, the system must handle the overflow based on engine-specific requirements. Currently, the API simply refuses to push new jobs until space is available.

### The Consumer Flow
Multiple consumers pull jobs from the buffer. Because multiple threads may attempt to claim a job simultaneously, we use `Real` and `Steal` to manage contention.

1. **Checking Availability:** A consumer checks if `Real` and `Steal` are equal. If they are, no other thread is currently accessing the buffer, and it is safe to proceed.
2. **Claiming a Job:** The consumer increments the `Real` index. Once `Real` is incremented, the discrepancy between `Steal` and `Real` signals to other consumers that a job is currently being processed.
3. **Synchronization:** Other threads that see `Real != Steal` will wait or spin-lock until the current consumer finishes.
4. **Finalizing:** Once the consumer finishes extracting the job, it updates `Steal` to match `Real`. This signals that the slot is now free and the operation is complete.

Currently, this design supports one producer and multiple consumers. Future iterations may allow the producer to also act as a consumer by stealing jobs directly from the buffer if necessary.

## References
- https://en.wikipedia.org/wiki/Circular_buffer
- https://stackoverflow.com/questions/76994297/where-is-the-ring-buffer-in-linux-kernel-networking
- https://docs.kernel.org/next/core-api/circular-buffers.html
- https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/scheduler/multi_thread/queue.rs
