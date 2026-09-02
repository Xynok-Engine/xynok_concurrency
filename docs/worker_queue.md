---
title: Worker Queue
excerpt: The mechanics behind worker queue.
cover img: "../images/work_stealing.png"
tags:
  - concurrency
  - data_structure
---
## Worker Queue Architecture in xynok_concurrency

The worker queue in `xynok_concurrency` is built upon the [Ring Buffer concept](ring_buffer.md), which utilizes a fixed-size ring buffer. Unlike standard implementations where producers and consumers interact with the same end of the buffer, this architecture separates these interactions to minimize contention in multi-threaded environments.

## Core Design Philosophy

The design deviates from traditional Single-Producer/Multi-Consumer (SPMC) models. Instead, it treats the producer and consumer logic as distinct entities with specific access patterns:

*   **Producer (Owner):** The worker that owns the buffer performs both push and pop operations at the same end of the ring.
*   **Consumer (Stealing Workers):** Other workers interact with the opposite end of the buffer to steal tasks.

By segregating these behaviors, we reduce contention between the owner and the stealing threads, allowing for more efficient parallel execution.

## Synchronization Strategy

In a traditional ring buffer, synchronization usually focuses on the `head` and `tail` pointers. In this custom architecture, we consolidate the `blocked` (the request for stealing) and the `tail` into a single synchronization point. The `stealing` index is managed as a separate `Atomic` value.

This approach ensures that synchronization occurs at the `tail` rather than being isolated to the `stealing` or `blocked` indices. Because this does not strictly follow standard SPMC patterns, this custom synchronization logic is essential for maintaining consistency across the queue.

## Operational Flow

The operations rely on Compare-And-Swap (CAS) loops to ensure thread safety without traditional locks.

### Producer (Owner) Operations
When the owner needs to push or pop data, it performs the following steps:
1.  It initiates a CAS operation to contend with potential consumers.
2.  It decrements the `tail` value.
3.  It continues to attempt the CAS until the value matches the expected state.

### Consumer (Stealer) Operations
When a consumer attempts to steal a task, it follows this sequence:
1.  It attempts to increment the `blocked` value using a CAS loop until it succeeds.
2.  Once the CAS succeeds, the consumer gains lock-free access to the buffer at the target position.
3.  After retrieving the data, the consumer updates the `stealing` index to match the `blocked` value, finalizing the steal operation.

