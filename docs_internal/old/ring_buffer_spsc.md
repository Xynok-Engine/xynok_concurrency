---
title: SPSC Ring Buffer
excerpt: One writer, one reader, wait-free on both ends. This is how the audio thread receives commands.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Ring_buffer.svg/1280px-Ring_buffer.svg.png
tags:
  - concurrency
  - data_structure
---
## Overview

The other two rings in this crate are single producer, multi consumer: one owner and many thieves. See [ring_buffer.md](ring_buffer.md). This one is the opposite shape, and the customer who ordered it is the audio thread.

The audio thread is called back by the driver every few milliseconds and has to return a filled buffer before the deadline, every single time. Inside that callback it may not allocate, may not take a lock, and may not park. Missing the deadline is not a frame dropping to 58 FPS, it is an audible pop.

So it is never a worker in any pool. A compute lane worker sends it commands, play this sound, change that volume, through this structure, and inside the callback it only ever reads.

## Data Structure

The ring holds a fixed array of slots plus two indices:

* **A tail**, the next slot the writer will fill. Only the writer writes it.
* **A head**, the next slot the reader will take. Only the reader writes it.

Because each side owns one index there is no CAS anywhere, so both pushing and popping are wait-free: they finish in a fixed number of steps regardless of what the other side is doing. That is the real requirement for a realtime callback, since lock-free on its own is not enough when a CAS loop can retry an unbounded number of times.

Each side also keeps a **private snapshot** of the other side's index. The hot path then reads only its own cache line, and the writer asks for the real head only when its snapshot claims the ring is full, which most of the time it is not. Without the snapshot every push pulls the reader's cache line across, and two cores spend the frame bouncing one line back and forth.

The snapshot is always stale in the safe direction, because the reader only ever makes more room. A writer acting on an old head underestimates the free space and can never overwrite live data.

> [!IMPORTANT]
> **Full means full, and the value comes back**
>
> Pushing into a full ring returns the value instead of swallowing it or waiting. The sender is a compute lane worker and it must not stall because the audio thread has not caught up. Full means audio is falling behind, and the caller is the one who knows which command is safe to drop.

## Operational Mechanism

### Publishing
1. Write the value into the slot the tail points at. That slot is outside the region the reader may touch, precisely because the tail has not published it yet.
2. Advance the tail with a release store, so a reader seeing the new tail also sees the contents.

Pushing a whole batch writes every slot first and publishes **once**. The reader either sees the entire batch or none of it, and the line the other core watches gets touched a single time.

### Taking the two ends
* Splitting the ring hands out both ends at once, using exclusive access as the proof that nobody holds one yet.
* When the ring lives behind a shared handle rather than a local variable, each end can be taken separately through an unsafe call where the caller carries that proof: exactly one of each, each used from one thread.

### Index arithmetic
* Indices are 32 bit and wrap, so the capacity is capped at `2^31` to keep the wrapping subtraction unambiguous.
* Capacity is rounded up to a power of two, so mapping an index to a slot is a mask rather than a division.

## References
- http://www.rossbencina.com/code/real-time-audio-programming-101-time-waits-for-nothing
- https://docs.kernel.org/next/core-api/circular-buffers.html
- https://en.wikipedia.org/wiki/Circular_buffer
