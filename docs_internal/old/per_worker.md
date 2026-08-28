---
title: PerWorker
excerpt: One slot per thread, so results get written without sharing anything and merged in a fixed order.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/1/1f/Cache_Fill.svg/1280px-Cache_Fill.svg.png
tags:
  - concurrency
  - data_structure
---
## Overview

A read-only parallel loop is the easy case. Every real use writes something, and none of those destinations can be shared.

An ECS worker writes a command buffer of spawns, despawns and component changes, and one shared buffer is a contention point for every worker at once. A particle system writes newly spawned particles at an even higher rate. A renderer writes graphics command buffers, and Vulkan and D3D12 both require a command pool owned by exactly one thread. A profiler writes steal counts and idle times, which is the reason cache padding exists in the first place.

So the answer is not a lock, it is to not share in the first place. Each thread writes into its own slot, each slot sits on its own cache line, and the results are merged afterwards.

> [!IMPORTANT]
> **Merging is where reproducibility lives**
>
> Work stealing makes the order threads finish in unpredictable, which is harmless for rendering and fatal for replays, lockstep netcode, or reproducing a bug. Merging walks slots by index rather than by who finished first, so any merge built on it gives the same answer every run. Merging in completion order is exactly the mistake this type exists to prevent.

## Data Structure

An array with one slot per participant, indexed by the seat number the pool hands out. Each slot holds two things:

* **The value**, in a cell that allows writing through a shared reference.
* **A borrow flag**, marking whether this slot's owner is currently inside it.

The flag is atomic because of the **reader**, not the writer. Only one thread ever writes it, so the two touches while borrowing do not need to be a single operation. But the frame boundary sweep inspects every slot's flag from the host thread, and reading a plain cell written by another thread is a data race no matter what value comes out.

The whole structure is shareable across threads as long as the value type can be **sent** between them, without needing to be shareable itself. Each thread reaches only its own slot, so the value moves between threads rather than being shared, which is the same condition a channel has.

## Operational Mechanism

### Borrowing a slot
* The caller borrows the slot belonging to the calling thread, and no two threads have the same seat, so two mutable borrows can never exist at once.
* Before handing it over, the borrow flag is checked, in release builds too. A guard clears it on the way out, including while a panic unwinds, so a failed job does not poison its slot permanently.
* That check is not paranoia, because this crate itself opens the door: a thread waiting at a join point runs **other** jobs, so a job can start underneath a job already holding a borrow, on the same thread, with the same seat. Neither job looks wrong, and the result would be two mutable references alive on one value. One load and one branch on a line this thread already owns is a cheap price for catching it.

### Getting the results back out
* **The safe merge** takes an exclusive reference to the whole array. That is the proof no job is still writing, and also the proof no borrow flag is still raised, since a borrow guard holds a shared borrow for as long as it lives.
* **The frame boundary sweep** does not have that proof and is therefore unsafe, with the caller responsible for knowing the pool is idle. It still asserts on every borrow flag, which is a check on that contract rather than a replacement for it, and it catches the case that really happens: a frame boundary declared while jobs are still holding their arenas.

## References
- https://en.wikipedia.org/wiki/False_sharing
- https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html
