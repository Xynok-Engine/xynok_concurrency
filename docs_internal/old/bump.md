---
title: Bump Arena
excerpt: Scratch memory where allocating is moving a pointer and freeing is forgetting about it.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/8/8a/ProgramCallStack2_en.png/1280px-ProgramCallStack2_en.png
tags:
  - memory
  - performance
---
## Overview

The frame pool promises never to block, and a single call into the global allocator quietly breaks that promise, because it takes a lock and one job can stall every other thread that wants memory at that moment.

One arena per worker removes the lock entirely. Allocating is an addition plus a bounds check, and there is no contention because no other thread can reach this worker's arena.

Use it for the throwaway buffers a frame produces: the visible entity list, the entities a system wants to spawn, the draw calls waiting to be sorted. Everything in it dies when the arena is reset at the frame boundary, so nothing should be held across it.

## Data Structure

The arena is two fields:

* **A buffer**, allocated once at startup as a run of 64 byte aligned chunks, and never moved or resized.
* **An offset**, the number of bytes handed out so far. A plain cell rather than an atomic, because an arena belongs to exactly one thread.

The bytes live inside a cell that permits writing through a shared reference. A pointer derived from a plain shared slice is read-only no matter how it is cast, and writing through it is undefined behaviour that miri rejects outright, so the cell is what gives a shared reference the right to write. It uses the plain standard library cell rather than the loom-swapped one, since there is no interleaving here for loom to check.

The tightest alignment an allocation can ask for is 64 bytes. A 32 byte AVX vector fits, and anything wider has to go to the global allocator.

> [!IMPORTANT]
> **Capacity is fixed, and values must be `Copy`**
>
> Growing a buffer means moving it, and moving it turns every reference already handed out into a dangling one. Holding one allocation forever is exactly what lets allocation take a shared reference and still return a mutable one: the regions never overlap and the memory never moves. A full arena returns nothing rather than growing, so running out is a decision the caller makes instead of a silent stall.
>
> Freeing everything at once means never running a destructor, and restricting to `Copy` makes that safe structurally rather than by convention. A vector or a string placed in here would leak its heap allocation every single frame.

## Operational Mechanism

### Allocating
1. Round the current offset up to the alignment the value asks for.
2. Add the size, and return nothing if that runs past the end of the buffer.
3. Store the new offset and write the value into the region.

Allocation takes a shared reference on purpose, since two allocations have to be alive at the same time and an exclusive reference forbids that. The regions never overlap, so the mutable references it hands out never alias.

### Resetting
* Resetting pulls the offset back to zero and takes an exclusive reference, which is the entire safety argument: no reference handed out earlier can still be alive, because those borrow the arena shared.
* The pool resets every participant's arena together at the frame boundary, and panics if any of them is still borrowed. A frame boundary declared while jobs are still running would otherwise pull memory out from under them.

## References
- https://www.rfleury.com/p/untangling-lifetimes-the-arena-allocator
- https://docs.rs/bumpalo/latest/bumpalo/
- https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html
