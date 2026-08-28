---
title: InlineFn
excerpt: The job type: a closure that fits in one cache line instead of one heap allocation.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/3/3b/Function_machine2.svg/1280px-Function_machine2.svg.png
tags:
  - performance
  - data_structure
---
## Overview

A job is a closure the pool owns and runs exactly once. The obvious way to store one is a boxed trait object, and the problem with that is where the closure ends up.

Boxing always puts the closure on the heap. Every spawn becomes an allocation, and at run time the worker has to follow a pointer into memory it has never touched, which is very close to a guaranteed cache miss.

`InlineFn` stores the closure inside itself when it fits. The budget is 48 bytes, enough for a few reference counted handles, and the whole structure comes to 64 bytes, one cache line. Put that in a ring and reading the job reads the closure with it. A larger closure boxes itself and hides the pointer in the same space, so the caller never has to think about what counts as small enough.

## Data Structure

Two fields, in this order:

* **A vtable pointer**, two function pointers wide: one to run the closure, one to drop it.
* **A fixed 48 byte buffer**, holding either the closure itself or a box pointing at it.

The vtables are associated constants on a generic marker type, so every concrete closure type gets its own static vtable with nothing built at run time. Which of the two is used gets decided at compile time from the closure's size and alignment, and the vtable is the only thing that knows whether the buffer holds a value or a pointer, so the layout never changes either way.

Running the job consumes it and suppresses the normal drop first, because the runner reads the closure out of the buffer and calls it. Dropping afterwards would drop it a second time.

> [!IMPORTANT]
> **The 48 bytes are a budget, not a law of nature**
>
> Forty-eight for the closure plus eight for the vtable pointer plus alignment lands the whole thing on one cache line for the architectures this engine targets. It is worth revisiting with numbers rather than by argument: the counters say how many jobs move through the rings, and the closures a codebase actually spawns say what the size distribution looks like.

## Operational Mechanism

### The normal path
Jobs handed to the pool have no lifetime attached, because the pool has no idea when one will run. The closure is written into the buffer or into a box, the matching vtable is recorded, and from then on running and dropping both go through those two function pointers.

### The borrowing path
* Jobs inside a [scope](scope.md) borrow the opener's stack, so they cannot be lifetime-free. Without a way in, the only option would be a boxed trait object with its lifetime erased, which works and costs one allocation per spawn, which is exactly what this type exists to avoid.
* So there is a second, unsafe constructor with a parallel set of vtable functions. They have to be separate because the normal ones require a lifetime-free closure. The four functions never touch the closure's lifetime at all: they read a value out of a buffer, call it, and drop it, which is valid for a closure of any lifetime.
* The caller has to guarantee the job runs to completion before anything the closure borrowed goes away. A scope guarantees it by not returning until every one of its jobs has reported in, including while unwinding from a panic. Without that guarantee this is a use after free waiting to happen.

## References
- https://doc.rust-lang.org/std/task/struct.RawWakerVTable.html
- https://en.cppreference.com/w/cpp/utility/functional/function
