---
title: Job Graph
excerpt: Letting a job wait on another job, so a frame becomes a dependency graph instead of a row of barriers.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/0/03/Directed_acyclic_graph_2.svg/1280px-Directed_acyclic_graph_2.svg.png
tags:
  - concurrency
  - scheduling
---
## Overview

A scope ends with "wait for everything". That is the right shape for a fan out and the wrong shape for a frame.

Physics, animation and culling do not all depend on each other. Animation needs physics, culling needs animation, but the tail end of physics has nothing to do with the first job of animation. Express that with scopes and you get a row of barriers, and a barrier costs as long as the slowest job in its stage while every other thread waits out the difference.

An ECS scheduler already knows which system reads what and writes what, so it is already holding the dependency graph. What it lacks is a way to say it. A handle plus `spawn_after` is the smallest surface that says it, and a fuller declarative graph later can be built on exactly this.

The API is two methods and one type. `spawn_with_handle` behaves exactly like a normal spawn but hands back something later work can hang off. `spawn_after` takes a list of handles and returns immediately, because it is not a join point: nothing blocks until the enclosing scope ends. Dependencies that are already finished are not waited on.

> [!IMPORTANT]
> **Cycles cannot be written, and there is no `wait`**
>
> A handle only exists after its job has been created, so every edge points backwards into the past and a cycle is unspellable. That matters, because a cycle here is a scope whose counter never reaches zero. A handle deliberately has no `wait` either: waiting on one job is what a plain spawn plus the scope's own join already does, and offering `wait` would invite the spawn-wait-spawn-wait pattern this type exists to replace.

## Data Structure

Each node in the graph holds four things:

* **A pending count**, the dependencies not yet finished, plus one extra for a setup guard.
* **The work**, taken exactly once by whichever thread runs the node.
* **A successor list**, wrapped in an option where `None` is the published state "this node is already done".
* **The pool's shared half**, never a full pool handle, so a live node can neither keep the pool alive nor trigger a shutdown from inside a worker.

The pending count starts at one rather than at the number of dependencies. That extra count is released at the end of setup, once every edge is wired. Without it a dependency finishing between two registrations could drag the count to zero and dispatch the job while the thread is still attaching the remaining edges. Releasing the guard is also what dispatches a node with no dependencies at all.

## Operational Mechanism

### Wiring an edge
* Registering a dependent takes the dependency's successor lock, and the pending count is bumped **under that same lock**.
* Bumping it outside the lock leaves a gap: the dependency reads as unfinished, an instant later it publishes a successor list without this dependent, and now a job exists that nobody will ever release. That job holds a latch ticket, so the scope waits for it forever.
* If the successor list is already `None` the dependency has finished, so nothing is registered and nothing is waited on.

### Finishing a node
1. Run the work.
2. Take the successor list, leaving `None` behind. Taking is what publishes "done" to any dependent that has not registered yet.
3. Release each successor, subtracting one from its pending count and dispatching it when the count reaches zero.

The subtraction acquires, so a job sees everything all of its dependencies wrote, not just the last one to report.

### What holds it together
* Panics cannot strand a successor, because the closure is already wrapped in its own `catch_unwind` one layer down, so the successor loop always runs. A stranded successor is never dispatched, never releases its latch ticket, and hangs the scope, which is much worse than an error message.
* The latch ticket is taken when the node is built, long before it is dispatched, so a node still waiting on its dependencies is already counted. The join at the end of a scope therefore waits for the whole graph to drain, which is why there is no separate graph join anywhere.

## References
- https://www.gdcvault.com/play/1022186/Parallelizing-the-Naughty-Dog-Engine
- https://www.gdcvault.com/play/1021926/Destiny-s-Multithreaded-Rendering
- https://doc.rust-lang.org/std/sync/struct.Arc.html
