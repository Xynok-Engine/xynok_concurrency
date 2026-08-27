---
title: Job Graph
excerpt: Letting a job wait on another job, so a frame becomes a dependency graph instead of a row of barriers.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/0/03/Directed_acyclic_graph_2.svg/1280px-Directed_acyclic_graph_2.svg.png
tags:
  - concurrency
  - scheduling
---

## Overview

A scope ends with "wait for everything". That is the right shape for a fan out and it is the wrong shape for a frame.

Physics, animation and culling do not all depend on each other. Animation needs physics, culling needs animation, but the second half of physics has nothing to do with the first job of animation. Express that with scopes and you get a row of barriers, and a barrier costs as long as the **slowest** job in its stage while every other thread sits idle for the difference.

```text
scopes only   [== physics ==][=====]│[= anim =][==========]│[== cull ==]
                                    ▲ the whole pool waits for one straggler

with handles  [== physics ==][= anim =][== cull ==]
              [=====][==========][··· picks up the next stage early ···]
```

An ECS scheduler already knows which system reads what and writes what, which means it is already holding the dependency graph. What it lacks is a way to **say** it. A handle plus `spawn_after` is the smallest surface that says it, and a fuller declarative graph later can be built on exactly this without changing anything above it.

## The API

Two methods on `Scope`, and one type.

```rust
pool.scope(|s| {
    let physics = s.spawn_with_handle(|| step_physics());

    // Will not run before physics is done, but this line does not wait for anything.
    let anim = s.spawn_after(&[&physics], || step_animation());

    s.spawn_after(&[&anim], || cull());
});
```

`spawn_with_handle` is exactly `spawn`, in timing and in cost, except it hands back something later work can hang off. Under the hood it is `spawn_after(&[], f)`.

`spawn_after` returns immediately. It is not a join point. Nothing blocks until the enclosing `scope` ends, which is the whole point: the thread describing the graph goes back to describing more of it, or to running jobs, instead of sitting on a barrier. Dependencies that are already finished are not waited on, and an empty `deps` is legal.

### `JobHandle` has no `wait`

That is deliberate. Waiting on one job is already covered by `spawn` plus the scope's own join, and offering `wait` here would invite the exact pattern this type exists to replace: spawn, wait, spawn, wait. Say the ordering as a dependency, and leave a single join at the end of the scope as the only place anyone has to stop.

The handle also carries `PhantomData<&'scope ()>`, which pins it to its scope. It cannot outlive the join that guarantees the references a job captured are still alive.

### Cycles cannot be written

`deps` takes handles, and a handle only exists after its job has been created, so every edge points backwards into the past. There is no way to name a job that does not exist yet, so there is no way to spell a cycle.

That matters more than it sounds. A cycle here is a scope whose counter never reaches zero, which is a process that never leaves the join point. Making it unspellable is better than detecting it.

## What a Node Is

```rust
struct Node
{
    pending:    AtomicUsize,              // dependencies left, plus one setup guard
    work:       Mutex<Option<Job>>,       // taken exactly once
    successors: Mutex<Option<Vec<Arc<Node>>>>,  // None means "this node is done"
    shared:     Arc<Shared>,              // the pool, minus the join handles
}
```

Each field is answering a race, so they are worth taking one at a time.

**`pending` starts at one, not at `deps.len()`.** The extra count is a setup guard, released at the end of `spawn_after` once every edge is wired. Without it, a dependency that finishes between two `register` calls could drag the counter to zero and dispatch the job while this thread is still attaching the remaining edges. Releasing the guard is also what dispatches a node with no dependencies, so an empty `deps` runs right away and needs no special case.

**`successors` is an `Option`, and finishing means `take`.** `None` is the published state "this node is already done". A late dependent that arrives after that sees `None` and simply does not register, because there is nothing left to wait for.

**`shared` is `Arc<Shared>`, never `ThreadPool`.** A `Shared` is the pool minus its worker join handles, so holding one does not keep the pool alive. That distinction has teeth, and it gets its own section below.

## How It Runs

```text
spawn_after(&[a, b], f)
        │
        ├─ pending = 1                       (setup guard)
        ├─ a.register(node) ─── a done?  no  → pending = 2, a.successors += node
        ├─ b.register(node) ─── b done?  yes → nothing, b will never call us
        └─ release(node)  ─── pending 2 → 1, not zero yet, so nobody dispatches

a finishes
        └─ run() → take successors → release(node) → pending 1 → 0 → inject
```

### Registering happens under the lock

```rust
fn register(&self, dependent: &Arc<Self>)
{
    let mut successors = ignore_poison(self.successors.lock());

    if let Some(list) = successors.as_mut()
    {
        dependent.pending.fetch_add(1, Ordering::Relaxed);
        list.push(Arc::clone(dependent));
    }
}
```

The counter is bumped **under the same lock** the finishing side has to take to hand the list out. That is what makes "already done" distinguishable from "about to be done".

Bump it outside the lock and there is a gap: this node reads as unfinished, an instant later it publishes a successor list that does not contain `dependent`, and now a job exists that nobody will ever release. That job holds a latch ticket, so the scope waits for it forever.

### Finishing publishes, then releases

```rust
fn run(node: Arc<Self>)
{
    if let Some(work) = ignore_poison(node.work.lock()).take()
    {
        work.run_once();
    }

    let successors = ignore_poison(node.successors.lock()).take().unwrap_or_default();

    for successor in successors
    {
        Node::release(&successor);
    }
}
```

`take` rather than a read, because taking is what publishes "done" to any dependent that has not registered yet.

`release` subtracts one with `AcqRel` and dispatches when it hits zero. The acquire half is what lets the job see everything its dependencies wrote, and by the usual release sequence argument it sees **all** of them, not just the last one to report.

Both `release` and `run` are associated functions instead of methods, for a boring reason: the receiver would have to be `Arc<Self>`, and under `--cfg loom` that is `loom::sync::Arc`, which Rust will not accept as a receiver type.

### Panics do not strand successors

`Scope::job` already wraps the closure in its own `catch_unwind`, so `run_once` cannot unwind here. The successor loop below it always runs.

That is not politeness, it is the difference between an error message and a hang. A stranded successor is a job that is never dispatched and never releases its latch ticket, so the scope counts forever. The behaviour on a panicking dependency is: the successor still runs, and the panic is re-raised at the end of the enclosing scope, once its siblings are done.

### The scope's latch covers pending nodes

`self.job(f)` is called inside `spawn_after`, which means the latch ticket is taken the moment the node is built, long before the node is dispatched. A node sitting in a successor list, waiting for its dependencies, is already counted by the scope.

So the join at the end of a scope is not just waiting for jobs that are running. It is waiting for the graph to drain, which is why there is no separate graph join anywhere.

## The `join itself` Trap

This one is worth writing down because miri caught it with four words: `trying to join itself`.

The sequence: a node's job finishes and drops its latch ticket, the scope sees zero and returns, the calling thread drops the last `ThreadPool` handle, and the worker is still inside `Node::run` for one more beat, clearing the successor list.

If a node held a `ThreadPool` instead of an `Arc<Shared>`, the `Arc` the worker drops on that beat is the last handle, and its `Drop` shuts the pool down, which joins every worker, including the one running that exact line. Joining yourself is undefined behaviour, not a tidy hang.

Holding only `Shared` means a live node can never keep the pool alive and can never trigger a shutdown from inside a worker. There is a test that reproduces the whole sequence and drops the pool immediately after the scope returns.

## Testing

The suite is built around orderings that a broken implementation would get wrong:

- a dependent runs after what it depends on, across pools of zero, one and three threads
- a node with eight dependencies waits for all eight, not just the first to report
- a dependency that has fully finished before anyone registers on it does not deadlock the late dependent
- a chain of 64 nodes comes out in exactly the right order
- a diamond, where two middle branches both wait on the root and a leaf waits on both
- dropping the last pool handle right after the scope, the `join itself` case
- a panicking dependency still releases its successor, and the panic still reaches the join

Everything runs under miri too, with loop counts dialled down, since miri interprets one instruction at a time.

## Related Documentation

- [`src/job_graph.rs`](../src/job_graph.rs): the implementation and its tests.
- [`src/scope.rs`](../src/scope.rs): where `spawn_after` lives and where the join happens.
- [`docs/latch.md`](latch.md): the counter that makes the end of a scope wait for the whole graph.
- [`docs/lane_queue.md`](lane_queue.md): where a dispatched node actually runs.

External reading:

- [Parallelizing the Naughty Dog engine using fibers](https://www.gdcvault.com/play/1022186/Parallelizing-the-Naughty-Dog-Engine): the job graph argument, made for a shipping engine.
- [Destiny's multithreaded rendering architecture](https://www.gdcvault.com/play/1021926/Destiny-s-Multithreaded-Rendering): why frame stages stop being barriers.
- [`Arc` and cycles](https://doc.rust-lang.org/std/sync/struct.Arc.html): the reference counting a node relies on to stay alive until its last dependent is released.
