---
title: Scope
excerpt: Fork-join with borrowed stack data, and the parallel helpers built on top of it.
cover img: https://upload.wikimedia.org/wikipedia/commons/thumb/8/88/Fork-join_computation.svg/1280px-Fork-join_computation.svg.png
tags:
  - concurrency
  - api
---

## Overview

A job handed to the pool has to be `'static`. That is the honest requirement: the pool has no idea when the job will run, so it cannot hold a reference to anything that might go away first.

Inside a scope that requirement lifts. The scope does not return until every job it spawned is finished, **including while it is unwinding from a panic**, so anything a job borrowed is guaranteed to outlive it.

```rust
let mut totals = [0usize; 4];

pool.scope(|s| {
    for (i, slot) in totals.iter_mut().enumerate()
    {
        s.spawn(move || *slot = i * i);
    }
});

assert_eq!(totals, [0, 1, 4, 9]);
```

`totals` lives on the stack. Four jobs each hold a mutable reference into it, running on whatever threads the pool feels like. No `Arc`, no `Mutex`, no channel to collect results out of.

This is the layer everything above calls into. An ECS scheduler chunking a system, a frame graph running passes in parallel, or just a `for` loop heavy enough to be worth splitting, all of it ends up here.

## Read This Before Using It

A thread waiting on a scope **does not sit idle**. It pulls other jobs from the pool and runs them. That is the only way nested parallelism works at all, and it is what makes `join` never much worse than running sequentially.

It also means that while one job is paused at a join point, an **unrelated** job can be running underneath it, on that same thread.

The practical consequence: **do not hold a lock while calling into this crate**. If the job that runs underneath asks for the same lock, the thread deadlocks against itself, and not a single line of either job is wrong.

```text
worker thread
  job A ── takes lock L ── calls pool.scope(...) ── waits, so it runs other work
                                                      │
                                                      └─ job B ── wants lock L ── stuck forever
```

Nothing here can detect that for you. Take the lock, get what you need, release it, then call in.

## The Type

```rust
pub struct Scope<'scope>
{
    shared: Arc<Shared>,
    latch:  Latch,
    panic:  PanicSlot,
    marker: PhantomData<&'scope mut &'scope ()>,
}
```

**`shared`, not `ThreadPool`.** A `Shared` is the pool without its worker join handles, so holding one does not keep the pool alive. Keep a whole `ThreadPool` here and a scope opened inside a job becomes a handle dropped on a worker thread. If it happens to be the last one, `Drop` runs `shutdown`, which joins every worker, including the one executing that very line. Miri names it out loud: `trying to join itself`.

**`latch`** is the counter. Everything about how the join actually works lives in [`docs/latch.md`](latch.md). One ticket per spawned job, taken before the job goes anywhere near the pool.

**`panic`** is a mutex holding the first panic any job threw, so it can be re-raised at the join point.

**`marker` makes `'scope` invariant.** `&'scope mut &'scope ()` is the standard trick. It stops the compiler from shrinking or stretching `'scope` to make a call typecheck, which would quietly break the exact promise the whole type rests on.

Two raw pointer wrappers show up as well. `ScopePtr` lets a job reach the scope that spawned it, and `PanicSink` lets it reach the panic slot. Neither can be a plain reference, because a reference borrows a scope that lives on someone else's stack and a job has to be independent of that frame in the type system. What keeps both valid is the same promise as always: the scope does not return while a ticket is alive.

## The Lifecycle

```rust
pub(crate) fn scope_in<'scope, R>(shared: &Arc<Shared>, f: impl FnOnce(&Scope<'scope>) -> R) -> R
{
    let scope = Scope { shared: Arc::clone(shared), latch: Latch::new(0), .. };

    let outcome = catch_unwind(AssertUnwindSafe(|| f(&scope)));
    shared.run_until(|| scope.latch.is_done());

    let job_panic = ignore_poison(scope.panic.lock()).take();

    match (outcome, job_panic)
    {
        (Err(payload), _) => resume_unwind(payload),
        (Ok(_), Some(payload)) => resume_unwind(payload),
        (Ok(value), None) => value,
    }
}
```

The wait happens **after** the `catch_unwind`, on every path. If the scope body panics, the jobs it already spawned are still out there holding references into a stack frame that is being torn down. Letting them run on during the unwind is precisely the use after free a scope exists to prevent, so the panic gets caught, the wait happens anyway, and only then is it re-raised.

`run_until` is the work-while-waiting loop from the pool. The waiting thread keeps running jobs until the latch reads zero.

The panic precedence is: a panic from the body wins over a panic from a job, and among jobs the first one recorded wins while the rest are dropped. Picking the last one instead would mean picking by finishing order, and finishing order is different every run.

## Spawning

```rust
pub fn spawn<F>(&self, f: F)
where F: FnOnce() + Send + 'scope
{
    self.shared.inject(self.job(f));
}
```

All the interesting parts are in `job`, which wraps the closure and erases its lifetime:

```rust
let ticket = self.latch.ticket();      // counted before the pool can see it
let sink = PanicSink(&self.panic as *const PanicSlot);

Job::new_unbound(move || {
    let _ticket = ticket;              // declared first, so it drops last
    let sink = sink;

    if let Err(payload) = catch_unwind(AssertUnwindSafe(f))
    {
        let mut first = ignore_poison((&*sink.0).lock());
        if first.is_none()
        {
            *first = Some(payload);
        }
    }
})
```

Three things are doing real work here.

**The ticket is taken at spawn time**, not when the job starts running. Otherwise there is a window where the counter reads zero while a job is already queued, and a waiter that looks in exactly that window walks away.

**Every job catches its own panic.** A panic escaping a worker would skip the ticket drop, leave the counter one short, and hang the join forever. Catching it turns a crash into a value that gets carried to the join point and re-raised there.

**Drop order is load bearing.** `_ticket` is declared before `sink` so it drops after the panic has been written. Release the ticket first and the waiter may return and take the panic slot with it while the job is still writing into it.

### `spawn_with`

Same thing, except the job gets the scope back so it can spawn into it:

```rust
fn split<'a>(s: &Scope<'a>, counter: &'a AtomicUsize, depth: usize)
{
    counter.fetch_add(1, Ordering::Relaxed);
    if depth == 0 { return; }

    for _ in 0..2
    {
        s.spawn_with(move |s| split(s, counter, depth - 1));
    }
}

pool.scope(|s| split(s, &counter, 4));
```

This is what every recursive divide and conquer needs. Without it each level of recursion has to open its own scope, and each new scope is another join point, which is another place the whole pool waits for the slowest straggler. With it there is one join, at the very top.

## `join`

```rust
let (left, right) = pool.join(
    || (1..=50u64).sum::<u64>(),
    || (51..=100u64).sum::<u64>(),
);
```

`b` runs on the calling thread, `a` goes to the pool. If nobody steals `a`, the calling thread runs it while waiting. So `join` is never worse than sequential by more than the bookkeeping, which is what makes it safe to use at the leaves of a recursion where the work might be tiny.

It is a scope with one spawn, nothing more.

## `parallel_for`

```rust
pool.parallel_for(n, batch, |i| { .. });
```

### Why you have to say `batch`

Spawning is not free. One push into a ring, maybe one steal (a CAS plus a cache line flying between cores), one latch touch on the way out, maybe one wakeup. Call it 1 to 5 microseconds.

Split 1000 elements at 2 ns each and every job runs for 250 ns while costing 2000 ns to dispatch. That is slower than not parallelising at all, and it looks like a win on the flamegraph because all the cores are busy.

The number worth aiming at is **20 microseconds per job**: big enough that dispatch is around 10% overhead, small enough that every core still gets a fair share. Work backwards from there. Measured 200 ns per element, so `batch = 20µs / 200ns = 100`.

`batch >= n` means one job, which is sequential, and that is a legitimate way to switch parallelism off at one call site without restructuring anything.

### How the split actually works

Not one job per batch. The number of jobs equals the number of participants, and batches are handed out through an atomic cursor:

```text
cursor: AtomicUsize          chunks: 0 1 2 3 4 5 6 7 8 9 ...
                                     ▲
worker A ── fetch_add ── chunk 0 ────┘
worker B ── fetch_add ── chunk 1
worker C ── fetch_add ── chunk 2
worker A ── fetch_add ── chunk 3   (finished early, takes the next one)
```

One `fetch_add` is the entire scheduling algorithm. With evenly sized batches, which is what an ECS query gives you, this tracks a work stealing deque closely for about a tenth of the code. A worker that finishes early just grabs the next chunk, so uneven core speeds sort themselves out.

Helper count is `worker_threads().min(chunks - 1)`: at most one helper per worker, and never more helpers than there are chunks left to hand out. The calling thread runs `run()` itself rather than sitting on the join.

The sequential fallback triggers on `n <= batch` or a single participant, and it skips the scope entirely rather than paying for a spawn to run one closure.

## `par_reduce`

Same chunking, plus a fold and a join:

```rust
let total = pool.par_reduce(1_000, 64, || 0u64, |acc, i| acc + i as u64, |a, b| a + b);
```

### Determinism is the point

Joining partial results as they arrive would be slightly faster and a lot less predictable. Which thread finishes first is a property of that particular run, and with floating point `(a + b) + c` is not `a + (b + c)`. A culling pass whose bounding box drifts by a hair every frame is a bug you spend a week finding.

So partials are joined **in chunk order**, after everything is done. `join` still has to be associative, since it is applied over adjacent groups rather than a fixed tree, but it never sees a different **grouping** between two runs.

### One slot per chunk, not per thread

```rust
let slots: Vec<UnsafeCell<Option<T>>> = (0..chunks).map(|_| UnsafeCell::new(None)).collect();
```

A slot per thread would be cheaper. It would also make the partial values themselves depend on timing, because which thread folded which indices changes run to run, even though the final join order would still be fixed. Per chunk keeps every intermediate value reproducible too.

Writes go through `ChunkSlots`, a wrapper that is `Sync` because each slot is touched by exactly one thread: whoever won that chunk index from the cursor. The wrapper exists so the closure captures it rather than the bare slice, since a bare slice of `UnsafeCell` is not `Sync`. Reads happen after the scope has joined, when the calling thread is provably the only one left.

## Testing

`scope_tests.rs` covers the things that break in production and not on a laptop:

- jobs borrowing the opener's stack, the core promise
- nested scopes, including three levels deep on a **single worker**, which is where a naive implementation deadlocks
- a panic in a job re-raised at the join, and a panic in the scope body that still waits for its jobs first
- `join` nested into a recursive tree
- `parallel_for` touching every index exactly once, running sequentially when `batch >= n`, and working on a pool with zero workers
- `par_reduce` giving identical results across repeated runs, using a non-commutative join (string concatenation) so any reordering shows up immediately
- dropping the pool right after a nested scope, the `join itself` case again

Counts scale down under miri, which interprets one instruction at a time.

## Related Documentation

- [`src/scope.rs`](../src/scope.rs): the implementation.
- [`docs/latch.md`](latch.md): the counter behind the join, and the safety rule it rests on.
- [`docs/job_graph.md`](job_graph.md): `spawn_after`, for when "wait for everything" is too coarse.
- [`docs/lane_queue.md`](lane_queue.md): where a spawned job actually lands.

External reading:

- [Rayon's scope](https://docs.rs/rayon/latest/rayon/fn.scope.html): the same API shape, with more knobs.
- [`std::thread::scope`](https://doc.rust-lang.org/std/thread/fn.scope.html): the borrowing argument, in the standard library.
- [Subtyping and variance](https://doc.rust-lang.org/nomicon/subtyping.html): why `PhantomData<&'scope mut &'scope ()>` and not something simpler.
