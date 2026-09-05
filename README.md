# xynok_concurrency



[![discord invite link](https://img.shields.io/discord/1495504680711880714?logo=discord)](https://discord.gg/a2qzfrFzWT)

This repo stores the tools, utilities, and data types that allow Xynok Engine to handle multi-threading and asynchronous tasks.

In this repo, you'll find various types that resemble synchronization primitives found in rayon, tokio, smol, or crossbeam, though they are often simpler or exhibit different behaviors.
This is because when I started this project, I had almost zero knowledge of concurrent programming, especially in Rust.
I learned these concepts and implemented the types myself to sharpen my concurrency skills.

## What is in here

**The lanes**, which is where you start:

- `src/lanes.rs`: the lane registry. A compute pool for CPU-bound frame work, a small async pool for anything that spends its time waiting, and a queue only the main thread drains.
- `src/pool/`: the work-stealing pool itself, one ring per participant, plus the sleep protocol that lets a worker go to sleep without missing a job.

**Building on the pool:**

- `src/scope.rs`: `scope`, `join`, `parallel_for`, `par_reduce`. A job inside a scope may borrow the caller's stack, and the thread waiting at the join runs other jobs instead of idling.
- `src/job_graph.rs`: `spawn_after(deps)`, so a frame is a dependency graph rather than a chain of barriers.
- `src/latch.rs`: the countdown every join point is built from.
- `src/per_worker.rs`: one slot per thread, so results are written without sharing and merged in index order.
- `src/bump.rs`: a per-worker scratch arena where allocating is a pointer bump.

- `src/task.rs`: the async lane's executor. A future becomes a task, and the task puts itself back in the lane every time it is woken.
- `src/channel.rs`: a one-shot channel, for a job that has to hand a value back. The receiving end is also a `Future`.
- `src/profile.rs`, `src/pool/counters.rs`: what the pool itself is doing, since none of it is visible from outside.

**The queues underneath:**

- `src/ring_buffer_fifo/`: bounded lock-free work-stealing ring buffer, batch steal. See [docs/ring_buffer.md](docs/ring_buffer.md).
- `src/ring_buffer_lifo/`: bounded Chase-Lev work-stealing deque, LIFO for the owner.
- `src/ring_buffer_spsc/`: one writer, one reader, wait-free on both ends. This is how the audio thread receives commands without ever taking a lock or allocating.
- `src/utils/queue_batching/`: the lane-wide queue every non-worker pushes into, with a batch handoff into a local ring. See [docs/lane_queue.md](docs/lane_queue.md).
- `src/utils/`: cache padding, backoff, spin lock, park/unpark waker, inline closures.

## Getting started

```rust
use xynok_concurrency::lanes::{LaneId, Lanes, LanesConfig};

let lanes = Lanes::new(LanesConfig::default());

// CPU-bound frame work, split across every core
lanes.compute().parallel_for(10_000, 64, |i| {
    let _ = i * i;
});

// Anything that spends its time waiting goes to the async lane and answers through a channel.
// The blocking syscall at the bottom of the chain is the only part holding a thread.
let lanes = std::sync::Arc::new(lanes);
let loader = std::sync::Arc::clone(&lanes);
let loading = lanes.spawn_async(async move {
    let bytes = loader.run_blocking(|| std::fs::read("scene.pak").ok()).await??;
    Some(bytes.len())
});

// Waiting runs other jobs rather than idling
let scene = loading.recv_in(lanes.compute());

// Work that has to happen on the main thread, drained at a fixed point in the frame
lanes.spawn_on_main(|| { /* present, window API, whatever the driver pins */ });
lanes.run_pending_on_main();

lanes.end_frame(); // every scratch arena is empty again
```

Set `XYNOK_LANE_THREADS=1` and every job runs inline on the calling thread, which answers
"is this bug caused by parallelism" in one run without touching any code.

## Design docs

**Start here:**

- [docs/lanes.md](docs/lanes.md): the lane registry, what runs where, and how work crosses between lanes.
- [docs/task.md](docs/task.md): the async lane's executor, and what it does not do yet.
- [docs/thread_pool.md](docs/thread_pool.md): one lane's work-stealing pool, job routing, and shutdown.
- [docs/scope.md](docs/scope.md): `scope`, `join`, `parallel_for`, `par_reduce`, and how a job borrows the caller's stack.

**The parts underneath:**

- [docs/latch.md](docs/latch.md): the countdown every join point is built from.
- [docs/job_graph.md](docs/job_graph.md): `spawn_after(deps)`, a frame as a dependency graph.
- [docs/sleep_protocol.md](docs/sleep_protocol.md): how a worker sleeps without missing a job.
- [docs/lane_queue.md](docs/lane_queue.md): the per-lane queue next to the per-worker rings.
- [docs/ring_buffer.md](docs/ring_buffer.md): the work-stealing rings.
- [docs/ring_buffer_spsc.md](docs/ring_buffer_spsc.md): one writer, one reader, wait-free, for the audio thread.
- [docs/inline_fn.md](docs/inline_fn.md): the job type, one cache line instead of one allocation.

**Data and memory:**

- [docs/per_worker.md](docs/per_worker.md): one slot per thread, merged in index order.
- [docs/bump.md](docs/bump.md): the per-worker scratch arena.
- [docs/channel.md](docs/channel.md): the one-shot channel a job answers through, and the future you can await.

**Tuning and platform:**

- [docs/profiling.md](docs/profiling.md): counters and the profiler sink.
- [docs/thread_priority.md](docs/thread_priority.md): QoS, nice, and EcoQoS, and how frame work is protected from background work.
- [docs/utils.md](docs/utils.md): cache padding, backoff, spin locks, slots, and the loom shim.

## Examples

To run an example, use the following command:

```bash
cargo run --release --example <example_name>
cargo run --release --example bench_spin      # This runs `examples/bench_spin.rs`
cargo run --release --example frame           # A frame's worth of lane traffic, end to end
```

## Tests

**`loom` test**

Loom runs every interleaving of a small model, which is the only way to be sure about the sleep
protocol: a bug there does not crash, it just quietly stops a thread forever.

```bash
LOOM_LOCATION=1 RUSTFLAGS="--cfg loom" cargo test --lib
```

**`miri` test**

Miri interprets the code against the C++ weak memory model, so it catches missing
`Acquire`/`Release` pairs that x86 and ARM64 hardware happen to hide at runtime.

```bash
rustup component add miri       # one-off; the toolchain is already pinned to nightly
cargo miri test --lib
cargo miri test --lib waker     # only the tests whose name contains `waker`
cargo miri test --lib ring_buffer
```

Each run explores one fixed interleaving. To sweep several:

```bash
MIRIFLAGS="-Zmiri-many-seeds=0..16" cargo miri test --lib
```


