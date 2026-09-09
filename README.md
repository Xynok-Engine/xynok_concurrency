# xynok_concurrency

[![discord invite link](https://img.shields.io/discord/1495504680711880714?logo=discord)](https://discord.gg/a2qzfrFzWT)

## Preface
This repository contains the tools, utilities, and data types that allow Xynok Engine to manage multi-threading and asynchronous tasks.

In this repo, you will find various types that resemble the synchronization data structures used in [rayon](https://github.com/rayon-rs/rayon), [tokio](https://github.com/tokio-rs/tokio), [smol](https://github.com/smol-rs/smol), or [crossbeam](https://github.com/crossbeam-rs/crossbeam). 
These are often simpler or exhibit different behaviors to better serve the specific logic required by the engine.

## Overview

## Install
```cargo
[dependencies]
xynok_concurrency = { git = "https://github.com/Xynok-Engine/xynok_concurrency.git", tag = "v0.1.75" }
```

## Examples

Use `run_batch` when the complete group of independent jobs is known up front:

```rust
use xynok_concurrency::thread_pool::{cfg::CfgThreadPool, ThreadPool};

let pool = ThreadPool::new(CfgThreadPool::new("frame", 4));
let mut values = [1, 2, 3, 4];
pool.run_batch(values.iter_mut().map(|value| move || *value *= 2));
assert_eq!(values, [2, 4, 6, 8]);
```

The pool collects the jobs and assigns them round-robin to the caller and background workers
before releasing the batch. Each participant runs its own work first, then steals pending work.
Calls made from a worker distribute across the background workers, without assigning an extra
share to an inactive caller thread. Assignment balances job counts; it does not pin jobs to threads.
Concurrent external callers share one host slot; callers without that slot help by stealing
while their batches are assigned to background workers.

`run_batch` waits for all jobs, including when a job panics. If iteration panics before publication,
collected jobs are dropped without running. Do not wait for a batch job from inside its iterator.
For incremental submission where jobs may start during the callback, keep using `scope` and
`Scope::spawn`. Batch jobs go directly into preallocated worker inboxes without temporary vectors. Inbox storage grows only when its capacity is exceeded.

To run an example, use the following command:

```bash
cargo run --release --example <example_name>
cargo run --release --example ring_buffer      # This runs `examples/ring_buffer.rs`
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



I have been working on the next phase of the Xynok concurrency library. My primary goal is to implement asynchronous tasks using `async` and `await` syntax and integrate them into the existing work-stealing thread pool. While I have explored these concepts previously, I am now focused on a robust, full-scale implementation that allows these tasks to function seamlessly within the library's architecture.

## The Challenge of Building from Scratch

I am genuinely surprised by how little documentation exists on this topic. While there are plenty of resources explaining high-level concepts and mental models, actual implementation details are almost nonexistent. Most available guides rely on external crates, and I have found very few people attempting to build an `async` runtime or a custom thread pool from scratch using only Rust's built-in primitives.

The only reliable reference I have found is the Tokio library. However, their documentation focuses primarily on usage rather than implementation. To truly understand how their system works, one is forced to dive deep into their source code.

It feels strange that even though Rust has been stable since version 1.0, there is still such a lack of detailed guides on building a complete system that includes:
* A custom thread pool.
* A polling IO reactor.
* A full `async` / `await` task lifecycle.

Building this from the ground up is a significant challenge. I wishing I could reference how others have approached this to ensure I am on the right track. Relying heavily on LLMs like Claude or ChatGPT has been helpful, but I sometimes feel a bit discouraged by the lack of human-led discussions or community-shared experiences on this specific path.

I am currently relying on LLMs to help navigate the complexities of this implementation, but I remain cautious. I am constantly questioning whether my approach is sound or if I am missing a fundamental piece of the puzzle. If I am going to document this process for others, I want to be certain that the foundation I am building is solid. For now, I will continue to dig through the code and iterate on my implementation, hoping that this effort will eventually provide the clarity I have been looking for.


