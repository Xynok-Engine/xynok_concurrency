# xynok_concurrency

[![discord invite link](https://img.shields.io/discord/1495504680711880714?logo=discord)](https://discord.gg/a2qzfrFzWT)

This repository contains the tools, utilities, and data types that allow Xynok Engine to manage multi-threading and asynchronous tasks.

In this repo, you will find various types that resemble the synchronization primitives found in rayon, tokio, smol, or crossbeam, though they are often simpler or exhibit different behaviors.
This is because when I started this project, I had almost zero knowledge of concurrent programming, especially in Rust.

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


