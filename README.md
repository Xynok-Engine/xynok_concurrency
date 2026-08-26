# xynok_concurrency

[![discord invite link](https://img.shields.io/discord/1495504680711880714?logo=discord)](https://discord.gg/a2qzfrFzWT)

This repo stores the tools, utilities, and data types that allow Xynok Engine to handle multi-threading and asynchronous tasks.

- `src/ring_buffer_fifo/`: bounded lock-free work-stealing ring buffer, batch steal — see [docs/ring-buffer-fifo.md](docs/ring-buffer-fifo.md)
- `src/ring_buffer_lifo/`: bounded Chase-Lev work-stealing deque, LIFO for the owner — see [docs/ring-buffer-lifo.md](docs/ring-buffer-lifo.md)
- `src/lane_queue.rs`: the lane-wide queue every non-worker pushes into, batch handoff into a local ring
- `src/utils/`: cache padding, backoff, spin lock, park/unpark waker, inline closures

## Design docs

- [docs/lane_queue.md](docs/lane_queue.md): the per-lane queue that sits next to the per-worker rings, why it exists, and what still needs wiring up
- [docs/lanes.md](docs/lanes.md): how the engine's lanes fit together across a frame, and the plan for `xynok_concurrency` and `xynok_ecs` that gets there

## Examples

To run an example, use the following command:

```bash
cargo run --release --example <example_name>
cargo run --release --example bench_spin      # This runs `examples/bench_spin.rs`
```

## Tests

**`loom` test**
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


