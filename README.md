# xynok_concurrency

[![discord invite link](https://img.shields.io/discord/1495504680711880714?logo=discord)](https://discord.gg/a2qzfrFzWT)

This repo stores the tools, utilities, and data types that allow Xynok Engine to handle multi-threading and asynchronous tasks.

- `src/lazy_atomic/`: contains park/unpark atomic types
- `src/lockfree_atomic/`: contains lock-free atomic types

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



