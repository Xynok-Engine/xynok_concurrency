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

Always pass `--release`. The default `dev` profile builds at `opt-level=0`, which inflates every
number by roughly 5x and does it unevenly: `#[inline]` is a no-op there, so the layers this crate is
built from (`CachePadded`, the `UnsafeCell::with_mut` closure, `Drop` on the guard) each cost a real
call, while `std`'s locks drop straight into an already-optimised `libpthread`. The example prints a
warning if you forget.

- `examples/bench_spin.rs`: measures `SpinLock` against `std::sync::Mutex` under contention. A spin
  lock buys throughput with fairness, so read the `unfair` column (slowest thread / fastest thread)
  and the `[min .. max]` spread next to `ns/op`, not `ns/op` alone - a lock that lets one thread
  barge through its whole loop while the rest starve scores *better* on `ns/op` than a fair one.


## Tests

**`loom` test**
```bash
LOOM_LOCATION=1 RUSTFLAGS="--cfg loom" cargo test --lib
```

