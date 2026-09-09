// CASE: blocking/CPU work and filesystem work inside an async application.
// async syntax does not make std::fs or long computations nonblocking.
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let result = tokio::task::spawn_blocking(|| (1_u64..=1000).sum::<u64>()).await.unwrap();
    assert_eq!(result, 500500);
    // Bounded CPU pools (such as xynok) can be more suitable for sustained CPU load.
    // Tokio filesystem operations generally delegate blocking work to a pool;
    // .await does not imply that every operation uses a socket-style reactor.
    let source = tokio::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).await?;
    assert!(source.contains("xynok-async-lessons"));
    println!("CPU result {result}; read {} bytes", source.len());
    Ok(())
}
