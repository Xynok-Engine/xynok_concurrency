// CASE: async methods in traits; generic dispatch vs erased boxed futures.
use std::{future::Future, pin::Pin};
trait Loader { async fn load(&self) -> u32; }
struct Asset;
impl Loader for Asset { async fn load(&self) -> u32 { 42 } }
async fn generic(loader: &impl Loader) -> u32 { loader.load().await }
// Native async methods are not directly dyn-compatible. Explicit boxing is one
// way to provide a dynamic interface; this allocation is an API choice.
trait DynLoader { fn load(&self) -> Pin<Box<dyn Future<Output = u32> + '_>>; }
impl DynLoader for Asset {
    fn load(&self) -> Pin<Box<dyn Future<Output = u32> + '_>> { Box::pin(Loader::load(self)) }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    assert_eq!(generic(&Asset).await, 42);
    let loader: &dyn DynLoader = &Asset;
    assert_eq!(loader.load().await, 42);
    println!("generic future and boxed dynamic future both return 42");
}
