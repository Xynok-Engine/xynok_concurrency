// CASE: async syntax, lazy execution, sequential await, Result and ?.
// Required: Future contract (compiler implements it). Custom: function/body/output.
mod async_support;
async fn number(text: &str) -> Result<u32, std::num::ParseIntError> {
    println!("parsing {text}");
    text.parse() // An async fn does NOT need an await.
}
async fn sum() -> Result<u32, std::num::ParseIntError> {
    let a = number("20").await?;
    let b = number("22").await?; // Starts after a completes; no spawned task.
    Ok(a + b)
}
fn main() {
    let unused = number("never parsed");
    drop(unused); // Its body never ran.
    let future = sum();
    println!("created; body has not run yet");
    assert_eq!(async_support::block_on(future).unwrap(), 42);
    assert!(async_support::block_on(number("invalid")).is_err());
}
