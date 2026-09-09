// CASE: producer/consumer; bounded capacity makes send await free space.
// Needs executor + channel wake logic. No I/O reactor needed by the channel.
use tokio::sync::mpsc;
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (tx, mut rx) = mpsc::channel(1);
    let producer = async move {
        for value in 1..=3 {
            tx.send(value).await.unwrap();
            println!("sent {value}");
        }
        // Dropping the final sender lets recv return None after draining.
    };
    let consumer = async move {
        let mut sum = 0;
        while let Some(value) = rx.recv().await { sum += value; }
        sum
    };
    let ((), sum) = tokio::join!(producer, consumer);
    assert_eq!(sum, 6);
    println!("channel drained and closed");
}
