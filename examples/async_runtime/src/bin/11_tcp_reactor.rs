// CASE: real nonblocking TCP I/O on loopback. No external service needed.
// Tokio's I/O driver tracks socket readiness and wakes the waiting task.
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = async {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0; 4];
            socket.read_exact(&mut request).await?;
            assert_eq!(&request, b"ping");
            socket.write_all(b"pong").await?;
            Ok::<_, std::io::Error>(())
        };
        let client = async {
            let mut socket = TcpStream::connect(address).await?;
            socket.write_all(b"ping").await?;
            let mut response = [0; 4];
            socket.read_exact(&mut response).await?;
            assert_eq!(&response, b"pong");
            println!("TCP ping -> pong");
            Ok::<_, std::io::Error>(())
        };
        tokio::try_join!(server, client)?;
        Ok::<_, std::io::Error>(())
    }).await.expect("loopback example timed out")
}
