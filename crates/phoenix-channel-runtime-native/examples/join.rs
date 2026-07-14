use phoenix_channel_client::{Options, Socket, static_join_payload};
use phoenix_channel_runtime_native::{NativeConnector, NativeTimer};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:4000/socket/websocket?vsn=2.0.0".into());
    let topic = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "room:lobby".into());

    tokio::task::LocalSet::new()
        .run_until(async move {
            let (socket, driver) =
                Socket::new(NativeConnector::new(url), NativeTimer, Options::default());
            tokio::task::spawn_local(driver);

            let channel = socket.channel(topic.clone(), static_join_payload(json!({})))?;
            let response = channel.join().await?;
            println!("joined {topic}: {response}");
            socket.shutdown().await?;
            Ok(())
        })
        .await
}
