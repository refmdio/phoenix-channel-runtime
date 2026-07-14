use phoenix_channel_runtime::{ProtocolEvent, Session};
use phoenix_channel_runtime_native::NativeTransport;
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:4000/socket/websocket?vsn=2.0.0".into());
    let topic = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "room:lobby".into());

    let transport = NativeTransport::connect(&url).await?;
    let mut session = Session::new(transport);
    match session.join(topic, json!({})).await? {
        ProtocolEvent::Joined {
            topic, response, ..
        } => println!("joined {topic}: {response}"),
        ProtocolEvent::JoinError {
            topic, response, ..
        } => return Err(format!("join rejected for {topic}: {response}").into()),
        event => return Err(format!("unexpected join result: {event:?}").into()),
    }
    session.close().await?;
    Ok(())
}
