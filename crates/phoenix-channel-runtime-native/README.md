# phoenix-channel-runtime-native

Native Tokio WebSocket transport for Phoenix Channels v2.

`NativeSocket` runs the managed client on a dedicated current-thread Tokio
runtime and exposes `Send + Sync` socket, channel, event, and Presence handles.

```rust,no_run
use phoenix_channel_client::ConnectionConfig;
use phoenix_channel_runtime_native::NativeSocket;
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = NativeSocket::spawn(
        "wss://example.com/socket",
        ConnectionConfig::default().auth_token("token"),
    )?;
    socket.connect().await?;

    let channel = socket.channel("room:lobby", json!({})).await?;
    channel.join().await?;
    let response: serde_json::Value = channel
        .call_json("profile", &json!({"user_id": "123"}))
        .await?;
    println!("{response}");

    socket.shutdown().await?;
    Ok(())
}
```

For applications that already run a local task set, `NativeConnector` and
`NativeTimer` can instead be passed directly to `phoenix-channel-client`.
Transport options support custom headers, Rustls configuration, HTTP CONNECT
proxies, message limits, and TCP `nodelay`.

## License

MIT
