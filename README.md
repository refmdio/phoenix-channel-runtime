# phoenix-channel-runtime

Rust crates for Phoenix Channels v2.

## Crates

- `phoenix-channel-runtime`: JSON frames and protocol state.
- `phoenix-channel-client`: managed `Socket` and `Channel` API.
- `phoenix-channel-runtime-web`: browser WebSocket connector and timer.
- `phoenix-channel-runtime-native`: Tokio WebSocket connector and timer.

## Managed client

```rust,ignore
use phoenix_channel_client::{Options, Socket, static_join_payload};
use phoenix_channel_runtime_native::{NativeConnector, NativeTimer};
use serde_json::json;

let (socket, driver) = Socket::new(
    NativeConnector::new("wss://example.test/socket/websocket?vsn=2.0.0"),
    NativeTimer,
    Options::default(),
);
tokio::task::spawn_local(driver);

let channel = socket.channel("room:lobby", static_join_payload(json!({})))?;
channel.join().await?;
let reply = channel.call("new_message", json!({"body": "hello"})).await?;
```

The driver sends heartbeats, reconnects the socket, rejoins channels, and
applies request timeouts. A `JoinPayloadLoader` is evaluated for every join
attempt.

Pushes waiting for a connection or channel join remain buffered. A call that
was transmitted before a disconnect returns `ClientError::Interrupted` and is
not sent again.

## Protocol API

```rust
use phoenix_channel_runtime::{Frame, Protocol, ProtocolEvent};
use serde_json::json;

let mut protocol = Protocol::new();
let join = protocol.join("room:lobby", json!({}))?;
let text = join.frame.encode_text()?;

let incoming = Frame::decode_text(
    r#"["1","1","room:lobby","phx_reply",{"status":"ok","response":{}}]"#,
)?;
assert!(matches!(
    protocol.receive(incoming)?,
    ProtocolEvent::Joined { .. }
));
# let _ = text;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Verification

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p phoenix-channel-client --target wasm32-unknown-unknown
cargo check -p phoenix-channel-runtime-web --target wasm32-unknown-unknown
```

Run the native example against a Phoenix endpoint:

```sh
cargo run -p phoenix-channel-runtime-native --example join -- \
  'ws://127.0.0.1:4000/socket/websocket?vsn=2.0.0' \
  'room:lobby'
```

## License

MIT
