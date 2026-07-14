# phoenix-channel-runtime

Rust crates for Phoenix Channels v2.

## Crates

- `phoenix-channel-runtime`: JSON frames and protocol state.
- `phoenix-channel-runtime-web`: browser WebSocket transport.
- `phoenix-channel-runtime-native`: Tokio WebSocket transport.

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

`Session` combines the protocol state with a `Transport` for sequential use.

```rust,ignore
use phoenix_channel_runtime::Session;
use phoenix_channel_runtime_native::NativeTransport;

let transport = NativeTransport::connect(
    "wss://example.test/socket/websocket?vsn=2.0.0"
).await?;
let mut session = Session::new(transport);
let joined = session.join("room:lobby", serde_json::json!({})).await?;
```

## Verification

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p phoenix-channel-runtime-web --target wasm32-unknown-unknown
```

## License

MIT
