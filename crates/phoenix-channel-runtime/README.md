# phoenix-channel-runtime

Phoenix Channels v2 protocol building blocks for Rust.

This crate contains the transport-independent frame codec, protocol state
machine, Presence synchronization, and session helper. Applications usually
use it through `phoenix-channel-client`; use it directly when implementing a
custom transport or execution model.

```rust
use phoenix_channel_runtime::{Frame, Protocol, ProtocolEvent};
use serde_json::json;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut protocol = Protocol::new();
let join = protocol.join("room:lobby", json!({}))?;
let encoded = join.frame.encode_text()?;

let reply = Frame::decode_text(
    r#"["1","1","room:lobby","phx_reply",{"status":"ok","response":{}}]"#,
)?;
assert!(matches!(
    protocol.receive(reply)?,
    ProtocolEvent::Joined { .. }
));
# let _ = encoded;
# Ok(())
# }
```

The default codec supports Phoenix's JSON and binary v2 representations.
`LimitedPhoenixV2Codec` can reject oversized frames and binary payloads before
they enter protocol state.

## License

MIT
