# Phoenix Channel Runtime

Rust clients and protocol building blocks for Phoenix Channels v2.

The workspace provides a managed client, native and browser transports, and a
lower-level protocol crate:

| Crate | Purpose |
| --- | --- |
| [`phoenix-channel-client`](crates/phoenix-channel-client) | Managed sockets, channels, reconnects, rejoins, Presence, and request timeouts |
| [`phoenix-channel-runtime-native`](crates/phoenix-channel-runtime-native) | Tokio WebSocket transport and a `Send + Sync` worker API |
| [`phoenix-channel-runtime-web`](crates/phoenix-channel-runtime-web) | Browser WebSocket transport with LongPoll fallback and page lifecycle handling |
| [`phoenix-channel-runtime`](crates/phoenix-channel-runtime) | Phoenix v2 frames, binary serialization, Presence synchronization, and protocol state |

## Native client

Add the managed client and native transport:

```toml
[dependencies]
phoenix-channel-client = "0.1"
phoenix-channel-runtime-native = "0.1"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt"] }
```

`NativeSocket` runs the client driver on its own thread. Its handles can be
moved between Tokio tasks.

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
    let channel = socket
        .channel("room:lobby", json!({"user_id": "123"}))
        .await?;
    channel.join().await?;

    let reply = channel
        .call("new_message", json!({"body": "hello"}))
        .await?;
    println!("{}", reply.response);

    channel.leave().await?;
    socket.shutdown().await?;
    Ok(())
}
```

## Browser client

The browser transport targets `wasm32-unknown-unknown`. Start the client driver
with `spawn_local` and keep `WebLifecycle` alive for as long as the socket is in
use.

```rust,ignore
use std::time::Duration;

use phoenix_channel_client::{
    ConnectionConfig, Endpoint, Options, Socket, static_join_payload,
};
use phoenix_channel_runtime_web::{WebConnector, WebLifecycle, WebTimer};
use serde_json::json;

let endpoint = Endpoint::new("wss://example.com/socket")?
    .connection_config(ConnectionConfig::default().auth_token("token"));
let connector = WebConnector::from_endpoint(endpoint)
    .long_poll_fallback(Duration::from_secs(5));
let (socket, driver) = Socket::new(connector, WebTimer, Options::default());
wasm_bindgen_futures::spawn_local(driver);
let lifecycle = WebLifecycle::attach(socket.clone())?;

let channel = socket.channel(
    "room:lobby",
    static_join_payload(json!({"user_id": "123"})),
)?;
channel.join().await?;
let reply = channel.call("new_message", json!({"body": "hello"})).await?;
```

`WebConnector::long_poll_fallback` verifies an opened WebSocket with a Phoenix
heartbeat before accepting it. If the WebSocket cannot open or answer the
heartbeat before the configured deadline, the connector uses Phoenix LongPoll.
LongPoll accepts JSON payloads; binary calls return `BinaryNotSupported` without
closing the connection.

## Endpoints and authentication

Pass a Phoenix socket base URL such as `wss://example.com/socket` to `Endpoint`.
The client appends `/websocket` and `vsn=2.0.0` for WebSocket connections and
uses `/longpoll` when the browser fallback is active.

```rust
use phoenix_channel_client::{ConnectionConfig, Endpoint};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = Endpoint::new("wss://example.com/socket")?
    .connection_config(
        ConnectionConfig::default()
            .param("locale", "en")
            .auth_token("short-lived-token"),
    );
# let _ = endpoint;
# Ok(())
# }
```

`auth_token` uses Phoenix 1.8's WebSocket subprotocol authentication and the
equivalent LongPoll header. Use `param` for ordinary query parameters or when
connecting to an older server that expects a token in the URL.

For rotating credentials, install a connection configuration loader. It is
called for every connection attempt. Join payload loaders are likewise called
for every join and rejoin.

## Socket and channel lifecycle

The managed client:

- sends heartbeats and disconnects connections whose heartbeat expires;
- reconnects abnormal transport failures according to the reconnect policy;
- rejoins active channels after reconnection;
- buffers calls made while a socket or channel is waiting to connect;
- returns `ClientError::Interrupted` for a transmitted call whose connection is
  lost instead of sending it twice;
- applies independent connect, join, call, leave, and heartbeat timeouts.

Socket and channel status streams expose lifecycle changes without consuming
the event streams. Event streams are bounded and report how many events were
dropped when a subscriber falls behind.

## Calls, casts, and subscriptions

`Channel::call` waits for a correlated `phx_reply`. `Channel::cast` sends a
message without waiting for a reply. `Channel::subscribe` filters broadcasts by
event name, while `Channel::events` receives the complete channel event stream.
Typed JSON helpers are available on the native worker API.

Phoenix binary v2 frames are supported over WebSocket. Both JSON and binary
replies retain their Phoenix reply status.

## Presence

`Channel::presence` combines `presence_state` and `presence_diff` messages into
a current `PresenceState` and a stream of joins and leaves. If its bounded event
subscription falls behind, it reports `PresenceStreamError::Desynchronized`.
Call `resync` to leave and rejoin the channel and request a fresh state.

## Telemetry

`Options::telemetry` receives socket, channel, frame, reconnect, rejoin, and
call lifecycle events. Enable the client crate's `tracing` feature to use
`tracing_telemetry_hook`.

## Protocol API

Applications that provide their own transport or execution model can use the
protocol crate directly.

```rust
use phoenix_channel_runtime::{Frame, Protocol, ProtocolEvent};
use serde_json::json;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut protocol = Protocol::new();
let join = protocol.join("room:lobby", json!({}))?;
let encoded = join.frame.encode_text()?;

let incoming = Frame::decode_text(
    r#"["1","1","room:lobby","phx_reply",{"status":"ok","response":{}}]"#,
)?;
assert!(matches!(
    protocol.receive(incoming)?,
    ProtocolEvent::Joined { .. }
));
# let _ = encoded;
# Ok(())
# }
```

## Compatibility

- Phoenix Channels serializer: v2 (`vsn=2.0.0`)
- Phoenix server integration tests: Phoenix 1.7 and 1.8
- Rust: 1.85 or newer
- Native: Tokio with Rustls WebSockets
- Browser: WebSocket and Phoenix LongPoll on `wasm32-unknown-unknown`

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
cargo check -p phoenix-channel-client --target wasm32-unknown-unknown --all-features
cargo check -p phoenix-channel-runtime-web --target wasm32-unknown-unknown
```

Browser integration tests require a running fixture and `wasm-pack`; the CI
suite runs them in Chrome, Firefox, and Safari.

## License

MIT
