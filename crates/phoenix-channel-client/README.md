# phoenix-channel-client

Managed sockets and channels for Phoenix Channels v2.

The client owns heartbeat, reconnect, rejoin, buffering, timeout, Presence, and
event subscription behavior. Supply a `Connector` and `Timer`, then run the
returned driver on the runtime used by the application.

```rust,ignore
use phoenix_channel_client::{Options, Socket, static_join_payload};
use phoenix_channel_runtime_native::{NativeConnector, NativeTimer};
use serde_json::json;

let (socket, driver) = Socket::new(
    NativeConnector::new("wss://example.com/socket/websocket?vsn=2.0.0"),
    NativeTimer,
    Options::default(),
);
tokio::task::spawn_local(driver);

let channel = socket.channel(
    "room:lobby",
    static_join_payload(json!({"user_id": "123"})),
)?;
channel.join().await?;
let reply = channel.call("new_message", json!({"body": "hello"})).await?;
```

Use `Endpoint` with a connection configuration loader when credentials must be
refreshed for each connection attempt. Join payload loaders are evaluated for
every join and rejoin.

Calls waiting for a connection or join are buffered. A call already sent when
the transport disconnects returns `ClientError::Interrupted` and is not retried.
Event subscribers are bounded and receive an explicit lag event if they fall
behind.

Enable the `tracing` feature to convert structured client telemetry into
`tracing` events.

## License

MIT
