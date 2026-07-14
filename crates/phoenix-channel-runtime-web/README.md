# phoenix-channel-runtime-web

Browser WebSocket and Phoenix LongPoll transports for
`wasm32-unknown-unknown`.

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

let channel = socket.channel("room:lobby", static_join_payload(json!({})))?;
channel.join().await?;
```

The fallback accepts a WebSocket only after a Phoenix heartbeat succeeds. A
failed open or health check falls back to LongPoll. LongPoll requests have a
configurable timeout and are aborted when the transport closes. LongPoll
supports JSON payloads but rejects binary calls without dropping the session.

`WebLifecycle` disconnects and resumes the socket around page and network
lifecycle events. Keep it alive while the socket is active.

## License

MIT
