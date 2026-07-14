#![cfg(target_arch = "wasm32")]

use phoenix_channel_client::{
    ChannelEvent, ConnectionConfig, Endpoint, Options, Socket, SocketEvent, static_join_payload,
};
use phoenix_channel_runtime::{Payload, ProtocolEvent, ReplyStatus};
use phoenix_channel_runtime_web::{WebConnector, WebLifecycle, WebTimer};
use serde_json::json;
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn interoperates_with_a_real_phoenix_server() {
    let endpoint_url = option_env!("PHOENIX_E2E_URL")
        .unwrap_or("ws://127.0.0.1:4056/socket")
        .to_owned();
    let endpoint = Endpoint::new(endpoint_url)
        .expect("endpoint should be valid")
        .connection_config(
            ConnectionConfig::default()
                .param("client", "rust")
                .param("token", "secret")
                .auth_token("secret"),
        );
    let (socket, driver) = Socket::new(
        WebConnector::from_endpoint(endpoint),
        WebTimer,
        Options::default(),
    );
    let mut socket_events = socket.events().expect("socket events should be available");
    let mut status_changes = socket.status_changes();
    let _lifecycle = WebLifecycle::attach(socket.clone()).expect("lifecycle hooks should attach");
    wasm_bindgen_futures::spawn_local(driver);
    loop {
        match socket_events.next().await {
            Some(SocketEvent::Connected) => break,
            Some(SocketEvent::Disconnected { reason }) => {
                panic!("Phoenix connection failed: {reason}")
            }
            Some(_) => {}
            None => panic!("socket event stream ended before connecting"),
        }
    }

    let mut channel = socket
        .channel("room:lobby", static_join_payload(json!({"name": "web"})))
        .expect("channel should be created");
    assert_eq!(
        channel.join().await.expect("join should succeed"),
        json!({"name": "web", "room": "lobby"})
    );

    let reply = channel
        .call("echo", json!({"value": 42}))
        .await
        .expect("call should succeed");
    assert_eq!(reply.status, ReplyStatus::Ok);
    assert_eq!(reply.response, json!({"value": 42}));

    let reply = channel
        .call("binary", vec![1, 2, 3, 4])
        .await
        .expect("binary call should succeed");
    assert_eq!(reply.response, Payload::Binary(vec![4, 3, 2, 1]));

    let window = web_sys::window().expect("window should be available");
    window
        .dispatch_event(&web_sys::Event::new("offline").unwrap())
        .expect("offline event should dispatch");
    loop {
        match status_changes.changed().await {
            Some(phoenix_channel_client::SocketStatus::Disconnected) => break,
            Some(_) => {}
            None => panic!("socket status stream ended while going offline"),
        }
    }
    window
        .dispatch_event(&web_sys::Event::new("online").unwrap())
        .expect("online event should dispatch");
    let reply = channel
        .call("echo", json!({"after": "online"}))
        .await
        .expect("channel should rejoin after returning online");
    assert_eq!(reply.response, json!({"after": "online"}));

    let reply = channel
        .call("broadcast", json!({"value": "hello"}))
        .await
        .expect("broadcast call should succeed");
    assert_eq!(reply.response, json!({"sent": true}));
    let message = loop {
        match channel.next_event().await {
            Some(ChannelEvent::Protocol(ProtocolEvent::Message(message)))
                if message.event == "broadcast" =>
            {
                break message;
            }
            Some(_) => {}
            None => panic!("channel event stream ended before the broadcast"),
        }
    };
    assert_eq!(message.topic, "room:lobby");
    assert_eq!(message.event, "broadcast");
    assert_eq!(message.payload, json!({"sender": "web", "value": "hello"}));

    assert_eq!(
        channel.leave().await.expect("leave should succeed"),
        json!({})
    );
    socket.shutdown().await.expect("shutdown should succeed");
}
