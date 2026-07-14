#![cfg(not(target_arch = "wasm32"))]

use std::{
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use phoenix_channel_client::{
    ChannelEvent, ConnectionConfig, Endpoint, Options, Socket, SocketEvent, static_join_payload,
};
use phoenix_channel_runtime::{ProtocolEvent, ReplyStatus};
use phoenix_channel_runtime_native::{NativeConnector, NativeSocket, NativeTimer};
use serde_json::json;

struct PhoenixServer(Child);

impl PhoenixServer {
    fn start(port: u16) -> Self {
        let fixture =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/phoenix_server");
        let child = Command::new("mix")
            .args(["run", "--no-halt"])
            .current_dir(fixture)
            .env("PORT", port.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to start the Phoenix fixture");
        let server = Self(child);
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return server;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("Phoenix fixture did not start within 30 seconds");
    }
}

impl Drop for PhoenixServer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn available_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("failed to reserve a port")
        .local_addr()
        .expect("failed to inspect the reserved port")
        .port()
}

#[tokio::test(flavor = "current_thread")]
async fn interoperates_with_a_real_phoenix_server() {
    if std::env::var_os("PHOENIX_E2E").is_none() {
        return;
    }

    let port = available_port();
    let _server = PhoenixServer::start(port);
    let endpoint = Endpoint::new(format!("ws://127.0.0.1:{port}/socket"))
        .expect("endpoint should be valid")
        .connection_config(
            ConnectionConfig::default()
                .param("client", "rust")
                .auth_token("secret"),
        );

    tokio::task::LocalSet::new()
        .run_until(async move {
            let (socket, driver) = Socket::new(
                NativeConnector::from_endpoint(endpoint),
                NativeTimer,
                Options::default(),
            );
            let mut socket_events = socket.events().expect("socket events should be available");
            tokio::task::spawn_local(driver);
            tokio::time::timeout(Duration::from_secs(5), async {
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
            })
            .await
            .expect("Phoenix connection timed out");

            let mut channel = socket
                .channel("room:lobby", static_join_payload(json!({"name": "native"})))
                .expect("channel should be created");
            assert_eq!(
                channel.join().await.expect("join should succeed"),
                json!({"name": "native", "room": "lobby"})
            );

            let reply = channel
                .call("echo", json!({"value": 42}))
                .await
                .expect("call should succeed");
            assert_eq!(reply.status, ReplyStatus::Ok);
            assert_eq!(reply.response, json!({"value": 42}));

            let reply = channel
                .call("broadcast", json!({"value": "hello"}))
                .await
                .expect("broadcast call should succeed");
            assert_eq!(reply.response, json!({"sent": true}));
            let message = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match channel.next_event().await {
                        Some(ChannelEvent::Protocol(ProtocolEvent::Message(message)))
                            if message.event == "broadcast" =>
                        {
                            break message;
                        }
                        Some(_) => {}
                        None => panic!("channel event stream ended before the broadcast"),
                    }
                }
            })
            .await
            .expect("Phoenix broadcast timed out");
            assert_eq!(message.topic, "room:lobby");
            assert_eq!(message.event, "broadcast");
            assert_eq!(
                message.payload,
                json!({"sender": "native", "value": "hello"})
            );

            assert_eq!(
                channel.leave().await.expect("leave should succeed"),
                json!({})
            );
            socket.shutdown().await.expect("shutdown should succeed");
        })
        .await;

    let socket = NativeSocket::spawn(
        format!("ws://127.0.0.1:{port}/socket"),
        ConnectionConfig::default()
            .param("client", "rust")
            .auth_token("secret"),
    )
    .expect("native worker should start");
    socket
        .connect()
        .await
        .expect("native socket should connect");
    let channel = socket
        .channel("room:threaded", json!({"name": "threaded"}))
        .await
        .expect("native channel should be created");
    assert_eq!(
        channel.join().await.expect("native join should succeed"),
        json!({"name": "threaded", "room": "threaded"})
    );

    let channel_from_another_task = channel.clone();
    let reply = tokio::spawn(async move {
        channel_from_another_task
            .call("echo", json!({"from": "tokio task"}))
            .await
    })
    .await
    .expect("Send channel task should complete")
    .expect("native call should succeed");
    assert_eq!(reply.response, json!({"from": "tokio task"}));

    socket
        .disconnect()
        .await
        .expect("native socket should disconnect");
    socket
        .connect()
        .await
        .expect("native socket should reconnect");
    let reply = channel
        .call("echo", json!({"after": "reconnect"}))
        .await
        .expect("native channel should rejoin");
    assert_eq!(reply.response, json!({"after": "reconnect"}));
    socket.shutdown().await.expect("native worker should stop");
}
