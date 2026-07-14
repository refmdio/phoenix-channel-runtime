use std::{cell::RefCell, collections::VecDeque, sync::OnceLock, time::Instant};

use futures::{channel::mpsc, executor::LocalPool, future::LocalBoxFuture, task::LocalSpawnExt};
use serde_json::json;

use super::*;

struct MockTransport {
    incoming: mpsc::UnboundedReceiver<WireMessage>,
    sent: mpsc::UnboundedSender<WireMessage>,
}

impl Transport for MockTransport {
    fn send<'a>(
        &'a mut self,
        message: WireMessage,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        let result = self
            .sent
            .unbounded_send(message)
            .map_err(|_| TransportError::new("test receiver closed"));
        Box::pin(async move { result })
    }

    fn receive<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
        Box::pin(async move {
            Ok(self.incoming.next().await.map_or_else(
                || TransportEvent::Closed(TransportClose::connection_ended()),
                TransportEvent::Message,
            ))
        })
    }

    fn close<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
struct MockConnector {
    transports: Rc<RefCell<VecDeque<Box<dyn Transport>>>>,
}

impl Connector for MockConnector {
    fn connect(
        &self,
        _context: ConnectContext,
    ) -> LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>> {
        let result = self
            .transports
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| TransportError::new("no test transport"));
        Box::pin(async move { result })
    }
}

#[derive(Clone, Copy)]
struct PendingConnector;

impl Connector for PendingConnector {
    fn connect(
        &self,
        _context: ConnectContext,
    ) -> LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>> {
        Box::pin(futures::future::pending())
    }
}

struct ClosingTransport {
    close: Option<TransportClose>,
}

struct CloseCaptureTransport {
    close: Rc<RefCell<Option<TransportCloseRequest>>>,
}

impl Transport for CloseCaptureTransport {
    fn send<'a>(
        &'a mut self,
        _message: WireMessage,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        Box::pin(async { Ok(()) })
    }

    fn receive<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
        Box::pin(futures::future::pending())
    }

    fn close<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        Box::pin(async { Ok(()) })
    }

    fn close_with<'a>(
        &'a mut self,
        request: TransportCloseRequest,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        *self.close.borrow_mut() = Some(request);
        Box::pin(async { Ok(()) })
    }
}

impl Transport for ClosingTransport {
    fn send<'a>(
        &'a mut self,
        _message: WireMessage,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        Box::pin(async { Ok(()) })
    }

    fn receive<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
        Box::pin(async move {
            let close = self
                .close
                .take()
                .unwrap_or_else(TransportClose::connection_ended);
            Ok(TransportEvent::Closed(close))
        })
    }

    fn close<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        Box::pin(async { Ok(()) })
    }
}

struct MockPeer {
    incoming: Option<mpsc::UnboundedSender<WireMessage>>,
    sent: mpsc::UnboundedReceiver<WireMessage>,
}

fn connection() -> (Box<dyn Transport>, MockPeer) {
    let (incoming_tx, incoming) = mpsc::unbounded();
    let (sent, sent_rx) = mpsc::unbounded();
    (
        Box::new(MockTransport { incoming, sent }),
        MockPeer {
            incoming: Some(incoming_tx),
            sent: sent_rx,
        },
    )
}

struct TimerRequest {
    duration: Duration,
    fire: oneshot::Sender<()>,
}

#[derive(Clone)]
struct ManualTimer {
    requests: mpsc::UnboundedSender<TimerRequest>,
}

#[derive(Clone, Copy)]
struct ImmediateZeroTimer;

impl Timer for ImmediateZeroTimer {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        if duration.is_zero() {
            Box::pin(async {})
        } else {
            Box::pin(futures::future::pending())
        }
    }

    fn now(&self) -> Duration {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        ORIGIN.get_or_init(Instant::now).elapsed()
    }
}

impl Timer for ManualTimer {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        let (fire, receiver) = oneshot::channel();
        let _ = self
            .requests
            .unbounded_send(TimerRequest { duration, fire });
        Box::pin(async move {
            let _ = receiver.await;
        })
    }

    fn now(&self) -> Duration {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        ORIGIN.get_or_init(Instant::now).elapsed()
    }
}

fn timer() -> (ManualTimer, mpsc::UnboundedReceiver<TimerRequest>) {
    let (requests, receiver) = mpsc::unbounded();
    (ManualTimer { requests }, receiver)
}

fn connector(transports: impl IntoIterator<Item = Box<dyn Transport>>) -> MockConnector {
    MockConnector {
        transports: Rc::new(RefCell::new(transports.into_iter().collect())),
    }
}

async fn next_frame(peer: &mut MockPeer) -> Frame {
    let WireMessage::Text(text) = peer.sent.next().await.expect("outbound frame") else {
        panic!("expected text frame")
    };
    Frame::decode_text(&text).unwrap()
}

fn reply(peer: &MockPeer, request: &Frame, status: &str, response: Value) {
    let frame = Frame::new(
        request.join_ref.clone(),
        request.reference.clone(),
        request.topic.clone(),
        "phx_reply",
        json!({"status": status, "response": response}),
    );
    peer.incoming
        .as_ref()
        .unwrap()
        .unbounded_send(WireMessage::Text(frame.encode_text().unwrap()))
        .unwrap();
}

#[test]
fn stops_after_a_clean_transport_close() {
    let transport: Box<dyn Transport> = Box::new(ClosingTransport {
        close: Some(TransportClose::new(Some(1000), "complete", true)),
    });
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    let mut events = socket.events().unwrap();
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        assert_eq!(
            events.next().await,
            Some(SocketEvent::Disconnected {
                reason: DisconnectReason::Closed(
                    TransportClose::new(Some(1000), "complete", true,)
                ),
            })
        );
        assert_eq!(socket.status(), SocketStatus::Disconnected);
        socket.shutdown().await.unwrap();
        assert!(matches!(
            events.next().await,
            Some(SocketEvent::ReconnectStopped { .. })
        ));
        assert_eq!(events.next().await, Some(SocketEvent::Closed));
        assert_eq!(socket.status(), SocketStatus::Closed);
    });
}

#[test]
fn reconnect_policy_can_override_close_classification() {
    let first: Box<dyn Transport> = Box::new(ClosingTransport {
        close: Some(TransportClose::new(Some(1008), "refresh auth", false)),
    });
    let (second, _peer) = connection();
    let options = Options::default().reconnect_policy(|context| {
        assert_eq!(context.attempt, 1);
        assert!(matches!(context.reason, DisconnectReason::Closed(_)));
        ReconnectAction::RetryAfter(Duration::ZERO)
    });
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector([first, second]), ImmediateZeroTimer, options);
    let mut events = socket.events().unwrap();
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let mut retry_scheduled = false;
        loop {
            match events.next().await {
                Some(SocketEvent::ReconnectScheduled { .. }) => retry_scheduled = true,
                Some(SocketEvent::Connected) if retry_scheduled => break,
                Some(_) => {}
                None => panic!("socket event stream ended"),
            }
        }
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn forwards_explicit_websocket_close_details() {
    let captured = Rc::new(RefCell::new(None));
    let transport: Box<dyn Transport> = Box::new(CloseCaptureTransport {
        close: captured.clone(),
    });
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        socket.disconnect_with(1000, "complete").await.unwrap();
        assert_eq!(
            captured.borrow().as_ref(),
            Some(&TransportCloseRequest::new(1000, "complete"))
        );
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn bounded_event_subscribers_report_dropped_events() {
    let (sender, mut receiver) = mpsc::channel(2);
    let mut subscriber = EventSubscriber { sender, dropped: 0 };

    assert!(send_bounded(&mut subscriber, SocketEvent::Connected));
    assert!(send_bounded(&mut subscriber, SocketEvent::Connected));
    assert!(send_bounded(&mut subscriber, SocketEvent::Connected));
    assert!(send_bounded(&mut subscriber, SocketEvent::Connected));
    assert_eq!(subscriber.dropped, 1);

    let mut pool = LocalPool::new();
    pool.run_until(async {
        assert_eq!(receiver.next().await, Some(SocketEvent::Connected));
        assert_eq!(receiver.next().await, Some(SocketEvent::Connected));
        assert_eq!(receiver.next().await, Some(SocketEvent::Connected));
    });

    assert!(send_bounded(&mut subscriber, SocketEvent::Connected));
    pool.run_until(async {
        assert_eq!(
            receiver.next().await,
            Some(SocketEvent::Lagged { dropped: 1 })
        );
        assert_eq!(receiver.next().await, Some(SocketEvent::Connected));
    });
}

#[test]
fn explicitly_connects_disconnects_and_connects_again() {
    let (first, _first_peer) = connection();
    let (second, _second_peer) = connection();
    let connector = connector([first, second]);
    let (timer, _timer_requests) = timer();
    let options = Options::default().connect_on_start(false);
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    let mut events = socket.events().unwrap();
    let mut statuses = socket.status_changes();
    assert_eq!(socket.status(), SocketStatus::Disconnected);
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        socket.connect().await.unwrap();
        assert_eq!(
            events.next().await,
            Some(SocketEvent::Connecting { attempt: 0 })
        );
        assert_eq!(events.next().await, Some(SocketEvent::Connected));
        assert_eq!(statuses.changed().await, Some(SocketStatus::Connected));

        socket.disconnect().await.unwrap();
        assert_eq!(
            events.next().await,
            Some(SocketEvent::Disconnected {
                reason: DisconnectReason::Requested,
            })
        );
        assert_eq!(socket.status(), SocketStatus::Disconnected);
        assert_eq!(statuses.changed().await, Some(SocketStatus::Disconnected));

        socket.connect().await.unwrap();
        assert_eq!(
            events.next().await,
            Some(SocketEvent::Connecting { attempt: 0 })
        );
        assert_eq!(events.next().await, Some(SocketEvent::Connected));
        socket.shutdown().await.unwrap();
        assert_eq!(events.next().await, Some(SocketEvent::Closed));
    });
}

#[test]
fn configures_operation_timeouts_independently() {
    let options = Options::default()
        .join_timeout(Duration::from_secs(11))
        .call_timeout(Duration::from_secs(12))
        .leave_timeout(Duration::from_secs(13))
        .event_capacity(7);
    assert_eq!(options.join_timeout, Duration::from_secs(11));
    assert_eq!(options.call_timeout, Duration::from_secs(12));
    assert_eq!(options.leave_timeout, Duration::from_secs(13));
    assert_eq!(options.event_capacity, 7);
}

#[test]
fn emits_structured_telemetry_for_socket_channel_and_frames() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let observed = Rc::new(RefCell::new(Vec::new()));
    let telemetry = {
        let observed = observed.clone();
        Rc::new(move |event: &TelemetryEvent| observed.borrow_mut().push(event.clone()))
    };
    let options = Options::default().telemetry(telemetry);
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(server, channel.join());
        joined.unwrap();
        socket.shutdown().await.unwrap();
    });

    let observed = observed.borrow();
    assert!(observed.iter().any(|event| matches!(
        event,
        TelemetryEvent::FrameSent { event, .. } if event == "phx_join"
    )));
    assert!(observed.iter().any(|event| matches!(
        event,
        TelemetryEvent::FrameReceived { event, .. } if event == "phx_reply"
    )));
    assert!(observed.iter().any(|event| matches!(
        event,
        TelemetryEvent::Channel { topic, .. } if topic == "room:lobby"
    )));
}

#[test]
fn times_out_a_stalled_connection_attempt() {
    let (timer, mut timer_requests) = timer();
    let timeout = Duration::from_millis(43);
    let options = Options::default().connect_timeout(timeout);
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(PendingConnector, timer, options);
    let mut events = socket.events().unwrap();
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let request = timer_requests.next().await.unwrap();
        assert_eq!(request.duration, timeout);
        request.fire.send(()).unwrap();

        let Some(SocketEvent::Disconnected {
            reason: DisconnectReason::Connect(error),
        }) = events.next().await
        else {
            panic!("expected a structured connection timeout")
        };
        assert_eq!(error.kind(), TransportErrorKind::Connect);
        assert!(error.message().contains("timed out"));
        socket.shutdown().await.unwrap();
        assert_eq!(
            events.next().await,
            Some(SocketEvent::ReconnectScheduled {
                attempt: 1,
                delay: Duration::from_secs(1),
            })
        );
        assert_eq!(events.next().await, Some(SocketEvent::Closed));
    });
}

#[test]
fn buffers_a_call_until_join_and_correlates_its_reply() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let options = Options::default();

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({"token": "a"})))
            .unwrap();
        let server = async {
            let join = next_frame(&mut peer).await;
            assert_eq!(join.event, "phx_join");
            reply(&peer, &join, "ok", json!({"ready": true}));

            let call = next_frame(&mut peer).await;
            assert_eq!(call.event, "new_message");
            reply(&peer, &call, "ok", json!({"id": 7}));
        };
        let client = async {
            let (reply, joined) = futures::join!(
                channel.call("new_message", json!({"body": "hello"})),
                channel.join()
            );
            assert_eq!(joined.unwrap(), json!({"ready": true}));
            assert_eq!(reply.unwrap().response, json!({"id": 7}));
            socket.shutdown().await.unwrap();
        };
        futures::join!(server, client);
    });
}

#[test]
fn pings_the_socket_and_correlates_the_heartbeat_reply() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let server = async {
            let heartbeat = next_frame(&mut peer).await;
            assert_eq!(heartbeat.topic, "phoenix");
            assert_eq!(heartbeat.event, "heartbeat");
            reply(&peer, &heartbeat, "ok", json!({}));
        };
        let client = async {
            let round_trip = socket.ping().await.unwrap();
            assert!(round_trip > Duration::ZERO);
            assert!(round_trip < Duration::from_secs(1));
            socket.shutdown().await.unwrap();
        };
        futures::join!(server, client);
    });
}

#[test]
fn removes_timed_out_pings_from_protocol_state() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, mut timer_requests) = timer();
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let timeout = Duration::from_millis(5);
        let server = async {
            let first = next_frame(&mut peer).await;
            loop {
                let request = timer_requests.next().await.unwrap();
                if request.duration == timeout {
                    request.fire.send(()).unwrap();
                    break;
                }
            }
            reply(&peer, &first, "ok", json!({}));
            let second = next_frame(&mut peer).await;
            reply(&peer, &second, "ok", json!({}));
        };
        let client = async {
            assert_eq!(
                socket.ping_with_timeout(timeout).await.unwrap_err(),
                ClientError::Timeout {
                    operation: ClientOperation::Ping,
                    timeout,
                }
            );
            socket.ping().await.unwrap();
            socket.shutdown().await.unwrap();
        };
        futures::join!(server, client);
    });
}

#[test]
fn integrates_presence_with_channel_events() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let channel = socket
            .channel("room:presence", static_join_payload(json!({})))
            .unwrap();
        let mut presence = channel.presence().unwrap();
        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
            let state = Frame::new(
                join.reference.clone(),
                None,
                join.topic.clone(),
                "presence_state",
                json!({"u1": {"metas": [{"phx_ref": "1", "online_at": 1}]}}),
            );
            peer.incoming
                .as_ref()
                .unwrap()
                .unbounded_send(WireMessage::Text(state.encode_text().unwrap()))
                .unwrap();
            let leave = next_frame(&mut peer).await;
            assert_eq!(leave.event, "phx_leave");
            reply(&peer, &leave, "ok", json!({}));
        };
        let client = async {
            channel.join().await.unwrap();
            let Some(Ok(PresenceEvent::Joined { key, current, .. })) = presence.next().await else {
                panic!("expected presence join")
            };
            assert_eq!(key, "u1");
            assert!(current.is_none());
            assert_eq!(presence.next().await, Some(Ok(PresenceEvent::Synced)));
            assert!(presence.state().get("u1").is_some());
            channel.leave().await.unwrap();
            assert_eq!(presence.next().await, Some(Ok(PresenceEvent::ChannelLeft)));
            assert!(presence.state().is_empty());
            socket.shutdown().await.unwrap();
        };
        futures::join!(server, client);
    });
}

#[test]
fn resynchronizes_presence_after_subscriber_lag() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let mut pool = LocalPool::new();
    let options = Options::default().event_capacity(1);
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let channel = socket
            .channel("room:presence-lag", static_join_payload(json!({})))
            .unwrap();
        let mut presence = channel.presence().unwrap();
        let (burst_sent, burst_received) = oneshot::channel();
        let (continue_burst, continue_received) = oneshot::channel();
        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
            for index in 0..8 {
                let frame = Frame::new(
                    join.reference.clone(),
                    None,
                    join.topic.clone(),
                    if index == 0 {
                        "presence_state"
                    } else {
                        "presence_diff"
                    },
                    if index == 0 {
                        json!({"old": {"metas": [{"phx_ref": "old"}]}})
                    } else {
                        json!({"joins": {}, "leaves": {}})
                    },
                );
                peer.incoming
                    .as_ref()
                    .unwrap()
                    .unbounded_send(WireMessage::Text(frame.encode_text().unwrap()))
                    .unwrap();
            }
            burst_sent.send(()).unwrap();
            continue_received.await.unwrap();
            let trigger = Frame::new(
                join.reference,
                None,
                join.topic,
                "presence_diff",
                json!({"joins": {}, "leaves": {}}),
            );
            peer.incoming
                .as_ref()
                .unwrap()
                .unbounded_send(WireMessage::Text(trigger.encode_text().unwrap()))
                .unwrap();

            let leave = next_frame(&mut peer).await;
            reply(&peer, &leave, "ok", json!({}));
            let rejoin = next_frame(&mut peer).await;
            reply(&peer, &rejoin, "ok", json!({}));
            let state = Frame::new(
                rejoin.reference,
                None,
                rejoin.topic,
                "presence_state",
                json!({"new": {"metas": [{"phx_ref": "new"}]}}),
            );
            peer.incoming
                .as_ref()
                .unwrap()
                .unbounded_send(WireMessage::Text(state.encode_text().unwrap()))
                .unwrap();
        };
        let client = async {
            channel.join().await.unwrap();
            burst_received.await.unwrap();
            let _ = presence.next().await;
            let _ = presence.next().await;
            continue_burst.send(()).unwrap();
            loop {
                if matches!(
                    presence.next().await,
                    Some(Err(PresenceStreamError::Desynchronized { .. }))
                ) {
                    break;
                }
            }
            assert!(presence.requires_resync());
            assert!(presence.state().is_empty());
            assert_eq!(
                presence.next().await,
                Some(Err(PresenceStreamError::ResyncRequired))
            );
            presence.resync().await.unwrap();
            assert!(!presence.requires_resync());
            loop {
                if matches!(
                    presence.next().await,
                    Some(Ok(PresenceEvent::Joined { ref key, .. })) if key == "new"
                ) {
                    break;
                }
            }
            assert!(presence.state().get("new").is_some());
            socket.shutdown().await.unwrap();
        };
        futures::join!(server, client);
    });
}

#[test]
fn provides_typed_calls_and_error_replies() {
    let ok = Reply {
        status: ReplyStatus::Ok,
        response: json!({"id": 7}).into(),
    };
    let value: Value = ok.deserialize_ok().unwrap();
    assert_eq!(value, json!({"id": 7}));

    let error = Reply {
        status: ReplyStatus::Error,
        response: json!({"reason": "denied"}).into(),
    };
    assert!(matches!(
        error.deserialize_ok::<Value>(),
        Err(ReplyError::Server(_))
    ));
}

#[test]
fn filters_event_subscriptions_and_decodes_typed_calls() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let channel = socket
            .channel("room:typed", static_join_payload(json!({})))
            .unwrap();
        let mut notices = channel.subscribe("notice").unwrap();
        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
            let call = next_frame(&mut peer).await;
            reply(&peer, &call, "ok", json!({"id": 9}));
            let notice = Frame::new(
                join.reference,
                None,
                join.topic,
                "notice",
                json!({"body": "ready"}),
            );
            peer.incoming
                .as_ref()
                .unwrap()
                .unbounded_send(WireMessage::Text(notice.encode_text().unwrap()))
                .unwrap();
        };
        let client = async {
            channel.join().await.unwrap();
            let response: Value = channel
                .call_json("typed", &json!({"request": true}))
                .await
                .unwrap();
            assert_eq!(response, json!({"id": 9}));
            assert_eq!(
                notices.next().await,
                Some(SubscriptionEvent::Message(json!({"body": "ready"}).into()))
            );
            socket.shutdown().await.unwrap();
        };
        futures::join!(server, client);
    });
}

#[test]
fn survives_repeated_disconnect_and_rejoin_cycles() {
    const CYCLES: usize = 64;
    let mut transports = Vec::with_capacity(CYCLES);
    let mut peers = Vec::with_capacity(CYCLES);
    for _ in 0..CYCLES {
        let (transport, peer) = connection();
        transports.push(transport);
        peers.push(peer);
    }
    let options = Options::default()
        .reconnect_delay(|_| Duration::ZERO)
        .rejoin_delay(|_| Duration::ZERO);
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector(transports), ImmediateZeroTimer, options);
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let channel = socket
            .channel("room:soak", static_join_payload(json!({})))
            .unwrap();
        let mut statuses = channel.status_changes();
        let (done_tx, done_rx) = oneshot::channel();
        let server = async move {
            for (index, mut peer) in peers.into_iter().enumerate() {
                let join = next_frame(&mut peer).await;
                assert_eq!(join.event, "phx_join");
                reply(&peer, &join, "ok", json!({"generation": index}));
                if index + 1 < CYCLES {
                    drop(peer.incoming.take());
                } else {
                    let _ = done_tx.send(peer);
                    return;
                }
            }
        };
        let client = async {
            channel.join().await.unwrap();
            let final_peer = done_rx.await.expect("final peer");
            while channel.status() != ChannelStatus::Joined {
                statuses.changed().await.expect("channel status stream");
            }
            assert_eq!(channel.status(), ChannelStatus::Joined);
            socket.shutdown().await.unwrap();
            drop(final_peer);
        };
        futures::join!(server, client);
    });
}

#[test]
fn interrupts_a_transmitted_call_on_disconnect() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let observed = Rc::new(RefCell::new(Vec::new()));
    let options = Options::default().telemetry({
        let observed = observed.clone();
        Rc::new(move |event| observed.borrow_mut().push(event.clone()))
    });

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let join_server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(join_server, channel.join());
        joined.unwrap();

        let disconnect = async {
            let call = next_frame(&mut peer).await;
            assert_eq!(call.event, "save");
            peer.incoming.take();
        };
        let ((), result) = futures::join!(disconnect, channel.call("save", json!({"value": 1})));
        assert_eq!(
            result.unwrap_err(),
            ClientError::Interrupted {
                operation: ClientOperation::Call,
            }
        );
    });
    assert!(observed.borrow().iter().any(|event| matches!(
        event,
        TelemetryEvent::CallCompleted {
            topic,
            event,
            outcome: CallOutcome::Interrupted,
            ..
        } if topic == "room:lobby" && event == "save"
    )));
}

#[test]
fn reconnects_and_reloads_the_join_payload() {
    let (transport_one, mut peer_one) = connection();
    let (transport_two, mut peer_two) = connection();
    let connector = connector([transport_one, transport_two]);
    let (timer, mut timer_requests) = timer();
    let reconnect_delay = Duration::from_millis(17);
    let options = Options::default()
        .heartbeat_interval(Duration::from_secs(60))
        .request_timeout(Duration::from_secs(120))
        .reconnect_delay(move |_| reconnect_delay);

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let loads = Rc::new(Cell::new(0));
        let loader: JoinPayloadLoader = {
            let loads = loads.clone();
            Rc::new(move |context| {
                let count = loads.get() + 1;
                loads.set(count);
                Box::pin(async move { Ok(json!({"count": count, "rejoin": context.is_rejoin})) })
            })
        };
        let channel = socket.channel("room:lobby", loader).unwrap();

        let first_server = async {
            let join = next_frame(&mut peer_one).await;
            assert_eq!(join.payload, json!({"count": 1, "rejoin": false}));
            reply(&peer_one, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(first_server, channel.join());
        joined.unwrap();
        peer_one.incoming.take();

        loop {
            let request = timer_requests.next().await.unwrap();
            if request.duration == reconnect_delay {
                request.fire.send(()).unwrap();
                break;
            }
        }

        let rejoin = next_frame(&mut peer_two).await;
        assert_eq!(rejoin.event, "phx_join");
        assert_eq!(rejoin.payload, json!({"count": 2, "rejoin": true}));
        reply(&peer_two, &rejoin, "ok", json!({}));
        assert_eq!(loads.get(), 2);
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn accepts_heartbeat_acknowledgements() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, mut timer_requests) = timer();
    let heartbeat_interval = Duration::from_millis(23);
    let options = Options::default()
        .heartbeat_interval(heartbeat_interval)
        .request_timeout(Duration::from_secs(120));

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(server, channel.join());
        joined.unwrap();

        for _ in 0..2 {
            loop {
                let request = timer_requests.next().await.unwrap();
                if request.duration == heartbeat_interval {
                    request.fire.send(()).unwrap();
                    break;
                }
            }
            let heartbeat = next_frame(&mut peer).await;
            assert_eq!(heartbeat.topic, "phoenix");
            assert_eq!(heartbeat.event, "heartbeat");
            reply(&peer, &heartbeat, "ok", json!({}));
        }

        socket.shutdown().await.unwrap();
    });
}

#[test]
fn disconnects_when_a_heartbeat_ack_times_out() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, mut timer_requests) = timer();
    let heartbeat_interval = Duration::from_millis(23);
    let heartbeat_timeout = Duration::from_millis(5);
    let options = Options::default()
        .heartbeat_interval(heartbeat_interval)
        .heartbeat_timeout(heartbeat_timeout);

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    let mut events = socket.events().unwrap();
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let mut held_timers = Vec::new();
        loop {
            let request = timer_requests.next().await.unwrap();
            if request.duration == heartbeat_interval {
                request.fire.send(()).unwrap();
                break;
            }
            held_timers.push(request);
        }
        let heartbeat = next_frame(&mut peer).await;
        assert_eq!(heartbeat.event, "heartbeat");

        loop {
            let request = timer_requests.next().await.unwrap();
            if request.duration == heartbeat_timeout {
                request.fire.send(()).unwrap();
                break;
            }
            held_timers.push(request);
        }
        assert_eq!(
            events.next().await,
            Some(SocketEvent::Disconnected {
                reason: DisconnectReason::HeartbeatTimeout,
            })
        );
        socket.shutdown().await.unwrap();
        drop(held_timers);
    });
}

#[test]
fn retries_a_join_after_its_wire_request_times_out() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, mut timer_requests) = timer();
    let request_timeout = Duration::from_millis(31);
    let rejoin_delay = Duration::from_millis(7);
    let options = Options::default()
        .heartbeat_interval(Duration::from_secs(60))
        .request_timeout(request_timeout)
        .rejoin_delay(move |_| rejoin_delay);

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let mut channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let id = channel.next_request_id();
        let (response, joined) = oneshot::channel();
        channel
            .send(Command::Join {
                id,
                topic: channel.topic.clone(),
                timeout: request_timeout,
                response,
            })
            .await
            .unwrap();

        let first_join = next_frame(&mut peer).await;
        assert_eq!(first_join.event, "phx_join");

        let mut held_timers = Vec::new();
        loop {
            let request = timer_requests.next().await.unwrap();
            if request.duration == request_timeout {
                request.fire.send(()).unwrap();
                break;
            }
            held_timers.push(request);
        }
        loop {
            let request = timer_requests.next().await.unwrap();
            if request.duration == rejoin_delay {
                request.fire.send(()).unwrap();
                break;
            }
            held_timers.push(request);
        }

        let second_join = next_frame(&mut peer).await;
        assert_eq!(second_join.event, "phx_join");
        assert_ne!(second_join.join_ref, first_join.join_ref);
        reply(&peer, &second_join, "ok", json!({"attempt": 2}));

        assert_eq!(
            joined.await.unwrap().unwrap_err(),
            ClientError::Timeout {
                operation: ClientOperation::Join,
                timeout: request_timeout,
            }
        );
        loop {
            if matches!(
                channel.next_event().await,
                Some(ChannelEvent::Protocol(ProtocolEvent::Joined { .. }))
            ) {
                break;
            }
        }
        assert_eq!(channel.status(), ChannelStatus::Joined);
        socket.shutdown().await.unwrap();
        drop(held_timers);
    });
}

#[test]
fn rejoins_after_an_error_while_joining() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, mut timer_requests) = timer();
    let request_timeout = Duration::from_millis(37);
    let rejoin_delay = Duration::from_millis(11);
    let options = Options::default()
        .heartbeat_interval(Duration::from_secs(60))
        .request_timeout(request_timeout)
        .rejoin_delay(move |_| rejoin_delay);

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let id = channel.next_request_id();
        let (response, joined) = oneshot::channel();
        channel
            .send(Command::Join {
                id,
                topic: channel.topic.clone(),
                timeout: request_timeout,
                response,
            })
            .await
            .unwrap();

        let first_join = next_frame(&mut peer).await;
        let channel_error = Frame::new(
            first_join.join_ref.clone(),
            None,
            first_join.topic.clone(),
            "phx_error",
            json!({}),
        );
        peer.incoming
            .as_ref()
            .unwrap()
            .unbounded_send(WireMessage::Text(channel_error.encode_text().unwrap()))
            .unwrap();

        let mut rejoin_timer = None;
        let mut held_timers = Vec::new();
        while rejoin_timer.is_none() {
            let request = timer_requests.next().await.unwrap();
            if request.duration == request_timeout {
                request.fire.send(()).unwrap();
            } else if request.duration == rejoin_delay {
                rejoin_timer = Some(request);
            } else {
                held_timers.push(request);
            }
        }
        rejoin_timer.unwrap().fire.send(()).unwrap();

        let second_join = next_frame(&mut peer).await;
        assert_ne!(second_join.join_ref, first_join.join_ref);
        reply(&peer, &second_join, "ok", json!({"rejoined": true}));

        assert_eq!(joined.await.unwrap().unwrap(), json!({"rejoined": true}));
        assert_eq!(channel.status(), ChannelStatus::Joined);
        socket.shutdown().await.unwrap();
        drop(held_timers);
    });
}

#[test]
fn times_out_and_removes_an_unsent_call() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, mut timer_requests) = timer();
    let request_timeout = Duration::from_millis(29);
    let options = Options::default()
        .heartbeat_interval(Duration::from_secs(60))
        .request_timeout(request_timeout);

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let fire_timeout = async {
            loop {
                let request = timer_requests.next().await.unwrap();
                if request.duration == request_timeout {
                    request.fire.send(()).unwrap();
                    break;
                }
            }
        };
        let (result, ()) = futures::join!(channel.call("never_sent", json!({})), fire_timeout);
        assert_eq!(
            result.unwrap_err(),
            ClientError::Timeout {
                operation: ClientOperation::Call,
                timeout: request_timeout,
            }
        );

        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(server, channel.join());
        joined.unwrap();
        assert!(peer.sent.next().now_or_never().is_none());
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn dropping_a_channel_leaves_and_allows_the_topic_to_be_recreated() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(server, channel.join());
        joined.unwrap();
        drop(channel);

        let replacement = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let server = async {
            let leave = next_frame(&mut peer).await;
            assert_eq!(leave.event, "phx_leave");
            let join = next_frame(&mut peer).await;
            assert_eq!(join.event, "phx_join");
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(server, replacement.join());
        joined.unwrap();
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn rejects_pushes_beyond_the_configured_buffer_capacity() {
    let (transport, _peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let options = Options::default().push_buffer_capacity(1);

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let first = channel.call("first", json!({})).fuse();
        futures::pin_mut!(first);
        assert!(first.as_mut().now_or_never().is_none());

        let second = channel.call("second", json!({})).await;
        assert_eq!(
            second.unwrap_err(),
            ClientError::PushBufferFull("room:lobby".into())
        );
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn exposes_socket_and_channel_lifecycle_status() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    let channel = socket
        .channel("room:lobby", static_join_payload(json!({})))
        .unwrap();
    assert_eq!(socket.status(), SocketStatus::Connecting);
    assert_eq!(channel.status(), ChannelStatus::Closed);
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let join_server = async {
            let join = next_frame(&mut peer).await;
            assert_eq!(socket.status(), SocketStatus::Connected);
            assert_eq!(channel.status(), ChannelStatus::Joining);
            reply(&peer, &join, "ok", json!({"ready": true}));
        };
        let (_, joined) = futures::join!(join_server, channel.join());
        assert_eq!(joined.unwrap(), json!({"ready": true}));
        assert_eq!(channel.status(), ChannelStatus::Joined);

        let leave_server = async {
            let leave = next_frame(&mut peer).await;
            assert_eq!(leave.event, "phx_leave");
            assert_eq!(channel.status(), ChannelStatus::Leaving);
            reply(&peer, &leave, "ok", json!({}));
        };
        let (_, left) = futures::join!(leave_server, channel.leave());
        left.unwrap();
        assert_eq!(channel.status(), ChannelStatus::Left);

        socket.shutdown().await.unwrap();
        assert_eq!(socket.status(), SocketStatus::Closed);
        assert_eq!(channel.status(), ChannelStatus::Closed);
    });
}

#[test]
fn enforces_channel_handle_and_join_state_invariants() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    let channel = socket
        .channel("room:lobby", static_join_payload(json!({})))
        .unwrap();
    let duplicate = socket.channel("room:lobby", static_join_payload(json!({})));
    assert!(matches!(
        duplicate,
        Err(ClientError::DuplicateChannel(topic)) if topic == "room:lobby"
    ));
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(server, channel.join());
        joined.unwrap();
        assert_eq!(
            channel.join().await.unwrap_err(),
            ClientError::AlreadyJoined("room:lobby".into())
        );

        let server = async {
            let leave = next_frame(&mut peer).await;
            reply(&peer, &leave, "ok", json!({}));
        };
        let (_, left) = futures::join!(server, channel.leave());
        left.unwrap();
        assert_eq!(
            channel.call("after_leave", json!({})).await.unwrap_err(),
            ClientError::ChannelNotJoined("room:lobby".into())
        );
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn applies_per_operation_timeout_overrides() {
    let (transport, _peer) = connection();
    let connector = connector([transport]);
    let (timer, mut timer_requests) = timer();
    let custom_timeout = Duration::from_millis(17);
    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    let channel = socket
        .channel("room:lobby", static_join_payload(json!({})))
        .unwrap();
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let call = channel.call_with_timeout("queued", json!({}), custom_timeout);
        let fire_timeout = async {
            loop {
                let request = timer_requests.next().await.unwrap();
                if request.duration == custom_timeout {
                    request.fire.send(()).unwrap();
                    break;
                }
            }
        };
        let (result, ()) = futures::join!(call, fire_timeout);
        assert_eq!(
            result.unwrap_err(),
            ClientError::Timeout {
                operation: ClientOperation::Call,
                timeout: custom_timeout,
            }
        );
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn broadcasts_channel_events_to_multiple_subscribers() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let mut channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let mut additional_events = channel.events().unwrap();
        let server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(server, channel.join());
        joined.unwrap();

        let primary = channel.next_event().await.unwrap();
        let additional = additional_events.next().await.unwrap();
        assert!(matches!(
            primary,
            ChannelEvent::Protocol(ProtocolEvent::Joined { .. })
        ));
        assert_eq!(additional, primary);
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn leaving_while_joining_sends_leave_after_the_join_reply() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let (join_sent, join_seen) = oneshot::channel();

        let server = async {
            let join = next_frame(&mut peer).await;
            assert_eq!(join.event, "phx_join");
            join_sent.send(()).unwrap();
            reply(&peer, &join, "ok", json!({}));

            let leave = next_frame(&mut peer).await;
            assert_eq!(leave.event, "phx_leave");
            reply(&peer, &leave, "ok", json!({}));
        };
        let client = async {
            let join = channel.join();
            let leave = async {
                join_seen.await.unwrap();
                channel.leave().await
            };
            let (joined, left) = futures::join!(join, leave);
            assert_eq!(
                joined.unwrap_err(),
                ClientError::Interrupted {
                    operation: ClientOperation::Join,
                }
            );
            left.unwrap();
            assert_eq!(channel.status(), ChannelStatus::Left);
        };
        futures::join!(server, client);
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn ignores_a_stale_join_payload_after_leave_and_rejoin() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let (loads, mut load_requests) = mpsc::unbounded();
    let (first_payload, first_payload_rx) = oneshot::channel();
    let (second_payload, second_payload_rx) = oneshot::channel();
    let payloads = Rc::new(RefCell::new(VecDeque::from([
        first_payload_rx,
        second_payload_rx,
    ])));
    let loader: JoinPayloadLoader = Rc::new(move |_| {
        let receiver = payloads.borrow_mut().pop_front().unwrap();
        loads.unbounded_send(()).unwrap();
        Box::pin(async move { receiver.await.unwrap() })
    });

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket.channel("room:lobby", loader).unwrap();
        let first_join = channel.join();
        let replace_join = async {
            load_requests.next().await.unwrap();
            channel.leave().await.unwrap();

            let second_join = channel.join();
            let server = async {
                load_requests.next().await.unwrap();
                first_payload.send(Ok(json!({"generation": 1}))).unwrap();
                second_payload.send(Ok(json!({"generation": 2}))).unwrap();
                let join = next_frame(&mut peer).await;
                assert_eq!(join.payload, json!({"generation": 2}));
                reply(&peer, &join, "ok", json!({}));
            };
            let (_, joined) = futures::join!(server, second_join);
            joined.unwrap();
        };
        let (first_result, ()) = futures::join!(first_join, replace_join);
        assert_eq!(
            first_result.unwrap_err(),
            ClientError::Interrupted {
                operation: ClientOperation::Join,
            }
        );
        assert_eq!(channel.status(), ChannelStatus::Joined);
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn lifecycle_cleanup_is_not_blocked_by_command_backpressure() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let options = Options::default().command_capacity(1);

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    let channel = socket
        .channel("room:lobby", static_join_payload(json!({"generation": 1})))
        .unwrap();
    drop(channel);
    let replacement = socket
        .channel("room:lobby", static_join_payload(json!({"generation": 2})))
        .unwrap();
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let server = async {
            let join = next_frame(&mut peer).await;
            assert_eq!(join.event, "phx_join");
            assert_eq!(join.payload, json!({"generation": 2}));
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(server, replacement.join());
        joined.unwrap();
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn queues_a_rejoin_requested_while_leave_is_in_flight() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, Options::default());
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();
        let first_join_server = async {
            let join = next_frame(&mut peer).await;
            reply(&peer, &join, "ok", json!({}));
        };
        let (_, joined) = futures::join!(first_join_server, channel.join());
        joined.unwrap();

        let (leave_sent, leave_seen) = oneshot::channel();
        let (rejoin_started, rejoin_queued) = oneshot::channel();
        let server = async {
            let leave = next_frame(&mut peer).await;
            assert_eq!(leave.event, "phx_leave");
            leave_sent.send(()).unwrap();
            rejoin_queued.await.unwrap();
            reply(&peer, &leave, "ok", json!({}));

            let rejoin = next_frame(&mut peer).await;
            assert_eq!(rejoin.event, "phx_join");
            reply(&peer, &rejoin, "ok", json!({}));
        };
        let client = async {
            let leave = channel.leave();
            let rejoin = async {
                leave_seen.await.unwrap();
                let join = channel.join().fuse();
                futures::pin_mut!(join);
                assert!(join.as_mut().now_or_never().is_none());
                rejoin_started.send(()).unwrap();
                join.await
            };
            let (left, rejoined) = futures::join!(leave, rejoin);
            left.unwrap();
            rejoined.unwrap();
        };
        futures::join!(server, client);
        assert_eq!(channel.status(), ChannelStatus::Joined);
        socket.shutdown().await.unwrap();
    });
}

#[test]
fn continues_a_queued_rejoin_after_leave_times_out() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, mut timer_requests) = timer();
    let request_timeout = Duration::from_millis(41);
    let options = Options::default()
        .heartbeat_interval(Duration::from_secs(60))
        .request_timeout(request_timeout);

    let mut pool = LocalPool::new();
    let (socket, driver) = Socket::new(connector, timer, options);
    pool.spawner().spawn_local(driver).unwrap();
    pool.run_until(async move {
        let channel = socket
            .channel("room:lobby", static_join_payload(json!({})))
            .unwrap();

        let join_id = channel.next_request_id();
        let (join_response, joined) = oneshot::channel();
        channel
            .send(Command::Join {
                id: join_id,
                topic: channel.topic.clone(),
                timeout: request_timeout,
                response: join_response,
            })
            .await
            .unwrap();
        let first_join = next_frame(&mut peer).await;
        reply(&peer, &first_join, "ok", json!({}));
        joined.await.unwrap().unwrap();

        let mut held_timers = Vec::new();
        loop {
            let request = timer_requests.next().await.unwrap();
            if request.duration == request_timeout {
                request.fire.send(()).unwrap();
                break;
            }
            held_timers.push(request);
        }

        let leave_id = channel.next_request_id();
        let (leave_response, left) = oneshot::channel();
        channel
            .send(Command::Leave {
                id: leave_id,
                topic: channel.topic.clone(),
                timeout: request_timeout,
                response: leave_response,
            })
            .await
            .unwrap();
        let leave = next_frame(&mut peer).await;
        assert_eq!(leave.event, "phx_leave");

        let rejoin_id = channel.next_request_id();
        let (rejoin_response, rejoined) = oneshot::channel();
        channel
            .send(Command::Join {
                id: rejoin_id,
                topic: channel.topic.clone(),
                timeout: request_timeout,
                response: rejoin_response,
            })
            .await
            .unwrap();

        loop {
            let request = timer_requests.next().await.unwrap();
            if request.duration == request_timeout {
                request.fire.send(()).unwrap();
                break;
            }
            held_timers.push(request);
        }

        let second_join = next_frame(&mut peer).await;
        assert_eq!(second_join.event, "phx_join");
        reply(&peer, &second_join, "ok", json!({"generation": 2}));

        assert_eq!(
            left.await.unwrap().unwrap_err(),
            ClientError::Timeout {
                operation: ClientOperation::Leave,
                timeout: request_timeout,
            }
        );
        assert_eq!(rejoined.await.unwrap().unwrap(), json!({"generation": 2}));
        assert_eq!(channel.status(), ChannelStatus::Joined);
        socket.shutdown().await.unwrap();
        drop(held_timers);
    });
}
