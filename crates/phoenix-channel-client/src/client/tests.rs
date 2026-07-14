use std::{cell::RefCell, collections::VecDeque};

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
        assert_eq!(events.next().await, Some(SocketEvent::Closed));
        assert_eq!(socket.status(), SocketStatus::Closed);
    });
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
fn interrupts_a_transmitted_call_on_disconnect() {
    let (transport, mut peer) = connection();
    let connector = connector([transport]);
    let (timer, _timer_requests) = timer();
    let options = Options::default();

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
        assert_eq!(result.unwrap_err(), ClientError::Interrupted);
    });
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

        assert_eq!(joined.await.unwrap().unwrap_err(), ClientError::Timeout);
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
        assert_eq!(result.unwrap_err(), ClientError::Timeout);

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
            assert_eq!(joined.unwrap_err(), ClientError::Interrupted);
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
        assert_eq!(first_result.unwrap_err(), ClientError::Interrupted);
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

        assert_eq!(left.await.unwrap().unwrap_err(), ClientError::Timeout);
        assert_eq!(rejoined.await.unwrap().unwrap(), json!({"generation": 2}));
        assert_eq!(channel.status(), ChannelStatus::Joined);
        socket.shutdown().await.unwrap();
        drop(held_timers);
    });
}
