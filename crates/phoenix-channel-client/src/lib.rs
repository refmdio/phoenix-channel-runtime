//! Managed Phoenix Channels client without an executor or platform dependency.

#![forbid(unsafe_code)]

use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    FutureExt, StreamExt,
    channel::{mpsc, oneshot},
    future::{LocalBoxFuture, pending},
    stream::FuturesUnordered,
};
use phoenix_channel_runtime::{
    ChannelState, Frame, Protocol, ProtocolEvent, ReplyStatus, Transport, TransportError,
    WireMessage,
};
use serde_json::{Value, json};
use thiserror::Error;

type RequestId = u64;

/// Creates target-specific WebSocket transports for the driver.
pub trait Connector {
    fn connect(&self) -> LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>;
}

/// Supplies sleeps without choosing Tokio, a browser API, or another executor.
pub trait Timer {
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JoinContext {
    pub attempt: u32,
    pub is_rejoin: bool,
}

pub type JoinPayloadLoader =
    Rc<dyn Fn(JoinContext) -> LocalBoxFuture<'static, Result<Value, String>>>;

pub fn static_join_payload(payload: Value) -> JoinPayloadLoader {
    Rc::new(move |_| {
        let payload = payload.clone();
        Box::pin(async move { Ok(payload) })
    })
}

#[derive(Clone)]
pub struct Options {
    heartbeat_interval: Duration,
    request_timeout: Duration,
    reconnect_delay: Rc<dyn Fn(u32) -> Duration>,
    rejoin_delay: Rc<dyn Fn(u32) -> Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
            reconnect_delay: Rc::new(|attempt| match attempt {
                0 => Duration::ZERO,
                1 => Duration::from_secs(1),
                2 => Duration::from_secs(2),
                3 => Duration::from_secs(5),
                _ => Duration::from_secs(10),
            }),
            rejoin_delay: Rc::new(|attempt| match attempt {
                0 => Duration::ZERO,
                1 => Duration::from_secs(1),
                2 => Duration::from_secs(2),
                3 => Duration::from_secs(5),
                _ => Duration::from_secs(10),
            }),
        }
    }
}

impl Options {
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn reconnect_delay(mut self, delay: impl Fn(u32) -> Duration + 'static) -> Self {
        self.reconnect_delay = Rc::new(delay);
        self
    }

    pub fn rejoin_delay(mut self, delay: impl Fn(u32) -> Duration + 'static) -> Self {
        self.rejoin_delay = Rc::new(delay);
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SocketEvent {
    Connecting { attempt: u32 },
    Connected,
    Disconnected { reason: String },
    ReconnectScheduled { attempt: u32, delay: Duration },
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ChannelEvent {
    Protocol(ProtocolEvent),
    Disconnected,
    JoinPayloadError(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Reply {
    pub status: ReplyStatus,
    pub response: Value,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum ClientError {
    #[error("the managed client driver stopped")]
    DriverStopped,
    #[error("a channel already exists for topic {0}")]
    DuplicateChannel(String),
    #[error("request timed out")]
    Timeout,
    #[error("request was interrupted by connection loss")]
    Interrupted,
    #[error("transport error: {0}")]
    Transport(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("join payload loader failed: {0}")]
    JoinPayload(String),
    #[error("channel join was rejected: {0}")]
    JoinRejected(Value),
}

#[derive(Clone)]
pub struct Socket {
    commands: mpsc::UnboundedSender<Command>,
    timer: Rc<dyn Timer>,
    options: Options,
    request_ids: Rc<Cell<RequestId>>,
    topics: Rc<RefCell<HashSet<String>>>,
}

impl Socket {
    pub fn new(
        connector: impl Connector + 'static,
        timer: impl Timer + 'static,
        options: Options,
    ) -> (Self, Driver) {
        let (commands, command_rx) = mpsc::unbounded();
        let timer: Rc<dyn Timer> = Rc::new(timer);
        let socket = Self {
            commands,
            timer: timer.clone(),
            options: options.clone(),
            request_ids: Rc::new(Cell::new(0)),
            topics: Rc::new(RefCell::new(HashSet::new())),
        };
        let state = DriverState::new(Rc::new(connector), timer, options, command_rx);
        let driver = Driver {
            inner: Box::pin(state.run()),
        };
        (socket, driver)
    }

    pub fn channel(
        &self,
        topic: impl Into<String>,
        payload_loader: JoinPayloadLoader,
    ) -> Result<Channel, ClientError> {
        let topic = topic.into();
        if !self.topics.borrow_mut().insert(topic.clone()) {
            return Err(ClientError::DuplicateChannel(topic));
        }
        let (events, event_rx) = mpsc::unbounded();
        if self
            .commands
            .unbounded_send(Command::Register {
                topic: topic.clone(),
                payload_loader,
                events,
            })
            .is_err()
        {
            self.topics.borrow_mut().remove(&topic);
            return Err(ClientError::DriverStopped);
        }
        Ok(Channel {
            topic,
            commands: self.commands.clone(),
            timer: self.timer.clone(),
            timeout: self.options.request_timeout,
            request_ids: self.request_ids.clone(),
            events: event_rx,
        })
    }

    pub fn events(&self) -> Result<SocketEvents, ClientError> {
        let (events, receiver) = mpsc::unbounded();
        self.commands
            .unbounded_send(Command::Subscribe { events })
            .map_err(|_| ClientError::DriverStopped)?;
        Ok(SocketEvents { receiver })
    }

    pub async fn shutdown(&self) -> Result<(), ClientError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .unbounded_send(Command::Shutdown { response })
            .map_err(|_| ClientError::DriverStopped)?;
        receiver.await.map_err(|_| ClientError::DriverStopped)
    }
}

pub struct SocketEvents {
    receiver: mpsc::UnboundedReceiver<SocketEvent>,
}

impl SocketEvents {
    pub async fn next(&mut self) -> Option<SocketEvent> {
        self.receiver.next().await
    }
}

pub struct Channel {
    topic: String,
    commands: mpsc::UnboundedSender<Command>,
    timer: Rc<dyn Timer>,
    timeout: Duration,
    request_ids: Rc<Cell<RequestId>>,
    events: mpsc::UnboundedReceiver<ChannelEvent>,
}

impl Channel {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub async fn join(&self) -> Result<Value, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Join {
            id,
            topic: self.topic.clone(),
            response,
        })?;
        self.wait(id, receiver).await
    }

    pub async fn call(
        &self,
        event: impl Into<String>,
        payload: Value,
    ) -> Result<Reply, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Call {
            id,
            topic: self.topic.clone(),
            event: event.into(),
            payload,
            response,
        })?;
        self.wait(id, receiver).await
    }

    /// Sends an event without asking Phoenix for a reply.
    pub async fn cast(&self, event: impl Into<String>, payload: Value) -> Result<(), ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Cast {
            id,
            topic: self.topic.clone(),
            event: event.into(),
            payload,
            response,
        })?;
        self.wait(id, receiver).await
    }

    pub async fn leave(&self) -> Result<Value, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Leave {
            id,
            topic: self.topic.clone(),
            response,
        })?;
        self.wait(id, receiver).await
    }

    pub async fn next_event(&mut self) -> Option<ChannelEvent> {
        self.events.next().await
    }

    fn send(&self, command: Command) -> Result<(), ClientError> {
        self.commands
            .unbounded_send(command)
            .map_err(|_| ClientError::DriverStopped)
    }

    fn next_request_id(&self) -> RequestId {
        let mut id = self.request_ids.get().wrapping_add(1);
        if id == 0 {
            id = 1;
        }
        self.request_ids.set(id);
        id
    }

    async fn wait<T>(
        &self,
        id: RequestId,
        receiver: oneshot::Receiver<Result<T, ClientError>>,
    ) -> Result<T, ClientError> {
        let response = receiver.fuse();
        let timeout = self.timer.sleep(self.timeout).fuse();
        futures::pin_mut!(response, timeout);
        futures::select! {
            response = response => response.map_err(|_| ClientError::DriverStopped)?,
            () = timeout => {
                let _ = self.commands.unbounded_send(Command::Cancel { id });
                Err(ClientError::Timeout)
            }
        }
    }
}

/// A future that owns all connection and protocol state.
///
/// It must be polled continuously by the host executor.
#[must_use = "the driver must be spawned or awaited"]
pub struct Driver {
    inner: LocalBoxFuture<'static, ()>,
}

impl Future for Driver {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

enum Command {
    Register {
        topic: String,
        payload_loader: JoinPayloadLoader,
        events: mpsc::UnboundedSender<ChannelEvent>,
    },
    Subscribe {
        events: mpsc::UnboundedSender<SocketEvent>,
    },
    Join {
        id: RequestId,
        topic: String,
        response: oneshot::Sender<Result<Value, ClientError>>,
    },
    Call {
        id: RequestId,
        topic: String,
        event: String,
        payload: Value,
        response: oneshot::Sender<Result<Reply, ClientError>>,
    },
    Cast {
        id: RequestId,
        topic: String,
        event: String,
        payload: Value,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Leave {
        id: RequestId,
        topic: String,
        response: oneshot::Sender<Result<Value, ClientError>>,
    },
    Cancel {
        id: RequestId,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

enum QueuedPush {
    Call {
        id: RequestId,
        event: String,
        payload: Value,
        response: oneshot::Sender<Result<Reply, ClientError>>,
    },
    Cast {
        id: RequestId,
        event: String,
        payload: Value,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
}

impl QueuedPush {
    fn id(&self) -> RequestId {
        match self {
            Self::Call { id, .. } | Self::Cast { id, .. } => *id,
        }
    }

    fn interrupt(self) {
        match self {
            Self::Call { response, .. } => {
                let _ = response.send(Err(ClientError::Interrupted));
            }
            Self::Cast { response, .. } => {
                let _ = response.send(Err(ClientError::Interrupted));
            }
        }
    }
}

struct ChannelRecord {
    payload_loader: JoinPayloadLoader,
    events: mpsc::UnboundedSender<ChannelEvent>,
    desired: bool,
    ever_joined: bool,
    loading_payload: bool,
    join_attempt: u32,
    join_waiters: HashMap<RequestId, oneshot::Sender<Result<Value, ClientError>>>,
    queued: VecDeque<QueuedPush>,
}

struct PendingCall {
    id: RequestId,
    response: oneshot::Sender<Result<Reply, ClientError>>,
}

struct PendingLeave {
    id: RequestId,
    response: oneshot::Sender<Result<Value, ClientError>>,
}

type PayloadResult = (String, Result<Value, String>);

struct DriverState {
    connector: Rc<dyn Connector>,
    timer: Rc<dyn Timer>,
    options: Options,
    commands: mpsc::UnboundedReceiver<Command>,
    protocol: Protocol,
    channels: HashMap<String, ChannelRecord>,
    socket_subscribers: Vec<mpsc::UnboundedSender<SocketEvent>>,
    pending_calls: HashMap<String, PendingCall>,
    pending_leaves: HashMap<String, PendingLeave>,
}

impl DriverState {
    fn new(
        connector: Rc<dyn Connector>,
        timer: Rc<dyn Timer>,
        options: Options,
        commands: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        Self {
            connector,
            timer,
            options,
            commands,
            protocol: Protocol::new(),
            channels: HashMap::new(),
            socket_subscribers: Vec::new(),
            pending_calls: HashMap::new(),
            pending_leaves: HashMap::new(),
        }
    }

    async fn run(mut self) {
        let mut attempt = 0;
        loop {
            self.emit_socket(SocketEvent::Connecting { attempt });
            let connection = self.connector.connect().fuse();
            futures::pin_mut!(connection);
            let connected = loop {
                let command = self.commands.next().fuse();
                futures::pin_mut!(command);
                futures::select! {
                    result = connection => break Some(result),
                    command = command => match command {
                        Some(command) => if self.handle_offline_command(command) { break None },
                        None => break None,
                    }
                }
            };
            let Some(connected) = connected else {
                self.emit_socket(SocketEvent::Closed);
                return;
            };

            match connected {
                Ok(mut transport) => {
                    attempt = 0;
                    self.emit_socket(SocketEvent::Connected);
                    match self.run_connected(&mut transport).await {
                        ConnectedExit::Shutdown(response) => {
                            let _ = transport.close().await;
                            let _ = response.send(());
                            self.emit_socket(SocketEvent::Closed);
                            return;
                        }
                        ConnectedExit::Disconnected(reason) => {
                            let _ = transport.close().await;
                            self.on_disconnect(&reason);
                        }
                    }
                }
                Err(error) => {
                    self.emit_socket(SocketEvent::Disconnected {
                        reason: error.to_string(),
                    });
                }
            }

            attempt = attempt.saturating_add(1);
            let delay = (self.options.reconnect_delay)(attempt);
            self.emit_socket(SocketEvent::ReconnectScheduled { attempt, delay });
            if self.wait_offline(delay).await {
                self.emit_socket(SocketEvent::Closed);
                return;
            }
        }
    }

    async fn wait_offline(&mut self, delay: Duration) -> bool {
        let sleep = self.timer.sleep(delay).fuse();
        futures::pin_mut!(sleep);
        loop {
            let command = self.commands.next().fuse();
            futures::pin_mut!(command);
            futures::select! {
                () = sleep => return false,
                command = command => match command {
                    Some(command) => if self.handle_offline_command(command) { return true },
                    None => return true,
                }
            }
        }
    }

    async fn run_connected(&mut self, transport: &mut Box<dyn Transport>) -> ConnectedExit {
        let mut payloads: FuturesUnordered<LocalBoxFuture<'static, PayloadResult>> =
            FuturesUnordered::new();
        let mut rejoins: FuturesUnordered<LocalBoxFuture<'static, String>> =
            FuturesUnordered::new();
        let desired = self
            .channels
            .iter()
            .filter(|(_, channel)| channel.desired)
            .map(|(topic, _)| topic.clone())
            .collect::<Vec<_>>();
        for topic in desired {
            self.load_payload(&topic, &mut payloads);
        }

        let mut heartbeat = self.timer.sleep(self.options.heartbeat_interval);
        let mut heartbeat_reference: Option<String> = None;

        loop {
            enum Action {
                Command(Option<Command>),
                Incoming(Result<Option<WireMessage>, TransportError>),
                Heartbeat,
                Payload(Option<PayloadResult>),
                Rejoin(Option<String>),
            }

            let action = {
                let command = self.commands.next().fuse();
                let incoming = transport.receive().fuse();
                let heartbeat_wait = heartbeat.as_mut().fuse();
                let payload: LocalBoxFuture<'_, Option<PayloadResult>> = if payloads.is_empty() {
                    Box::pin(pending())
                } else {
                    Box::pin(payloads.next())
                };
                let payload = payload.fuse();
                let rejoin: LocalBoxFuture<'_, Option<String>> = if rejoins.is_empty() {
                    Box::pin(pending())
                } else {
                    Box::pin(rejoins.next())
                };
                let rejoin = rejoin.fuse();
                futures::pin_mut!(command, incoming, heartbeat_wait, payload, rejoin);
                futures::select_biased! {
                    command = command => Action::Command(command),
                    incoming = incoming => Action::Incoming(incoming),
                    () = heartbeat_wait => Action::Heartbeat,
                    payload = payload => Action::Payload(payload),
                    topic = rejoin => Action::Rejoin(topic),
                }
            };

            let result = match action {
                Action::Command(Some(Command::Shutdown { response })) => {
                    return ConnectedExit::Shutdown(response);
                }
                Action::Command(Some(command)) => {
                    self.handle_connected_command(command, transport, &mut payloads)
                        .await
                }
                Action::Command(None) => {
                    return ConnectedExit::Disconnected("command channel closed".into());
                }
                Action::Incoming(Ok(Some(message))) => {
                    self.handle_incoming(message, transport, &mut rejoins, &mut heartbeat_reference)
                        .await
                }
                Action::Incoming(Ok(None)) => Err("WebSocket connection closed".into()),
                Action::Incoming(Err(error)) => Err(error.to_string()),
                Action::Heartbeat => {
                    heartbeat = self.timer.sleep(self.options.heartbeat_interval);
                    if heartbeat_reference.is_some() {
                        Err("heartbeat acknowledgement timed out".into())
                    } else {
                        let outbound = self.protocol.heartbeat();
                        heartbeat_reference = Some(outbound.reference);
                        self.send_frame(transport, outbound.frame).await
                    }
                }
                Action::Payload(Some((topic, payload))) => {
                    self.handle_payload(topic, payload, transport).await
                }
                Action::Payload(None) => Ok(()),
                Action::Rejoin(Some(topic)) => {
                    if let Some(channel) = self.channels.get(&topic)
                        && channel.desired
                    {
                        self.load_payload(&topic, &mut payloads);
                    }
                    Ok(())
                }
                Action::Rejoin(None) => Ok(()),
            };

            if let Err(reason) = result {
                return ConnectedExit::Disconnected(reason);
            }
        }
    }

    fn handle_offline_command(&mut self, command: Command) -> bool {
        match command {
            Command::Register {
                topic,
                payload_loader,
                events,
            } => self.register(topic, payload_loader, events),
            Command::Subscribe { events } => self.socket_subscribers.push(events),
            Command::Join {
                id,
                topic,
                response,
            } => {
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.desired = true;
                    channel.join_waiters.insert(id, response);
                } else {
                    let _ = response.send(Err(ClientError::Protocol(format!(
                        "unknown topic: {topic}"
                    ))));
                }
            }
            Command::Call {
                id,
                topic,
                event,
                payload,
                response,
            } => self.queue(
                &topic,
                QueuedPush::Call {
                    id,
                    event,
                    payload,
                    response,
                },
            ),
            Command::Cast {
                id,
                topic,
                event,
                payload,
                response,
            } => self.queue(
                &topic,
                QueuedPush::Cast {
                    id,
                    event,
                    payload,
                    response,
                },
            ),
            Command::Leave {
                topic, response, ..
            } => {
                self.stop_channel(&topic);
                let _ = response.send(Ok(json!({})));
            }
            Command::Cancel { id } => self.cancel(id),
            Command::Shutdown { response } => {
                let _ = response.send(());
                return true;
            }
        }
        false
    }

    async fn handle_connected_command(
        &mut self,
        command: Command,
        transport: &mut Box<dyn Transport>,
        payloads: &mut FuturesUnordered<LocalBoxFuture<'static, PayloadResult>>,
    ) -> Result<(), String> {
        match command {
            Command::Register {
                topic,
                payload_loader,
                events,
            } => self.register(topic, payload_loader, events),
            Command::Subscribe { events } => self.socket_subscribers.push(events),
            Command::Join {
                id,
                topic,
                response,
            } => {
                let joined = self.protocol.channel_state(&topic) == Some(ChannelState::Joined);
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.desired = true;
                    if joined {
                        let _ = response.send(Ok(json!({})));
                    } else {
                        channel.join_waiters.insert(id, response);
                        self.load_payload(&topic, payloads);
                    }
                } else {
                    let _ = response.send(Err(ClientError::Protocol(format!(
                        "unknown topic: {topic}"
                    ))));
                }
            }
            Command::Call {
                id,
                topic,
                event,
                payload,
                response,
            } => {
                let push = QueuedPush::Call {
                    id,
                    event,
                    payload,
                    response,
                };
                if self.protocol.channel_state(&topic) == Some(ChannelState::Joined) {
                    self.send_push(&topic, push, transport).await?;
                } else {
                    self.queue(&topic, push);
                }
            }
            Command::Cast {
                id,
                topic,
                event,
                payload,
                response,
            } => {
                let push = QueuedPush::Cast {
                    id,
                    event,
                    payload,
                    response,
                };
                if self.protocol.channel_state(&topic) == Some(ChannelState::Joined) {
                    self.send_push(&topic, push, transport).await?;
                } else {
                    self.queue(&topic, push);
                }
            }
            Command::Leave {
                id,
                topic,
                response,
            } => {
                self.stop_channel(&topic);
                if self.protocol.channel_state(&topic) == Some(ChannelState::Joined) {
                    let outbound = self
                        .protocol
                        .leave(&topic)
                        .map_err(|error| error.to_string())?;
                    self.pending_leaves
                        .insert(outbound.reference.clone(), PendingLeave { id, response });
                    self.send_frame(transport, outbound.frame).await?;
                } else {
                    let _ = response.send(Ok(json!({})));
                }
            }
            Command::Cancel { id } => self.cancel(id),
            Command::Shutdown { .. } => unreachable!("handled by run_connected"),
        }
        Ok(())
    }

    fn register(
        &mut self,
        topic: String,
        payload_loader: JoinPayloadLoader,
        events: mpsc::UnboundedSender<ChannelEvent>,
    ) {
        self.channels.entry(topic).or_insert(ChannelRecord {
            payload_loader,
            events,
            desired: false,
            ever_joined: false,
            loading_payload: false,
            join_attempt: 0,
            join_waiters: HashMap::new(),
            queued: VecDeque::new(),
        });
    }

    fn queue(&mut self, topic: &str, push: QueuedPush) {
        if let Some(channel) = self.channels.get_mut(topic) {
            channel.queued.push_back(push);
        } else {
            push.interrupt();
        }
    }

    fn load_payload(
        &mut self,
        topic: &str,
        payloads: &mut FuturesUnordered<LocalBoxFuture<'static, PayloadResult>>,
    ) {
        let Some(channel) = self.channels.get_mut(topic) else {
            return;
        };
        if !channel.desired || channel.loading_payload {
            return;
        }
        if matches!(
            self.protocol.channel_state(topic),
            Some(ChannelState::Joining | ChannelState::Joined | ChannelState::Leaving)
        ) {
            return;
        }
        channel.loading_payload = true;
        let context = JoinContext {
            attempt: channel.join_attempt,
            is_rejoin: channel.ever_joined,
        };
        let loader = channel.payload_loader.clone();
        let topic = topic.to_owned();
        payloads.push(Box::pin(async move {
            let result = loader(context).await;
            (topic, result)
        }));
    }

    async fn handle_payload(
        &mut self,
        topic: String,
        payload: Result<Value, String>,
        transport: &mut Box<dyn Transport>,
    ) -> Result<(), String> {
        let Some(channel) = self.channels.get_mut(&topic) else {
            return Ok(());
        };
        channel.loading_payload = false;
        if !channel.desired {
            return Ok(());
        }
        let payload = match payload {
            Ok(payload) => payload,
            Err(error) => {
                channel.desired = false;
                for (_, waiter) in channel.join_waiters.drain() {
                    let _ = waiter.send(Err(ClientError::JoinPayload(error.clone())));
                }
                let _ = channel
                    .events
                    .unbounded_send(ChannelEvent::JoinPayloadError(error));
                return Ok(());
            }
        };
        let outbound = match self.protocol.channel_state(&topic) {
            Some(ChannelState::Disconnected | ChannelState::Errored | ChannelState::Closed) => {
                self.protocol.rejoin(&topic, payload)
            }
            None => self.protocol.join(&topic, payload),
            Some(_) => return Ok(()),
        }
        .map_err(|error| error.to_string())?;
        self.send_frame(transport, outbound.frame).await
    }

    async fn handle_incoming(
        &mut self,
        message: WireMessage,
        transport: &mut Box<dyn Transport>,
        rejoins: &mut FuturesUnordered<LocalBoxFuture<'static, String>>,
        heartbeat_reference: &mut Option<String>,
    ) -> Result<(), String> {
        let WireMessage::Text(text) = message else {
            return Err("Phoenix binary serializer is not implemented".into());
        };
        let frame = Frame::decode_text(&text).map_err(|error| error.to_string())?;
        let event = self
            .protocol
            .receive(frame)
            .map_err(|error| error.to_string())?;

        match &event {
            ProtocolEvent::Joined {
                topic, response, ..
            } => {
                if let Some(channel) = self.channels.get_mut(topic) {
                    channel.ever_joined = true;
                    channel.join_attempt = 0;
                    for (_, waiter) in channel.join_waiters.drain() {
                        let _ = waiter.send(Ok(response.clone()));
                    }
                }
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
                self.flush(topic, transport).await?;
            }
            ProtocolEvent::JoinError {
                topic, response, ..
            } => {
                if let Some(channel) = self.channels.get_mut(topic) {
                    for (_, waiter) in channel.join_waiters.drain() {
                        let _ = waiter.send(Err(ClientError::JoinRejected(response.clone())));
                    }
                }
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
                self.schedule_rejoin(topic, rejoins);
            }
            ProtocolEvent::Reply {
                reference,
                status,
                response,
                topic,
                ..
            } => {
                if let Some(pending) = self.pending_calls.remove(reference) {
                    let _ = pending.response.send(Ok(Reply {
                        status: *status,
                        response: response.clone(),
                    }));
                }
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
            }
            ProtocolEvent::Left {
                reference,
                response,
                topic,
            } => {
                if let Some(pending) = self.pending_leaves.remove(reference) {
                    let _ = pending.response.send(Ok(response.clone()));
                }
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
            }
            ProtocolEvent::HeartbeatAck { reference, .. } => {
                if heartbeat_reference.as_deref() == Some(reference) {
                    *heartbeat_reference = None;
                }
            }
            ProtocolEvent::ChannelError { topic, .. } => {
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
                self.schedule_rejoin(topic, rejoins);
            }
            ProtocolEvent::ChannelClosed { topic, .. } => {
                if let Some(channel) = self.channels.get_mut(topic) {
                    channel.desired = false;
                }
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
            }
            ProtocolEvent::Message(frame) | ProtocolEvent::StaleMessage(frame) => {
                self.emit_channel(&frame.topic, ChannelEvent::Protocol(event.clone()));
            }
            ProtocolEvent::RequestInterrupted { topic, .. } => {
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
            }
            ProtocolEvent::UnmatchedReply(frame) => {
                self.emit_channel(&frame.topic, ChannelEvent::Protocol(event.clone()));
            }
        }

        if let ProtocolEvent::ChannelClosed { topic, .. } = &event {
            self.stop_channel(topic);
        }
        Ok(())
    }

    fn schedule_rejoin(
        &mut self,
        topic: &str,
        rejoins: &mut FuturesUnordered<LocalBoxFuture<'static, String>>,
    ) {
        let Some(channel) = self.channels.get_mut(topic) else {
            return;
        };
        if !channel.desired {
            return;
        }
        channel.join_attempt = channel.join_attempt.saturating_add(1);
        let delay = (self.options.rejoin_delay)(channel.join_attempt);
        let timer = self.timer.clone();
        let topic = topic.to_owned();
        rejoins.push(Box::pin(async move {
            timer.sleep(delay).await;
            topic
        }));
    }

    async fn flush(
        &mut self,
        topic: &str,
        transport: &mut Box<dyn Transport>,
    ) -> Result<(), String> {
        loop {
            let push = self
                .channels
                .get_mut(topic)
                .and_then(|channel| channel.queued.pop_front());
            let Some(push) = push else {
                return Ok(());
            };
            self.send_push(topic, push, transport).await?;
        }
    }

    async fn send_push(
        &mut self,
        topic: &str,
        push: QueuedPush,
        transport: &mut Box<dyn Transport>,
    ) -> Result<(), String> {
        match push {
            QueuedPush::Call {
                id,
                event,
                payload,
                response,
            } => {
                let outbound = self
                    .protocol
                    .push(topic, event, payload)
                    .map_err(|error| error.to_string())?;
                self.pending_calls
                    .insert(outbound.reference.clone(), PendingCall { id, response });
                self.send_frame(transport, outbound.frame).await
            }
            QueuedPush::Cast {
                event,
                payload,
                response,
                ..
            } => {
                let frame = self
                    .protocol
                    .cast(topic, event, payload)
                    .map_err(|error| error.to_string())?;
                match self.send_frame(transport, frame).await {
                    Ok(()) => {
                        let _ = response.send(Ok(()));
                        Ok(())
                    }
                    Err(error) => {
                        let _ = response.send(Err(ClientError::Interrupted));
                        Err(error)
                    }
                }
            }
        }
    }

    async fn send_frame(
        &mut self,
        transport: &mut Box<dyn Transport>,
        frame: Frame,
    ) -> Result<(), String> {
        let text = frame.encode_text().map_err(|error| error.to_string())?;
        transport
            .send(WireMessage::Text(text))
            .await
            .map_err(|error| error.to_string())
    }

    fn cancel(&mut self, id: RequestId) {
        for channel in self.channels.values_mut() {
            channel.join_waiters.remove(&id);
            if let Some(index) = channel.queued.iter().position(|push| push.id() == id) {
                channel.queued.remove(index);
                return;
            }
        }
        let reference = self
            .pending_calls
            .iter()
            .find_map(|(reference, pending)| (pending.id == id).then(|| reference.clone()));
        if let Some(reference) = reference {
            self.pending_calls.remove(&reference);
            self.protocol.forget_push(&reference);
        }
        let reference = self
            .pending_leaves
            .iter()
            .find_map(|(reference, pending)| (pending.id == id).then(|| reference.clone()));
        if let Some(reference) = reference {
            self.pending_leaves.remove(&reference);
        }
    }

    fn stop_channel(&mut self, topic: &str) {
        if let Some(channel) = self.channels.get_mut(topic) {
            channel.desired = false;
            channel.loading_payload = false;
            for (_, waiter) in channel.join_waiters.drain() {
                let _ = waiter.send(Err(ClientError::Interrupted));
            }
            for push in channel.queued.drain(..) {
                push.interrupt();
            }
        }
    }

    fn on_disconnect(&mut self, reason: &str) {
        let events = self.protocol.reset_connection();
        for (_, pending) in self.pending_calls.drain() {
            let _ = pending.response.send(Err(ClientError::Interrupted));
        }
        for (_, pending) in self.pending_leaves.drain() {
            let _ = pending.response.send(Err(ClientError::Interrupted));
        }
        for channel in self.channels.values_mut() {
            channel.loading_payload = false;
            let _ = channel.events.unbounded_send(ChannelEvent::Disconnected);
        }
        for event in events {
            if let ProtocolEvent::RequestInterrupted { topic, .. } = &event {
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
            }
        }
        self.emit_socket(SocketEvent::Disconnected {
            reason: reason.to_owned(),
        });
    }

    fn emit_socket(&mut self, event: SocketEvent) {
        self.socket_subscribers
            .retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok());
    }

    fn emit_channel(&mut self, topic: &str, event: ChannelEvent) {
        if let Some(channel) = self.channels.get_mut(topic) {
            let _ = channel.events.unbounded_send(event);
        }
    }
}

enum ConnectedExit {
    Shutdown(oneshot::Sender<()>),
    Disconnected(String),
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::VecDeque};

    use futures::{
        channel::mpsc, executor::LocalPool, future::LocalBoxFuture, task::LocalSpawnExt,
    };
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

        fn receive<'a>(
            &'a mut self,
        ) -> LocalBoxFuture<'a, Result<Option<WireMessage>, TransportError>> {
            Box::pin(async move { Ok(self.incoming.next().await) })
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
        fn connect(&self) -> LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>> {
            let result = self
                .transports
                .borrow_mut()
                .pop_front()
                .ok_or_else(|| TransportError::new("no test transport"));
            Box::pin(async move { result })
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
            let ((), result) =
                futures::join!(disconnect, channel.call("save", json!({"value": 1})));
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
                    Box::pin(
                        async move { Ok(json!({"count": count, "rejoin": context.is_rejoin})) },
                    )
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
}
