//! Socket, channel, and connection driver implementation.

mod config;

pub use config::{Connector, JoinContext, JoinPayloadLoader, Options, Timer, static_join_payload};

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
    FutureExt, SinkExt, StreamExt,
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

#[derive(Clone, Debug, PartialEq)]
pub enum SocketEvent {
    Connecting { attempt: u32 },
    Connected,
    Disconnected { reason: String },
    ReconnectScheduled { attempt: u32, delay: Duration },
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketStatus {
    Connecting,
    Connected,
    WaitingToReconnect,
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ChannelEvent {
    Protocol(ProtocolEvent),
    Disconnected,
    JoinPayloadError(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelStatus {
    WaitingForSocket,
    WaitingToJoin,
    Joining,
    Joined,
    Leaving,
    Left,
    Errored,
    Closed,
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
    #[error("the client command queue is full")]
    CommandQueueFull,
    #[error("the unsent push buffer is full for topic {0}")]
    PushBufferFull(String),
    #[error("a channel already exists for topic {0}")]
    DuplicateChannel(String),
    #[error("request timed out")]
    Timeout,
    #[error("request was interrupted by connection loss")]
    Interrupted,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("join payload loader failed: {0}")]
    JoinPayload(String),
    #[error("channel join was rejected: {0}")]
    JoinRejected(Value),
}

#[derive(Clone)]
pub struct Socket {
    commands: mpsc::Sender<Command>,
    lifecycle: mpsc::UnboundedSender<LifecycleCommand>,
    timer: Rc<dyn Timer>,
    options: Options,
    request_ids: Rc<Cell<RequestId>>,
    topics: Rc<RefCell<HashSet<String>>>,
    status: Rc<Cell<SocketStatus>>,
}

impl Socket {
    pub fn new(
        connector: impl Connector + 'static,
        timer: impl Timer + 'static,
        options: Options,
    ) -> (Self, Driver) {
        let (commands, command_rx) = mpsc::channel(options.command_capacity);
        let (lifecycle, lifecycle_rx) = mpsc::unbounded();
        let timer: Rc<dyn Timer> = Rc::new(timer);
        let status = Rc::new(Cell::new(SocketStatus::Connecting));
        let socket = Self {
            commands,
            lifecycle: lifecycle.clone(),
            timer: timer.clone(),
            options: options.clone(),
            request_ids: Rc::new(Cell::new(0)),
            topics: Rc::new(RefCell::new(HashSet::new())),
            status: status.clone(),
        };
        let state = DriverState::new(
            Rc::new(connector),
            timer,
            options,
            command_rx,
            lifecycle_rx,
            status,
        );
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
        let status = Rc::new(Cell::new(ChannelStatus::Closed));
        if self
            .lifecycle
            .unbounded_send(LifecycleCommand::Register {
                topic: topic.clone(),
                payload_loader,
                events,
                status: status.clone(),
            })
            .is_err()
        {
            self.topics.borrow_mut().remove(&topic);
            return Err(ClientError::DriverStopped);
        }
        Ok(Channel {
            topic,
            commands: self.commands.clone(),
            lifecycle: self.lifecycle.clone(),
            timer: self.timer.clone(),
            timeout: self.options.request_timeout,
            request_ids: self.request_ids.clone(),
            topics: self.topics.clone(),
            events: event_rx,
            status,
        })
    }

    pub fn events(&self) -> Result<SocketEvents, ClientError> {
        let (events, receiver) = mpsc::unbounded();
        let mut commands = self.commands.clone();
        commands
            .try_send(Command::Subscribe { events })
            .map_err(command_send_error)?;
        Ok(SocketEvents { receiver })
    }

    pub fn status(&self) -> SocketStatus {
        self.status.get()
    }

    pub async fn shutdown(&self) -> Result<(), ClientError> {
        let (response, receiver) = oneshot::channel();
        let mut commands = self.commands.clone();
        commands
            .send(Command::Shutdown { response })
            .await
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
    commands: mpsc::Sender<Command>,
    lifecycle: mpsc::UnboundedSender<LifecycleCommand>,
    timer: Rc<dyn Timer>,
    timeout: Duration,
    request_ids: Rc<Cell<RequestId>>,
    topics: Rc<RefCell<HashSet<String>>>,
    events: mpsc::UnboundedReceiver<ChannelEvent>,
    status: Rc<Cell<ChannelStatus>>,
}

impl Channel {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn status(&self) -> ChannelStatus {
        self.status.get()
    }

    pub async fn join(&self) -> Result<Value, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Join {
            id,
            topic: self.topic.clone(),
            response,
        })
        .await?;
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
        })
        .await?;
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
        })
        .await?;
        self.wait(id, receiver).await
    }

    pub async fn leave(&self) -> Result<Value, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Leave {
            id,
            topic: self.topic.clone(),
            response,
        })
        .await?;
        self.wait(id, receiver).await
    }

    pub async fn next_event(&mut self) -> Option<ChannelEvent> {
        self.events.next().await
    }

    pub fn events(&self) -> Result<ChannelEvents, ClientError> {
        let (events, receiver) = mpsc::unbounded();
        let mut commands = self.commands.clone();
        commands
            .try_send(Command::SubscribeChannel {
                topic: self.topic.clone(),
                events,
            })
            .map_err(command_send_error)?;
        Ok(ChannelEvents { receiver })
    }

    async fn send(&self, command: Command) -> Result<(), ClientError> {
        let mut commands = self.commands.clone();
        commands
            .send(command)
            .await
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
        let mut guard = RequestGuard {
            id,
            lifecycle: self.lifecycle.clone(),
            armed: true,
        };
        let response = receiver.fuse();
        let timeout = self.timer.sleep(self.timeout).fuse();
        futures::pin_mut!(response, timeout);
        futures::select! {
            response = response => {
                guard.armed = false;
                response.map_err(|_| ClientError::DriverStopped)?
            },
            () = timeout => Err(ClientError::Timeout),
        }
    }
}

pub struct ChannelEvents {
    receiver: mpsc::UnboundedReceiver<ChannelEvent>,
}

impl ChannelEvents {
    pub async fn next(&mut self) -> Option<ChannelEvent> {
        self.receiver.next().await
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        self.status.set(ChannelStatus::Closed);
        self.topics.borrow_mut().remove(&self.topic);
        let _ = self.lifecycle.unbounded_send(LifecycleCommand::Unregister {
            topic: self.topic.clone(),
        });
    }
}

struct RequestGuard {
    id: RequestId,
    lifecycle: mpsc::UnboundedSender<LifecycleCommand>,
    armed: bool,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self
                .lifecycle
                .unbounded_send(LifecycleCommand::Cancel { id: self.id });
        }
    }
}

fn command_send_error<T>(error: mpsc::TrySendError<T>) -> ClientError {
    if error.is_full() {
        ClientError::CommandQueueFull
    } else {
        ClientError::DriverStopped
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

enum LifecycleCommand {
    Register {
        topic: String,
        payload_loader: JoinPayloadLoader,
        events: mpsc::UnboundedSender<ChannelEvent>,
        status: Rc<Cell<ChannelStatus>>,
    },
    Unregister {
        topic: String,
    },
    Cancel {
        id: RequestId,
    },
}

enum Command {
    Subscribe {
        events: mpsc::UnboundedSender<SocketEvent>,
    },
    SubscribeChannel {
        topic: String,
        events: mpsc::UnboundedSender<ChannelEvent>,
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
        self.fail(ClientError::Interrupted);
    }

    fn fail(self, error: ClientError) {
        match self {
            Self::Call { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::Cast { response, .. } => {
                let _ = response.send(Err(error));
            }
        }
    }
}

struct ChannelRecord {
    payload_loader: JoinPayloadLoader,
    subscribers: Vec<mpsc::UnboundedSender<ChannelEvent>>,
    status: Rc<Cell<ChannelStatus>>,
    desired: bool,
    ever_joined: bool,
    active_payload: Option<u64>,
    join_attempt: u32,
    rejoin_scheduled: bool,
    join_waiters: HashMap<RequestId, oneshot::Sender<Result<Value, ClientError>>>,
    queued: VecDeque<QueuedPush>,
    deferred_leave: Option<PendingLeave>,
}

struct PendingCall {
    id: RequestId,
    response: oneshot::Sender<Result<Reply, ClientError>>,
}

struct PendingLeave {
    id: RequestId,
    response: Option<oneshot::Sender<Result<Value, ClientError>>>,
}

type PayloadResult = (String, u64, Result<Value, String>);

enum OperationTimeout {
    Join { topic: String, reference: String },
    Leave { topic: String, reference: String },
}

struct DriverState {
    connector: Rc<dyn Connector>,
    timer: Rc<dyn Timer>,
    options: Options,
    commands: mpsc::Receiver<Command>,
    lifecycle: mpsc::UnboundedReceiver<LifecycleCommand>,
    protocol: Protocol,
    channels: HashMap<String, ChannelRecord>,
    socket_subscribers: Vec<mpsc::UnboundedSender<SocketEvent>>,
    pending_joins: HashMap<String, String>,
    pending_calls: HashMap<String, PendingCall>,
    pending_leaves: HashMap<String, PendingLeave>,
    next_payload_id: u64,
    socket_status: Rc<Cell<SocketStatus>>,
}

impl DriverState {
    fn new(
        connector: Rc<dyn Connector>,
        timer: Rc<dyn Timer>,
        options: Options,
        commands: mpsc::Receiver<Command>,
        lifecycle: mpsc::UnboundedReceiver<LifecycleCommand>,
        socket_status: Rc<Cell<SocketStatus>>,
    ) -> Self {
        Self {
            connector,
            timer,
            options,
            commands,
            lifecycle,
            protocol: Protocol::new(),
            channels: HashMap::new(),
            socket_subscribers: Vec::new(),
            pending_joins: HashMap::new(),
            pending_calls: HashMap::new(),
            pending_leaves: HashMap::new(),
            next_payload_id: 0,
            socket_status,
        }
    }

    async fn run(mut self) {
        let mut attempt = 0;
        loop {
            self.emit_socket(SocketEvent::Connecting { attempt });
            let connection = self.connector.connect().fuse();
            futures::pin_mut!(connection);
            let connected = loop {
                let lifecycle = self.lifecycle.next().fuse();
                let command = self.commands.next().fuse();
                futures::pin_mut!(lifecycle, command);
                futures::select_biased! {
                    lifecycle = lifecycle => match lifecycle {
                        Some(command) => self.handle_offline_lifecycle(command),
                        None => break None,
                    },
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
            let lifecycle = self.lifecycle.next().fuse();
            let command = self.commands.next().fuse();
            futures::pin_mut!(lifecycle, command);
            futures::select_biased! {
                lifecycle = lifecycle => match lifecycle {
                    Some(command) => self.handle_offline_lifecycle(command),
                    None => return true,
                },
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
        let mut operation_timeouts: FuturesUnordered<LocalBoxFuture<'static, OperationTimeout>> =
            FuturesUnordered::new();
        for channel in self.channels.values_mut() {
            channel.rejoin_scheduled = false;
        }
        let desired = self
            .channels
            .iter()
            .filter(|(_, channel)| channel.desired)
            .map(|(topic, _)| topic.clone())
            .collect::<Vec<_>>();
        for topic in desired {
            if let Some(channel) = self.channels.get(&topic) {
                channel.status.set(ChannelStatus::WaitingToJoin);
            }
            self.load_payload(&topic, &mut payloads);
        }

        let mut heartbeat = self.timer.sleep(self.options.heartbeat_interval);
        let mut heartbeat_reference: Option<String> = None;

        loop {
            enum Action {
                Lifecycle(Option<LifecycleCommand>),
                Command(Option<Command>),
                Incoming(Result<Option<WireMessage>, TransportError>),
                Heartbeat,
                Payload(Option<PayloadResult>),
                Rejoin(Option<String>),
                OperationTimeout(Option<OperationTimeout>),
            }

            let action = {
                let lifecycle = self.lifecycle.next().fuse();
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
                let operation_timeout: LocalBoxFuture<'_, Option<OperationTimeout>> =
                    if operation_timeouts.is_empty() {
                        Box::pin(pending())
                    } else {
                        Box::pin(operation_timeouts.next())
                    };
                let operation_timeout = operation_timeout.fuse();
                futures::pin_mut!(
                    lifecycle,
                    command,
                    incoming,
                    heartbeat_wait,
                    payload,
                    rejoin,
                    operation_timeout
                );
                futures::select_biased! {
                    lifecycle = lifecycle => Action::Lifecycle(lifecycle),
                    command = command => Action::Command(command),
                    incoming = incoming => Action::Incoming(incoming),
                    () = heartbeat_wait => Action::Heartbeat,
                    payload = payload => Action::Payload(payload),
                    topic = rejoin => Action::Rejoin(topic),
                    timeout = operation_timeout => Action::OperationTimeout(timeout),
                }
            };

            let result = match action {
                Action::Lifecycle(Some(command)) => {
                    self.handle_connected_lifecycle(command, transport).await
                }
                Action::Lifecycle(None) => {
                    return ConnectedExit::Disconnected("lifecycle channel closed".into());
                }
                Action::Command(Some(Command::Shutdown { response })) => {
                    return ConnectedExit::Shutdown(response);
                }
                Action::Command(Some(command)) => {
                    self.handle_connected_command(
                        command,
                        transport,
                        &mut payloads,
                        &mut operation_timeouts,
                    )
                    .await
                }
                Action::Command(None) => {
                    return ConnectedExit::Disconnected("command channel closed".into());
                }
                Action::Incoming(Ok(Some(message))) => {
                    self.handle_incoming(
                        message,
                        transport,
                        &mut payloads,
                        &mut rejoins,
                        &mut operation_timeouts,
                        &mut heartbeat_reference,
                    )
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
                Action::Payload(Some((topic, payload_id, payload))) => {
                    self.handle_payload(
                        topic,
                        payload_id,
                        payload,
                        transport,
                        &mut operation_timeouts,
                    )
                    .await
                }
                Action::Payload(None) => Ok(()),
                Action::Rejoin(Some(topic)) => {
                    let desired = self.channels.get_mut(&topic).is_some_and(|channel| {
                        channel.rejoin_scheduled = false;
                        channel.desired
                    });
                    if desired {
                        self.load_payload(&topic, &mut payloads);
                    }
                    Ok(())
                }
                Action::Rejoin(None) => Ok(()),
                Action::OperationTimeout(Some(timeout)) => {
                    self.handle_operation_timeout(timeout, &mut payloads, &mut rejoins)
                }
                Action::OperationTimeout(None) => Ok(()),
            };

            if let Err(reason) = result {
                return ConnectedExit::Disconnected(reason);
            }
        }
    }

    fn handle_offline_lifecycle(&mut self, command: LifecycleCommand) {
        match command {
            LifecycleCommand::Register {
                topic,
                payload_loader,
                events,
                status,
            } => self.register(topic, payload_loader, events, status),
            LifecycleCommand::Unregister { topic } => self.unregister(&topic),
            LifecycleCommand::Cancel { id } => self.cancel(id),
        }
    }

    fn handle_offline_command(&mut self, command: Command) -> bool {
        match command {
            Command::Subscribe { events } => self.socket_subscribers.push(events),
            Command::SubscribeChannel { topic, events } => self.subscribe_channel(&topic, events),
            Command::Join {
                id,
                topic,
                response,
            } => {
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.desired = true;
                    channel.status.set(ChannelStatus::WaitingForSocket);
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
                if let Some(channel) = self.channels.get(&topic) {
                    channel.status.set(ChannelStatus::Left);
                }
                self.stop_channel(&topic);
                let _ = response.send(Ok(json!({})));
            }
            Command::Shutdown { response } => {
                let _ = response.send(());
                return true;
            }
        }
        false
    }

    async fn handle_connected_lifecycle(
        &mut self,
        command: LifecycleCommand,
        transport: &mut Box<dyn Transport>,
    ) -> Result<(), String> {
        match command {
            LifecycleCommand::Register {
                topic,
                payload_loader,
                events,
                status,
            } => self.register(topic, payload_loader, events, status),
            LifecycleCommand::Unregister { topic } => {
                let leave = if self.protocol.channel_state(&topic) == Some(ChannelState::Joined) {
                    self.protocol
                        .leave(&topic)
                        .ok()
                        .map(|outbound| outbound.frame)
                } else {
                    None
                };
                self.unregister(&topic);
                if let Some(frame) = leave {
                    self.send_frame(transport, frame).await?;
                }
            }
            LifecycleCommand::Cancel { id } => self.cancel(id),
        }
        Ok(())
    }

    async fn handle_connected_command(
        &mut self,
        command: Command,
        transport: &mut Box<dyn Transport>,
        payloads: &mut FuturesUnordered<LocalBoxFuture<'static, PayloadResult>>,
        operation_timeouts: &mut FuturesUnordered<LocalBoxFuture<'static, OperationTimeout>>,
    ) -> Result<(), String> {
        match command {
            Command::Subscribe { events } => self.socket_subscribers.push(events),
            Command::SubscribeChannel { topic, events } => self.subscribe_channel(&topic, events),
            Command::Join {
                id,
                topic,
                response,
            } => {
                let joined = self.protocol.channel_state(&topic) == Some(ChannelState::Joined);
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.desired = true;
                    if joined {
                        channel.status.set(ChannelStatus::Joined);
                        let _ = response.send(Ok(json!({})));
                    } else {
                        channel.status.set(ChannelStatus::WaitingToJoin);
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
                let state = self.protocol.channel_state(&topic);
                if let Some(channel) = self.channels.get(&topic) {
                    let status = match state {
                        Some(ChannelState::Joined | ChannelState::Joining) => {
                            ChannelStatus::Leaving
                        }
                        _ => ChannelStatus::Left,
                    };
                    channel.status.set(status);
                }
                self.stop_channel(&topic);
                if state == Some(ChannelState::Joined) {
                    let outbound = self
                        .protocol
                        .leave(&topic)
                        .map_err(|error| error.to_string())?;
                    self.pending_leaves.insert(
                        outbound.reference.clone(),
                        PendingLeave {
                            id,
                            response: Some(response),
                        },
                    );
                    self.schedule_operation_timeout(
                        OperationTimeout::Leave {
                            topic,
                            reference: outbound.reference.clone(),
                        },
                        operation_timeouts,
                    );
                    self.send_frame(transport, outbound.frame).await?;
                } else if state == Some(ChannelState::Joining) {
                    if let Some(channel) = self.channels.get_mut(&topic) {
                        channel.deferred_leave = Some(PendingLeave {
                            id,
                            response: Some(response),
                        });
                    } else {
                        let _ = response.send(Ok(json!({})));
                    }
                } else {
                    let _ = response.send(Ok(json!({})));
                }
            }
            Command::Shutdown { .. } => unreachable!("handled by run_connected"),
        }
        Ok(())
    }

    fn register(
        &mut self,
        topic: String,
        payload_loader: JoinPayloadLoader,
        events: mpsc::UnboundedSender<ChannelEvent>,
        status: Rc<Cell<ChannelStatus>>,
    ) {
        self.channels.entry(topic).or_insert(ChannelRecord {
            payload_loader,
            subscribers: vec![events],
            status,
            desired: false,
            ever_joined: false,
            active_payload: None,
            join_attempt: 0,
            rejoin_scheduled: false,
            join_waiters: HashMap::new(),
            queued: VecDeque::new(),
            deferred_leave: None,
        });
    }

    fn subscribe_channel(&mut self, topic: &str, events: mpsc::UnboundedSender<ChannelEvent>) {
        if let Some(channel) = self.channels.get_mut(topic) {
            channel.subscribers.push(events);
        }
    }

    fn unregister(&mut self, topic: &str) {
        self.stop_channel(topic);
        self.channels.remove(topic);
        self.protocol.discard_channel(topic);
    }

    fn queue(&mut self, topic: &str, push: QueuedPush) {
        if let Some(channel) = self.channels.get_mut(topic) {
            if channel.queued.len() < self.options.push_buffer_capacity {
                channel.queued.push_back(push);
            } else {
                push.fail(ClientError::PushBufferFull(topic.to_owned()));
            }
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
        if !channel.desired || channel.active_payload.is_some() {
            return;
        }
        if matches!(
            self.protocol.channel_state(topic),
            Some(ChannelState::Joining | ChannelState::Joined | ChannelState::Leaving)
        ) {
            return;
        }
        channel.status.set(ChannelStatus::WaitingToJoin);
        self.next_payload_id = self.next_payload_id.wrapping_add(1);
        if self.next_payload_id == 0 {
            self.next_payload_id = 1;
        }
        let payload_id = self.next_payload_id;
        channel.active_payload = Some(payload_id);
        let context = JoinContext {
            attempt: channel.join_attempt,
            is_rejoin: channel.ever_joined,
        };
        let loader = channel.payload_loader.clone();
        let topic = topic.to_owned();
        payloads.push(Box::pin(async move {
            let result = loader(context).await;
            (topic, payload_id, result)
        }));
    }

    async fn handle_payload(
        &mut self,
        topic: String,
        payload_id: u64,
        payload: Result<Value, String>,
        transport: &mut Box<dyn Transport>,
        operation_timeouts: &mut FuturesUnordered<LocalBoxFuture<'static, OperationTimeout>>,
    ) -> Result<(), String> {
        let Some(channel) = self.channels.get_mut(&topic) else {
            return Ok(());
        };
        if channel.active_payload != Some(payload_id) {
            return Ok(());
        }
        channel.active_payload = None;
        if !channel.desired {
            return Ok(());
        }
        let payload = match payload {
            Ok(payload) => payload,
            Err(error) => {
                channel.desired = false;
                channel.status.set(ChannelStatus::Errored);
                for (_, waiter) in channel.join_waiters.drain() {
                    let _ = waiter.send(Err(ClientError::JoinPayload(error.clone())));
                }
                self.emit_channel(&topic, ChannelEvent::JoinPayloadError(error));
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
        if let Some(channel) = self.channels.get(&topic) {
            channel.status.set(ChannelStatus::Joining);
        }
        self.pending_joins
            .insert(outbound.reference.clone(), topic.clone());
        self.schedule_operation_timeout(
            OperationTimeout::Join {
                topic,
                reference: outbound.reference.clone(),
            },
            operation_timeouts,
        );
        self.send_frame(transport, outbound.frame).await
    }

    async fn handle_incoming(
        &mut self,
        message: WireMessage,
        transport: &mut Box<dyn Transport>,
        payloads: &mut FuturesUnordered<LocalBoxFuture<'static, PayloadResult>>,
        rejoins: &mut FuturesUnordered<LocalBoxFuture<'static, String>>,
        operation_timeouts: &mut FuturesUnordered<LocalBoxFuture<'static, OperationTimeout>>,
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
                topic,
                reference,
                response,
            } => {
                self.pending_joins.remove(reference);
                let deferred_leave = if let Some(channel) = self.channels.get_mut(topic) {
                    channel.ever_joined = true;
                    channel.join_attempt = 0;
                    channel.rejoin_scheduled = false;
                    let deferred_leave = channel.deferred_leave.take();
                    channel.status.set(if deferred_leave.is_some() {
                        ChannelStatus::Leaving
                    } else {
                        ChannelStatus::Joined
                    });
                    for (_, waiter) in channel.join_waiters.drain() {
                        let _ = waiter.send(Ok(response.clone()));
                    }
                    deferred_leave
                } else {
                    None
                };
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
                if let Some(pending) = deferred_leave {
                    let outbound = self
                        .protocol
                        .leave(topic)
                        .map_err(|error| error.to_string())?;
                    self.pending_leaves
                        .insert(outbound.reference.clone(), pending);
                    self.schedule_operation_timeout(
                        OperationTimeout::Leave {
                            topic: topic.clone(),
                            reference: outbound.reference.clone(),
                        },
                        operation_timeouts,
                    );
                    self.send_frame(transport, outbound.frame).await?;
                } else {
                    self.flush(topic, transport).await?;
                }
            }
            ProtocolEvent::JoinError {
                topic,
                reference,
                response,
            } => {
                self.pending_joins.remove(reference);
                if let Some(channel) = self.channels.get_mut(topic) {
                    channel.status.set(ChannelStatus::Errored);
                    if let Some(pending) = channel.deferred_leave.take() {
                        if let Some(response) = pending.response {
                            let _ = response.send(Ok(json!({})));
                        }
                        channel.status.set(ChannelStatus::Left);
                    }
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
                    if let Some(response_tx) = pending.response {
                        let _ = response_tx.send(Ok(response.clone()));
                    }
                }
                let should_rejoin = self
                    .channels
                    .get(topic)
                    .is_some_and(|channel| channel.desired);
                if let Some(channel) = self.channels.get(topic) {
                    channel.status.set(if should_rejoin {
                        ChannelStatus::WaitingToJoin
                    } else {
                        ChannelStatus::Left
                    });
                }
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
                if should_rejoin {
                    self.load_payload(topic, payloads);
                }
            }
            ProtocolEvent::HeartbeatAck { reference, .. } => {
                if heartbeat_reference.as_deref() == Some(reference) {
                    *heartbeat_reference = None;
                }
            }
            ProtocolEvent::ChannelError { topic, .. } => {
                self.pending_joins
                    .retain(|_, pending_topic| pending_topic != topic);
                if let Some(channel) = self.channels.get(topic) {
                    channel.status.set(ChannelStatus::Errored);
                }
                self.emit_channel(topic, ChannelEvent::Protocol(event.clone()));
                self.schedule_rejoin(topic, rejoins);
            }
            ProtocolEvent::ChannelClosed { topic, .. } => {
                self.pending_joins
                    .retain(|_, pending_topic| pending_topic != topic);
                if let Some(channel) = self.channels.get_mut(topic) {
                    channel.desired = false;
                    channel.status.set(ChannelStatus::Closed);
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
        if !channel.desired || channel.rejoin_scheduled {
            return;
        }
        channel.rejoin_scheduled = true;
        channel.join_attempt = channel.join_attempt.saturating_add(1);
        let delay = (self.options.rejoin_delay)(channel.join_attempt);
        let timer = self.timer.clone();
        let topic = topic.to_owned();
        rejoins.push(Box::pin(async move {
            timer.sleep(delay).await;
            topic
        }));
    }

    fn schedule_operation_timeout(
        &self,
        operation: OperationTimeout,
        timeouts: &mut FuturesUnordered<LocalBoxFuture<'static, OperationTimeout>>,
    ) {
        let timer = self.timer.clone();
        let timeout = self.options.request_timeout;
        timeouts.push(Box::pin(async move {
            timer.sleep(timeout).await;
            operation
        }));
    }

    fn handle_operation_timeout(
        &mut self,
        timeout: OperationTimeout,
        payloads: &mut FuturesUnordered<LocalBoxFuture<'static, PayloadResult>>,
        rejoins: &mut FuturesUnordered<LocalBoxFuture<'static, String>>,
    ) -> Result<(), String> {
        match timeout {
            OperationTimeout::Join { topic, reference } => {
                if self.pending_joins.remove(&reference).as_deref() != Some(topic.as_str()) {
                    return Ok(());
                }
                self.protocol.discard_channel(&topic);
                let mut should_rejoin = false;
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.active_payload = None;
                    if let Some(pending) = channel.deferred_leave.take() {
                        if let Some(response) = pending.response {
                            let _ = response.send(Err(ClientError::Timeout));
                        }
                        channel.status.set(ChannelStatus::Left);
                    } else if channel.desired {
                        channel.status.set(ChannelStatus::Errored);
                        for (_, waiter) in channel.join_waiters.drain() {
                            let _ = waiter.send(Err(ClientError::Timeout));
                        }
                        should_rejoin = true;
                    } else {
                        channel.status.set(ChannelStatus::Left);
                    }
                }
                if should_rejoin {
                    self.schedule_rejoin(&topic, rejoins);
                }
            }
            OperationTimeout::Leave { topic, reference } => {
                let Some(pending) = self.pending_leaves.remove(&reference) else {
                    return Ok(());
                };
                if let Some(response) = pending.response {
                    let _ = response.send(Err(ClientError::Timeout));
                }
                self.protocol.discard_channel(&topic);
                let should_rejoin = self
                    .channels
                    .get(&topic)
                    .is_some_and(|channel| channel.desired);
                if let Some(channel) = self.channels.get(&topic) {
                    channel.status.set(if should_rejoin {
                        ChannelStatus::WaitingToJoin
                    } else {
                        ChannelStatus::Left
                    });
                }
                if should_rejoin {
                    self.load_payload(&topic, payloads);
                }
            }
        }
        Ok(())
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
            if let Some(pending) = channel
                .deferred_leave
                .as_mut()
                .filter(|pending| pending.id == id)
            {
                pending.response.take();
                return;
            }
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
            if let Some(pending) = self.pending_leaves.get_mut(&reference) {
                pending.response.take();
            }
        }
    }

    fn stop_channel(&mut self, topic: &str) {
        if let Some(channel) = self.channels.get_mut(topic) {
            channel.desired = false;
            channel.active_payload = None;
            channel.rejoin_scheduled = false;
            if let Some(pending) = channel.deferred_leave.take() {
                if let Some(response) = pending.response {
                    let _ = response.send(Err(ClientError::Interrupted));
                }
            }
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
        self.pending_joins.clear();
        for (_, pending) in self.pending_calls.drain() {
            let _ = pending.response.send(Err(ClientError::Interrupted));
        }
        for (_, pending) in self.pending_leaves.drain() {
            if let Some(response) = pending.response {
                let _ = response.send(Err(ClientError::Interrupted));
            }
        }
        let topics = self.channels.keys().cloned().collect::<Vec<_>>();
        for channel in self.channels.values_mut() {
            channel.active_payload = None;
            channel.rejoin_scheduled = false;
            if let Some(pending) = channel.deferred_leave.take() {
                if let Some(response) = pending.response {
                    let _ = response.send(Err(ClientError::Interrupted));
                }
            }
            let status = if channel.desired {
                ChannelStatus::WaitingForSocket
            } else {
                ChannelStatus::Left
            };
            channel.status.set(status);
        }
        for topic in topics {
            self.emit_channel(&topic, ChannelEvent::Disconnected);
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
        let status = match &event {
            SocketEvent::Connecting { .. } => SocketStatus::Connecting,
            SocketEvent::Connected => SocketStatus::Connected,
            SocketEvent::Disconnected { .. } | SocketEvent::ReconnectScheduled { .. } => {
                SocketStatus::WaitingToReconnect
            }
            SocketEvent::Closed => SocketStatus::Closed,
        };
        self.socket_status.set(status);
        if status == SocketStatus::Closed {
            for channel in self.channels.values() {
                channel.status.set(ChannelStatus::Closed);
            }
        }
        self.socket_subscribers
            .retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok());
    }

    fn emit_channel(&mut self, topic: &str, event: ChannelEvent) {
        if let Some(channel) = self.channels.get_mut(topic) {
            channel
                .subscribers
                .retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok());
        }
    }
}

enum ConnectedExit {
    Shutdown(oneshot::Sender<()>),
    Disconnected(String),
}

#[cfg(test)]
mod tests;
