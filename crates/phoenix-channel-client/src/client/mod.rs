//! Socket, channel, and connection driver implementation.

mod config;
mod endpoint;
mod presence;
#[cfg(feature = "tracing")]
mod tracing_support;

pub use config::{
    ConnectContext, Connector, JoinContext, JoinPayloadLoader, Options, ReconnectAction,
    ReconnectContext, ReconnectPolicy, Timer, static_join_payload,
};
pub use endpoint::{
    ConnectionConfig, ConnectionConfigLoader, Endpoint, EndpointError, ResolvedEndpoint,
    static_connection_config,
};
pub use presence::{ChannelPresence, PresenceEvent, PresenceStreamError};
#[cfg(feature = "tracing")]
pub use tracing_support::tracing_telemetry_hook;

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
    ChannelState, Codec, EventRoute, Frame, Payload, PayloadError, PhoenixV2Codec, Protocol,
    ProtocolEvent, ReplyStatus, Transport, TransportClose, TransportCloseRequest, TransportError,
    TransportErrorKind, TransportEvent, WireMessage,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use thiserror::Error;

type RequestId = u64;

#[derive(Clone, Debug, PartialEq)]
pub enum SocketEvent {
    Connecting {
        attempt: u32,
    },
    Connected,
    Disconnected {
        reason: DisconnectReason,
    },
    ReconnectScheduled {
        attempt: u32,
        delay: Duration,
    },
    ReconnectStopped {
        attempt: u32,
        reason: DisconnectReason,
    },
    Closed,
    Lagged {
        dropped: u64,
    },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DisconnectReason {
    #[error("connection closed by request")]
    Requested,
    #[error("connection failed: {0}")]
    Connect(TransportError),
    #[error("transport failed: {0}")]
    Transport(TransportError),
    #[error("connection closed: {0:?}")]
    Closed(TransportClose),
    #[error("heartbeat acknowledgement timed out")]
    HeartbeatTimeout,
    #[error("protocol failed: {0}")]
    Protocol(String),
    #[error("client driver stopped")]
    DriverStopped,
}

impl DisconnectReason {
    fn should_reconnect(&self) -> bool {
        match self {
            Self::Closed(close) => close.should_reconnect(),
            Self::Requested | Self::DriverStopped => false,
            Self::Connect(_) | Self::Transport(_) | Self::HeartbeatTimeout | Self::Protocol(_) => {
                true
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketStatus {
    Disconnected,
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
    Lagged { dropped: u64 },
}

impl ChannelEvent {
    pub fn route<R: EventRoute>(&self) -> Result<Option<R::Output>, PayloadError> {
        match self {
            Self::Protocol(ProtocolEvent::Message(frame)) => frame.route::<R>(),
            _ => Ok(None),
        }
    }
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
    pub response: Payload,
}

impl Reply {
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self) -> Result<T, PayloadError> {
        self.response.deserialize()
    }

    pub fn into_result(self) -> Result<Payload, Payload> {
        match self.status {
            ReplyStatus::Ok => Ok(self.response),
            ReplyStatus::Error => Err(self.response),
        }
    }

    pub fn deserialize_ok<T: DeserializeOwned>(self) -> Result<T, ReplyError> {
        self.into_result()
            .map_err(ReplyError::Server)?
            .deserialize()
            .map_err(ReplyError::Decode)
    }
}

#[derive(Debug, Error)]
pub enum ReplyError {
    #[error("Phoenix returned an error reply: {0:?}")]
    Server(Payload),
    #[error("failed to decode reply payload: {0}")]
    Decode(PayloadError),
}

#[derive(Debug, Error)]
pub enum CallJsonError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("failed to encode request payload: {0}")]
    Encode(serde_json::Error),
    #[error(transparent)]
    Reply(#[from] ReplyError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientOperation {
    Join,
    Call,
    Cast,
    Leave,
    Ping,
}

impl std::fmt::Display for ClientOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Join => "join",
            Self::Call => "call",
            Self::Cast => "cast",
            Self::Leave => "leave",
            Self::Ping => "ping",
        })
    }
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
    #[error("channel {0} is already joined")]
    AlreadyJoined(String),
    #[error("channel {0} must be joined again before sending events")]
    ChannelNotJoined(String),
    #[error("the socket must be connected for this operation")]
    SocketNotConnected,
    #[error("the active transport does not support binary Phoenix frames")]
    BinaryNotSupported,
    #[error("{operation} timed out after {timeout:?}")]
    Timeout {
        operation: ClientOperation,
        timeout: Duration,
    },
    #[error("{operation} was interrupted by connection loss")]
    Interrupted { operation: ClientOperation },
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("join payload loader failed for {topic}: {message}")]
    JoinPayload { topic: String, message: String },
    #[error("channel join was rejected for {topic}: {response:?}")]
    JoinRejected { topic: String, response: Payload },
    #[error("unknown channel topic: {0}")]
    UnknownTopic(String),
}

struct ObservableStatus<T> {
    value: Cell<T>,
    subscribers: RefCell<Vec<mpsc::Sender<()>>>,
}

impl<T: Copy + Eq> ObservableStatus<T> {
    fn new(value: T) -> Self {
        Self {
            value: Cell::new(value),
            subscribers: RefCell::new(Vec::new()),
        }
    }

    fn get(&self) -> T {
        self.value.get()
    }

    fn set(&self, value: T) {
        if self.value.replace(value) == value {
            return;
        }
        self.subscribers
            .borrow_mut()
            .retain_mut(|subscriber| match subscriber.try_send(()) {
                Ok(()) => true,
                Err(error) => error.is_full(),
            });
    }

    fn subscribe(self: &Rc<Self>) -> StatusChanges<T> {
        let (sender, receiver) = mpsc::channel(1);
        self.subscribers.borrow_mut().push(sender);
        StatusChanges {
            receiver,
            status: self.clone(),
        }
    }
}

pub struct StatusChanges<T> {
    receiver: mpsc::Receiver<()>,
    status: Rc<ObservableStatus<T>>,
}

impl<T: Copy + Eq> StatusChanges<T> {
    pub fn current(&self) -> T {
        self.status.get()
    }

    pub async fn changed(&mut self) -> Option<T> {
        self.receiver.next().await.map(|()| self.status.get())
    }
}

pub type SocketStatusChanges = StatusChanges<SocketStatus>;
pub type ChannelStatusChanges = StatusChanges<ChannelStatus>;

#[derive(Clone, Debug, PartialEq)]
pub enum TelemetryEvent {
    Socket(SocketEvent),
    Channel {
        topic: String,
        event: ChannelEvent,
    },
    FrameSent {
        topic: String,
        event: String,
        binary: bool,
        bytes: usize,
    },
    FrameReceived {
        topic: String,
        event: String,
        binary: bool,
        bytes: usize,
    },
    ConnectionAttemptFinished {
        attempt: u32,
        duration: Duration,
        connected: bool,
    },
    CallCompleted {
        topic: String,
        event: String,
        outcome: CallOutcome,
        duration: Duration,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallOutcome {
    Reply(ReplyStatus),
    Cancelled,
    Interrupted,
    Rejected,
}

pub type TelemetryHook = Rc<dyn Fn(&TelemetryEvent)>;

#[derive(Clone)]
pub struct Socket {
    commands: mpsc::Sender<Command>,
    lifecycle: mpsc::UnboundedSender<LifecycleCommand>,
    timer: Rc<dyn Timer>,
    options: Options,
    request_ids: Rc<Cell<RequestId>>,
    topics: Rc<RefCell<HashSet<String>>>,
    status: Rc<ObservableStatus<SocketStatus>>,
}

impl Socket {
    pub fn new(
        connector: impl Connector + 'static,
        timer: impl Timer + 'static,
        options: Options,
    ) -> (Self, Driver) {
        Self::new_with_codec(
            connector,
            timer,
            options,
            PhoenixV2Codec::limited(Default::default()),
        )
    }

    pub fn new_with_codec(
        connector: impl Connector + 'static,
        timer: impl Timer + 'static,
        options: Options,
        codec: impl Codec + 'static,
    ) -> (Self, Driver) {
        let (commands, command_rx) = mpsc::channel(options.command_capacity);
        let (lifecycle, lifecycle_rx) = mpsc::unbounded();
        let timer: Rc<dyn Timer> = Rc::new(timer);
        let status = Rc::new(ObservableStatus::new(if options.connect_on_start {
            SocketStatus::Connecting
        } else {
            SocketStatus::Disconnected
        }));
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
            Rc::new(codec),
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
        let (events, event_rx) = mpsc::channel(self.options.event_capacity);
        let status = Rc::new(ObservableStatus::new(ChannelStatus::Closed));
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
            timeouts: OperationTimeouts {
                join: self.options.join_timeout,
                call: self.options.call_timeout,
                leave: self.options.leave_timeout,
            },
            event_capacity: self.options.event_capacity,
            request_ids: self.request_ids.clone(),
            topics: self.topics.clone(),
            events: event_rx,
            status,
        })
    }

    pub fn events(&self) -> Result<SocketEvents, ClientError> {
        let (events, receiver) = mpsc::channel(self.options.event_capacity);
        let mut commands = self.commands.clone();
        commands
            .try_send(Command::Subscribe { events })
            .map_err(command_send_error)?;
        Ok(SocketEvents { receiver })
    }

    pub fn status(&self) -> SocketStatus {
        self.status.get()
    }

    pub fn status_changes(&self) -> SocketStatusChanges {
        self.status.subscribe()
    }

    pub async fn connect(&self) -> Result<(), ClientError> {
        let (response, receiver) = oneshot::channel();
        let mut commands = self.commands.clone();
        commands
            .send(Command::Connect { response })
            .await
            .map_err(|_| ClientError::DriverStopped)?;
        receiver.await.map_err(|_| ClientError::DriverStopped)
    }

    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.disconnect_inner(None).await
    }

    pub async fn disconnect_with(
        &self,
        code: u16,
        reason: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.disconnect_inner(Some(TransportCloseRequest::new(code, reason)))
            .await
    }

    async fn disconnect_inner(
        &self,
        close: Option<TransportCloseRequest>,
    ) -> Result<(), ClientError> {
        let (response, receiver) = oneshot::channel();
        let mut commands = self.commands.clone();
        commands
            .send(Command::Disconnect { close, response })
            .await
            .map_err(|_| ClientError::DriverStopped)?;
        receiver.await.map_err(|_| ClientError::DriverStopped)
    }

    pub async fn reconnect(&self) -> Result<(), ClientError> {
        self.disconnect().await?;
        self.connect().await
    }

    pub async fn ping(&self) -> Result<Duration, ClientError> {
        self.ping_with_timeout(self.options.call_timeout).await
    }

    pub async fn ping_with_timeout(&self, timeout: Duration) -> Result<Duration, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        let mut commands = self.commands.clone();
        commands
            .send(Command::Ping { id, response })
            .await
            .map_err(|_| ClientError::DriverStopped)?;
        let mut guard = RequestGuard {
            id,
            lifecycle: self.lifecycle.clone(),
            armed: true,
        };
        let response = receiver.fuse();
        let timeout_future = self.timer.sleep(timeout).fuse();
        futures::pin_mut!(response, timeout_future);
        futures::select! {
            response = response => {
                guard.armed = false;
                response.map_err(|_| ClientError::DriverStopped)?
            },
            () = timeout_future => Err(ClientError::Timeout {
                operation: ClientOperation::Ping,
                timeout,
            }),
        }
    }

    fn next_request_id(&self) -> RequestId {
        let mut id = self.request_ids.get().wrapping_add(1);
        if id == 0 {
            id = 1;
        }
        self.request_ids.set(id);
        id
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
    receiver: mpsc::Receiver<SocketEvent>,
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
    timeouts: OperationTimeouts,
    event_capacity: usize,
    request_ids: Rc<Cell<RequestId>>,
    topics: Rc<RefCell<HashSet<String>>>,
    events: mpsc::Receiver<ChannelEvent>,
    status: Rc<ObservableStatus<ChannelStatus>>,
}

#[derive(Clone, Copy)]
struct OperationTimeouts {
    join: Duration,
    call: Duration,
    leave: Duration,
}

impl Channel {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn status(&self) -> ChannelStatus {
        self.status.get()
    }

    pub fn status_changes(&self) -> ChannelStatusChanges {
        self.status.subscribe()
    }

    pub async fn join(&self) -> Result<Payload, ClientError> {
        self.join_with_timeout(self.timeouts.join).await
    }

    pub async fn join_with_timeout(&self, timeout: Duration) -> Result<Payload, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Join {
            id,
            topic: self.topic.clone(),
            timeout,
            response,
        })
        .await?;
        self.wait(id, ClientOperation::Join, timeout, receiver)
            .await
    }

    pub async fn call(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> Result<Reply, ClientError> {
        self.call_with_timeout(event, payload, self.timeouts.call)
            .await
    }

    pub async fn call_with_timeout(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
        timeout: Duration,
    ) -> Result<Reply, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Call {
            id,
            topic: self.topic.clone(),
            event: event.into(),
            payload: payload.into(),
            started: self.timer.now(),
            response,
        })
        .await?;
        self.wait(id, ClientOperation::Call, timeout, receiver)
            .await
    }

    pub async fn call_json<Request, Response>(
        &self,
        event: impl Into<String>,
        request: &Request,
    ) -> Result<Response, CallJsonError>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let payload = serde_json::to_value(request).map_err(CallJsonError::Encode)?;
        self.call(event, payload)
            .await?
            .deserialize_ok()
            .map_err(Into::into)
    }

    /// Sends an event without asking Phoenix for a reply.
    pub async fn cast(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> Result<(), ClientError> {
        self.cast_with_timeout(event, payload, self.timeouts.call)
            .await
    }

    pub async fn cast_with_timeout(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
        timeout: Duration,
    ) -> Result<(), ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Cast {
            id,
            topic: self.topic.clone(),
            event: event.into(),
            payload: payload.into(),
            response,
        })
        .await?;
        self.wait(id, ClientOperation::Cast, timeout, receiver)
            .await
    }

    pub async fn leave(&self) -> Result<Payload, ClientError> {
        self.leave_with_timeout(self.timeouts.leave).await
    }

    pub async fn leave_with_timeout(&self, timeout: Duration) -> Result<Payload, ClientError> {
        let id = self.next_request_id();
        let (response, receiver) = oneshot::channel();
        self.send(Command::Leave {
            id,
            topic: self.topic.clone(),
            timeout,
            response,
        })
        .await?;
        self.wait(id, ClientOperation::Leave, timeout, receiver)
            .await
    }

    pub async fn next_event(&mut self) -> Option<ChannelEvent> {
        self.events.next().await
    }

    pub fn events(&self) -> Result<ChannelEvents, ClientError> {
        let (events, receiver) = mpsc::channel(self.event_capacity);
        let mut commands = self.commands.clone();
        commands
            .try_send(Command::SubscribeChannel {
                topic: self.topic.clone(),
                events,
            })
            .map_err(command_send_error)?;
        Ok(ChannelEvents { receiver })
    }

    pub fn subscribe(&self, event: impl Into<String>) -> Result<EventSubscription, ClientError> {
        Ok(EventSubscription {
            event: event.into(),
            events: self.events()?,
        })
    }

    pub fn presence(&self) -> Result<ChannelPresence<'_>, ClientError> {
        ChannelPresence::new(self)
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
        operation: ClientOperation,
        timeout_duration: Duration,
        receiver: oneshot::Receiver<Result<T, ClientError>>,
    ) -> Result<T, ClientError> {
        let mut guard = RequestGuard {
            id,
            lifecycle: self.lifecycle.clone(),
            armed: true,
        };
        let response = receiver.fuse();
        let timeout = self.timer.sleep(timeout_duration).fuse();
        futures::pin_mut!(response, timeout);
        futures::select! {
            response = response => {
                guard.armed = false;
                response.map_err(|_| ClientError::DriverStopped)?
            },
            () = timeout => Err(ClientError::Timeout {
                operation,
                timeout: timeout_duration,
            }),
        }
    }
}

pub struct ChannelEvents {
    receiver: mpsc::Receiver<ChannelEvent>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SubscriptionEvent {
    Message(Payload),
    Disconnected,
    ChannelError(Payload),
    ChannelClosed(Payload),
    Lagged { dropped: u64 },
}

pub struct EventSubscription {
    event: String,
    events: ChannelEvents,
}

impl EventSubscription {
    pub fn event(&self) -> &str {
        &self.event
    }

    pub async fn next(&mut self) -> Option<SubscriptionEvent> {
        loop {
            match self.events.next().await? {
                ChannelEvent::Protocol(ProtocolEvent::Message(frame))
                    if frame.event == self.event =>
                {
                    return Some(SubscriptionEvent::Message(frame.payload));
                }
                ChannelEvent::Protocol(ProtocolEvent::ChannelError { payload, .. }) => {
                    return Some(SubscriptionEvent::ChannelError(payload));
                }
                ChannelEvent::Protocol(ProtocolEvent::ChannelClosed { payload, .. }) => {
                    return Some(SubscriptionEvent::ChannelClosed(payload));
                }
                ChannelEvent::Disconnected => return Some(SubscriptionEvent::Disconnected),
                ChannelEvent::Lagged { dropped } => {
                    return Some(SubscriptionEvent::Lagged { dropped });
                }
                ChannelEvent::Protocol(_) | ChannelEvent::JoinPayloadError(_) => {}
            }
        }
    }
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
        events: mpsc::Sender<ChannelEvent>,
        status: Rc<ObservableStatus<ChannelStatus>>,
    },
    Unregister {
        topic: String,
    },
    Cancel {
        id: RequestId,
    },
}

enum Command {
    Connect {
        response: oneshot::Sender<()>,
    },
    Disconnect {
        close: Option<TransportCloseRequest>,
        response: oneshot::Sender<()>,
    },
    Subscribe {
        events: mpsc::Sender<SocketEvent>,
    },
    SubscribeChannel {
        topic: String,
        events: mpsc::Sender<ChannelEvent>,
    },
    Ping {
        id: RequestId,
        response: oneshot::Sender<Result<Duration, ClientError>>,
    },
    Join {
        id: RequestId,
        topic: String,
        timeout: Duration,
        response: oneshot::Sender<Result<Payload, ClientError>>,
    },
    Call {
        id: RequestId,
        topic: String,
        event: String,
        payload: Payload,
        started: Duration,
        response: oneshot::Sender<Result<Reply, ClientError>>,
    },
    Cast {
        id: RequestId,
        topic: String,
        event: String,
        payload: Payload,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Leave {
        id: RequestId,
        topic: String,
        timeout: Duration,
        response: oneshot::Sender<Result<Payload, ClientError>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

enum QueuedPush {
    Call {
        id: RequestId,
        event: String,
        payload: Payload,
        started: Duration,
        response: oneshot::Sender<Result<Reply, ClientError>>,
    },
    Cast {
        id: RequestId,
        event: String,
        payload: Payload,
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
        let operation = match &self {
            Self::Call { .. } => ClientOperation::Call,
            Self::Cast { .. } => ClientOperation::Cast,
        };
        self.fail(ClientError::Interrupted { operation });
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
    subscribers: Vec<EventSubscriber<ChannelEvent>>,
    status: Rc<ObservableStatus<ChannelStatus>>,
    desired: bool,
    ever_joined: bool,
    active_payload: Option<u64>,
    join_attempt: u32,
    join_timeout: Duration,
    rejoin_scheduled: bool,
    join_waiters: HashMap<RequestId, oneshot::Sender<Result<Payload, ClientError>>>,
    queued: VecDeque<QueuedPush>,
    deferred_leave: Option<PendingLeave>,
}

struct EventSubscriber<T> {
    sender: mpsc::Sender<T>,
    dropped: u64,
}

struct PendingCall {
    id: RequestId,
    topic: String,
    event: String,
    started: Duration,
    response: oneshot::Sender<Result<Reply, ClientError>>,
}

struct PendingPing {
    id: RequestId,
    started: Duration,
    response: oneshot::Sender<Result<Duration, ClientError>>,
}

struct PendingLeave {
    id: RequestId,
    timeout: Duration,
    response: Option<oneshot::Sender<Result<Payload, ClientError>>>,
}

type PayloadResult = (String, u64, Result<Value, String>);

enum OperationTimeout {
    Join {
        topic: String,
        reference: String,
        duration: Duration,
    },
    Leave {
        topic: String,
        reference: String,
        duration: Duration,
    },
}

struct DriverState {
    connector: Rc<dyn Connector>,
    timer: Rc<dyn Timer>,
    options: Options,
    codec: Rc<dyn Codec>,
    commands: mpsc::Receiver<Command>,
    lifecycle: mpsc::UnboundedReceiver<LifecycleCommand>,
    protocol: Protocol,
    channels: HashMap<String, ChannelRecord>,
    socket_subscribers: Vec<EventSubscriber<SocketEvent>>,
    pending_joins: HashMap<String, String>,
    pending_calls: HashMap<String, PendingCall>,
    pending_pings: HashMap<String, PendingPing>,
    pending_leaves: HashMap<String, PendingLeave>,
    next_payload_id: u64,
    socket_status: Rc<ObservableStatus<SocketStatus>>,
}

impl DriverState {
    fn new(
        connector: Rc<dyn Connector>,
        timer: Rc<dyn Timer>,
        options: Options,
        codec: Rc<dyn Codec>,
        commands: mpsc::Receiver<Command>,
        lifecycle: mpsc::UnboundedReceiver<LifecycleCommand>,
        socket_status: Rc<ObservableStatus<SocketStatus>>,
    ) -> Self {
        Self {
            connector,
            timer,
            options,
            codec,
            commands,
            lifecycle,
            protocol: Protocol::new(),
            channels: HashMap::new(),
            socket_subscribers: Vec::new(),
            pending_joins: HashMap::new(),
            pending_calls: HashMap::new(),
            pending_pings: HashMap::new(),
            pending_leaves: HashMap::new(),
            next_payload_id: 0,
            socket_status,
        }
    }

    async fn run(mut self) {
        let mut connection_enabled = self.options.connect_on_start;
        let mut attempt = 0;
        loop {
            if !connection_enabled {
                match self.wait_idle().await {
                    IdleExit::Connect(response) => {
                        let _ = response.send(());
                        connection_enabled = true;
                        attempt = 0;
                    }
                    IdleExit::Shutdown(response) => {
                        let _ = response.send(());
                        self.emit_socket(SocketEvent::Closed);
                        return;
                    }
                }
            }

            self.emit_socket(SocketEvent::Connecting { attempt });
            let connection_started = self.timer.now();
            let connection = self.connector.connect(ConnectContext { attempt }).fuse();
            let connect_timeout = self.timer.sleep(self.options.connect_timeout).fuse();
            futures::pin_mut!(connection, connect_timeout);
            let connected = loop {
                let lifecycle = self.lifecycle.next().fuse();
                let command = self.commands.next().fuse();
                futures::pin_mut!(lifecycle, command);
                futures::select_biased! {
                    lifecycle = lifecycle => match lifecycle {
                        Some(command) => self.handle_offline_lifecycle(command),
                        None => break None,
                    },
                    result = connection => break Some(ConnectAttemptExit::Connected(result)),
                    () = connect_timeout => break Some(ConnectAttemptExit::Connected(Err(TransportError::with_kind(
                        TransportErrorKind::Connect,
                        format!("connection attempt timed out after {:?}", self.options.connect_timeout),
                    )))),
                    command = command => match command {
                        Some(Command::Connect { response }) => {
                            let _ = response.send(());
                        }
                        Some(Command::Disconnect { response, .. }) => {
                            break Some(ConnectAttemptExit::Disconnect(response));
                        }
                        Some(Command::Shutdown { response }) => {
                            break Some(ConnectAttemptExit::Shutdown(response));
                        }
                        Some(command) => {
                            self.handle_offline_command(command);
                        }
                        None => break None,
                    }
                }
            };
            let Some(connected) = connected else {
                self.emit_socket(SocketEvent::Closed);
                return;
            };
            self.telemetry(TelemetryEvent::ConnectionAttemptFinished {
                attempt,
                duration: self.timer.now().saturating_sub(connection_started),
                connected: matches!(&connected, ConnectAttemptExit::Connected(Ok(_))),
            });

            let retry_reason = match connected {
                ConnectAttemptExit::Connected(Ok(mut transport)) => {
                    attempt = 0;
                    self.emit_socket(SocketEvent::Connected);
                    match self.run_connected(&mut transport).await {
                        ConnectedExit::Shutdown(response) => {
                            let _ = transport.close().await;
                            let _ = response.send(());
                            self.emit_socket(SocketEvent::Closed);
                            return;
                        }
                        ConnectedExit::Disconnect { response, close } => {
                            if let Some(close) = close {
                                let _ = transport.close_with(close).await;
                            } else {
                                let _ = transport.close().await;
                            }
                            self.on_disconnect(&DisconnectReason::Requested);
                            let _ = response.send(());
                            connection_enabled = false;
                            continue;
                        }
                        ConnectedExit::Disconnected(reason) => {
                            let _ = transport.close().await;
                            self.on_disconnect(&reason);
                            reason
                        }
                    }
                }
                ConnectAttemptExit::Connected(Err(error)) => {
                    let reason = DisconnectReason::Connect(error);
                    self.emit_socket(SocketEvent::Disconnected {
                        reason: reason.clone(),
                    });
                    reason
                }
                ConnectAttemptExit::Disconnect(response) => {
                    self.on_disconnect(&DisconnectReason::Requested);
                    let _ = response.send(());
                    connection_enabled = false;
                    continue;
                }
                ConnectAttemptExit::Shutdown(response) => {
                    let _ = response.send(());
                    self.emit_socket(SocketEvent::Closed);
                    return;
                }
            };

            attempt = attempt.saturating_add(1);
            let action = self.options.reconnect_policy.as_ref().map_or_else(
                || {
                    if retry_reason.should_reconnect() {
                        ReconnectAction::RetryAfter((self.options.reconnect_delay)(attempt))
                    } else {
                        ReconnectAction::Stop
                    }
                },
                |policy| {
                    policy(ReconnectContext {
                        attempt,
                        reason: retry_reason.clone(),
                    })
                },
            );
            let ReconnectAction::RetryAfter(delay) = action else {
                self.emit_socket(SocketEvent::ReconnectStopped {
                    attempt,
                    reason: retry_reason,
                });
                connection_enabled = false;
                continue;
            };
            self.emit_socket(SocketEvent::ReconnectScheduled { attempt, delay });
            match self.wait_offline(delay).await {
                OfflineExit::Retry => {}
                OfflineExit::Disconnect(response) => {
                    self.on_disconnect(&DisconnectReason::Requested);
                    let _ = response.send(());
                    connection_enabled = false;
                }
                OfflineExit::Shutdown(response) => {
                    let _ = response.send(());
                    self.emit_socket(SocketEvent::Closed);
                    return;
                }
            }
        }
    }

    async fn wait_idle(&mut self) -> IdleExit {
        loop {
            let lifecycle = self.lifecycle.next().fuse();
            let command = self.commands.next().fuse();
            futures::pin_mut!(lifecycle, command);
            futures::select_biased! {
                lifecycle = lifecycle => match lifecycle {
                    Some(command) => self.handle_offline_lifecycle(command),
                    None => return IdleExit::Shutdown(closed_response()),
                },
                command = command => match command {
                    Some(Command::Connect { response }) => return IdleExit::Connect(response),
                    Some(Command::Disconnect { response, .. }) => {
                        let _ = response.send(());
                    }
                    Some(Command::Shutdown { response }) => return IdleExit::Shutdown(response),
                    Some(command) => {
                        self.handle_offline_command(command);
                    }
                    None => return IdleExit::Shutdown(closed_response()),
                }
            }
        }
    }

    async fn wait_offline(&mut self, delay: Duration) -> OfflineExit {
        let sleep = self.timer.sleep(delay).fuse();
        futures::pin_mut!(sleep);
        loop {
            let lifecycle = self.lifecycle.next().fuse();
            let command = self.commands.next().fuse();
            futures::pin_mut!(lifecycle, command);
            futures::select_biased! {
                lifecycle = lifecycle => match lifecycle {
                    Some(command) => self.handle_offline_lifecycle(command),
                    None => return OfflineExit::Shutdown(closed_response()),
                },
                () = sleep => return OfflineExit::Retry,
                command = command => match command {
                    Some(Command::Connect { response }) => {
                        let _ = response.send(());
                    }
                    Some(Command::Disconnect { response, .. }) => {
                        return OfflineExit::Disconnect(response);
                    }
                    Some(Command::Shutdown { response }) => return OfflineExit::Shutdown(response),
                    Some(command) => {
                        self.handle_offline_command(command);
                    }
                    None => return OfflineExit::Shutdown(closed_response()),
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
                Incoming(Result<TransportEvent, TransportError>),
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
                    return ConnectedExit::Disconnected(DisconnectReason::DriverStopped);
                }
                Action::Command(Some(Command::Shutdown { response })) => {
                    return ConnectedExit::Shutdown(response);
                }
                Action::Command(Some(Command::Disconnect { close, response })) => {
                    return ConnectedExit::Disconnect { response, close };
                }
                Action::Command(Some(Command::Connect { response })) => {
                    let _ = response.send(());
                    Ok(())
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
                    return ConnectedExit::Disconnected(DisconnectReason::DriverStopped);
                }
                Action::Incoming(Ok(TransportEvent::Message(message))) => {
                    let awaiting_heartbeat = heartbeat_reference.is_some();
                    let result = self
                        .handle_incoming(
                            message,
                            transport,
                            &mut payloads,
                            &mut rejoins,
                            &mut operation_timeouts,
                            &mut heartbeat_reference,
                        )
                        .await;
                    if result.is_ok() && awaiting_heartbeat && heartbeat_reference.is_none() {
                        heartbeat = self.timer.sleep(self.options.heartbeat_interval);
                    }
                    result
                }
                Action::Incoming(Ok(TransportEvent::Closed(close))) => {
                    Err(DisconnectReason::Closed(close))
                }
                Action::Incoming(Err(error)) => Err(DisconnectReason::Transport(error)),
                Action::Heartbeat => {
                    if heartbeat_reference.is_some() {
                        Err(DisconnectReason::HeartbeatTimeout)
                    } else {
                        let outbound = self.protocol.heartbeat();
                        heartbeat_reference = Some(outbound.reference);
                        let result = self.send_frame(transport, outbound.frame).await;
                        if result.is_ok() {
                            heartbeat = self.timer.sleep(self.options.heartbeat_timeout);
                        }
                        result
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
            Command::Connect { .. } | Command::Disconnect { .. } => {
                unreachable!("connection controls are handled by the driver loop")
            }
            Command::Subscribe { events } => self.socket_subscribers.push(EventSubscriber {
                sender: events,
                dropped: 0,
            }),
            Command::SubscribeChannel { topic, events } => self.subscribe_channel(&topic, events),
            Command::Ping { response, .. } => {
                let _ = response.send(Err(ClientError::SocketNotConnected));
            }
            Command::Join {
                id,
                topic,
                timeout,
                response,
            } => {
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.desired = true;
                    channel.join_timeout = channel.join_timeout.max(timeout);
                    channel.status.set(ChannelStatus::WaitingForSocket);
                    channel.join_waiters.insert(id, response);
                } else {
                    let _ = response.send(Err(ClientError::UnknownTopic(topic)));
                }
            }
            Command::Call {
                id,
                topic,
                event,
                payload,
                started,
                response,
            } => self.queue(
                &topic,
                QueuedPush::Call {
                    id,
                    event,
                    payload,
                    started,
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
                let _ = response.send(Ok(json!({}).into()));
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
    ) -> Result<(), DisconnectReason> {
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
    ) -> Result<(), DisconnectReason> {
        match command {
            Command::Subscribe { events } => self.socket_subscribers.push(EventSubscriber {
                sender: events,
                dropped: 0,
            }),
            Command::SubscribeChannel { topic, events } => self.subscribe_channel(&topic, events),
            Command::Ping { id, response } => {
                let outbound = self.protocol.heartbeat();
                self.pending_pings.insert(
                    outbound.reference.clone(),
                    PendingPing {
                        id,
                        started: self.timer.now(),
                        response,
                    },
                );
                self.send_frame(transport, outbound.frame).await?;
            }
            Command::Join {
                id,
                topic,
                timeout,
                response,
            } => {
                let joined = self.protocol.channel_state(&topic) == Some(ChannelState::Joined);
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.desired = true;
                    if joined {
                        channel.status.set(ChannelStatus::Joined);
                        let _ = response.send(Err(ClientError::AlreadyJoined(topic)));
                    } else {
                        channel.status.set(ChannelStatus::WaitingToJoin);
                        channel.join_timeout = channel.join_timeout.max(timeout);
                        channel.join_waiters.insert(id, response);
                        self.load_payload(&topic, payloads);
                    }
                } else {
                    let _ = response.send(Err(ClientError::UnknownTopic(topic)));
                }
            }
            Command::Call {
                id,
                topic,
                event,
                payload,
                started,
                response,
            } => {
                let push = QueuedPush::Call {
                    id,
                    event,
                    payload,
                    started,
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
                timeout,
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
                        .map_err(|error| DisconnectReason::Protocol(error.to_string()))?;
                    self.pending_leaves.insert(
                        outbound.reference.clone(),
                        PendingLeave {
                            id,
                            timeout,
                            response: Some(response),
                        },
                    );
                    self.schedule_operation_timeout(
                        OperationTimeout::Leave {
                            topic,
                            reference: outbound.reference.clone(),
                            duration: timeout,
                        },
                        operation_timeouts,
                    );
                    self.send_frame(transport, outbound.frame).await?;
                } else if state == Some(ChannelState::Joining) {
                    if let Some(channel) = self.channels.get_mut(&topic) {
                        channel.deferred_leave = Some(PendingLeave {
                            id,
                            timeout,
                            response: Some(response),
                        });
                    } else {
                        let _ = response.send(Ok(json!({}).into()));
                    }
                } else {
                    let _ = response.send(Ok(json!({}).into()));
                }
            }
            Command::Shutdown { .. } => unreachable!("handled by run_connected"),
            Command::Connect { .. } | Command::Disconnect { .. } => {
                unreachable!("connection controls are handled by run_connected")
            }
        }
        Ok(())
    }

    fn register(
        &mut self,
        topic: String,
        payload_loader: JoinPayloadLoader,
        events: mpsc::Sender<ChannelEvent>,
        status: Rc<ObservableStatus<ChannelStatus>>,
    ) {
        self.channels.entry(topic).or_insert(ChannelRecord {
            payload_loader,
            subscribers: vec![EventSubscriber {
                sender: events,
                dropped: 0,
            }],
            status,
            desired: false,
            ever_joined: false,
            active_payload: None,
            join_attempt: 0,
            join_timeout: self.options.join_timeout,
            rejoin_scheduled: false,
            join_waiters: HashMap::new(),
            queued: VecDeque::new(),
            deferred_leave: None,
        });
    }

    fn subscribe_channel(&mut self, topic: &str, events: mpsc::Sender<ChannelEvent>) {
        if let Some(channel) = self.channels.get_mut(topic) {
            channel.subscribers.push(EventSubscriber {
                sender: events,
                dropped: 0,
            });
        }
    }

    fn unregister(&mut self, topic: &str) {
        self.stop_channel(topic);
        self.channels.remove(topic);
        self.protocol.discard_channel(topic);
    }

    fn queue(&mut self, topic: &str, push: QueuedPush) {
        if let Some(channel) = self.channels.get_mut(topic) {
            if channel.ever_joined && !channel.desired {
                push.fail(ClientError::ChannelNotJoined(topic.to_owned()));
                return;
            }
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
    ) -> Result<(), DisconnectReason> {
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
                    let _ = waiter.send(Err(ClientError::JoinPayload {
                        topic: topic.clone(),
                        message: error.clone(),
                    }));
                }
                self.emit_channel(&topic, ChannelEvent::JoinPayloadError(error));
                return Ok(());
            }
        };
        let join_timeout = channel.join_timeout;
        let outbound = match self.protocol.channel_state(&topic) {
            Some(ChannelState::Disconnected | ChannelState::Errored | ChannelState::Closed) => {
                self.protocol.rejoin(&topic, payload)
            }
            None => self.protocol.join(&topic, payload),
            Some(_) => return Ok(()),
        }
        .map_err(|error| DisconnectReason::Protocol(error.to_string()))?;
        if let Some(channel) = self.channels.get(&topic) {
            channel.status.set(ChannelStatus::Joining);
        }
        self.pending_joins
            .insert(outbound.reference.clone(), topic.clone());
        self.schedule_operation_timeout(
            OperationTimeout::Join {
                topic,
                reference: outbound.reference.clone(),
                duration: join_timeout,
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
    ) -> Result<(), DisconnectReason> {
        let (binary, bytes) = wire_metadata(&message);
        let frame = self
            .codec
            .decode(message)
            .map_err(|error| DisconnectReason::Protocol(error.to_string()))?;
        self.telemetry(TelemetryEvent::FrameReceived {
            topic: frame.topic.clone(),
            event: frame.event.clone(),
            binary,
            bytes,
        });
        let event = self
            .protocol
            .receive(frame)
            .map_err(|error| DisconnectReason::Protocol(error.to_string()))?;

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
                    let timeout = pending.timeout;
                    let outbound = self
                        .protocol
                        .leave(topic)
                        .map_err(|error| DisconnectReason::Protocol(error.to_string()))?;
                    self.pending_leaves
                        .insert(outbound.reference.clone(), pending);
                    self.schedule_operation_timeout(
                        OperationTimeout::Leave {
                            topic: topic.clone(),
                            reference: outbound.reference.clone(),
                            duration: timeout,
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
                            let _ = response.send(Ok(json!({}).into()));
                        }
                        channel.status.set(ChannelStatus::Left);
                    }
                    for (_, waiter) in channel.join_waiters.drain() {
                        let _ = waiter.send(Err(ClientError::JoinRejected {
                            topic: topic.clone(),
                            response: response.clone(),
                        }));
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
                event: _,
            } => {
                if let Some(pending) = self.pending_calls.remove(reference) {
                    self.telemetry(TelemetryEvent::CallCompleted {
                        topic: pending.topic.clone(),
                        event: pending.event.clone(),
                        outcome: CallOutcome::Reply(*status),
                        duration: self.timer.now().saturating_sub(pending.started),
                    });
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
                } else if let Some(ping) = self.pending_pings.remove(reference) {
                    let elapsed = self.timer.now().saturating_sub(ping.started);
                    let _ = ping.response.send(Ok(elapsed));
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
        let timeout = match operation {
            OperationTimeout::Join { duration, .. } | OperationTimeout::Leave { duration, .. } => {
                duration
            }
        };
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
    ) -> Result<(), DisconnectReason> {
        match timeout {
            OperationTimeout::Join {
                topic,
                reference,
                duration,
            } => {
                if self.pending_joins.remove(&reference).as_deref() != Some(topic.as_str()) {
                    return Ok(());
                }
                self.protocol.discard_channel(&topic);
                let mut should_rejoin = false;
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.active_payload = None;
                    if let Some(pending) = channel.deferred_leave.take() {
                        if let Some(response) = pending.response {
                            let _ = response.send(Err(ClientError::Timeout {
                                operation: ClientOperation::Leave,
                                timeout: pending.timeout,
                            }));
                        }
                        channel.status.set(ChannelStatus::Left);
                    } else if channel.desired {
                        channel.status.set(ChannelStatus::Errored);
                        for (_, waiter) in channel.join_waiters.drain() {
                            let _ = waiter.send(Err(ClientError::Timeout {
                                operation: ClientOperation::Join,
                                timeout: duration,
                            }));
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
            OperationTimeout::Leave {
                topic,
                reference,
                duration,
            } => {
                let Some(pending) = self.pending_leaves.remove(&reference) else {
                    return Ok(());
                };
                if let Some(response) = pending.response {
                    let _ = response.send(Err(ClientError::Timeout {
                        operation: ClientOperation::Leave,
                        timeout: duration,
                    }));
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
    ) -> Result<(), DisconnectReason> {
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
    ) -> Result<(), DisconnectReason> {
        match push {
            QueuedPush::Call {
                id,
                event,
                payload,
                started,
                response,
            } => {
                if matches!(&payload, Payload::Binary(_)) && !transport.supports_binary() {
                    self.emit_call_completed(
                        topic.to_owned(),
                        event,
                        started,
                        CallOutcome::Rejected,
                    );
                    let _ = response.send(Err(ClientError::BinaryNotSupported));
                    return Ok(());
                }
                let outbound = self
                    .protocol
                    .push(topic, event, payload)
                    .map_err(|error| DisconnectReason::Protocol(error.to_string()))?;
                self.pending_calls.insert(
                    outbound.reference.clone(),
                    PendingCall {
                        id,
                        topic: topic.to_owned(),
                        event: outbound.frame.event.clone(),
                        started,
                        response,
                    },
                );
                self.send_frame(transport, outbound.frame).await
            }
            QueuedPush::Cast {
                event,
                payload,
                response,
                ..
            } => {
                if matches!(&payload, Payload::Binary(_)) && !transport.supports_binary() {
                    let _ = response.send(Err(ClientError::BinaryNotSupported));
                    return Ok(());
                }
                let frame = self
                    .protocol
                    .cast(topic, event, payload)
                    .map_err(|error| DisconnectReason::Protocol(error.to_string()))?;
                match self.send_frame(transport, frame).await {
                    Ok(()) => {
                        let _ = response.send(Ok(()));
                        Ok(())
                    }
                    Err(error) => {
                        let _ = response.send(Err(ClientError::Interrupted {
                            operation: ClientOperation::Cast,
                        }));
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
    ) -> Result<(), DisconnectReason> {
        let topic = frame.topic.clone();
        let event = frame.event.clone();
        let message = self
            .codec
            .encode(&frame)
            .map_err(|error| DisconnectReason::Protocol(error.to_string()))?;
        let (binary, bytes) = wire_metadata(&message);
        self.telemetry(TelemetryEvent::FrameSent {
            topic,
            event,
            binary,
            bytes,
        });
        transport
            .send(message)
            .await
            .map_err(DisconnectReason::Transport)
    }

    fn cancel(&mut self, id: RequestId) {
        let cancelled_join = self.channels.iter_mut().find_map(|(topic, channel)| {
            channel.join_waiters.remove(&id).map(|_| {
                (
                    topic.clone(),
                    channel.join_waiters.is_empty() && !channel.ever_joined,
                )
            })
        });
        if let Some((topic, stop_join)) = cancelled_join {
            if stop_join {
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.desired = false;
                    channel.active_payload = None;
                    channel.status.set(ChannelStatus::Left);
                }
                self.pending_joins
                    .retain(|_, pending_topic| pending_topic != &topic);
                self.protocol.discard_channel(&topic);
            }
            return;
        }
        let mut removed_queued = false;
        let mut cancelled_call = None;
        for (topic, channel) in &mut self.channels {
            if let Some(pending) = channel
                .deferred_leave
                .as_mut()
                .filter(|pending| pending.id == id)
            {
                pending.response.take();
                return;
            }
            if let Some(index) = channel.queued.iter().position(|push| push.id() == id) {
                if let Some(QueuedPush::Call { event, started, .. }) = channel.queued.remove(index)
                {
                    cancelled_call = Some((topic.clone(), event, started));
                }
                removed_queued = true;
                break;
            }
        }
        if let Some((topic, event, started)) = cancelled_call {
            self.emit_call_completed(topic, event, started, CallOutcome::Cancelled);
        }
        if removed_queued {
            return;
        }
        let reference = self
            .pending_calls
            .iter()
            .find_map(|(reference, pending)| (pending.id == id).then(|| reference.clone()));
        if let Some(reference) = reference {
            if let Some(pending) = self.pending_calls.remove(&reference) {
                self.emit_call_completed(
                    pending.topic,
                    pending.event,
                    pending.started,
                    CallOutcome::Cancelled,
                );
            }
            self.protocol.forget_push(&reference);
        }
        let reference = self
            .pending_pings
            .iter()
            .find_map(|(reference, pending)| (pending.id == id).then(|| reference.clone()));
        if let Some(reference) = reference {
            self.pending_pings.remove(&reference);
            self.protocol.forget_heartbeat(&reference);
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
        let mut interrupted_calls = Vec::new();
        if let Some(channel) = self.channels.get_mut(topic) {
            channel.desired = false;
            channel.active_payload = None;
            channel.rejoin_scheduled = false;
            if let Some(pending) = channel.deferred_leave.take() {
                if let Some(response) = pending.response {
                    let _ = response.send(Err(ClientError::Interrupted {
                        operation: ClientOperation::Leave,
                    }));
                }
            }
            for (_, waiter) in channel.join_waiters.drain() {
                let _ = waiter.send(Err(ClientError::Interrupted {
                    operation: ClientOperation::Join,
                }));
            }
            for push in channel.queued.drain(..) {
                match push {
                    QueuedPush::Call {
                        event,
                        started,
                        response,
                        ..
                    } => {
                        interrupted_calls.push((event, started));
                        let _ = response.send(Err(ClientError::Interrupted {
                            operation: ClientOperation::Call,
                        }));
                    }
                    push @ QueuedPush::Cast { .. } => push.interrupt(),
                }
            }
        }
        for (event, started) in interrupted_calls {
            self.emit_call_completed(topic.to_owned(), event, started, CallOutcome::Interrupted);
        }
    }

    fn on_disconnect(&mut self, reason: &DisconnectReason) {
        let events = self.protocol.reset_connection();
        self.pending_joins.clear();
        let interrupted_calls = self
            .pending_calls
            .drain()
            .map(|(_, pending)| pending)
            .collect::<Vec<_>>();
        for pending in interrupted_calls {
            self.emit_call_completed(
                pending.topic,
                pending.event,
                pending.started,
                CallOutcome::Interrupted,
            );
            let _ = pending.response.send(Err(ClientError::Interrupted {
                operation: ClientOperation::Call,
            }));
        }
        for (_, pending) in self.pending_pings.drain() {
            let _ = pending.response.send(Err(ClientError::Interrupted {
                operation: ClientOperation::Ping,
            }));
        }
        for (_, pending) in self.pending_leaves.drain() {
            if let Some(response) = pending.response {
                let _ = response.send(Err(ClientError::Interrupted {
                    operation: ClientOperation::Leave,
                }));
            }
        }
        let topics = self.channels.keys().cloned().collect::<Vec<_>>();
        for channel in self.channels.values_mut() {
            channel.active_payload = None;
            channel.rejoin_scheduled = false;
            if let Some(pending) = channel.deferred_leave.take() {
                if let Some(response) = pending.response {
                    let _ = response.send(Err(ClientError::Interrupted {
                        operation: ClientOperation::Leave,
                    }));
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
            reason: reason.clone(),
        });
    }

    fn emit_socket(&mut self, event: SocketEvent) {
        self.telemetry(TelemetryEvent::Socket(event.clone()));
        let status = match &event {
            SocketEvent::Connecting { .. } => SocketStatus::Connecting,
            SocketEvent::Connected => SocketStatus::Connected,
            SocketEvent::Disconnected { reason } if !reason.should_reconnect() => {
                SocketStatus::Disconnected
            }
            SocketEvent::ReconnectStopped { .. } => SocketStatus::Disconnected,
            SocketEvent::Disconnected { .. } | SocketEvent::ReconnectScheduled { .. } => {
                SocketStatus::WaitingToReconnect
            }
            SocketEvent::Closed => SocketStatus::Closed,
            SocketEvent::Lagged { .. } => self.socket_status.get(),
        };
        self.socket_status.set(status);
        if status == SocketStatus::Closed {
            for channel in self.channels.values() {
                channel.status.set(ChannelStatus::Closed);
            }
        }
        self.socket_subscribers
            .retain_mut(|subscriber| send_bounded(subscriber, event.clone()));
    }

    fn emit_channel(&mut self, topic: &str, event: ChannelEvent) {
        self.telemetry(TelemetryEvent::Channel {
            topic: topic.to_owned(),
            event: event.clone(),
        });
        if let Some(channel) = self.channels.get_mut(topic) {
            channel
                .subscribers
                .retain_mut(|subscriber| send_bounded(subscriber, event.clone()));
        }
    }

    fn telemetry(&self, event: TelemetryEvent) {
        if let Some(hook) = &self.options.telemetry {
            hook(&event);
        }
    }

    fn emit_call_completed(
        &self,
        topic: String,
        event: String,
        started: Duration,
        outcome: CallOutcome,
    ) {
        self.telemetry(TelemetryEvent::CallCompleted {
            topic,
            event,
            outcome,
            duration: self.timer.now().saturating_sub(started),
        });
    }
}

fn wire_metadata(message: &WireMessage) -> (bool, usize) {
    match message {
        WireMessage::Text(value) => (false, value.len()),
        WireMessage::Binary(value) => (true, value.len()),
    }
}

trait LaggedEvent {
    fn lagged(dropped: u64) -> Self;
}

impl LaggedEvent for SocketEvent {
    fn lagged(dropped: u64) -> Self {
        Self::Lagged { dropped }
    }
}

impl LaggedEvent for ChannelEvent {
    fn lagged(dropped: u64) -> Self {
        Self::Lagged { dropped }
    }
}

fn send_bounded<T: Clone + LaggedEvent>(subscriber: &mut EventSubscriber<T>, event: T) -> bool {
    if subscriber.dropped > 0 {
        match subscriber.sender.try_send(T::lagged(subscriber.dropped)) {
            Ok(()) => subscriber.dropped = 0,
            Err(error) if error.is_full() => {
                subscriber.dropped = subscriber.dropped.saturating_add(1);
                return true;
            }
            Err(_) => return false,
        }
    }
    match subscriber.sender.try_send(event) {
        Ok(()) => true,
        Err(error) if error.is_full() => {
            subscriber.dropped = 1;
            true
        }
        Err(_) => false,
    }
}

enum ConnectedExit {
    Shutdown(oneshot::Sender<()>),
    Disconnect {
        response: oneshot::Sender<()>,
        close: Option<TransportCloseRequest>,
    },
    Disconnected(DisconnectReason),
}

enum ConnectAttemptExit {
    Connected(Result<Box<dyn Transport>, TransportError>),
    Disconnect(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

enum OfflineExit {
    Retry,
    Disconnect(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

enum IdleExit {
    Connect(oneshot::Sender<()>),
    Shutdown(oneshot::Sender<()>),
}

fn closed_response() -> oneshot::Sender<()> {
    let (response, receiver) = oneshot::channel();
    drop(receiver);
    response
}

#[cfg(test)]
mod tests;
