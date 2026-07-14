use std::{
    any::Any,
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use futures::{FutureExt, future::BoxFuture};
use phoenix_channel_client::{
    Channel, ChannelEvent, ChannelStatus, ClientError, ConnectContext, ConnectionConfig, Endpoint,
    EndpointError, JoinContext, Options, PresenceEvent, ReconnectAction, ReconnectContext, Reply,
    Socket, SocketEvent, SocketStatus, SubscriptionEvent, TelemetryEvent,
};
use phoenix_channel_runtime::{
    Payload, PresenceError, PresenceState, PresenceTracker, PresenceUpdate, ProtocolEvent,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::{NativeConnector, NativeTimer, NativeTransportOptions};

pub type NativeConnectionConfigLoader = Arc<
    dyn Fn(ConnectContext) -> BoxFuture<'static, Result<ConnectionConfig, String>> + Send + Sync,
>;

pub type NativeJoinPayloadLoader =
    Arc<dyn Fn(JoinContext) -> BoxFuture<'static, Result<Value, String>> + Send + Sync>;

pub type NativeTelemetryHook = Arc<dyn Fn(&TelemetryEvent) + Send + Sync>;
pub type NativeReconnectPolicy = Arc<dyn Fn(ReconnectContext) -> ReconnectAction + Send + Sync>;

pub fn native_static_connection_config(config: ConnectionConfig) -> NativeConnectionConfigLoader {
    Arc::new(move |_| {
        let config = config.clone();
        async move { Ok(config) }.boxed()
    })
}

pub fn native_static_join_payload(payload: Value) -> NativeJoinPayloadLoader {
    Arc::new(move |_| {
        let payload = payload.clone();
        async move { Ok(payload) }.boxed()
    })
}

#[derive(Clone)]
pub struct NativeOptions {
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    connect_timeout: Duration,
    join_timeout: Duration,
    call_timeout: Duration,
    leave_timeout: Duration,
    reconnect_delays: Vec<Duration>,
    rejoin_delays: Vec<Duration>,
    command_capacity: usize,
    push_buffer_capacity: usize,
    event_capacity: usize,
    transport: NativeTransportOptions,
    telemetry: Option<NativeTelemetryHook>,
    reconnect_policy: Option<NativeReconnectPolicy>,
}

impl Default for NativeOptions {
    fn default() -> Self {
        let retry_delays = vec![
            Duration::ZERO,
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(5),
            Duration::from_secs(10),
        ];
        Self {
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            join_timeout: Duration::from_secs(10),
            call_timeout: Duration::from_secs(10),
            leave_timeout: Duration::from_secs(10),
            reconnect_delays: retry_delays.clone(),
            rejoin_delays: retry_delays,
            command_capacity: 64,
            push_buffer_capacity: 64,
            event_capacity: 256,
            transport: NativeTransportOptions::default(),
            telemetry: None,
            reconnect_policy: None,
        }
    }
}

impl NativeOptions {
    pub fn heartbeat_interval(mut self, value: Duration) -> Self {
        self.heartbeat_interval = value;
        self
    }

    pub fn heartbeat_timeout(mut self, value: Duration) -> Self {
        self.heartbeat_timeout = value;
        self
    }

    pub fn connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    pub fn request_timeout(mut self, value: Duration) -> Self {
        self.join_timeout = value;
        self.call_timeout = value;
        self.leave_timeout = value;
        self
    }

    pub fn join_timeout(mut self, value: Duration) -> Self {
        self.join_timeout = value;
        self
    }

    pub fn call_timeout(mut self, value: Duration) -> Self {
        self.call_timeout = value;
        self
    }

    pub fn leave_timeout(mut self, value: Duration) -> Self {
        self.leave_timeout = value;
        self
    }

    pub fn reconnect_delays(mut self, values: impl IntoIterator<Item = Duration>) -> Self {
        self.reconnect_delays = normalized_delays(values);
        self
    }

    pub fn rejoin_delays(mut self, values: impl IntoIterator<Item = Duration>) -> Self {
        self.rejoin_delays = normalized_delays(values);
        self
    }

    pub fn command_capacity(mut self, value: usize) -> Self {
        self.command_capacity = value.max(1);
        self
    }

    pub fn push_buffer_capacity(mut self, value: usize) -> Self {
        self.push_buffer_capacity = value;
        self
    }

    pub fn event_capacity(mut self, value: usize) -> Self {
        self.event_capacity = value.max(1);
        self
    }

    pub fn telemetry(mut self, hook: NativeTelemetryHook) -> Self {
        self.telemetry = Some(hook);
        self
    }

    pub fn reconnect_policy(mut self, policy: NativeReconnectPolicy) -> Self {
        self.reconnect_policy = Some(policy);
        self
    }

    pub fn transport(mut self, options: NativeTransportOptions) -> Self {
        self.transport = options;
        self
    }

    fn client_options(&self) -> Options {
        let reconnect_delays = self.reconnect_delays.clone();
        let rejoin_delays = self.rejoin_delays.clone();
        let mut options = Options::default()
            .connect_on_start(false)
            .heartbeat_interval(self.heartbeat_interval)
            .heartbeat_timeout(self.heartbeat_timeout)
            .connect_timeout(self.connect_timeout)
            .join_timeout(self.join_timeout)
            .call_timeout(self.call_timeout)
            .leave_timeout(self.leave_timeout)
            .event_capacity(self.event_capacity)
            .reconnect_delay(move |attempt| retry_delay(&reconnect_delays, attempt))
            .rejoin_delay(move |attempt| retry_delay(&rejoin_delays, attempt))
            .command_capacity(self.command_capacity)
            .push_buffer_capacity(self.push_buffer_capacity);
        if let Some(hook) = &self.telemetry {
            let hook = hook.clone();
            options = options.telemetry(Rc::new(move |event| hook(event)));
        }
        if let Some(policy) = &self.reconnect_policy {
            let policy = policy.clone();
            options = options.reconnect_policy(move |context| policy(context));
        }
        options
    }
}

fn normalized_delays(values: impl IntoIterator<Item = Duration>) -> Vec<Duration> {
    let values = values.into_iter().collect::<Vec<_>>();
    if values.is_empty() {
        vec![Duration::ZERO]
    } else {
        values
    }
}

fn retry_delay(delays: &[Duration], attempt: u32) -> Duration {
    delays
        .get(attempt as usize)
        .or_else(|| delays.last())
        .copied()
        .unwrap_or(Duration::ZERO)
}

#[derive(Debug, Error)]
pub enum NativeRuntimeError {
    #[error(transparent)]
    Endpoint(#[from] EndpointError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("failed to start native client worker: {0}")]
    WorkerStart(String),
    #[error("native client worker stopped")]
    WorkerStopped,
    #[error("native client worker failed: {0}")]
    WorkerFailed(String),
    #[error("failed to join native client worker: {0}")]
    WorkerJoin(String),
}

#[derive(Debug, Error)]
pub enum NativeCallJsonError {
    #[error(transparent)]
    Runtime(#[from] NativeRuntimeError),
    #[error("failed to encode request payload: {0}")]
    Encode(serde_json::Error),
    #[error(transparent)]
    Reply(#[from] phoenix_channel_client::ReplyError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeWorkerStatus {
    Running,
    Stopped,
    Failed(String),
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NativeEventError {
    #[error("event receiver lagged by {0} messages")]
    Lagged(u64),
    #[error("event stream closed")]
    Closed,
}

struct WorkerOwner {
    commands: mpsc::Sender<HostCommand>,
    control: mpsc::UnboundedSender<ControlCommand>,
    next_request: AtomicU64,
    worker: Mutex<Option<JoinHandle<()>>>,
    status: watch::Receiver<NativeWorkerStatus>,
}

impl WorkerOwner {
    fn next_request_id(&self) -> u64 {
        let id = self.next_request.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            self.next_request.store(2, Ordering::Relaxed);
            1
        } else {
            id
        }
    }

    fn join(&self) -> Result<(), NativeRuntimeError> {
        let Some(worker) = self
            .worker
            .lock()
            .map_err(|error| NativeRuntimeError::WorkerJoin(error.to_string()))?
            .take()
        else {
            return self.worker_result();
        };
        worker
            .join()
            .map_err(|panic| NativeRuntimeError::WorkerJoin(panic_message(panic.as_ref())))?;
        self.worker_result()
    }

    fn worker_result(&self) -> Result<(), NativeRuntimeError> {
        match self.status.borrow().clone() {
            NativeWorkerStatus::Failed(message) => Err(NativeRuntimeError::WorkerFailed(message)),
            NativeWorkerStatus::Running | NativeWorkerStatus::Stopped => Ok(()),
        }
    }
}

impl Drop for WorkerOwner {
    fn drop(&mut self) {
        let _ = self.control.send(ControlCommand::Shutdown);
        if let Ok(worker) = self.worker.get_mut() {
            if let Some(worker) = worker.take() {
                let _ = worker.join();
            }
        }
    }
}

#[derive(Clone)]
pub struct NativeSocket {
    owner: Arc<WorkerOwner>,
    events: broadcast::Sender<SocketEvent>,
    status: watch::Receiver<SocketStatus>,
}

impl NativeSocket {
    pub fn spawn(
        endpoint: impl Into<String>,
        config: ConnectionConfig,
    ) -> Result<Self, NativeRuntimeError> {
        Self::spawn_with_loader(
            endpoint,
            native_static_connection_config(config),
            NativeOptions::default(),
        )
    }

    pub fn spawn_with_options(
        endpoint: impl Into<String>,
        config: ConnectionConfig,
        options: NativeOptions,
    ) -> Result<Self, NativeRuntimeError> {
        Self::spawn_with_loader(endpoint, native_static_connection_config(config), options)
    }

    pub fn spawn_with_loader(
        endpoint: impl Into<String>,
        config_loader: NativeConnectionConfigLoader,
        options: NativeOptions,
    ) -> Result<Self, NativeRuntimeError> {
        let endpoint = endpoint.into();
        Endpoint::new(&endpoint)?;
        let (commands, command_rx) = mpsc::channel(options.command_capacity);
        let (control, control_rx) = mpsc::unbounded_channel();
        let worker_control = control.clone();
        let (events, _) = broadcast::channel(options.event_capacity);
        let (status_tx, status) = watch::channel(SocketStatus::Disconnected);
        let (worker_status_tx, worker_status) = watch::channel(NativeWorkerStatus::Running);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let worker_events = events.clone();
        let worker = std::thread::Builder::new()
            .name("phoenix-channel-runtime".into())
            .spawn(move || {
                let fallback_ready = ready_tx.clone();
                let result = catch_unwind(AssertUnwindSafe(|| {
                    run_worker(WorkerBootstrap {
                        endpoint_url: endpoint,
                        config_loader,
                        options,
                        commands: command_rx,
                        control: control_rx,
                        control_tx: worker_control,
                        events: worker_events,
                        status: status_tx,
                        ready: ready_tx,
                    })
                }));
                let final_status = match result {
                    Ok(Ok(())) => NativeWorkerStatus::Stopped,
                    Ok(Err(message)) => {
                        let _ = fallback_ready.send(Err(message.clone()));
                        NativeWorkerStatus::Failed(message)
                    }
                    Err(panic) => {
                        let message = panic_message(panic.as_ref());
                        let _ = fallback_ready.send(Err(message.clone()));
                        NativeWorkerStatus::Failed(message)
                    }
                };
                worker_status_tx.send_replace(final_status);
            })
            .map_err(|error| NativeRuntimeError::WorkerStart(error.to_string()))?;
        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = worker.join();
                return Err(NativeRuntimeError::WorkerStart(error));
            }
            Err(error) => {
                let _ = worker.join();
                return Err(NativeRuntimeError::WorkerStart(error.to_string()));
            }
        }
        Ok(Self {
            owner: Arc::new(WorkerOwner {
                commands,
                control,
                next_request: AtomicU64::new(1),
                worker: Mutex::new(Some(worker)),
                status: worker_status,
            }),
            events,
            status,
        })
    }

    pub fn status(&self) -> SocketStatus {
        *self.status.borrow()
    }

    pub fn events(&self) -> NativeSocketEvents {
        NativeSocketEvents {
            receiver: self.events.subscribe(),
        }
    }

    pub fn status_changes(&self) -> NativeSocketStatusChanges {
        NativeSocketStatusChanges {
            receiver: self.status.clone(),
        }
    }

    pub fn worker_status(&self) -> NativeWorkerStatus {
        self.owner.status.borrow().clone()
    }

    pub fn worker_status_changes(&self) -> NativeWorkerStatusChanges {
        NativeWorkerStatusChanges {
            receiver: self.owner.status.clone(),
        }
    }

    pub async fn connect(&self) -> Result<(), NativeRuntimeError> {
        self.unit_request(|request_id, response| HostCommand::Connect {
            request_id,
            response,
        })
        .await
    }

    pub async fn ping(&self) -> Result<Duration, NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Ping {
            request_id,
            timeout: None,
            response,
        })
        .await
    }

    pub async fn ping_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Duration, NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Ping {
            request_id,
            timeout: Some(timeout),
            response,
        })
        .await
    }

    pub async fn disconnect(&self) -> Result<(), NativeRuntimeError> {
        self.unit_request(|request_id, response| HostCommand::Disconnect {
            request_id,
            response,
        })
        .await
    }

    pub async fn disconnect_with(
        &self,
        code: u16,
        reason: impl Into<String>,
    ) -> Result<(), NativeRuntimeError> {
        let reason = reason.into();
        self.unit_request(|request_id, response| HostCommand::DisconnectWith {
            request_id,
            code,
            reason,
            response,
        })
        .await
    }

    pub async fn channel(
        &self,
        topic: impl Into<String>,
        payload: Value,
    ) -> Result<NativeChannel, NativeRuntimeError> {
        self.channel_with_loader(topic, native_static_join_payload(payload))
            .await
    }

    pub async fn channel_with_loader(
        &self,
        topic: impl Into<String>,
        payload_loader: NativeJoinPayloadLoader,
    ) -> Result<NativeChannel, NativeRuntimeError> {
        let registration = self
            .request(|request_id, response| HostCommand::Channel {
                request_id,
                topic: topic.into(),
                payload_loader,
                response,
            })
            .await?;
        Ok(NativeChannel {
            inner: Arc::new(NativeChannelInner {
                id: registration.id,
                topic: registration.topic,
                owner: self.owner.clone(),
                events: registration.events,
                status: registration.status,
            }),
        })
    }

    pub async fn shutdown(&self) -> Result<(), NativeRuntimeError> {
        let command_result = self
            .unit_command(|response| HostCommand::Shutdown { response })
            .await;
        match self.owner.join() {
            Err(error) => Err(error),
            Ok(()) => command_result,
        }
    }

    pub fn join_worker(&self) -> Result<(), NativeRuntimeError> {
        self.owner.join()
    }

    async fn unit_request(
        &self,
        build: impl FnOnce(u64, oneshot::Sender<Result<(), ClientError>>) -> HostCommand,
    ) -> Result<(), NativeRuntimeError> {
        self.request(build).await
    }

    async fn unit_command(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<(), ClientError>>) -> HostCommand,
    ) -> Result<(), NativeRuntimeError> {
        let (response, receiver) = oneshot::channel();
        if self.owner.commands.send(build(response)).await.is_err() {
            return Err(wait_for_worker_error(&self.owner).await);
        }
        let result = match receiver.await {
            Ok(result) => result,
            Err(_) => return Err(wait_for_worker_error(&self.owner).await),
        };
        result?;
        Ok(())
    }

    async fn request<T: Send + 'static>(
        &self,
        build: impl FnOnce(u64, oneshot::Sender<Result<T, ClientError>>) -> HostCommand,
    ) -> Result<T, NativeRuntimeError> {
        request_worker(&self.owner, build).await
    }
}

pub struct NativeSocketEvents {
    receiver: broadcast::Receiver<SocketEvent>,
}

impl NativeSocketEvents {
    pub async fn next(&mut self) -> Result<SocketEvent, NativeEventError> {
        receive_event(&mut self.receiver).await
    }
}

pub struct NativeSocketStatusChanges {
    receiver: watch::Receiver<SocketStatus>,
}

pub struct NativeWorkerStatusChanges {
    receiver: watch::Receiver<NativeWorkerStatus>,
}

impl NativeWorkerStatusChanges {
    pub fn current(&self) -> NativeWorkerStatus {
        self.receiver.borrow().clone()
    }

    pub async fn changed(&mut self) -> Result<NativeWorkerStatus, NativeEventError> {
        self.receiver
            .changed()
            .await
            .map_err(|_| NativeEventError::Closed)?;
        Ok(self.receiver.borrow_and_update().clone())
    }
}

impl NativeSocketStatusChanges {
    pub fn current(&self) -> SocketStatus {
        *self.receiver.borrow()
    }

    pub async fn changed(&mut self) -> Result<SocketStatus, NativeEventError> {
        self.receiver
            .changed()
            .await
            .map_err(|_| NativeEventError::Closed)?;
        Ok(*self.receiver.borrow_and_update())
    }
}

#[derive(Clone)]
pub struct NativeChannel {
    inner: Arc<NativeChannelInner>,
}

struct NativeChannelInner {
    id: u64,
    topic: String,
    owner: Arc<WorkerOwner>,
    events: broadcast::Sender<ChannelEvent>,
    status: watch::Receiver<ChannelStatus>,
}

impl Drop for NativeChannelInner {
    fn drop(&mut self) {
        let _ = self
            .owner
            .control
            .send(ControlCommand::RemoveChannel(self.id));
    }
}

impl NativeChannel {
    pub fn topic(&self) -> &str {
        &self.inner.topic
    }

    pub fn status(&self) -> ChannelStatus {
        *self.inner.status.borrow()
    }

    pub fn events(&self) -> NativeChannelEvents {
        NativeChannelEvents {
            receiver: self.inner.events.subscribe(),
        }
    }

    pub fn status_changes(&self) -> NativeChannelStatusChanges {
        NativeChannelStatusChanges {
            receiver: self.inner.status.clone(),
        }
    }

    pub fn presence(&self) -> NativeChannelPresence {
        NativeChannelPresence {
            channel: self.clone(),
            events: self.events(),
            tracker: PresenceTracker::new(),
            pending: VecDeque::new(),
            desynchronized: false,
        }
    }

    pub fn subscribe(&self, event: impl Into<String>) -> NativeEventSubscription {
        NativeEventSubscription {
            event: event.into(),
            events: self.events(),
        }
    }

    pub async fn join(&self) -> Result<Payload, NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Join {
            request_id,
            id: self.inner.id,
            timeout: None,
            response,
        })
        .await
    }

    pub async fn join_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Payload, NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Join {
            request_id,
            id: self.inner.id,
            timeout: Some(timeout),
            response,
        })
        .await
    }

    pub async fn call(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> Result<Reply, NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Call {
            request_id,
            id: self.inner.id,
            event: event.into(),
            payload: payload.into(),
            timeout: None,
            response,
        })
        .await
    }

    pub async fn call_with_timeout(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
        timeout: Duration,
    ) -> Result<Reply, NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Call {
            request_id,
            id: self.inner.id,
            event: event.into(),
            payload: payload.into(),
            timeout: Some(timeout),
            response,
        })
        .await
    }

    pub async fn call_json<Request, Response>(
        &self,
        event: impl Into<String>,
        request: &Request,
    ) -> Result<Response, NativeCallJsonError>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let payload = serde_json::to_value(request).map_err(NativeCallJsonError::Encode)?;
        self.call(event, payload)
            .await?
            .deserialize_ok()
            .map_err(Into::into)
    }

    pub async fn cast(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> Result<(), NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Cast {
            request_id,
            id: self.inner.id,
            event: event.into(),
            payload: payload.into(),
            timeout: None,
            response,
        })
        .await
    }

    pub async fn cast_with_timeout(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
        timeout: Duration,
    ) -> Result<(), NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Cast {
            request_id,
            id: self.inner.id,
            event: event.into(),
            payload: payload.into(),
            timeout: Some(timeout),
            response,
        })
        .await
    }

    pub async fn leave(&self) -> Result<Payload, NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Leave {
            request_id,
            id: self.inner.id,
            timeout: None,
            response,
        })
        .await
    }

    pub async fn leave_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Payload, NativeRuntimeError> {
        self.request(|request_id, response| HostCommand::Leave {
            request_id,
            id: self.inner.id,
            timeout: Some(timeout),
            response,
        })
        .await
    }

    async fn request<T: Send + 'static>(
        &self,
        build: impl FnOnce(u64, oneshot::Sender<Result<T, ClientError>>) -> HostCommand,
    ) -> Result<T, NativeRuntimeError> {
        request_worker(&self.inner.owner, build).await
    }
}

async fn request_worker<T: Send + 'static>(
    owner: &Arc<WorkerOwner>,
    build: impl FnOnce(u64, oneshot::Sender<Result<T, ClientError>>) -> HostCommand,
) -> Result<T, NativeRuntimeError> {
    let permit = match owner.commands.reserve().await {
        Ok(permit) => permit,
        Err(_) => return Err(wait_for_worker_error(owner).await),
    };
    let request_id = owner.next_request_id();
    let (response, receiver) = oneshot::channel();
    if owner
        .control
        .send(ControlCommand::Register(request_id))
        .is_err()
    {
        return Err(wait_for_worker_error(owner).await);
    }
    let mut guard = HostRequestGuard {
        request_id,
        control: owner.control.clone(),
        armed: true,
    };
    permit.send(build(request_id, response));
    let result = match receiver.await {
        Ok(result) => result?,
        Err(_) => return Err(wait_for_worker_error(owner).await),
    };
    guard.armed = false;
    Ok(result)
}

fn worker_error(owner: &WorkerOwner) -> NativeRuntimeError {
    match owner.status.borrow().clone() {
        NativeWorkerStatus::Failed(message) => NativeRuntimeError::WorkerFailed(message),
        NativeWorkerStatus::Running | NativeWorkerStatus::Stopped => {
            NativeRuntimeError::WorkerStopped
        }
    }
}

async fn wait_for_worker_error(owner: &WorkerOwner) -> NativeRuntimeError {
    let mut status = owner.status.clone();
    if matches!(*status.borrow(), NativeWorkerStatus::Running) {
        let _ = status.changed().await;
    }
    worker_error(owner)
}

struct HostRequestGuard {
    request_id: u64,
    control: mpsc::UnboundedSender<ControlCommand>,
    armed: bool,
}

impl Drop for HostRequestGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.control.send(ControlCommand::Cancel(self.request_id));
        }
    }
}

pub struct NativeChannelEvents {
    receiver: broadcast::Receiver<ChannelEvent>,
}

pub struct NativeChannelPresence {
    channel: NativeChannel,
    events: NativeChannelEvents,
    tracker: PresenceTracker,
    pending: VecDeque<PresenceEvent>,
    desynchronized: bool,
}

pub struct NativeEventSubscription {
    event: String,
    events: NativeChannelEvents,
}

impl NativeEventSubscription {
    pub fn event(&self) -> &str {
        &self.event
    }

    pub async fn next(&mut self) -> Result<SubscriptionEvent, NativeEventError> {
        loop {
            match self.events.next().await? {
                ChannelEvent::Protocol(ProtocolEvent::Message(frame))
                    if frame.event == self.event =>
                {
                    return Ok(SubscriptionEvent::Message(frame.payload));
                }
                ChannelEvent::Protocol(ProtocolEvent::ChannelError { payload, .. }) => {
                    return Ok(SubscriptionEvent::ChannelError(payload));
                }
                ChannelEvent::Protocol(ProtocolEvent::ChannelClosed { payload, .. }) => {
                    return Ok(SubscriptionEvent::ChannelClosed(payload));
                }
                ChannelEvent::Disconnected => return Ok(SubscriptionEvent::Disconnected),
                ChannelEvent::Lagged { dropped } => {
                    return Ok(SubscriptionEvent::Lagged { dropped });
                }
                ChannelEvent::Protocol(_) | ChannelEvent::JoinPayloadError(_) => {}
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum NativePresenceError {
    #[error(transparent)]
    Events(#[from] NativeEventError),
    #[error(transparent)]
    Decode(#[from] PresenceError),
    #[error("presence event stream dropped {dropped} events and must be resynchronized")]
    Desynchronized { dropped: u64 },
    #[error("presence state must be resynchronized before consuming more events")]
    ResyncRequired,
}

impl NativeChannelPresence {
    pub fn state(&self) -> &PresenceState {
        self.tracker.state()
    }

    pub fn requires_resync(&self) -> bool {
        self.desynchronized
    }

    pub async fn resync(&mut self) -> Result<(), NativeRuntimeError> {
        self.tracker.reset();
        self.pending.clear();
        self.channel.leave().await?;
        self.events = self.channel.events();
        self.channel.join().await?;
        self.desynchronized = false;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<PresenceEvent, NativePresenceError> {
        if self.desynchronized {
            return Err(NativePresenceError::ResyncRequired);
        }
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            match self.events.next().await {
                Ok(ChannelEvent::Protocol(ProtocolEvent::Message(frame))) => {
                    let previous = self.tracker.state().clone();
                    match self.tracker.apply(&frame)? {
                        PresenceUpdate::Synced(diff) => {
                            for (key, joined) in diff.joins.0 {
                                self.pending.push_back(PresenceEvent::Joined {
                                    current: previous.get(&key).cloned(),
                                    key,
                                    joined,
                                });
                            }
                            for (key, left) in diff.leaves.0 {
                                if let Some(current) = previous.get(&key).cloned() {
                                    self.pending.push_back(PresenceEvent::Left {
                                        key,
                                        current,
                                        left,
                                    });
                                }
                            }
                            self.pending.push_back(PresenceEvent::Synced);
                        }
                        PresenceUpdate::Ignored | PresenceUpdate::Pending => {}
                    }
                }
                Ok(ChannelEvent::Disconnected) => {
                    self.tracker.reset();
                    return Ok(PresenceEvent::Disconnected);
                }
                Ok(ChannelEvent::Lagged { dropped }) | Err(NativeEventError::Lagged(dropped)) => {
                    self.tracker.reset();
                    self.pending.clear();
                    self.desynchronized = true;
                    return Err(NativePresenceError::Desynchronized { dropped });
                }
                Ok(ChannelEvent::Protocol(ProtocolEvent::Left { .. })) => {
                    self.tracker.reset();
                    return Ok(PresenceEvent::ChannelLeft);
                }
                Ok(ChannelEvent::Protocol(ProtocolEvent::ChannelClosed { .. })) => {
                    self.tracker.reset();
                    return Ok(PresenceEvent::ChannelClosed);
                }
                Ok(ChannelEvent::Protocol(ProtocolEvent::ChannelError { .. })) => {
                    self.tracker.reset();
                    return Ok(PresenceEvent::ChannelError);
                }
                Ok(ChannelEvent::Protocol(_) | ChannelEvent::JoinPayloadError(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
}

impl NativeChannelEvents {
    pub async fn next(&mut self) -> Result<ChannelEvent, NativeEventError> {
        receive_event(&mut self.receiver).await
    }
}

pub struct NativeChannelStatusChanges {
    receiver: watch::Receiver<ChannelStatus>,
}

impl NativeChannelStatusChanges {
    pub fn current(&self) -> ChannelStatus {
        *self.receiver.borrow()
    }

    pub async fn changed(&mut self) -> Result<ChannelStatus, NativeEventError> {
        self.receiver
            .changed()
            .await
            .map_err(|_| NativeEventError::Closed)?;
        Ok(*self.receiver.borrow_and_update())
    }
}

async fn receive_event<T: Clone>(
    receiver: &mut broadcast::Receiver<T>,
) -> Result<T, NativeEventError> {
    receiver.recv().await.map_err(|error| match error {
        broadcast::error::RecvError::Closed => NativeEventError::Closed,
        broadcast::error::RecvError::Lagged(count) => NativeEventError::Lagged(count),
    })
}

struct ChannelRegistration {
    id: u64,
    topic: String,
    events: broadcast::Sender<ChannelEvent>,
    status: watch::Receiver<ChannelStatus>,
}

enum HostCommand {
    Connect {
        request_id: u64,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Disconnect {
        request_id: u64,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    DisconnectWith {
        request_id: u64,
        code: u16,
        reason: String,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Ping {
        request_id: u64,
        timeout: Option<Duration>,
        response: oneshot::Sender<Result<Duration, ClientError>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Channel {
        request_id: u64,
        topic: String,
        payload_loader: NativeJoinPayloadLoader,
        response: oneshot::Sender<Result<ChannelRegistration, ClientError>>,
    },
    Join {
        request_id: u64,
        id: u64,
        timeout: Option<Duration>,
        response: oneshot::Sender<Result<Payload, ClientError>>,
    },
    Call {
        request_id: u64,
        id: u64,
        event: String,
        payload: Payload,
        timeout: Option<Duration>,
        response: oneshot::Sender<Result<Reply, ClientError>>,
    },
    Cast {
        request_id: u64,
        id: u64,
        event: String,
        payload: Payload,
        timeout: Option<Duration>,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Leave {
        request_id: u64,
        id: u64,
        timeout: Option<Duration>,
        response: oneshot::Sender<Result<Payload, ClientError>>,
    },
}

enum ControlCommand {
    Register(u64),
    Cancel(u64),
    Finished(u64),
    RemoveChannel(u64),
    Shutdown,
}

struct WorkerBootstrap {
    endpoint_url: String,
    config_loader: NativeConnectionConfigLoader,
    options: NativeOptions,
    commands: mpsc::Receiver<HostCommand>,
    control: mpsc::UnboundedReceiver<ControlCommand>,
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    events: broadcast::Sender<SocketEvent>,
    status: watch::Sender<SocketStatus>,
    ready: std::sync::mpsc::SyncSender<Result<(), String>>,
}

fn run_worker(bootstrap: WorkerBootstrap) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    tokio::task::LocalSet::new().block_on(&runtime, worker_main(bootstrap))
}

async fn worker_main(bootstrap: WorkerBootstrap) -> Result<(), String> {
    let WorkerBootstrap {
        endpoint_url,
        config_loader,
        options,
        mut commands,
        mut control,
        control_tx,
        events,
        status,
        ready,
    } = bootstrap;
    let local_loader = Rc::new(move |context| {
        let config_loader = config_loader.clone();
        async move { config_loader(context).await }.boxed_local()
    });
    let endpoint = Endpoint::new(endpoint_url)
        .map_err(|error| error.to_string())?
        .connection_config_loader(local_loader);
    let event_capacity = options.event_capacity;
    let transport_options = options.transport.clone();
    let (socket, driver) = Socket::new(
        NativeConnector::from_endpoint(endpoint).options(transport_options),
        NativeTimer,
        options.client_options(),
    );
    let mut socket_events = socket.events().map_err(|error| error.to_string())?;
    let mut socket_statuses = socket.status_changes();
    let mut driver_task = tokio::task::spawn_local(driver);
    let next_id = AtomicU64::new(1);
    let mut channels: HashMap<u64, Rc<Channel>> = HashMap::new();
    let mut operations: HashMap<u64, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut known_requests = HashSet::new();
    let mut cancelled = HashSet::new();
    let _ = ready.send(Ok(()));

    loop {
        tokio::select! {
            driver_result = &mut driver_task => match driver_result {
                Ok(()) => break,
                Err(error) => return Err(format!("client driver task failed: {error}")),
            },
            control_command = control.recv() => match control_command {
                Some(ControlCommand::Register(request_id)) => {
                    known_requests.insert(request_id);
                }
                Some(ControlCommand::Cancel(request_id)) => {
                    if let Some(operation) = operations.remove(&request_id) {
                        operation.abort();
                    } else if known_requests.contains(&request_id) {
                        cancelled.insert(request_id);
                    }
                }
                Some(ControlCommand::Finished(request_id)) => {
                    operations.remove(&request_id);
                    known_requests.remove(&request_id);
                    cancelled.remove(&request_id);
                }
                Some(ControlCommand::RemoveChannel(id)) => {
                    channels.remove(&id);
                }
                Some(ControlCommand::Shutdown) | None => {
                    let _ = socket.shutdown().await;
                    break;
                }
            },
            event = socket_events.next() => match event {
                Some(event) => {
                    let _ = events.send(event);
                }
                None => break,
            },
            changed = socket_statuses.changed() => match changed {
                Some(changed) => {
                    status.send_replace(changed);
                }
                None => break,
            },
            command = commands.recv() => {
                let Some(command) = command else {
                    let _ = socket.shutdown().await;
                    break;
                };
                let mut host = HostState {
                    channels: &mut channels,
                    next_id: &next_id,
                    event_capacity,
                    control: &control_tx,
                    operations: &mut operations,
                    known_requests: &mut known_requests,
                    cancelled: &mut cancelled,
                };
                if handle_host_command(command, &socket, &mut host).await {
                    break;
                }
            }
        }
    }
    status.send_replace(SocketStatus::Closed);
    for (_, operation) in operations {
        operation.abort();
    }
    if driver_task.is_finished() {
        driver_task
            .await
            .map_err(|error| format!("client driver task failed: {error}"))?;
    } else {
        driver_task.abort();
    }
    Ok(())
}

struct HostState<'a> {
    channels: &'a mut HashMap<u64, Rc<Channel>>,
    next_id: &'a AtomicU64,
    event_capacity: usize,
    control: &'a mpsc::UnboundedSender<ControlCommand>,
    operations: &'a mut HashMap<u64, tokio::task::JoinHandle<()>>,
    known_requests: &'a mut HashSet<u64>,
    cancelled: &'a mut HashSet<u64>,
}

async fn handle_host_command(
    command: HostCommand,
    socket: &Socket,
    state: &mut HostState<'_>,
) -> bool {
    let HostState {
        channels,
        next_id,
        event_capacity,
        control,
        operations,
        known_requests,
        cancelled,
    } = state;
    match command {
        HostCommand::Connect {
            request_id,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let socket = socket.clone();
            track_operation(request_id, control, operations, async move {
                let _ = response.send(socket.connect().await);
            });
        }
        HostCommand::Disconnect {
            request_id,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let socket = socket.clone();
            track_operation(request_id, control, operations, async move {
                let _ = response.send(socket.disconnect().await);
            });
        }
        HostCommand::DisconnectWith {
            request_id,
            code,
            reason,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let socket = socket.clone();
            track_operation(request_id, control, operations, async move {
                let _ = response.send(socket.disconnect_with(code, reason).await);
            });
        }
        HostCommand::Ping {
            request_id,
            timeout,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let socket = socket.clone();
            track_operation(request_id, control, operations, async move {
                let result = match timeout {
                    Some(timeout) => socket.ping_with_timeout(timeout).await,
                    None => socket.ping().await,
                };
                let _ = response.send(result);
            });
        }
        HostCommand::Shutdown { response } => {
            let result = socket.shutdown().await;
            let _ = response.send(result);
            return true;
        }
        HostCommand::Channel {
            request_id,
            topic,
            payload_loader,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let registration_topic = topic.clone();
            let local_loader = Rc::new(move |context| {
                let payload_loader = payload_loader.clone();
                async move { payload_loader(context).await }.boxed_local()
            });
            match socket.channel(topic, local_loader) {
                Ok(channel) => {
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    let channel = Rc::new(channel);
                    let (event_tx, _) = broadcast::channel(*event_capacity);
                    let (status_tx, status_rx) = watch::channel(channel.status());
                    let mut status_changes = channel.status_changes();
                    let mut event_rx = match channel.events() {
                        Ok(events) => events,
                        Err(error) => {
                            let _ = response.send(Err(error));
                            return false;
                        }
                    };
                    let pump_tx = event_tx.clone();
                    tokio::task::spawn_local(async move {
                        while let Some(event) = event_rx.next().await {
                            let _ = pump_tx.send(event);
                        }
                    });
                    tokio::task::spawn_local(async move {
                        while let Some(changed) = status_changes.changed().await {
                            status_tx.send_replace(changed);
                        }
                    });
                    channels.insert(id, channel);
                    let _ = response.send(Ok(ChannelRegistration {
                        id,
                        topic: registration_topic,
                        events: event_tx,
                        status: status_rx,
                    }));
                }
                Err(error) => {
                    let _ = response.send(Err(error));
                }
            }
        }
        HostCommand::Join {
            request_id,
            id,
            timeout,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let channel = channels.get(&id).cloned();
            track_operation(request_id, control, operations, async move {
                send_channel_response(channel, response, move |channel| async move {
                    match timeout {
                        Some(timeout) => channel.join_with_timeout(timeout).await,
                        None => channel.join().await,
                    }
                })
                .await;
            });
        }
        HostCommand::Call {
            request_id,
            id,
            event,
            payload,
            timeout,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let channel = channels.get(&id).cloned();
            track_operation(request_id, control, operations, async move {
                send_channel_response(channel, response, move |channel| async move {
                    match timeout {
                        Some(timeout) => channel.call_with_timeout(event, payload, timeout).await,
                        None => channel.call(event, payload).await,
                    }
                })
                .await;
            });
        }
        HostCommand::Cast {
            request_id,
            id,
            event,
            payload,
            timeout,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let channel = channels.get(&id).cloned();
            track_operation(request_id, control, operations, async move {
                send_channel_response(channel, response, move |channel| async move {
                    match timeout {
                        Some(timeout) => channel.cast_with_timeout(event, payload, timeout).await,
                        None => channel.cast(event, payload).await,
                    }
                })
                .await;
            });
        }
        HostCommand::Leave {
            request_id,
            id,
            timeout,
            response,
        } => {
            known_requests.remove(&request_id);
            if cancelled.remove(&request_id) {
                return false;
            }
            let channel = channels.get(&id).cloned();
            track_operation(request_id, control, operations, async move {
                send_channel_response(channel, response, move |channel| async move {
                    match timeout {
                        Some(timeout) => channel.leave_with_timeout(timeout).await,
                        None => channel.leave().await,
                    }
                })
                .await;
            });
        }
    }
    false
}

async fn send_channel_response<T, F, Fut>(
    channel: Option<Rc<Channel>>,
    response: oneshot::Sender<Result<T, ClientError>>,
    call: F,
) where
    T: 'static,
    F: FnOnce(Rc<Channel>) -> Fut + 'static,
    Fut: Future<Output = Result<T, ClientError>> + 'static,
{
    let Some(channel) = channel else {
        let _ = response.send(Err(ClientError::DriverStopped));
        return;
    };
    let result = call(channel).await;
    let _ = response.send(result);
}

fn track_operation(
    request_id: u64,
    control: &mpsc::UnboundedSender<ControlCommand>,
    operations: &mut HashMap<u64, tokio::task::JoinHandle<()>>,
    operation: impl Future<Output = ()> + 'static,
) {
    let control = control.clone();
    let handle = tokio::task::spawn_local(async move {
        operation.await;
        let _ = control.send(ControlCommand::Finished(request_id));
    });
    operations.insert(request_id, handle);
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "worker panicked without a string payload".to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use futures::StreamExt;

    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn native_handles_are_send_and_sync() {
        assert_send_sync::<NativeSocket>();
        assert_send_sync::<NativeChannel>();
        assert_send_sync::<NativeChannelPresence>();
        assert_send_sync::<NativeEventSubscription>();
        assert_send_sync::<NativeOptions>();
    }

    #[test]
    fn retry_schedules_hold_the_last_delay() {
        let delays = [Duration::from_secs(1), Duration::from_secs(3)];
        assert_eq!(retry_delay(&delays, 0), Duration::from_secs(1));
        assert_eq!(retry_delay(&delays, 8), Duration::from_secs(3));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_subscriptions_report_broadcast_lag() {
        let (events, receiver) = broadcast::channel(1);
        let mut subscription = NativeEventSubscription {
            event: "notice".into(),
            events: NativeChannelEvents { receiver },
        };
        events.send(ChannelEvent::Disconnected).unwrap();
        events.send(ChannelEvent::Disconnected).unwrap();
        assert!(matches!(
            subscription.next().await,
            Err(NativeEventError::Lagged(1))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconnect_policy_runs_on_the_native_worker() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let attempts = Arc::new(AtomicUsize::new(0));
        let options = NativeOptions::default().reconnect_policy({
            let attempts = attempts.clone();
            Arc::new(move |_| {
                attempts.fetch_add(1, AtomicOrdering::SeqCst);
                ReconnectAction::Stop
            })
        });
        let socket = NativeSocket::spawn_with_options(
            format!("ws://127.0.0.1:{port}/socket"),
            ConnectionConfig::default(),
            options,
        )
        .unwrap();
        socket.connect().await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while attempts.load(AtomicOrdering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native reconnect policy was not invoked");
        socket.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_ping_timeout_cancels_the_host_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while websocket.next().await.is_some() {}
        });
        let socket = NativeSocket::spawn(
            format!("ws://127.0.0.1:{port}/socket"),
            ConnectionConfig::default(),
        )
        .unwrap();
        socket.connect().await.unwrap();
        let mut statuses = socket.status_changes();
        tokio::time::timeout(Duration::from_secs(2), async {
            while socket.status() != SocketStatus::Connected {
                statuses.changed().await.unwrap();
            }
        })
        .await
        .expect("native test socket did not connect");

        assert!(matches!(
            socket.ping_with_timeout(Duration::from_millis(20)).await,
            Err(NativeRuntimeError::Client(ClientError::Timeout {
                operation: phoenix_channel_client::ClientOperation::Ping,
                ..
            }))
        ));
        socket.shutdown().await.unwrap();
        server.abort();
        let _ = server.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_host_requests_do_not_prevent_shutdown() {
        let socket =
            NativeSocket::spawn("ws://127.0.0.1:9/socket", ConnectionConfig::default()).unwrap();
        let channel = socket
            .channel("room:lobby", serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(channel.topic(), "room:lobby");

        let pending = tokio::spawn({
            let channel = channel.clone();
            async move { channel.call("queued", serde_json::json!({})).await }
        });
        tokio::task::yield_now().await;
        pending.abort();
        let _ = pending.await;

        socket.shutdown().await.unwrap();
        assert_eq!(socket.worker_status(), NativeWorkerStatus::Stopped);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reports_driver_panics_as_worker_failures() {
        let options = NativeOptions::default().telemetry(Arc::new(|_| {
            panic!("telemetry failure");
        }));
        let socket = NativeSocket::spawn_with_options(
            "ws://127.0.0.1:9/socket",
            ConnectionConfig::default(),
            options,
        )
        .unwrap();
        let _ = socket.connect().await;
        assert!(matches!(
            socket.shutdown().await,
            Err(NativeRuntimeError::WorkerFailed(_))
        ));
        assert!(matches!(
            socket.join_worker(),
            Err(NativeRuntimeError::WorkerFailed(message)) if message.contains("client driver task failed")
        ));
        assert!(matches!(
            socket.worker_status(),
            NativeWorkerStatus::Failed(_)
        ));
    }
}
