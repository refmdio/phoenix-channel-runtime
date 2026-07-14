use std::{
    collections::HashMap,
    future::Future,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::{FutureExt, future::BoxFuture};
use phoenix_channel_client::{
    Channel, ChannelEvent, ChannelStatus, ClientError, ConnectContext, ConnectionConfig, Endpoint,
    EndpointError, JoinContext, Options, Reply, Socket, SocketEvent, SocketStatus, TelemetryEvent,
};
use phoenix_channel_runtime::Payload;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::{NativeConnector, NativeTimer};

pub type NativeConnectionConfigLoader = Arc<
    dyn Fn(ConnectContext) -> BoxFuture<'static, Result<ConnectionConfig, String>> + Send + Sync,
>;

pub type NativeJoinPayloadLoader =
    Arc<dyn Fn(JoinContext) -> BoxFuture<'static, Result<Value, String>> + Send + Sync>;

pub type NativeTelemetryHook = Arc<dyn Fn(&TelemetryEvent) + Send + Sync>;

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
    telemetry: Option<NativeTelemetryHook>,
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
            telemetry: None,
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
        let (events, _) = broadcast::channel(options.event_capacity);
        let (status_tx, status) = watch::channel(SocketStatus::Disconnected);
        let worker_events = events.clone();
        std::thread::Builder::new()
            .name("phoenix-channel-runtime".into())
            .spawn(move || {
                run_worker(
                    endpoint,
                    config_loader,
                    options,
                    command_rx,
                    worker_events,
                    status_tx,
                );
            })
            .map_err(|error| NativeRuntimeError::WorkerStart(error.to_string()))?;
        Ok(Self {
            owner: Arc::new(WorkerOwner { commands }),
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

    pub async fn connect(&self) -> Result<(), NativeRuntimeError> {
        self.unit_command(|response| HostCommand::Connect { response })
            .await
    }

    pub async fn disconnect(&self) -> Result<(), NativeRuntimeError> {
        self.unit_command(|response| HostCommand::Disconnect { response })
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
        let (response, receiver) = oneshot::channel();
        self.owner
            .commands
            .send(HostCommand::Channel {
                topic: topic.into(),
                payload_loader,
                response,
            })
            .await
            .map_err(|_| NativeRuntimeError::WorkerStopped)?;
        let registration = receiver
            .await
            .map_err(|_| NativeRuntimeError::WorkerStopped)??;
        Ok(NativeChannel {
            inner: Arc::new(NativeChannelInner {
                id: registration.id,
                owner: self.owner.clone(),
                events: registration.events,
                status: registration.status,
            }),
        })
    }

    pub async fn shutdown(&self) -> Result<(), NativeRuntimeError> {
        self.unit_command(|response| HostCommand::Shutdown { response })
            .await
    }

    async fn unit_command(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<(), ClientError>>) -> HostCommand,
    ) -> Result<(), NativeRuntimeError> {
        let (response, receiver) = oneshot::channel();
        self.owner
            .commands
            .send(build(response))
            .await
            .map_err(|_| NativeRuntimeError::WorkerStopped)?;
        receiver
            .await
            .map_err(|_| NativeRuntimeError::WorkerStopped)??;
        Ok(())
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
    owner: Arc<WorkerOwner>,
    events: broadcast::Sender<ChannelEvent>,
    status: watch::Receiver<ChannelStatus>,
}

impl Drop for NativeChannelInner {
    fn drop(&mut self) {
        let _ = self
            .owner
            .commands
            .try_send(HostCommand::RemoveChannel { id: self.id });
    }
}

impl NativeChannel {
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

    pub async fn join(&self) -> Result<Payload, NativeRuntimeError> {
        self.request(|response| HostCommand::Join {
            id: self.inner.id,
            response,
        })
        .await
    }

    pub async fn call(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> Result<Reply, NativeRuntimeError> {
        self.request(|response| HostCommand::Call {
            id: self.inner.id,
            event: event.into(),
            payload: payload.into(),
            response,
        })
        .await
    }

    pub async fn cast(
        &self,
        event: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> Result<(), NativeRuntimeError> {
        self.request(|response| HostCommand::Cast {
            id: self.inner.id,
            event: event.into(),
            payload: payload.into(),
            response,
        })
        .await
    }

    pub async fn leave(&self) -> Result<Payload, NativeRuntimeError> {
        self.request(|response| HostCommand::Leave {
            id: self.inner.id,
            response,
        })
        .await
    }

    async fn request<T: Send + 'static>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, ClientError>>) -> HostCommand,
    ) -> Result<T, NativeRuntimeError> {
        let (response, receiver) = oneshot::channel();
        self.inner
            .owner
            .commands
            .send(build(response))
            .await
            .map_err(|_| NativeRuntimeError::WorkerStopped)?;
        Ok(receiver
            .await
            .map_err(|_| NativeRuntimeError::WorkerStopped)??)
    }
}

pub struct NativeChannelEvents {
    receiver: broadcast::Receiver<ChannelEvent>,
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
    events: broadcast::Sender<ChannelEvent>,
    status: watch::Receiver<ChannelStatus>,
}

enum HostCommand {
    Connect {
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Disconnect {
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Channel {
        topic: String,
        payload_loader: NativeJoinPayloadLoader,
        response: oneshot::Sender<Result<ChannelRegistration, ClientError>>,
    },
    RemoveChannel {
        id: u64,
    },
    Join {
        id: u64,
        response: oneshot::Sender<Result<Payload, ClientError>>,
    },
    Call {
        id: u64,
        event: String,
        payload: Payload,
        response: oneshot::Sender<Result<Reply, ClientError>>,
    },
    Cast {
        id: u64,
        event: String,
        payload: Payload,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    Leave {
        id: u64,
        response: oneshot::Sender<Result<Payload, ClientError>>,
    },
}

fn run_worker(
    endpoint_url: String,
    config_loader: NativeConnectionConfigLoader,
    options: NativeOptions,
    command_rx: mpsc::Receiver<HostCommand>,
    events: broadcast::Sender<SocketEvent>,
    status: watch::Sender<SocketStatus>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("native client runtime should be created");
    tokio::task::LocalSet::new().block_on(
        &runtime,
        worker_main(
            endpoint_url,
            config_loader,
            options,
            command_rx,
            events,
            status,
        ),
    );
}

async fn worker_main(
    endpoint_url: String,
    config_loader: NativeConnectionConfigLoader,
    options: NativeOptions,
    mut commands: mpsc::Receiver<HostCommand>,
    events: broadcast::Sender<SocketEvent>,
    status: watch::Sender<SocketStatus>,
) {
    let local_loader = Rc::new(move |context| {
        let config_loader = config_loader.clone();
        async move { config_loader(context).await }.boxed_local()
    });
    let endpoint = Endpoint::new(endpoint_url)
        .expect("endpoint was validated before starting the worker")
        .connection_config_loader(local_loader);
    let event_capacity = options.event_capacity;
    let (socket, driver) = Socket::new(
        NativeConnector::from_endpoint(endpoint),
        NativeTimer,
        options.client_options(),
    );
    let mut socket_events = socket
        .events()
        .expect("new socket event subscription should succeed");
    let mut socket_statuses = socket.status_changes();
    tokio::task::spawn_local(driver);
    let next_id = AtomicU64::new(1);
    let mut channels: HashMap<u64, Rc<Channel>> = HashMap::new();

    loop {
        tokio::select! {
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
                if handle_host_command(
                    command,
                    &socket,
                    &mut channels,
                    &next_id,
                    event_capacity,
                ).await {
                    break;
                }
            }
        }
    }
    status.send_replace(SocketStatus::Closed);
}

async fn handle_host_command(
    command: HostCommand,
    socket: &Socket,
    channels: &mut HashMap<u64, Rc<Channel>>,
    next_id: &AtomicU64,
    event_capacity: usize,
) -> bool {
    match command {
        HostCommand::Connect { response } => {
            let socket = socket.clone();
            tokio::task::spawn_local(async move {
                let _ = response.send(socket.connect().await);
            });
        }
        HostCommand::Disconnect { response } => {
            let socket = socket.clone();
            tokio::task::spawn_local(async move {
                let _ = response.send(socket.disconnect().await);
            });
        }
        HostCommand::Shutdown { response } => {
            let result = socket.shutdown().await;
            let _ = response.send(result);
            return true;
        }
        HostCommand::Channel {
            topic,
            payload_loader,
            response,
        } => {
            let local_loader = Rc::new(move |context| {
                let payload_loader = payload_loader.clone();
                async move { payload_loader(context).await }.boxed_local()
            });
            match socket.channel(topic, local_loader) {
                Ok(channel) => {
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    let channel = Rc::new(channel);
                    let (event_tx, _) = broadcast::channel(event_capacity);
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
                        events: event_tx,
                        status: status_rx,
                    }));
                }
                Err(error) => {
                    let _ = response.send(Err(error));
                }
            }
        }
        HostCommand::RemoveChannel { id } => {
            channels.remove(&id);
        }
        HostCommand::Join { id, response } => {
            spawn_channel_response(channels.get(&id), response, |channel| async move {
                channel.join().await
            });
        }
        HostCommand::Call {
            id,
            event,
            payload,
            response,
        } => {
            spawn_channel_response(channels.get(&id), response, |channel| async move {
                channel.call(event, payload).await
            });
        }
        HostCommand::Cast {
            id,
            event,
            payload,
            response,
        } => {
            spawn_channel_response(channels.get(&id), response, |channel| async move {
                channel.cast(event, payload).await
            });
        }
        HostCommand::Leave { id, response } => {
            spawn_channel_response(channels.get(&id), response, |channel| async move {
                channel.leave().await
            });
        }
    }
    false
}

fn spawn_channel_response<T, F, Fut>(
    channel: Option<&Rc<Channel>>,
    response: oneshot::Sender<Result<T, ClientError>>,
    call: F,
) where
    T: 'static,
    F: FnOnce(Rc<Channel>) -> Fut + 'static,
    Fut: Future<Output = Result<T, ClientError>> + 'static,
{
    let Some(channel) = channel.cloned() else {
        let _ = response.send(Err(ClientError::DriverStopped));
        return;
    };
    tokio::task::spawn_local(async move {
        let result = call(channel).await;
        let _ = response.send(result);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn native_handles_are_send_and_sync() {
        assert_send_sync::<NativeSocket>();
        assert_send_sync::<NativeChannel>();
        assert_send_sync::<NativeOptions>();
    }

    #[test]
    fn retry_schedules_hold_the_last_delay() {
        let delays = [Duration::from_secs(1), Duration::from_secs(3)];
        assert_eq!(retry_delay(&delays, 0), Duration::from_secs(1));
        assert_eq!(retry_delay(&delays, 8), Duration::from_secs(3));
    }
}
