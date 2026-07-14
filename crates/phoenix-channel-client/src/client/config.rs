use std::{rc::Rc, time::Duration};

use futures::future::LocalBoxFuture;
use phoenix_channel_runtime::{Transport, TransportError};
use serde_json::Value;

use super::{DisconnectReason, TelemetryHook};

/// Information supplied to a [`Connector`] for one connection attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectContext {
    /// Zero-based connection-attempt number for the current connection cycle.
    pub attempt: u32,
}

/// Creates target-specific WebSocket transports for the driver.
pub trait Connector {
    /// Starts one transport connection attempt.
    fn connect(
        &self,
        context: ConnectContext,
    ) -> LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>;
}

/// Supplies sleeps without choosing an executor.
pub trait Timer {
    /// Completes after `duration` according to the runtime clock.
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()>;
    /// Returns a monotonic duration from an implementation-defined origin.
    fn now(&self) -> Duration;
}

/// Information supplied when loading a channel's join payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JoinContext {
    /// Zero-based join-attempt number for the current join cycle.
    pub attempt: u32,
    /// Whether this join follows a prior successful join.
    pub is_rejoin: bool,
}

/// Information supplied to a custom reconnect policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconnectContext {
    /// Reconnect attempt number after the disconnect.
    pub attempt: u32,
    /// Transport or client condition that caused the disconnect.
    pub reason: DisconnectReason,
}

/// Decision returned by a custom reconnect policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconnectAction {
    /// Retry after the specified delay.
    RetryAfter(Duration),
    /// Remain disconnected until an explicit connect or reconnect request.
    Stop,
}

/// Callback that decides whether and when to reconnect.
pub type ReconnectPolicy = Rc<dyn Fn(ReconnectContext) -> ReconnectAction>;

/// Async callback that produces a fresh JSON payload for each join attempt.
pub type JoinPayloadLoader =
    Rc<dyn Fn(JoinContext) -> LocalBoxFuture<'static, Result<Value, String>>>;

/// Creates a join payload loader that clones the same value for every attempt.
pub fn static_join_payload(payload: Value) -> JoinPayloadLoader {
    Rc::new(move |_| {
        let payload = payload.clone();
        Box::pin(async move { Ok(payload) })
    })
}

/// Heartbeat, retry, timeout, buffering, and telemetry settings.
#[derive(Clone)]
pub struct Options {
    pub(crate) heartbeat_interval: Duration,
    pub(crate) heartbeat_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) join_timeout: Duration,
    pub(crate) call_timeout: Duration,
    pub(crate) leave_timeout: Duration,
    pub(crate) reconnect_delay: Rc<dyn Fn(u32) -> Duration>,
    pub(crate) reconnect_policy: Option<ReconnectPolicy>,
    pub(crate) rejoin_delay: Rc<dyn Fn(u32) -> Duration>,
    pub(crate) command_capacity: usize,
    pub(crate) push_buffer_capacity: usize,
    pub(crate) event_capacity: usize,
    pub(crate) connect_on_start: bool,
    pub(crate) telemetry: Option<TelemetryHook>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            join_timeout: Duration::from_secs(10),
            call_timeout: Duration::from_secs(10),
            leave_timeout: Duration::from_secs(10),
            reconnect_delay: Rc::new(default_retry_delay),
            reconnect_policy: None,
            rejoin_delay: Rc::new(default_retry_delay),
            command_capacity: 64,
            push_buffer_capacity: 64,
            event_capacity: 256,
            connect_on_start: true,
            telemetry: None,
        }
    }
}

impl Options {
    /// Sets how often the connected driver sends automatic heartbeats.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Sets how long an automatic heartbeat acknowledgement may take.
    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }

    /// Sets the maximum duration of one transport connection attempt.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the join, call, and leave timeout to one common value.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.join_timeout = timeout;
        self.call_timeout = timeout;
        self.leave_timeout = timeout;
        self
    }

    /// Sets the default channel join timeout.
    pub fn join_timeout(mut self, timeout: Duration) -> Self {
        self.join_timeout = timeout;
        self
    }

    /// Sets the default correlated call timeout.
    pub fn call_timeout(mut self, timeout: Duration) -> Self {
        self.call_timeout = timeout;
        self
    }

    /// Sets the default channel leave timeout.
    pub fn leave_timeout(mut self, timeout: Duration) -> Self {
        self.leave_timeout = timeout;
        self
    }

    /// Sets the delay function used by the default reconnect policy.
    pub fn reconnect_delay(mut self, delay: impl Fn(u32) -> Duration + 'static) -> Self {
        self.reconnect_delay = Rc::new(delay);
        self
    }

    /// Installs a policy that classifies every disconnect and selects a retry.
    pub fn reconnect_policy(
        mut self,
        policy: impl Fn(ReconnectContext) -> ReconnectAction + 'static,
    ) -> Self {
        self.reconnect_policy = Some(Rc::new(policy));
        self
    }

    /// Sets the delay before each automatic channel rejoin attempt.
    pub fn rejoin_delay(mut self, delay: impl Fn(u32) -> Duration + 'static) -> Self {
        self.rejoin_delay = Rc::new(delay);
        self
    }

    /// Sets the bounded capacity of the driver command queue.
    pub fn command_capacity(mut self, capacity: usize) -> Self {
        self.command_capacity = capacity.max(1);
        self
    }

    /// Sets how many calls may wait for a socket connection or channel join.
    pub fn push_buffer_capacity(mut self, capacity: usize) -> Self {
        self.push_buffer_capacity = capacity;
        self
    }

    /// Sets the capacity of each socket or channel event subscriber.
    pub fn event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity.max(1);
        self
    }

    /// Selects whether the driver connects immediately when it starts.
    pub fn connect_on_start(mut self, enabled: bool) -> Self {
        self.connect_on_start = enabled;
        self
    }

    /// Installs a callback for structured client telemetry.
    pub fn telemetry(mut self, hook: TelemetryHook) -> Self {
        self.telemetry = Some(hook);
        self
    }
}

fn default_retry_delay(attempt: u32) -> Duration {
    match attempt {
        0 => Duration::ZERO,
        1 => Duration::from_secs(1),
        2 => Duration::from_secs(2),
        3 => Duration::from_secs(5),
        _ => Duration::from_secs(10),
    }
}
