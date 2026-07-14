use std::{rc::Rc, time::Duration};

use futures::future::LocalBoxFuture;
use phoenix_channel_runtime::{Transport, TransportError};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectContext {
    pub attempt: u32,
}

/// Creates target-specific WebSocket transports for the driver.
pub trait Connector {
    fn connect(
        &self,
        context: ConnectContext,
    ) -> LocalBoxFuture<'static, Result<Box<dyn Transport>, TransportError>>;
}

/// Supplies sleeps without choosing an executor.
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
    pub(crate) heartbeat_interval: Duration,
    pub(crate) heartbeat_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) reconnect_delay: Rc<dyn Fn(u32) -> Duration>,
    pub(crate) rejoin_delay: Rc<dyn Fn(u32) -> Duration>,
    pub(crate) command_capacity: usize,
    pub(crate) push_buffer_capacity: usize,
    pub(crate) connect_on_start: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
            reconnect_delay: Rc::new(default_retry_delay),
            rejoin_delay: Rc::new(default_retry_delay),
            command_capacity: 64,
            push_buffer_capacity: 64,
            connect_on_start: true,
        }
    }
}

impl Options {
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
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

    pub fn command_capacity(mut self, capacity: usize) -> Self {
        self.command_capacity = capacity.max(1);
        self
    }

    pub fn push_buffer_capacity(mut self, capacity: usize) -> Self {
        self.push_buffer_capacity = capacity;
        self
    }

    pub fn connect_on_start(mut self, enabled: bool) -> Self {
        self.connect_on_start = enabled;
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
