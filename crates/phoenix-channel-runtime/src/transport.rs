use futures::future::LocalBoxFuture;
use thiserror::Error;

/// A transport-level WebSocket message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireMessage {
    Text(String),
    Binary(Vec<u8>),
}

/// A runtime-neutral, object-safe transport interface.
///
/// `LocalBoxFuture` is intentional: browser WebSocket futures are not `Send`.
/// Native adapters can still be driven on a dedicated local executor.
pub trait Transport {
    fn send<'a>(
        &'a mut self,
        message: WireMessage,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>>;

    fn receive<'a>(&'a mut self)
    -> LocalBoxFuture<'a, Result<Option<WireMessage>, TransportError>>;

    fn close<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), TransportError>>;
}

#[derive(Debug, Error)]
#[error("transport error: {message}")]
pub struct TransportError {
    message: String,
}

impl TransportError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
