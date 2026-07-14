use futures::future::LocalBoxFuture;
use thiserror::Error;

/// A transport-level WebSocket message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireMessage {
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportClose {
    pub code: Option<u16>,
    pub reason: String,
    pub was_clean: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportCloseRequest {
    pub code: u16,
    pub reason: String,
}

impl TransportCloseRequest {
    pub fn new(code: u16, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }
}

impl TransportClose {
    pub fn new(code: Option<u16>, reason: impl Into<String>, was_clean: bool) -> Self {
        Self {
            code,
            reason: reason.into(),
            was_clean,
        }
    }

    pub fn connection_ended() -> Self {
        Self::new(
            None,
            "WebSocket connection ended without a close frame",
            false,
        )
    }

    pub fn should_reconnect(&self) -> bool {
        !self.was_clean && !matches!(self.code, Some(1000 | 1008))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransportEvent {
    Message(WireMessage),
    Closed(TransportClose),
}

/// A runtime-neutral, object-safe transport interface.
///
/// `LocalBoxFuture` is intentional: browser WebSocket futures are not `Send`.
/// Native adapters can still be driven on a dedicated local executor.
pub trait Transport {
    fn supports_binary(&self) -> bool {
        true
    }

    fn send<'a>(
        &'a mut self,
        message: WireMessage,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>>;

    fn receive<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<TransportEvent, TransportError>>;

    fn close<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), TransportError>>;

    fn close_with<'a>(
        &'a mut self,
        request: TransportCloseRequest,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        let _ = request;
        self.close()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportErrorKind {
    Connect,
    Send,
    Receive,
    Close,
    Other,
}

impl std::fmt::Display for TransportErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Connect => "connect",
            Self::Send => "send",
            Self::Receive => "receive",
            Self::Close => "close",
            Self::Other => "transport",
        })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind} error: {message}")]
pub struct TransportError {
    kind: TransportErrorKind,
    message: String,
}

impl TransportError {
    pub fn new(message: impl Into<String>) -> Self {
        Self::with_kind(TransportErrorKind::Other, message)
    }

    pub fn with_kind(kind: TransportErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> TransportErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnects_only_after_abnormal_reconnectable_closes() {
        assert!(!TransportClose::new(Some(1000), "normal", true).should_reconnect());
        assert!(!TransportClose::new(Some(1008), "policy", false).should_reconnect());
        assert!(TransportClose::new(Some(1006), "abnormal", false).should_reconnect());
        assert!(TransportClose::connection_ended().should_reconnect());
    }
}
