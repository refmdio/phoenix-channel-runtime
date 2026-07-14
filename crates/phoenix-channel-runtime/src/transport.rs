use futures::future::LocalBoxFuture;
use thiserror::Error;

/// A transport-level WebSocket message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireMessage {
    /// A UTF-8 WebSocket or LongPoll message.
    Text(String),
    /// A binary WebSocket message.
    Binary(Vec<u8>),
}

/// Details reported when a transport connection closes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportClose {
    /// WebSocket close code, if a close frame was received.
    pub code: Option<u16>,
    /// Human-readable close reason.
    pub reason: String,
    /// Whether the transport considered the close handshake clean.
    pub was_clean: bool,
}

/// Close code and reason requested by the client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportCloseRequest {
    /// WebSocket close code to send.
    pub code: u16,
    /// Close reason to send.
    pub reason: String,
}

impl TransportCloseRequest {
    /// Creates a close request.
    pub fn new(code: u16, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }
}

impl TransportClose {
    /// Creates close details reported by a transport.
    pub fn new(code: Option<u16>, reason: impl Into<String>, was_clean: bool) -> Self {
        Self {
            code,
            reason: reason.into(),
            was_clean,
        }
    }

    /// Creates an abnormal close for a stream that ended without a close frame.
    pub fn connection_ended() -> Self {
        Self::new(
            None,
            "WebSocket connection ended without a close frame",
            false,
        )
    }

    /// Returns whether the managed client should reconnect this close by default.
    pub fn should_reconnect(&self) -> bool {
        !self.was_clean && !matches!(self.code, Some(1000 | 1008))
    }
}

/// Message or close notification received from a transport.
#[derive(Clone, Debug, PartialEq)]
pub enum TransportEvent {
    /// An incoming text or binary message.
    Message(WireMessage),
    /// The connection closed.
    Closed(TransportClose),
}

/// A runtime-neutral, object-safe transport interface.
///
/// `LocalBoxFuture` is intentional: browser WebSocket futures are not `Send`.
/// Native adapters can still be driven on a dedicated local executor.
pub trait Transport {
    /// Returns whether this transport can send Phoenix binary frames.
    fn supports_binary(&self) -> bool {
        true
    }

    /// Sends one complete wire message.
    fn send<'a>(
        &'a mut self,
        message: WireMessage,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>>;

    /// Waits for the next message or close notification.
    fn receive<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<TransportEvent, TransportError>>;

    /// Closes the transport using its default close behavior.
    fn close<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), TransportError>>;

    /// Closes the transport with an explicit WebSocket code and reason.
    fn close_with<'a>(
        &'a mut self,
        request: TransportCloseRequest,
    ) -> LocalBoxFuture<'a, Result<(), TransportError>> {
        let _ = request;
        self.close()
    }
}

/// Operation in which a transport failure occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportErrorKind {
    /// Establishing the connection failed.
    Connect,
    /// Sending a message failed.
    Send,
    /// Receiving a message failed.
    Receive,
    /// Closing the connection failed.
    Close,
    /// A transport failure without a more specific operation.
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

/// Error returned by a runtime-specific transport.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind} error: {message}")]
pub struct TransportError {
    kind: TransportErrorKind,
    message: String,
}

impl TransportError {
    /// Creates an unclassified transport error.
    pub fn new(message: impl Into<String>) -> Self {
        Self::with_kind(TransportErrorKind::Other, message)
    }

    /// Creates an error for a specific transport operation.
    pub fn with_kind(kind: TransportErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Returns the operation that failed.
    pub fn kind(&self) -> TransportErrorKind {
        self.kind
    }

    /// Returns the transport-provided error message.
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
