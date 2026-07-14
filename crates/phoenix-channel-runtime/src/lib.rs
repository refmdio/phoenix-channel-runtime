//! Runtime-independent building blocks for Phoenix Channels clients.
//!
//! This crate owns the Phoenix Channels v2 wire format and protocol state. It
//! deliberately does not choose a WebSocket implementation, async executor,
//! timer, authentication scheme, or UI framework.

#![forbid(unsafe_code)]

mod codec;
mod frame;
mod payload;
mod presence;
mod protocol;
mod session;
mod transport;

pub use codec::{Codec, CodecError, PhoenixV2Codec};
pub use frame::{Frame, FrameCodecError};
pub use payload::{EventRoute, Payload, PayloadError};
pub use presence::{
    Presence, PresenceDiff, PresenceError, PresenceState, PresenceTracker, PresenceUpdate,
    sync_diff as sync_presence_diff, sync_state as sync_presence_state,
};
pub use protocol::{ChannelState, Outbound, Protocol, ProtocolError, ProtocolEvent, ReplyStatus};
pub use session::{Session, SessionError};
pub use transport::{
    Transport, TransportClose, TransportError, TransportErrorKind, TransportEvent, WireMessage,
};

/// Phoenix's current JSON serializer protocol version.
pub const V2_PROTOCOL_VERSION: &str = "2.0.0";
