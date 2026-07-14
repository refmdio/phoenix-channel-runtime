#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

mod codec;
mod frame;
mod payload;
mod presence;
mod protocol;
mod session;
mod transport;

pub use codec::{Codec, CodecError, CodecLimits, LimitedPhoenixV2Codec, PhoenixV2Codec};
pub use frame::{Frame, FrameCodecError};
pub use payload::{EventRoute, Payload, PayloadError};
pub use presence::{
    Presence, PresenceDiff, PresenceError, PresenceState, PresenceTracker, PresenceUpdate,
    sync_diff as sync_presence_diff, sync_state as sync_presence_state,
};
pub use protocol::{ChannelState, Outbound, Protocol, ProtocolError, ProtocolEvent, ReplyStatus};
pub use session::{Session, SessionError};
pub use transport::{
    Transport, TransportClose, TransportCloseRequest, TransportError, TransportErrorKind,
    TransportEvent, WireMessage,
};

/// Phoenix's current JSON serializer protocol version.
pub const V2_PROTOCOL_VERSION: &str = "2.0.0";
