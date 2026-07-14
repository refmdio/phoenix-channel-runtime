use std::collections::VecDeque;

use phoenix_channel_runtime::{
    Presence, PresenceError, PresenceState, PresenceTracker, PresenceUpdate, ProtocolEvent,
};
use thiserror::Error;

use super::{Channel, ChannelEvent, ChannelEvents, ClientError};

/// A synchronized Presence change or channel lifecycle event.
#[derive(Clone, Debug, PartialEq)]
pub enum PresenceEvent {
    /// A key or one of its metas joined.
    Joined {
        /// Application-defined Presence key.
        key: String,
        /// Entry before the join, if the key was already present.
        current: Option<Presence>,
        /// Entry or metas added by this update.
        joined: Presence,
    },
    /// A key or one of its metas left.
    Left {
        /// Application-defined Presence key.
        key: String,
        /// Entry before the leave was applied.
        current: Presence,
        /// Entry or metas removed by this update.
        left: Presence,
    },
    /// A full state or diff was applied.
    Synced,
    /// The socket disconnected and local Presence state was cleared.
    Disconnected,
    /// The channel was explicitly left and local state was cleared.
    ChannelLeft,
    /// The server closed the channel and local state was cleared.
    ChannelClosed,
    /// The channel errored and local state was cleared.
    ChannelError,
}

/// Failure while decoding or consuming a Presence event stream.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum PresenceStreamError {
    /// A Presence state or diff payload was invalid.
    #[error(transparent)]
    Decode(#[from] PresenceError),
    /// The bounded channel event stream dropped events.
    #[error("presence event stream dropped {dropped} events and must be resynchronized")]
    Desynchronized {
        /// Number of channel events dropped before lag was observed.
        dropped: u64,
    },
    /// [`ChannelPresence::resync`] is required before consuming more events.
    #[error("presence state must be resynchronized before consuming more events")]
    ResyncRequired,
}

/// Current Presence state and changes for a joined channel.
///
/// The value borrows its [`Channel`] so a resynchronization can leave and
/// rejoin the same channel with a freshly loaded join payload.
pub struct ChannelPresence<'a> {
    channel: &'a Channel,
    events: ChannelEvents,
    tracker: PresenceTracker,
    pending: VecDeque<PresenceEvent>,
    desynchronized: bool,
}

impl<'a> ChannelPresence<'a> {
    pub(super) fn new(channel: &'a Channel) -> Result<Self, ClientError> {
        Ok(Self {
            channel,
            events: channel.events()?,
            tracker: PresenceTracker::new(),
            pending: VecDeque::new(),
            desynchronized: false,
        })
    }

    /// Returns the current synchronized state.
    pub fn state(&self) -> &PresenceState {
        self.tracker.state()
    }

    /// Returns whether event lag invalidated local state.
    pub fn requires_resync(&self) -> bool {
        self.desynchronized
    }

    /// Clears local state, leaves, and rejoins to request a fresh full state.
    pub async fn resync(&mut self) -> Result<(), ClientError> {
        self.tracker.reset();
        self.pending.clear();
        self.channel.leave().await?;
        self.events = self.channel.events()?;
        self.channel.join().await?;
        self.desynchronized = false;
        Ok(())
    }

    /// Returns the next Presence change or lifecycle event.
    pub async fn next(&mut self) -> Option<Result<PresenceEvent, PresenceStreamError>> {
        if self.desynchronized {
            return Some(Err(PresenceStreamError::ResyncRequired));
        }
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(Ok(event));
            }

            match self.events.next().await? {
                ChannelEvent::Protocol(ProtocolEvent::Message(frame)) => {
                    let previous = self.tracker.state().clone();
                    match self.tracker.apply(&frame) {
                        Ok(PresenceUpdate::Synced(diff)) => {
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
                        Ok(PresenceUpdate::Ignored | PresenceUpdate::Pending) => {}
                        Err(error) => return Some(Err(error.into())),
                    }
                }
                ChannelEvent::Disconnected => {
                    self.tracker.reset();
                    return Some(Ok(PresenceEvent::Disconnected));
                }
                ChannelEvent::Lagged { dropped } => {
                    self.tracker.reset();
                    self.pending.clear();
                    self.desynchronized = true;
                    return Some(Err(PresenceStreamError::Desynchronized { dropped }));
                }
                ChannelEvent::Protocol(ProtocolEvent::Left { .. }) => {
                    self.tracker.reset();
                    return Some(Ok(PresenceEvent::ChannelLeft));
                }
                ChannelEvent::Protocol(ProtocolEvent::ChannelClosed { .. }) => {
                    self.tracker.reset();
                    return Some(Ok(PresenceEvent::ChannelClosed));
                }
                ChannelEvent::Protocol(ProtocolEvent::ChannelError { .. }) => {
                    self.tracker.reset();
                    return Some(Ok(PresenceEvent::ChannelError));
                }
                ChannelEvent::Protocol(_) | ChannelEvent::JoinPayloadError(_) => {}
            }
        }
    }
}
