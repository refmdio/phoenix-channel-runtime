use std::collections::VecDeque;

use phoenix_channel_runtime::{
    Presence, PresenceError, PresenceState, PresenceTracker, PresenceUpdate, ProtocolEvent,
};
use thiserror::Error;

use super::{Channel, ChannelEvent, ChannelEvents, ClientError};

#[derive(Clone, Debug, PartialEq)]
pub enum PresenceEvent {
    Joined {
        key: String,
        current: Option<Presence>,
        joined: Presence,
    },
    Left {
        key: String,
        current: Presence,
        left: Presence,
    },
    Synced,
    Disconnected,
    ChannelLeft,
    ChannelClosed,
    ChannelError,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum PresenceStreamError {
    #[error(transparent)]
    Decode(#[from] PresenceError),
    #[error("presence event stream dropped {dropped} events and must be resynchronized")]
    Desynchronized { dropped: u64 },
    #[error("presence state must be resynchronized before consuming more events")]
    ResyncRequired,
}

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

    pub fn state(&self) -> &PresenceState {
        self.tracker.state()
    }

    pub fn requires_resync(&self) -> bool {
        self.desynchronized
    }

    pub async fn resync(&mut self) -> Result<(), ClientError> {
        self.tracker.reset();
        self.pending.clear();
        self.channel.leave().await?;
        self.events = self.channel.events()?;
        self.channel.join().await?;
        self.desynchronized = false;
        Ok(())
    }

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
