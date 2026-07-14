use std::collections::HashMap;

use serde_json::{Value, json};
use thiserror::Error;

use crate::Frame;

const PHOENIX_TOPIC: &str = "phoenix";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelState {
    Joining,
    Joined,
    Leaving,
    Closed,
    Errored,
    Disconnected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyStatus {
    Ok,
    Error,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Outbound {
    pub reference: String,
    pub frame: Frame,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProtocolEvent {
    Joined {
        topic: String,
        reference: String,
        response: Value,
    },
    JoinError {
        topic: String,
        reference: String,
        response: Value,
    },
    Left {
        topic: String,
        reference: String,
        response: Value,
    },
    Reply {
        topic: String,
        event: String,
        reference: String,
        status: ReplyStatus,
        response: Value,
    },
    Message(Frame),
    ChannelClosed {
        topic: String,
        payload: Value,
    },
    ChannelError {
        topic: String,
        payload: Value,
    },
    HeartbeatAck {
        reference: String,
        status: ReplyStatus,
    },
    RequestInterrupted {
        topic: String,
        event: String,
        reference: String,
    },
    StaleMessage(Frame),
    UnmatchedReply(Frame),
}

#[derive(Clone, Debug)]
struct Channel {
    state: ChannelState,
    join_ref: String,
    params: Value,
}

#[derive(Clone, Debug)]
enum Pending {
    Join { topic: String },
    Leave { topic: String },
    Push { topic: String, event: String },
    Heartbeat,
}

/// Pure Phoenix Channels protocol state.
///
/// The caller owns I/O, clocks, authentication refresh, and retry scheduling.
/// That makes the same state machine usable from browser WASM, Tokio, smol, or
/// a UI framework executor.
#[derive(Debug, Default)]
pub struct Protocol {
    next_reference: u64,
    channels: HashMap<String, Channel>,
    pending: HashMap<String, Pending>,
}

impl Protocol {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn channel_state(&self, topic: &str) -> Option<ChannelState> {
        self.channels.get(topic).map(|channel| channel.state)
    }

    pub fn join(
        &mut self,
        topic: impl Into<String>,
        params: Value,
    ) -> Result<Outbound, ProtocolError> {
        let topic = topic.into();
        if let Some(channel) = self.channels.get(&topic)
            && matches!(
                channel.state,
                ChannelState::Joining | ChannelState::Joined | ChannelState::Leaving
            )
        {
            return Err(ProtocolError::AlreadyActive(topic));
        }

        let reference = self.allocate_reference();
        self.channels.insert(
            topic.clone(),
            Channel {
                state: ChannelState::Joining,
                join_ref: reference.clone(),
                params: params.clone(),
            },
        );
        self.pending.insert(
            reference.clone(),
            Pending::Join {
                topic: topic.clone(),
            },
        );

        Ok(Outbound {
            reference: reference.clone(),
            frame: Frame::new(
                Some(reference.clone()),
                Some(reference),
                topic,
                "phx_join",
                params,
            ),
        })
    }

    pub fn rejoin(
        &mut self,
        topic: impl Into<String>,
        refreshed_params: Value,
    ) -> Result<Outbound, ProtocolError> {
        let topic = topic.into();
        match self.channels.get(&topic).map(|channel| channel.state) {
            Some(ChannelState::Disconnected | ChannelState::Errored | ChannelState::Closed) => {
                self.channels.remove(&topic);
                self.join(topic, refreshed_params)
            }
            Some(_) => Err(ProtocolError::AlreadyActive(topic)),
            None => self.join(topic, refreshed_params),
        }
    }

    pub fn rejoin_all_with_stored_params(&mut self) -> Vec<Outbound> {
        let channels = self
            .channels
            .iter()
            .filter(|(_, channel)| {
                matches!(
                    channel.state,
                    ChannelState::Disconnected | ChannelState::Errored | ChannelState::Closed
                )
            })
            .map(|(topic, channel)| (topic.clone(), channel.params.clone()))
            .collect::<Vec<_>>();

        channels
            .into_iter()
            .filter_map(|(topic, params)| self.rejoin(topic, params).ok())
            .collect()
    }

    pub fn leave(&mut self, topic: &str) -> Result<Outbound, ProtocolError> {
        let reference = self.allocate_reference();
        let channel = self
            .channels
            .get_mut(topic)
            .ok_or_else(|| ProtocolError::UnknownTopic(topic.to_owned()))?;
        if channel.state != ChannelState::Joined {
            return Err(ProtocolError::NotJoined(topic.to_owned()));
        }

        channel.state = ChannelState::Leaving;
        let join_ref = channel.join_ref.clone();
        self.pending.insert(
            reference.clone(),
            Pending::Leave {
                topic: topic.to_owned(),
            },
        );

        Ok(Outbound {
            reference: reference.clone(),
            frame: Frame::new(
                Some(join_ref),
                Some(reference.clone()),
                topic,
                "phx_leave",
                json!({}),
            ),
        })
    }

    pub fn push(
        &mut self,
        topic: &str,
        event: impl Into<String>,
        payload: Value,
    ) -> Result<Outbound, ProtocolError> {
        let event = event.into();
        let join_ref = self
            .channels
            .get(topic)
            .filter(|channel| channel.state == ChannelState::Joined)
            .map(|channel| channel.join_ref.clone())
            .ok_or_else(|| ProtocolError::NotJoined(topic.to_owned()))?;
        let reference = self.allocate_reference();
        self.pending.insert(
            reference.clone(),
            Pending::Push {
                topic: topic.to_owned(),
                event: event.clone(),
            },
        );

        Ok(Outbound {
            reference: reference.clone(),
            frame: Frame::new(
                Some(join_ref),
                Some(reference.clone()),
                topic,
                event,
                payload,
            ),
        })
    }

    /// Builds a push that does not request a reply.
    ///
    /// The frame has no `ref`, so it is not tracked as an in-flight request.
    pub fn cast(
        &self,
        topic: &str,
        event: impl Into<String>,
        payload: Value,
    ) -> Result<Frame, ProtocolError> {
        let join_ref = self
            .channels
            .get(topic)
            .filter(|channel| channel.state == ChannelState::Joined)
            .map(|channel| channel.join_ref.clone())
            .ok_or_else(|| ProtocolError::NotJoined(topic.to_owned()))?;

        Ok(Frame::new(Some(join_ref), None, topic, event, payload))
    }

    /// Stops correlating a push reply, for example after an API timeout.
    ///
    /// Join and leave requests cannot be forgotten through this method because
    /// their replies also transition channel state.
    pub fn forget_push(&mut self, reference: &str) -> bool {
        if matches!(self.pending.get(reference), Some(Pending::Push { .. })) {
            self.pending.remove(reference);
            true
        } else {
            false
        }
    }

    pub fn heartbeat(&mut self) -> Outbound {
        let reference = self.allocate_reference();
        self.pending.insert(reference.clone(), Pending::Heartbeat);
        Outbound {
            reference: reference.clone(),
            frame: Frame::new(
                None,
                Some(reference.clone()),
                PHOENIX_TOPIC,
                "heartbeat",
                json!({}),
            ),
        }
    }

    pub fn receive(&mut self, frame: Frame) -> Result<ProtocolEvent, ProtocolError> {
        if frame.event == "phx_reply" {
            return self.receive_reply(frame);
        }

        if let Some(channel) = self.channels.get(&frame.topic)
            && frame.join_ref.is_some()
            && frame.join_ref.as_deref() != Some(channel.join_ref.as_str())
        {
            return Ok(ProtocolEvent::StaleMessage(frame));
        }

        match frame.event.as_str() {
            "phx_close" => {
                if let Some(channel) = self.channels.get_mut(&frame.topic) {
                    channel.state = ChannelState::Closed;
                }
                Ok(ProtocolEvent::ChannelClosed {
                    topic: frame.topic,
                    payload: frame.payload,
                })
            }
            "phx_error" => {
                if let Some(channel) = self.channels.get_mut(&frame.topic) {
                    channel.state = ChannelState::Errored;
                }
                Ok(ProtocolEvent::ChannelError {
                    topic: frame.topic,
                    payload: frame.payload,
                })
            }
            _ => Ok(ProtocolEvent::Message(frame)),
        }
    }

    pub fn reset_connection(&mut self) -> Vec<ProtocolEvent> {
        let interrupted = self
            .pending
            .drain()
            .filter_map(|(reference, pending)| match pending {
                Pending::Join { topic } => Some(ProtocolEvent::RequestInterrupted {
                    topic,
                    event: "phx_join".into(),
                    reference,
                }),
                Pending::Leave { topic } => Some(ProtocolEvent::RequestInterrupted {
                    topic,
                    event: "phx_leave".into(),
                    reference,
                }),
                Pending::Push { topic, event } => Some(ProtocolEvent::RequestInterrupted {
                    topic,
                    event,
                    reference,
                }),
                Pending::Heartbeat => None,
            })
            .collect();

        for channel in self.channels.values_mut() {
            channel.state = ChannelState::Disconnected;
        }
        interrupted
    }

    fn receive_reply(&mut self, frame: Frame) -> Result<ProtocolEvent, ProtocolError> {
        let Some(reference) = frame.reference.clone() else {
            return Ok(ProtocolEvent::UnmatchedReply(frame));
        };
        let Some(pending) = self.pending.remove(&reference) else {
            return Ok(ProtocolEvent::UnmatchedReply(frame));
        };
        let (status, response) = decode_reply_payload(&frame.payload)?;

        match pending {
            Pending::Join { topic } => {
                if let Some(channel) = self.channels.get_mut(&topic) {
                    channel.state = match status {
                        ReplyStatus::Ok => ChannelState::Joined,
                        ReplyStatus::Error => ChannelState::Errored,
                    };
                }
                match status {
                    ReplyStatus::Ok => Ok(ProtocolEvent::Joined {
                        topic,
                        reference,
                        response,
                    }),
                    ReplyStatus::Error => Ok(ProtocolEvent::JoinError {
                        topic,
                        reference,
                        response,
                    }),
                }
            }
            Pending::Leave { topic } => {
                self.channels.remove(&topic);
                Ok(ProtocolEvent::Left {
                    topic,
                    reference,
                    response,
                })
            }
            Pending::Push { topic, event } => Ok(ProtocolEvent::Reply {
                topic,
                event,
                reference,
                status,
                response,
            }),
            Pending::Heartbeat => Ok(ProtocolEvent::HeartbeatAck { reference, status }),
        }
    }

    fn allocate_reference(&mut self) -> String {
        self.next_reference = self.next_reference.wrapping_add(1);
        if self.next_reference == 0 {
            self.next_reference = 1;
        }
        self.next_reference.to_string()
    }
}

fn decode_reply_payload(payload: &Value) -> Result<(ReplyStatus, Value), ProtocolError> {
    let object = payload
        .as_object()
        .ok_or(ProtocolError::InvalidReplyPayload)?;
    let status = match object.get("status").and_then(Value::as_str) {
        Some("ok") => ReplyStatus::Ok,
        Some("error") => ReplyStatus::Error,
        _ => return Err(ProtocolError::InvalidReplyStatus),
    };
    let response = object.get("response").cloned().unwrap_or_else(|| json!({}));
    Ok((status, response))
}

#[derive(Debug, Error, PartialEq)]
pub enum ProtocolError {
    #[error("topic is already active: {0}")]
    AlreadyActive(String),
    #[error("topic is not joined: {0}")]
    NotJoined(String),
    #[error("unknown topic: {0}")]
    UnknownTopic(String),
    #[error("phx_reply payload must be an object")]
    InvalidReplyPayload,
    #[error("phx_reply status must be ok or error")]
    InvalidReplyStatus,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn reply(outbound: &Outbound, status: &str, response: Value) -> Frame {
        Frame::new(
            outbound.frame.join_ref.clone(),
            Some(outbound.reference.clone()),
            outbound.frame.topic.clone(),
            "phx_reply",
            json!({"status": status, "response": response}),
        )
    }

    fn joined_protocol() -> Protocol {
        let mut protocol = Protocol::new();
        let join = protocol
            .join("room:lobby", json!({"token": "abc"}))
            .unwrap();
        protocol.receive(reply(&join, "ok", json!({}))).unwrap();
        protocol
    }

    #[test]
    fn joins_a_topic_and_correlates_the_reply() {
        let mut protocol = Protocol::new();
        let outbound = protocol
            .join("room:lobby", json!({"token": "abc"}))
            .unwrap();

        assert_eq!(outbound.frame.event, "phx_join");
        assert_eq!(outbound.frame.join_ref, outbound.frame.reference);
        assert_eq!(
            protocol.channel_state("room:lobby"),
            Some(ChannelState::Joining)
        );

        let event = protocol
            .receive(reply(&outbound, "ok", json!({"ready": true})))
            .unwrap();
        assert_eq!(
            event,
            ProtocolEvent::Joined {
                topic: "room:lobby".into(),
                reference: outbound.reference,
                response: json!({"ready": true}),
            }
        );
        assert_eq!(
            protocol.channel_state("room:lobby"),
            Some(ChannelState::Joined)
        );
    }

    #[test]
    fn correlates_push_replies_without_swallowing_server_events() {
        let mut protocol = joined_protocol();
        let push = protocol
            .push("room:lobby", "new_message", json!({"body": "hello"}))
            .unwrap();
        let broadcast = Frame::new(
            push.frame.join_ref.clone(),
            None,
            "room:lobby",
            "presence_changed",
            json!({"online": 2}),
        );

        assert_eq!(
            protocol.receive(broadcast.clone()).unwrap(),
            ProtocolEvent::Message(broadcast)
        );
        assert_eq!(
            protocol
                .receive(reply(&push, "ok", json!({"id": 99})))
                .unwrap(),
            ProtocolEvent::Reply {
                topic: "room:lobby".into(),
                event: "new_message".into(),
                reference: push.reference,
                status: ReplyStatus::Ok,
                response: json!({"id": 99}),
            }
        );
    }

    #[test]
    fn rejects_pushes_before_join_completes() {
        let mut protocol = Protocol::new();
        protocol.join("room:lobby", json!({})).unwrap();

        assert_eq!(
            protocol.push("room:lobby", "event", json!({})).unwrap_err(),
            ProtocolError::NotJoined("room:lobby".into())
        );
    }

    #[test]
    fn resets_and_rejoins_with_a_fresh_join_reference() {
        let mut protocol = joined_protocol();
        let push = protocol
            .push("room:lobby", "new_message", json!({}))
            .unwrap();
        let old_join_ref = push.frame.join_ref.clone();

        let interrupted = protocol.reset_connection();
        assert_eq!(interrupted.len(), 1);
        assert_eq!(
            protocol.channel_state("room:lobby"),
            Some(ChannelState::Disconnected)
        );

        let rejoins = protocol.rejoin_all_with_stored_params();
        assert_eq!(rejoins.len(), 1);
        assert_ne!(rejoins[0].frame.join_ref, old_join_ref);
    }

    #[test]
    fn identifies_messages_from_an_old_join_generation() {
        let mut protocol = joined_protocol();
        protocol.reset_connection();
        let rejoin = protocol.rejoin_all_with_stored_params().remove(0);
        protocol.receive(reply(&rejoin, "ok", json!({}))).unwrap();

        let stale = Frame::new(
            Some("1".into()),
            None,
            "room:lobby",
            "new_message",
            json!({}),
        );
        assert_eq!(
            protocol.receive(stale.clone()).unwrap(),
            ProtocolEvent::StaleMessage(stale)
        );
    }
}
