use std::collections::VecDeque;

use serde_json::Value;
use thiserror::Error;

use crate::{
    Frame, FrameCodecError, Protocol, ProtocolError, ProtocolEvent, Transport, TransportError,
    TransportEvent, WireMessage,
};

/// A sequential, runtime-neutral Phoenix Channels session.
///
/// `Session` owns a transport and preserves server events that arrive while a
/// join, push, leave, or heartbeat reply is being awaited. Applications that
/// need commands from multiple tasks should own the session in one task and
/// communicate with it over their executor's channel type.
pub struct Session<T> {
    protocol: Protocol,
    transport: T,
    buffered_events: VecDeque<ProtocolEvent>,
}

impl<T: Transport> Session<T> {
    pub fn new(transport: T) -> Self {
        Self {
            protocol: Protocol::new(),
            transport,
            buffered_events: VecDeque::new(),
        }
    }

    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    pub fn protocol_mut(&mut self) -> &mut Protocol {
        &mut self.protocol
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn into_parts(self) -> (Protocol, T) {
        (self.protocol, self.transport)
    }

    pub async fn join(
        &mut self,
        topic: impl Into<String>,
        params: Value,
    ) -> Result<ProtocolEvent, SessionError> {
        let outbound = self.protocol.join(topic, params)?;
        let reference = outbound.reference.clone();
        self.send(outbound.frame).await?;
        self.wait_for_reference(&reference).await
    }

    pub async fn rejoin(
        &mut self,
        topic: impl Into<String>,
        refreshed_params: Value,
    ) -> Result<ProtocolEvent, SessionError> {
        let outbound = self.protocol.rejoin(topic, refreshed_params)?;
        let reference = outbound.reference.clone();
        self.send(outbound.frame).await?;
        self.wait_for_reference(&reference).await
    }

    pub async fn push(
        &mut self,
        topic: &str,
        event: impl Into<String>,
        payload: Value,
    ) -> Result<ProtocolEvent, SessionError> {
        let outbound = self.protocol.push(topic, event, payload)?;
        let reference = outbound.reference.clone();
        self.send(outbound.frame).await?;
        self.wait_for_reference(&reference).await
    }

    pub async fn leave(&mut self, topic: &str) -> Result<ProtocolEvent, SessionError> {
        let outbound = self.protocol.leave(topic)?;
        let reference = outbound.reference.clone();
        self.send(outbound.frame).await?;
        self.wait_for_reference(&reference).await
    }

    pub async fn heartbeat(&mut self) -> Result<ProtocolEvent, SessionError> {
        let outbound = self.protocol.heartbeat();
        let reference = outbound.reference.clone();
        self.send(outbound.frame).await?;
        self.wait_for_reference(&reference).await
    }

    pub async fn next_event(&mut self) -> Result<ProtocolEvent, SessionError> {
        if let Some(event) = self.buffered_events.pop_front() {
            return Ok(event);
        }
        self.receive_event().await
    }

    /// Marks all channels disconnected and returns the requests interrupted by
    /// transport loss. The caller can then reconnect a transport and call
    /// `rejoin` with refreshed authentication parameters.
    pub fn reset_connection(&mut self) -> Vec<ProtocolEvent> {
        self.protocol.reset_connection()
    }

    pub async fn close(&mut self) -> Result<(), SessionError> {
        self.transport.close().await?;
        Ok(())
    }

    async fn send(&mut self, frame: Frame) -> Result<(), SessionError> {
        self.transport
            .send(WireMessage::Text(frame.encode_text()?))
            .await?;
        Ok(())
    }

    async fn wait_for_reference(
        &mut self,
        expected_reference: &str,
    ) -> Result<ProtocolEvent, SessionError> {
        loop {
            let event = self.receive_event().await?;
            if event_reference(&event) == Some(expected_reference) {
                return Ok(event);
            }
            self.buffered_events.push_back(event);
        }
    }

    async fn receive_event(&mut self) -> Result<ProtocolEvent, SessionError> {
        let message = match self.transport.receive().await? {
            TransportEvent::Message(message) => message,
            TransportEvent::Closed(close) => return Err(SessionError::ConnectionClosed(close)),
        };
        let text = match message {
            WireMessage::Text(text) => text,
            WireMessage::Binary(_) => return Err(SessionError::BinarySerializerNotImplemented),
        };
        Ok(self.protocol.receive(Frame::decode_text(&text)?)?)
    }
}

fn event_reference(event: &ProtocolEvent) -> Option<&str> {
    match event {
        ProtocolEvent::Joined { reference, .. }
        | ProtocolEvent::JoinError { reference, .. }
        | ProtocolEvent::Left { reference, .. }
        | ProtocolEvent::Reply { reference, .. }
        | ProtocolEvent::HeartbeatAck { reference, .. }
        | ProtocolEvent::RequestInterrupted { reference, .. } => Some(reference),
        ProtocolEvent::Message(_)
        | ProtocolEvent::ChannelClosed { .. }
        | ProtocolEvent::ChannelError { .. }
        | ProtocolEvent::StaleMessage(_)
        | ProtocolEvent::UnmatchedReply(_) => None,
    }
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Codec(#[from] FrameCodecError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("WebSocket connection closed: {0:?}")]
    ConnectionClosed(crate::TransportClose),
    #[error("Phoenix binary serializer is not implemented")]
    BinarySerializerNotImplemented,
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::VecDeque, rc::Rc};

    use futures::future::LocalBoxFuture;
    use serde_json::json;

    use super::*;

    #[derive(Default)]
    struct MockState {
        incoming: VecDeque<WireMessage>,
        sent: Vec<WireMessage>,
        closed: bool,
    }

    struct MockTransport {
        state: Rc<RefCell<MockState>>,
    }

    impl MockTransport {
        fn with_incoming(
            messages: impl IntoIterator<Item = WireMessage>,
        ) -> (Self, Rc<RefCell<MockState>>) {
            let state = Rc::new(RefCell::new(MockState {
                incoming: messages.into_iter().collect(),
                ..MockState::default()
            }));
            (
                Self {
                    state: state.clone(),
                },
                state,
            )
        }
    }

    impl Transport for MockTransport {
        fn send<'a>(
            &'a mut self,
            message: WireMessage,
        ) -> LocalBoxFuture<'a, Result<(), TransportError>> {
            self.state.borrow_mut().sent.push(message);
            Box::pin(async { Ok(()) })
        }

        fn receive<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<TransportEvent, TransportError>> {
            let message = self.state.borrow_mut().incoming.pop_front();
            Box::pin(async move {
                Ok(message.map_or_else(
                    || TransportEvent::Closed(crate::TransportClose::connection_ended()),
                    TransportEvent::Message,
                ))
            })
        }

        fn close<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<(), TransportError>> {
            self.state.borrow_mut().closed = true;
            Box::pin(async { Ok(()) })
        }
    }

    fn text(frame: &str) -> WireMessage {
        WireMessage::Text(frame.into())
    }

    #[test]
    fn joins_and_sends_a_v2_frame() {
        futures::executor::block_on(async {
            let (transport, state) = MockTransport::with_incoming([text(
                r#"["1","1","room:lobby","phx_reply",{"status":"ok","response":{"ready":true}}]"#,
            )]);
            let mut session = Session::new(transport);

            let event = session
                .join("room:lobby", json!({"token": "abc"}))
                .await
                .unwrap();

            assert!(matches!(
                event,
                ProtocolEvent::Joined { response, .. } if response == json!({"ready": true})
            ));
            let sent = state.borrow().sent[0].clone();
            let WireMessage::Text(sent) = sent else {
                panic!("expected text frame")
            };
            let frame = Frame::decode_text(&sent).unwrap();
            assert_eq!(frame.event, "phx_join");
            assert_eq!(frame.payload, json!({"token": "abc"}));
        });
    }

    #[test]
    fn buffers_broadcasts_received_while_waiting_for_a_reply() {
        futures::executor::block_on(async {
            let (transport, _) = MockTransport::with_incoming([
                text(r#"["1",null,"room:lobby","new_message",{"body":"early"}]"#),
                text(r#"["1","1","room:lobby","phx_reply",{"status":"ok","response":{}}]"#),
            ]);
            let mut session = Session::new(transport);

            session.join("room:lobby", json!({})).await.unwrap();
            let event = session.next_event().await.unwrap();

            assert!(matches!(
                event,
                ProtocolEvent::Message(Frame { event, .. }) if event == "new_message"
            ));
        });
    }
}
