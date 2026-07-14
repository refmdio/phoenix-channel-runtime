use serde_json::Value;
use thiserror::Error;

use crate::{EventRoute, Payload, PayloadError};

/// A Phoenix Channels v2 JSON frame.
///
/// The serialized representation is
/// `[join_ref, ref, topic, event, payload]`.
#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    pub join_ref: Option<String>,
    pub reference: Option<String>,
    pub topic: String,
    pub event: String,
    pub payload: Payload,
}

impl Frame {
    pub fn new(
        join_ref: Option<String>,
        reference: Option<String>,
        topic: impl Into<String>,
        event: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> Self {
        Self {
            join_ref,
            reference,
            topic: topic.into(),
            event: event.into(),
            payload: payload.into(),
        }
    }

    pub fn encode_text(&self) -> Result<String, FrameCodecError> {
        serde_json::to_string(&(
            &self.join_ref,
            &self.reference,
            &self.topic,
            &self.event,
            self.payload
                .as_json()
                .ok_or(FrameCodecError::BinaryPayloadRequiresBinaryFrame)?,
        ))
        .map_err(FrameCodecError::Encode)
    }

    pub fn decode_text(input: &str) -> Result<Self, FrameCodecError> {
        let values: Vec<Value> = serde_json::from_str(input).map_err(FrameCodecError::Decode)?;
        if values.len() != 5 {
            return Err(FrameCodecError::InvalidLength(values.len()));
        }

        let join_ref = decode_reference(&values[0], "join_ref")?;
        let reference = decode_reference(&values[1], "ref")?;
        let topic = values[2]
            .as_str()
            .ok_or(FrameCodecError::InvalidField("topic"))?
            .to_owned();
        let event = values[3]
            .as_str()
            .ok_or(FrameCodecError::InvalidField("event"))?
            .to_owned();

        Ok(Self {
            join_ref,
            reference,
            topic,
            event,
            payload: Payload::Json(values[4].clone()),
        })
    }

    pub fn route<R: EventRoute>(&self) -> Result<Option<R::Output>, PayloadError> {
        if self.event == R::EVENT {
            self.payload.deserialize().map(Some)
        } else {
            Ok(None)
        }
    }
}

fn decode_reference(value: &Value, field: &'static str) -> Result<Option<String>, FrameCodecError> {
    match value {
        Value::Null => Ok(None),
        Value::String(value) => Ok(Some(value.clone())),
        Value::Number(value) => Ok(Some(value.to_string())),
        _ => Err(FrameCodecError::InvalidField(field)),
    }
}

#[derive(Debug, Error)]
pub enum FrameCodecError {
    #[error("failed to decode a Phoenix frame: {0}")]
    Decode(#[source] serde_json::Error),
    #[error("failed to encode a Phoenix frame: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("Phoenix v2 frame must contain five values, received {0}")]
    InvalidLength(usize),
    #[error("Phoenix frame contains an invalid {0} field")]
    InvalidField(&'static str),
    #[error("binary payloads require a binary Phoenix frame")]
    BinaryPayloadRequiresBinaryFrame,
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[test]
    fn round_trips_v2_text_frames() {
        let frame = Frame::new(
            Some("7".into()),
            Some("8".into()),
            "document:123",
            "update",
            json!({"body": "hello"}),
        );

        let encoded = frame.encode_text().unwrap();
        assert_eq!(Frame::decode_text(&encoded).unwrap(), frame);
    }

    #[test]
    fn accepts_numeric_references_from_non_javascript_clients() {
        let frame = Frame::decode_text(r#"[1,2,"room:lobby","phx_reply",{}]"#).unwrap();

        assert_eq!(frame.join_ref.as_deref(), Some("1"));
        assert_eq!(frame.reference.as_deref(), Some("2"));
    }

    #[test]
    fn rejects_frames_with_the_wrong_shape() {
        let error = Frame::decode_text(r#"[null,"1","topic","event"]"#).unwrap_err();

        assert!(matches!(error, FrameCodecError::InvalidLength(4)));
    }

    #[test]
    fn routes_typed_event_payloads() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Updated {
            version: u64,
        }

        struct UpdatedRoute;

        impl EventRoute for UpdatedRoute {
            const EVENT: &'static str = "updated";
            type Output = Updated;
        }

        let frame = Frame::new(None, None, "room", "updated", json!({"version": 4}));
        assert_eq!(
            frame.route::<UpdatedRoute>().unwrap(),
            Some(Updated { version: 4 })
        );
    }
}
