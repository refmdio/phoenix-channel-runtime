use thiserror::Error;

use crate::{Frame, FrameCodecError, Payload, WireMessage};

const PUSH: u8 = 0;
const REPLY: u8 = 1;
const BROADCAST: u8 = 2;

pub trait Codec {
    fn encode(&self, frame: &Frame) -> Result<WireMessage, CodecError>;
    fn decode(&self, message: WireMessage) -> Result<Frame, CodecError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CodecLimits {
    pub max_frame_bytes: usize,
    pub max_binary_payload_bytes: usize,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_binary_payload_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PhoenixV2Codec;

impl PhoenixV2Codec {
    pub fn limited(limits: CodecLimits) -> LimitedPhoenixV2Codec {
        LimitedPhoenixV2Codec { limits }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LimitedPhoenixV2Codec {
    limits: CodecLimits,
}

impl LimitedPhoenixV2Codec {
    pub fn limits(&self) -> CodecLimits {
        self.limits
    }
}

impl Codec for PhoenixV2Codec {
    fn encode(&self, frame: &Frame) -> Result<WireMessage, CodecError> {
        match &frame.payload {
            Payload::Json(_) => Ok(WireMessage::Text(frame.encode_text()?)),
            Payload::Binary(payload) => Ok(WireMessage::Binary(encode_push(frame, payload)?)),
            Payload::Reply { .. } => Err(CodecError::InvalidOutboundReplyPayload),
        }
    }

    fn decode(&self, message: WireMessage) -> Result<Frame, CodecError> {
        match message {
            WireMessage::Text(text) => Ok(Frame::decode_text(&text)?),
            WireMessage::Binary(bytes) => decode_binary(&bytes),
        }
    }
}

impl Codec for LimitedPhoenixV2Codec {
    fn encode(&self, frame: &Frame) -> Result<WireMessage, CodecError> {
        validate_payload_size(&frame.payload, self.limits)?;
        let message = PhoenixV2Codec.encode(frame)?;
        validate_frame_size(&message, self.limits)?;
        Ok(message)
    }

    fn decode(&self, message: WireMessage) -> Result<Frame, CodecError> {
        validate_frame_size(&message, self.limits)?;
        let frame = PhoenixV2Codec.decode(message)?;
        validate_payload_size(&frame.payload, self.limits)?;
        Ok(frame)
    }
}

fn validate_frame_size(message: &WireMessage, limits: CodecLimits) -> Result<(), CodecError> {
    let length = match message {
        WireMessage::Text(text) => text.len(),
        WireMessage::Binary(bytes) => bytes.len(),
    };
    if length > limits.max_frame_bytes {
        return Err(CodecError::FrameTooLarge {
            length,
            maximum: limits.max_frame_bytes,
        });
    }
    Ok(())
}

fn validate_payload_size(payload: &Payload, limits: CodecLimits) -> Result<(), CodecError> {
    let length = match payload {
        Payload::Binary(bytes) => Some(bytes.len()),
        Payload::Reply { response, .. } => match response.as_ref() {
            Payload::Binary(bytes) => Some(bytes.len()),
            _ => None,
        },
        Payload::Json(_) => None,
    };
    if let Some(length) = length {
        if length > limits.max_binary_payload_bytes {
            return Err(CodecError::BinaryPayloadTooLarge {
                length,
                maximum: limits.max_binary_payload_bytes,
            });
        }
    }
    Ok(())
}

fn encode_push(frame: &Frame, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
    let join_ref = frame.join_ref.as_deref().unwrap_or_default().as_bytes();
    let reference = frame.reference.as_deref().unwrap_or_default().as_bytes();
    let topic = frame.topic.as_bytes();
    let event = frame.event.as_bytes();
    let lengths = [
        field_size(join_ref, "join_ref")?,
        field_size(reference, "ref")?,
        field_size(topic, "topic")?,
        field_size(event, "event")?,
    ];
    let mut encoded = Vec::with_capacity(
        5 + join_ref.len() + reference.len() + topic.len() + event.len() + payload.len(),
    );
    encoded.push(PUSH);
    encoded.extend_from_slice(&lengths);
    encoded.extend_from_slice(join_ref);
    encoded.extend_from_slice(reference);
    encoded.extend_from_slice(topic);
    encoded.extend_from_slice(event);
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

fn decode_binary(input: &[u8]) -> Result<Frame, CodecError> {
    let Some(kind) = input.first().copied() else {
        return Err(CodecError::TruncatedBinaryFrame);
    };
    match kind {
        PUSH => {
            let sizes = binary_header(input, 4)?;
            let (fields, payload) = binary_fields(input, 4, sizes)?;
            Ok(Frame::new(
                optional_utf8(fields[0], "join_ref")?,
                None,
                utf8(fields[1], "topic")?,
                utf8(fields[2], "event")?,
                payload.to_vec(),
            ))
        }
        REPLY => {
            let sizes = binary_header(input, 5)?;
            let (fields, payload) = binary_fields(input, 5, sizes)?;
            Ok(Frame::new(
                optional_utf8(fields[0], "join_ref")?,
                optional_utf8(fields[1], "ref")?,
                utf8(fields[2], "topic")?,
                "phx_reply",
                Payload::Reply {
                    status: utf8(fields[3], "status")?,
                    response: Box::new(Payload::Binary(payload.to_vec())),
                },
            ))
        }
        BROADCAST => {
            let sizes = binary_header(input, 3)?;
            let (fields, payload) = binary_fields(input, 3, sizes)?;
            Ok(Frame::new(
                None,
                None,
                utf8(fields[0], "topic")?,
                utf8(fields[1], "event")?,
                payload.to_vec(),
            ))
        }
        other => Err(CodecError::UnknownBinaryKind(other)),
    }
}

fn binary_header(input: &[u8], header_length: usize) -> Result<&[u8], CodecError> {
    if input.len() < header_length {
        return Err(CodecError::TruncatedBinaryFrame);
    }
    Ok(&input[1..header_length])
}

fn binary_fields<'a>(
    input: &'a [u8],
    header_length: usize,
    sizes: &[u8],
) -> Result<(Vec<&'a [u8]>, &'a [u8]), CodecError> {
    let metadata_length = sizes.iter().map(|size| usize::from(*size)).sum::<usize>();
    if input.len() < header_length + metadata_length {
        return Err(CodecError::TruncatedBinaryFrame);
    }
    let mut offset = header_length;
    let mut fields = Vec::with_capacity(sizes.len());
    for size in sizes {
        let end = offset + usize::from(*size);
        fields.push(&input[offset..end]);
        offset = end;
    }
    Ok((fields, &input[offset..]))
}

fn optional_utf8(value: &[u8], field: &'static str) -> Result<Option<String>, CodecError> {
    if value.is_empty() {
        Ok(None)
    } else {
        utf8(value, field).map(Some)
    }
}

fn utf8(value: &[u8], field: &'static str) -> Result<String, CodecError> {
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| CodecError::InvalidUtf8(field))
}

fn field_size(value: &[u8], field: &'static str) -> Result<u8, CodecError> {
    u8::try_from(value.len()).map_err(|_| CodecError::FieldTooLong {
        field,
        length: value.len(),
    })
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error(transparent)]
    Text(#[from] FrameCodecError),
    #[error("Phoenix binary frame is truncated")]
    TruncatedBinaryFrame,
    #[error("unknown Phoenix binary frame kind: {0}")]
    UnknownBinaryKind(u8),
    #[error("Phoenix binary frame contains invalid UTF-8 in {0}")]
    InvalidUtf8(&'static str),
    #[error("Phoenix binary {field} field exceeds 255 bytes: {length}")]
    FieldTooLong { field: &'static str, length: usize },
    #[error("reply envelope cannot be sent as an application payload")]
    InvalidOutboundReplyPayload,
    #[error("Phoenix frame is {length} bytes, exceeding the {maximum}-byte limit")]
    FrameTooLarge { length: usize, maximum: usize },
    #[error("Phoenix binary payload is {length} bytes, exceeding the {maximum}-byte limit")]
    BinaryPayloadTooLarge { length: usize, maximum: usize },
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    #[test]
    fn encodes_client_binary_pushes() {
        let frame = Frame::new(
            Some("1".into()),
            Some("2".into()),
            "room:lobby",
            "binary",
            vec![7, 8, 9],
        );
        let WireMessage::Binary(encoded) = PhoenixV2Codec.encode(&frame).unwrap() else {
            panic!("expected a binary WebSocket message");
        };
        assert_eq!(&encoded[..5], &[0, 1, 1, 10, 6]);
        assert_eq!(&encoded[5..], b"12room:lobbybinary\x07\x08\x09");
    }

    #[test]
    fn decodes_binary_push_reply_and_broadcast_frames() {
        let push = [vec![0, 1, 4, 4], b"1roomping".to_vec(), vec![1, 2]].concat();
        let frame = PhoenixV2Codec.decode(WireMessage::Binary(push)).unwrap();
        assert_eq!(frame.topic, "room");
        assert_eq!(frame.event, "ping");
        assert_eq!(frame.payload, Payload::Binary(vec![1, 2]));

        let reply = [vec![1, 1, 1, 4, 2], b"12roomok".to_vec(), vec![3, 4]].concat();
        let frame = PhoenixV2Codec.decode(WireMessage::Binary(reply)).unwrap();
        assert_eq!(frame.event, "phx_reply");
        assert_eq!(
            frame.payload,
            Payload::Reply {
                status: "ok".into(),
                response: Box::new(Payload::Binary(vec![3, 4])),
            }
        );

        let broadcast = [vec![2, 4, 5], b"roomevent".to_vec(), vec![5, 6]].concat();
        let frame = PhoenixV2Codec
            .decode(WireMessage::Binary(broadcast))
            .unwrap();
        assert_eq!(frame.join_ref, None);
        assert_eq!(frame.payload, Payload::Binary(vec![5, 6]));

        let text = Frame::new(None, None, "room", "event", json!({}));
        assert!(matches!(
            PhoenixV2Codec.encode(&text).unwrap(),
            WireMessage::Text(_)
        ));
    }

    #[test]
    fn enforces_frame_and_binary_payload_limits() {
        let codec = PhoenixV2Codec::limited(CodecLimits {
            max_frame_bytes: 64,
            max_binary_payload_bytes: 2,
        });
        let frame = Frame::new(None, None, "room", "binary", vec![1, 2, 3]);
        assert!(matches!(
            codec.encode(&frame),
            Err(CodecError::BinaryPayloadTooLarge { .. })
        ));

        let codec = PhoenixV2Codec::limited(CodecLimits {
            max_frame_bytes: 4,
            max_binary_payload_bytes: 4,
        });
        assert!(matches!(
            codec.decode(WireMessage::Text("[null,null,\"room\",\"e\",{}]".into())),
            Err(CodecError::FrameTooLarge { .. })
        ));
    }

    proptest! {
        #[test]
        fn arbitrary_binary_frames_never_panic(input in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let codec = PhoenixV2Codec::limited(CodecLimits {
                max_frame_bytes: 2048,
                max_binary_payload_bytes: 1024,
            });
            let _ = codec.decode(WireMessage::Binary(input));
        }

        #[test]
        fn arbitrary_text_frames_never_panic(input in ".{0,4096}") {
            let codec = PhoenixV2Codec::limited(CodecLimits {
                max_frame_bytes: 2048,
                max_binary_payload_bytes: 1024,
            });
            let _ = codec.decode(WireMessage::Text(input));
        }
    }
}
