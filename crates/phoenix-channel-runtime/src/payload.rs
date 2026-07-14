use serde_json::Value;
use thiserror::Error;

/// Associates an application event name with its deserialized payload type.
pub trait EventRoute {
    /// Event name to match.
    const EVENT: &'static str;
    /// Payload type produced for a matching event.
    type Output: serde::de::DeserializeOwned;
}

/// Payload carried by a Phoenix protocol frame.
#[derive(Clone, Debug, PartialEq)]
pub enum Payload {
    /// An ordinary JSON payload.
    Json(Value),
    /// An opaque binary payload.
    Binary(Vec<u8>),
    /// The status and response object inside a `phx_reply` frame.
    Reply {
        /// Phoenix reply status string, normally `ok` or `error`.
        status: String,
        /// JSON or binary response returned by the server.
        response: Box<Payload>,
    },
}

impl Payload {
    /// Borrows the JSON value when this is [`Payload::Json`].
    pub fn as_json(&self) -> Option<&Value> {
        match self {
            Self::Json(value) => Some(value),
            Self::Binary(_) | Self::Reply { .. } => None,
        }
    }

    /// Borrows the bytes when this is [`Payload::Binary`].
    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(value) => Some(value),
            Self::Json(_) | Self::Reply { .. } => None,
        }
    }

    /// Consumes this payload and returns its JSON value.
    ///
    /// Returns the original payload when it is not JSON.
    pub fn into_json(self) -> Result<Value, Self> {
        match self {
            Self::Json(value) => Ok(value),
            other => Err(other),
        }
    }

    /// Consumes this payload and returns its binary bytes.
    ///
    /// Returns the original payload when it is not binary.
    pub fn into_binary(self) -> Result<Vec<u8>, Self> {
        match self {
            Self::Binary(value) => Ok(value),
            other => Err(other),
        }
    }

    /// Deserializes a JSON payload into `T`.
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self) -> Result<T, PayloadError> {
        let value = self.as_json().ok_or(PayloadError::ExpectedJson)?;
        serde_json::from_value(value.clone()).map_err(PayloadError::Deserialize)
    }
}

impl From<Value> for Payload {
    fn from(value: Value) -> Self {
        Self::Json(value)
    }
}

impl From<Vec<u8>> for Payload {
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(value)
    }
}

impl PartialEq<Value> for Payload {
    fn eq(&self, other: &Value) -> bool {
        self.as_json() == Some(other)
    }
}

/// Failure while reading a typed value from a [`Payload`].
#[derive(Debug, Error)]
pub enum PayloadError {
    /// The payload was not JSON.
    #[error("expected a JSON payload, received binary data")]
    ExpectedJson,
    /// The JSON payload could not be deserialized into the requested type.
    #[error("failed to deserialize a JSON payload: {0}")]
    Deserialize(#[source] serde_json::Error),
}
