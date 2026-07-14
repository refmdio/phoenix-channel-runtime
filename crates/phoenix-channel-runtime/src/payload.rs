use serde_json::Value;
use thiserror::Error;

pub trait EventRoute {
    const EVENT: &'static str;
    type Output: serde::de::DeserializeOwned;
}

#[derive(Clone, Debug, PartialEq)]
pub enum Payload {
    Json(Value),
    Binary(Vec<u8>),
    Reply {
        status: String,
        response: Box<Payload>,
    },
}

impl Payload {
    pub fn as_json(&self) -> Option<&Value> {
        match self {
            Self::Json(value) => Some(value),
            Self::Binary(_) | Self::Reply { .. } => None,
        }
    }

    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(value) => Some(value),
            Self::Json(_) | Self::Reply { .. } => None,
        }
    }

    pub fn into_json(self) -> Result<Value, Self> {
        match self {
            Self::Json(value) => Ok(value),
            other => Err(other),
        }
    }

    pub fn into_binary(self) -> Result<Vec<u8>, Self> {
        match self {
            Self::Binary(value) => Ok(value),
            other => Err(other),
        }
    }

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

#[derive(Debug, Error)]
pub enum PayloadError {
    #[error("expected a JSON payload, received binary data")]
    ExpectedJson,
    #[error("failed to deserialize a JSON payload: {0}")]
    Deserialize(#[source] serde_json::Error),
}
