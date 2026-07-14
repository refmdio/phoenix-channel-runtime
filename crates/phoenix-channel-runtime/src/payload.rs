use serde_json::Value;

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
