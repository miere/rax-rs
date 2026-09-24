use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// A closed set of variants selected by one tag field.
pub trait Tagged {
    const TAG: &'static str;
    const NAMES: &'static [&'static str];
}

/// A value from a peer that may be newer than this build. An unknown tag decodes instead of failing,
/// so the receiver can answer with [`Unhandled`] rather than dropping the whole message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Open<T> {
    Known(T),
    Unknown { name: String, raw: Value },
}

impl<T> Open<T> {
    pub fn known(&self) -> Option<&T> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown { .. } => None,
        }
    }
}

impl<T> From<T> for Open<T> {
    fn from(value: T) -> Self {
        Self::Known(value)
    }
}

impl<T: Serialize> Serialize for Open<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Known(value) => value.serialize(serializer),
            Self::Unknown { raw, .. } => raw.serialize(serializer),
        }
    }
}

/// A known tag with a malformed body is an error, not an unknown: reporting it as unsupported
/// would send the peer looking for a version skew that is not there.
impl<'de, T: Tagged + DeserializeOwned> Deserialize<'de> for Open<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Value::deserialize(deserializer)?;
        let name = raw
            .get(T::TAG)
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom(format!("missing string field `{}`", T::TAG)))?
            .to_owned();
        if T::NAMES.contains(&name.as_str()) {
            return serde_json::from_value(raw)
                .map(Self::Known)
                .map_err(D::Error::custom);
        }
        Ok(Self::Unknown { name, raw })
    }
}

/// What one side could not act on, sent back so the other can degrade on purpose instead of
/// waiting for something that will never happen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unhandled {
    #[serde(flatten)]
    pub subject: Subject,
    pub reason: UnhandledReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "subject", rename_all = "snake_case")]
pub enum Subject {
    /// Position within the content list the unhandled block arrived in.
    Block {
        index: usize,
    },
    Uri {
        uri: String,
    },
    EventType {
        name: String,
    },
    MessageKind {
        name: String,
    },
    /// A metadata key the gateway ignored, whoever owns it.
    MetadataKey {
        key: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnhandledReason {
    UnsupportedType,
    UnsupportedScheme,
    Unreachable,
    Forbidden,
    #[serde(other)]
    Other,
}

macro_rules! tagged {
    ($type:ty, $tag:literal, [$($name:literal),+ $(,)?]) => {
        impl $crate::open::Tagged for $type {
            const TAG: &'static str = $tag;
            const NAMES: &'static [&'static str] = &[$($name),+];
        }
    };
}
pub(crate) use tagged;
