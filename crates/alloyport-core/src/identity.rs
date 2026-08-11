//! Validated stable identities shared by application and persistence contracts.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

/// Stable identity of one immutable process attempt.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct AttemptId(String);

impl AttemptId {
    /// Returns the validated identity text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for AttemptId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Deref for AttemptId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for AttemptId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for AttemptId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl From<AttemptId> for String {
    fn from(attempt_id: AttemptId) -> Self {
        attempt_id.0
    }
}

impl TryFrom<String> for AttemptId {
    type Error = AttemptIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.trim().is_empty() {
            Err(AttemptIdError)
        } else {
            Ok(Self(value))
        }
    }
}

impl TryFrom<&str> for AttemptId {
    type Error = AttemptIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_owned())
    }
}

/// Attempt identity text is empty or contains only whitespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptIdError;

impl Display for AttemptIdError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("attempt ID must not be empty")
    }
}

impl Error for AttemptIdError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempt_id_is_validated_and_serializes_as_its_existing_string() -> Result<(), Box<dyn Error>>
    {
        let attempt_id = AttemptId::try_from("attempt-1")?;
        let encoded = serde_json::to_string(&attempt_id)?;
        assert_eq!(encoded, r#""attempt-1""#);
        assert_eq!(serde_json::from_str::<AttemptId>(&encoded)?, attempt_id);
        assert!(AttemptId::try_from("  ").is_err());
        assert!(serde_json::from_str::<AttemptId>(r#""""#).is_err());
        Ok(())
    }
}
