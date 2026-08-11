//! Validated stable identities shared by application and persistence contracts.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

macro_rules! validated_id {
    ($(#[$metadata:meta])* $name:ident, $error:ident, $label:literal) => {
        $(#[$metadata])*
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Returns the validated identity text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                self.as_str()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl From<$name> for String {
            fn from(identity: $name) -> Self {
                identity.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                if value.trim().is_empty() {
                    Err($error)
                } else {
                    Ok(Self(value))
                }
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::try_from(value.to_owned())
            }
        }

        #[doc = concat!($label, " identity text is empty or contains only whitespace.")]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $error;

        impl Display for $error {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                write!(formatter, "{} ID must not be empty", $label)
            }
        }

        impl Error for $error {}
    };
}

validated_id!(
    /// Stable identity of one immutable process attempt.
    AttemptId,
    AttemptIdError,
    "attempt"
);

validated_id!(
    /// Stable identity of an assignment envelope across retries and delivery sessions.
    AssignmentId,
    AssignmentIdError,
    "assignment"
);

validated_id!(
    /// Stable identity of one user-visible task or run.
    TaskId,
    TaskIdError,
    "task"
);

validated_id!(
    /// Stable identity of one immutable implementation candidate.
    CandidateId,
    CandidateIdError,
    "candidate"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_are_validated_and_keep_their_existing_json_strings() -> Result<(), Box<dyn Error>>
    {
        let attempt_id = AttemptId::try_from("attempt-1")?;
        let encoded = serde_json::to_string(&attempt_id)?;
        assert_eq!(encoded, r#""attempt-1""#);
        assert_eq!(serde_json::from_str::<AttemptId>(&encoded)?, attempt_id);
        assert!(AttemptId::try_from("  ").is_err());
        assert!(serde_json::from_str::<AttemptId>(r#""""#).is_err());

        let assignment_id = AssignmentId::try_from("assignment-1")?;
        assert_eq!(serde_json::to_string(&assignment_id)?, r#""assignment-1""#);
        assert!(AssignmentId::try_from("").is_err());

        let task_id = TaskId::try_from("task-1")?;
        assert_eq!(serde_json::to_string(&task_id)?, r#""task-1""#);
        assert!(TaskId::try_from("\t").is_err());

        let candidate_id = CandidateId::try_from("candidate-1")?;
        assert_eq!(serde_json::to_string(&candidate_id)?, r#""candidate-1""#);
        assert!(CandidateId::try_from("  ").is_err());
        Ok(())
    }
}
