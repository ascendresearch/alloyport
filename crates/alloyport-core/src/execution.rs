//! Stable execution vocabulary shared by server and worker application layers.

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Valid executor selected by an immutable assignment contract.
///
/// Numeric values intentionally match the versioned wire and persisted JSON representation, while
/// the domain type excludes the transport-only unspecified value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(i32)]
#[serde(try_from = "i32", into = "i32")]
pub enum ExecutionKind {
    Process = 1,
    Container = 2,
    Shell = 3,
    CudaFixture = 4,
}

impl ExecutionKind {
    /// Stable diagnostic name matching the corresponding wire enum constant.
    #[must_use]
    pub const fn as_str_name(self) -> &'static str {
        match self {
            Self::Process => "EXECUTOR_KIND_PROCESS",
            Self::Container => "EXECUTOR_KIND_CONTAINER",
            Self::Shell => "EXECUTOR_KIND_SHELL",
            Self::CudaFixture => "EXECUTOR_KIND_CUDA_FIXTURE",
        }
    }
}

impl From<ExecutionKind> for i32 {
    fn from(kind: ExecutionKind) -> Self {
        kind as Self
    }
}

impl TryFrom<i32> for ExecutionKind {
    type Error = ExecutionKindError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Process),
            2 => Ok(Self::Container),
            3 => Ok(Self::Shell),
            4 => Ok(Self::CudaFixture),
            _ => Err(ExecutionKindError(value)),
        }
    }
}

/// A wire or persisted executor number is not part of the domain vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionKindError(pub i32);

impl Display for ExecutionKindError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid executor kind {}", self.0)
    }
}

impl Error for ExecutionKindError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_representation_remains_numeric() -> Result<(), Box<dyn Error>> {
        let encoded = serde_json::to_string(&ExecutionKind::CudaFixture)?;
        assert_eq!(encoded, "4");
        assert_eq!(
            serde_json::from_str::<ExecutionKind>(&encoded)?,
            ExecutionKind::CudaFixture
        );
        assert!(serde_json::from_str::<ExecutionKind>("0").is_err());
        Ok(())
    }
}
