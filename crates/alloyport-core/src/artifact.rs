//! Validated content identities and transport-independent Artifact descriptors.

use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter, Write as _};
use std::str::FromStr;

const SHA256_PREFIX: &str = "sha256:";
const SHA256_BYTES: usize = 32;

/// Canonical SHA-256 content identity.
#[derive(Clone, Copy, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct Sha256Digest([u8; SHA256_BYTES]);

impl Sha256Digest {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; SHA256_BYTES]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; SHA256_BYTES] {
        self.0
    }

    /// Computes the canonical SHA-256 identity of one byte slice.
    #[must_use]
    pub fn digest_bytes(bytes: &[u8]) -> Self {
        let mut context = Context::new(&SHA256);
        context.update(bytes);
        let digest = context.finish();
        let mut value = [0_u8; SHA256_BYTES];
        value.copy_from_slice(digest.as_ref());
        Self(value)
    }

    /// Returns the lowercase hexadecimal body without the `sha256:` algorithm prefix.
    #[must_use]
    pub fn hexadecimal(self) -> String {
        let mut value = String::with_capacity(SHA256_BYTES * 2);
        for byte in self.0 {
            write!(value, "{byte:02x}").expect("writing to a String cannot fail");
        }
        value
    }
}

impl Debug for Sha256Digest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, formatter)
    }
}

impl Display for Sha256Digest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{SHA256_PREFIX}{}", self.hexadecimal())
    }
}

impl From<Sha256Digest> for String {
    fn from(digest: Sha256Digest) -> Self {
        digest.to_string()
    }
}

impl FromStr for Sha256Digest {
    type Err = DigestParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let hexadecimal = value
            .strip_prefix(SHA256_PREFIX)
            .ok_or(DigestParseError::MissingPrefix)?;
        if hexadecimal.len() != SHA256_BYTES * 2 {
            return Err(DigestParseError::WrongLength(hexadecimal.len()));
        }
        let mut bytes = [0_u8; SHA256_BYTES];
        for (index, pair) in hexadecimal.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0]).ok_or(DigestParseError::NonHexadecimal)?;
            let low = hex_nibble(pair[1]).ok_or(DigestParseError::NonHexadecimal)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }
}

impl TryFrom<String> for Sha256Digest {
    type Error = DigestParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<&str> for Sha256Digest {
    type Error = DigestParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// A SHA-256 digest string is malformed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestParseError {
    MissingPrefix,
    WrongLength(usize),
    NonHexadecimal,
}

impl Display for DigestParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPrefix => write!(formatter, "digest must start with sha256:"),
            Self::WrongLength(length) => {
                write!(
                    formatter,
                    "SHA-256 hexadecimal length is {length}, expected 64"
                )
            }
            Self::NonHexadecimal => write!(formatter, "SHA-256 digest contains non-hex data"),
        }
    }
}

impl Error for DigestParseError {}

/// Immutable Artifact metadata carried by assignments and lifecycle observations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactDescriptor {
    pub digest: Sha256Digest,
    pub size_bytes: u64,
    pub media_type: String,
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_descriptor_keeps_existing_json_shape() -> Result<(), Box<dyn Error>> {
        let descriptor = ArtifactDescriptor {
            digest: Sha256Digest::digest_bytes(b"artifact"),
            size_bytes: 8,
            media_type: "application/octet-stream".to_owned(),
        };
        let encoded = serde_json::to_string(&descriptor)?;
        assert!(encoded.contains(r#""digest":"sha256:"#));
        assert_eq!(
            serde_json::from_str::<ArtifactDescriptor>(&encoded)?,
            descriptor
        );
        assert!(
            serde_json::from_str::<ArtifactDescriptor>(
                r#"{"digest":"sha256:bad","size_bytes":1,"media_type":"x"}"#
            )
            .is_err()
        );
        Ok(())
    }
}
