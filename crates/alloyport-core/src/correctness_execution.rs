//! Controller-authored inputs for independent reduction reference and candidate executions.

use crate::{
    BundlePath, CandidateId, ReductionCorrectnessError, ReductionCorrectnessExperiment,
    ReductionRunRole, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const REDUCTION_EXECUTION_BUNDLE_SCHEMA_V1: u16 = 1;
pub const REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE: &str =
    "application/vnd.alloyport.reduction-execution-bundle.v1+json";
pub const CUDA_REDUCTION_CORRECTNESS_FEATURE: &str = "cuda-reduction-correctness-v1";
pub const ASCEND_REDUCTION_CORRECTNESS_FEATURE: &str = "ascend-reduction-correctness-v1";
const MAX_EXECUTION_SOURCE_BYTES: usize = 16 * 1024 * 1024;

#[cfg(test)]
#[path = "correctness_execution_tests.rs"]
mod tests;

/// Public-API mode exercised by one hidden corpus case.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReductionCaseKind {
    Valid,
    NullInput,
    NullOutput,
    UnsupportedSize,
}

/// Deterministic case recipe delivered identically to reference and DUT runners.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionCorpusCase {
    pub case_id: String,
    pub repetition: u16,
    pub elements: u64,
    pub seed: u32,
    pub kind: ReductionCaseKind,
}

/// Versioned controller-owned workload. It contains inputs, never expected outputs or tolerances.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReductionCorpus {
    revision: String,
    cases: Vec<ReductionCorpusCase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReductionCorpusDocument {
    revision: String,
    cases: Vec<ReductionCorpusCase>,
}

impl<'de> Deserialize<'de> for ReductionCorpus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = ReductionCorpusDocument::deserialize(deserializer)?;
        Self::new(document.revision, document.cases).map_err(serde::de::Error::custom)
    }
}

impl ReductionCorpus {
    /// Creates and validates a bounded corpus.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty revision/corpus, unsafe IDs, duplicate keys, or invalid modes.
    pub fn new(
        revision: impl Into<String>,
        cases: Vec<ReductionCorpusCase>,
    ) -> Result<Self, ReductionCorrectnessError> {
        let revision = revision.into();
        if revision.trim().is_empty() || cases.is_empty() || cases.len() > 256 {
            return Err(ReductionCorrectnessError::InvalidCorpus);
        }
        let mut keys = BTreeSet::new();
        for case in &cases {
            if case.case_id.is_empty()
                || case.case_id.len() > 128
                || !case
                    .case_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                || case.repetition == 0
                || !keys.insert((case.case_id.clone(), case.repetition))
                || matches!(case.kind, ReductionCaseKind::NullInput) && case.elements == 0
                || matches!(case.kind, ReductionCaseKind::UnsupportedSize)
                    && case.elements <= 1_048_576
            {
                return Err(ReductionCorrectnessError::InvalidCorpus);
            }
        }
        Ok(Self { revision, cases })
    }

    /// Frozen first-product workload with boundaries, randomized sizes, and error behavior.
    #[must_use]
    pub fn fixture_v1() -> Self {
        let valid = [0_u64, 1, 3, 255, 256, 257, 4097, 65_536, 1_048_576];
        let mut cases = Vec::new();
        for (index, elements) in valid.into_iter().enumerate() {
            for repetition in 1..=2 {
                cases.push(ReductionCorpusCase {
                    case_id: format!("valid-{elements}"),
                    repetition,
                    elements,
                    seed: 0x5eed_1234_u32.wrapping_add(u32::try_from(index).unwrap_or(u32::MAX)),
                    kind: ReductionCaseKind::Valid,
                });
            }
        }
        for (case_id, kind, elements) in [
            ("invalid-null-input", ReductionCaseKind::NullInput, 1),
            ("invalid-null-output", ReductionCaseKind::NullOutput, 1),
            (
                "unsupported-size",
                ReductionCaseKind::UnsupportedSize,
                1_048_577,
            ),
        ] {
            for repetition in 1..=2 {
                cases.push(ReductionCorpusCase {
                    case_id: case_id.to_owned(),
                    repetition,
                    elements,
                    seed: 0x5eed_9000,
                    kind,
                });
            }
        }
        Self {
            revision: "cuda-reduction-corpus-v1".to_owned(),
            cases,
        }
    }

    #[must_use]
    pub fn cases(&self) -> &[ReductionCorpusCase] {
        &self.cases
    }

    /// Computes the exact serialized corpus identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

impl ReductionCorpusCase {
    /// Identity of the exact generated input/mode delivered to both execution paths.
    #[must_use]
    pub fn input_digest(&self) -> Sha256Digest {
        let mut bytes = b"alloyport-reduction-input-v1\0".to_vec();
        bytes.extend_from_slice(self.case_id.as_bytes());
        bytes.extend_from_slice(&self.repetition.to_be_bytes());
        bytes.extend_from_slice(&self.elements.to_be_bytes());
        bytes.extend_from_slice(&self.seed.to_be_bytes());
        bytes.push(match self.kind {
            ReductionCaseKind::Valid => 1,
            ReductionCaseKind::NullInput => 2,
            ReductionCaseKind::NullOutput => 3,
            ReductionCaseKind::UnsupportedSize => 4,
        });
        Sha256Digest::digest_bytes(&bytes)
    }
}

/// One immutable source file delivered to a trusted runner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReductionExecutionFile {
    path: BundlePath,
    digest: Sha256Digest,
    size_bytes: u64,
    contents: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReductionExecutionFileDocument {
    path: BundlePath,
    digest: Sha256Digest,
    size_bytes: u64,
    contents: String,
}

impl<'de> Deserialize<'de> for ReductionExecutionFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = ReductionExecutionFileDocument::deserialize(deserializer)?;
        Self::new(document.path, document.contents)
            .and_then(|file| {
                if file.digest == document.digest && file.size_bytes == document.size_bytes {
                    Ok(file)
                } else {
                    Err(ReductionCorrectnessError::InvalidExecutionSource)
                }
            })
            .map_err(serde::de::Error::custom)
    }
}

impl ReductionExecutionFile {
    /// Constructs a source file and derives its exact identity.
    ///
    /// # Errors
    ///
    /// Returns an error for empty or oversized bytes.
    pub fn new(
        path: BundlePath,
        contents: impl Into<String>,
    ) -> Result<Self, ReductionCorrectnessError> {
        let contents = contents.into();
        if contents.is_empty() || contents.len() > MAX_EXECUTION_SOURCE_BYTES {
            return Err(ReductionCorrectnessError::InvalidExecutionSource);
        }
        Ok(Self {
            path,
            digest: Sha256Digest::digest_bytes(contents.as_bytes()),
            size_bytes: u64::try_from(contents.len()).unwrap_or(u64::MAX),
            contents,
        })
    }

    #[must_use]
    pub const fn path(&self) -> &BundlePath {
        &self.path
    }

    #[must_use]
    pub fn contents(&self) -> &str {
        &self.contents
    }
}

/// Exact input for one side of a paired correctness experiment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReductionExecutionBundle {
    schema_version: u16,
    experiment: ReductionCorrectnessExperiment,
    role: ReductionRunRole,
    candidate_id: Option<CandidateId>,
    implementation_digest: Sha256Digest,
    callable: CorrectnessCallable,
    corpus: ReductionCorpus,
    files: Vec<ReductionExecutionFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReductionExecutionBundleDocument {
    schema_version: u16,
    experiment: ReductionCorrectnessExperiment,
    role: ReductionRunRole,
    candidate_id: Option<CandidateId>,
    implementation_digest: Sha256Digest,
    callable: CorrectnessCallable,
    corpus: ReductionCorpus,
    files: Vec<ReductionExecutionFile>,
}

impl<'de> Deserialize<'de> for ReductionExecutionBundle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = ReductionExecutionBundleDocument::deserialize(deserializer)?;
        if document.schema_version != REDUCTION_EXECUTION_BUNDLE_SCHEMA_V1 {
            return Err(serde::de::Error::custom(
                "unsupported reduction execution bundle schema",
            ));
        }
        let bundle = Self::new(
            document.experiment,
            document.role,
            document.callable.clone(),
            document.corpus,
            document.files,
        )
        .map_err(serde::de::Error::custom)?;
        if bundle.candidate_id != document.candidate_id
            || bundle.implementation_digest != document.implementation_digest
        {
            return Err(serde::de::Error::custom(
                "reduction execution bundle identity mismatch",
            ));
        }
        Ok(bundle)
    }
}

/// Names the trusted harness must use, carried as data instead of compiled into it.
///
/// The harness runs inside the trust boundary and used to hard-code this specimen's public symbol
/// and both `CMake` target names, so onboarding a second operator family meant editing a trusted
/// runner. The names come from `MigrationSpec`; the call shape it generates around them
/// — `int (const float *, size_t, float *)` — is still fixed for the phase-1 scope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CorrectnessCallable {
    pub public_symbol: String,
    pub reference_build_target: String,
    pub candidate_build_target: String,
}

impl CorrectnessCallable {
    /// Rejects blank or non-identifier names before they reach generated source.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        [
            &self.public_symbol,
            &self.reference_build_target,
            &self.candidate_build_target,
        ]
        .into_iter()
        .all(|name| {
            !name.is_empty()
                && name.len() <= 128
                && !name.starts_with(|first: char| first.is_ascii_digit())
                && name
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
        })
    }

    /// Build target for the role under execution.
    #[must_use]
    pub fn build_target(&self, role: ReductionRunRole) -> &str {
        match role {
            ReductionRunRole::CudaReference => &self.reference_build_target,
            ReductionRunRole::AscendCandidate => &self.candidate_build_target,
        }
    }
}

impl ReductionExecutionBundle {
    /// Creates a role-isolated source bundle for a trusted runner.
    ///
    /// # Errors
    ///
    /// Returns an error for crossed candidate identity, corpus, path roots, duplicates, or bounds.
    pub fn new(
        experiment: ReductionCorrectnessExperiment,
        role: ReductionRunRole,
        callable: CorrectnessCallable,
        corpus: ReductionCorpus,
        files: Vec<ReductionExecutionFile>,
    ) -> Result<Self, ReductionCorrectnessError> {
        if !callable.is_valid() {
            return Err(ReductionCorrectnessError::InvalidExecutionBundle);
        }
        if files.is_empty()
            || corpus.digest()? != experiment.corpus_digest()
            || files
                .iter()
                .map(|file| file.contents.len())
                .try_fold(0_usize, usize::checked_add)
                .is_none_or(|size| size > MAX_EXECUTION_SOURCE_BYTES)
        {
            return Err(ReductionCorrectnessError::InvalidExecutionBundle);
        }
        let expected_root = match role {
            ReductionRunRole::CudaReference => "input/",
            ReductionRunRole::AscendCandidate => "generated/",
        };
        let mut paths = BTreeSet::new();
        if files.iter().any(|file| {
            !file.path.as_str().starts_with(expected_root) || !paths.insert(file.path.clone())
        }) {
            return Err(ReductionCorrectnessError::InvalidExecutionBundle);
        }
        let candidate_id = match role {
            ReductionRunRole::CudaReference => None,
            ReductionRunRole::AscendCandidate => Some(experiment.candidate_id().clone()),
        };
        let mut identity = b"alloyport-reduction-implementation-v1\0".to_vec();
        for file in &files {
            identity.extend_from_slice(file.path.as_str().as_bytes());
            identity.extend_from_slice(&file.digest.bytes());
        }
        Ok(Self {
            schema_version: REDUCTION_EXECUTION_BUNDLE_SCHEMA_V1,
            callable,
            experiment,
            role,
            candidate_id,
            implementation_digest: Sha256Digest::digest_bytes(&identity),
            corpus,
            files,
        })
    }

    #[must_use]
    pub const fn experiment(&self) -> &ReductionCorrectnessExperiment {
        &self.experiment
    }

    #[must_use]
    pub const fn callable(&self) -> &CorrectnessCallable {
        &self.callable
    }

    #[must_use]
    pub const fn role(&self) -> ReductionRunRole {
        self.role
    }

    #[must_use]
    pub const fn implementation_digest(&self) -> Sha256Digest {
        self.implementation_digest
    }

    #[must_use]
    pub const fn corpus(&self) -> &ReductionCorpus {
        &self.corpus
    }

    #[must_use]
    pub fn files(&self) -> &[ReductionExecutionFile] {
        &self.files
    }

    /// Computes the exact serialized execution-bundle identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}
