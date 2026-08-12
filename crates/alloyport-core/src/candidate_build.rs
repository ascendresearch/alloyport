//! Immutable Ascend build input, attempt observations, and independent Build Gate receipts.

use crate::{
    ArtifactDescriptor, AssignmentContract, AssignmentId, AttemptId, AttemptOutcome, BundlePath,
    CandidateId, CandidateSourceManifest, GeneratedSourceKind, GenerationStrategy, Sha256Digest,
    TaskId, evaluate_source_gate,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

pub const CANDIDATE_BUILD_BUNDLE_SCHEMA_V1: u16 = 1;
pub const ASCEND_BUILD_RECEIPT_SCHEMA_V1: u16 = 1;
pub const ASCEND_BUILD_GATE_REVISION_V1: &str = "ascend-build-gate-v1";
pub const ASCEND_BUILD_BUNDLE_MEDIA_TYPE: &str =
    "application/vnd.alloyport.ascend-build-bundle.v1+json";
pub const ASCEND_BUILD_FEATURE: &str = "ascend-build-v1";
const MAX_BUILD_SOURCE_BYTES: usize = 8 * 1024 * 1024;

/// One generated source embedded in the controller-authored worker input Artifact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CandidateBuildFile {
    path: BundlePath,
    kind: GeneratedSourceKind,
    digest: Sha256Digest,
    size_bytes: u64,
    contents: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateBuildFileDocument {
    path: BundlePath,
    kind: GeneratedSourceKind,
    digest: Sha256Digest,
    size_bytes: u64,
    contents: String,
}

impl<'de> Deserialize<'de> for CandidateBuildFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = CandidateBuildFileDocument::deserialize(deserializer)?;
        Self::new(
            document.path,
            document.kind,
            document.digest,
            document.size_bytes,
            document.contents,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl CandidateBuildFile {
    fn new(
        path: BundlePath,
        kind: GeneratedSourceKind,
        digest: Sha256Digest,
        size_bytes: u64,
        contents: String,
    ) -> Result<Self, CandidateBuildError> {
        let actual_size = u64::try_from(contents.len()).unwrap_or(u64::MAX);
        if !path.as_str().starts_with("generated/")
            || size_bytes == 0
            || size_bytes != actual_size
            || digest != Sha256Digest::digest_bytes(contents.as_bytes())
        {
            return Err(CandidateBuildError::InvalidSourceIdentity);
        }
        Ok(Self {
            path,
            kind,
            digest,
            size_bytes,
            contents,
        })
    }

    #[must_use]
    pub const fn path(&self) -> &BundlePath {
        &self.path
    }

    #[must_use]
    pub const fn kind(&self) -> GeneratedSourceKind {
        self.kind
    }

    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    #[must_use]
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    #[must_use]
    pub fn contents(&self) -> &str {
        &self.contents
    }
}

/// Exact bounded input delivered to the policy-bound Ascend build worker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CandidateBuildBundle {
    schema_version: u16,
    candidate_id: CandidateId,
    task_id: TaskId,
    manifest_digest: Sha256Digest,
    source_gate_receipt_digest: Sha256Digest,
    migration_spec_digest: Sha256Digest,
    generation_strategy: GenerationStrategy,
    public_symbol: String,
    target_architecture: String,
    files: Vec<CandidateBuildFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateBuildBundleDocument {
    schema_version: u16,
    candidate_id: CandidateId,
    task_id: TaskId,
    manifest_digest: Sha256Digest,
    source_gate_receipt_digest: Sha256Digest,
    migration_spec_digest: Sha256Digest,
    generation_strategy: GenerationStrategy,
    public_symbol: String,
    target_architecture: String,
    files: Vec<CandidateBuildFile>,
}

impl<'de> Deserialize<'de> for CandidateBuildBundle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = CandidateBuildBundleDocument::deserialize(deserializer)?;
        if document.schema_version != CANDIDATE_BUILD_BUNDLE_SCHEMA_V1 {
            return Err(serde::de::Error::custom("unsupported build bundle schema"));
        }
        Self::from_parts(
            document.candidate_id,
            document.task_id,
            document.manifest_digest,
            document.source_gate_receipt_digest,
            document.migration_spec_digest,
            document.generation_strategy,
            document.public_symbol,
            document.target_architecture,
            document.files,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl CandidateBuildBundle {
    /// Builds a worker input only when the exact candidate independently passes Source Gate.
    ///
    /// # Errors
    ///
    /// Returns an error for a failing/mismatched Source receipt or incomplete source bytes.
    pub fn new(
        manifest: &CandidateSourceManifest,
        manifest_digest: Sha256Digest,
        source_gate_receipt_digest: Sha256Digest,
        target_architecture: impl Into<String>,
        sources: &BTreeMap<BundlePath, Vec<u8>>,
    ) -> Result<Self, CandidateBuildError> {
        let receipt = evaluate_source_gate(manifest, manifest_digest, sources);
        if !receipt.passed() {
            return Err(CandidateBuildError::SourceGateDidNotPass);
        }
        if receipt
            .digest()
            .map_err(|_| CandidateBuildError::SourceGateReceiptMismatch)?
            != source_gate_receipt_digest
        {
            return Err(CandidateBuildError::SourceGateReceiptMismatch);
        }
        let mut files = Vec::with_capacity(manifest.files().len());
        for source in manifest.files() {
            let bytes = sources
                .get(source.path())
                .ok_or(CandidateBuildError::InvalidSourceSet)?;
            let contents = std::str::from_utf8(bytes)
                .map_err(|_| CandidateBuildError::InvalidSourceIdentity)?
                .to_owned();
            files.push(CandidateBuildFile::new(
                source.path().clone(),
                source.kind(),
                source.artifact().digest,
                source.artifact().size_bytes,
                contents,
            )?);
        }
        Self::from_parts(
            manifest.candidate_id().clone(),
            manifest.task_id().clone(),
            manifest_digest,
            source_gate_receipt_digest,
            manifest.migration_spec_digest(),
            manifest.generation_strategy(),
            manifest.public_symbol().to_owned(),
            target_architecture.into(),
            files,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        candidate_id: CandidateId,
        task_id: TaskId,
        manifest_digest: Sha256Digest,
        source_gate_receipt_digest: Sha256Digest,
        migration_spec_digest: Sha256Digest,
        generation_strategy: GenerationStrategy,
        public_symbol: String,
        target_architecture: String,
        files: Vec<CandidateBuildFile>,
    ) -> Result<Self, CandidateBuildError> {
        if public_symbol.trim().is_empty() || target_architecture.trim().is_empty() {
            return Err(CandidateBuildError::IncompleteContext);
        }
        let total = files.iter().try_fold(0_usize, |total, file| {
            total.checked_add(file.contents.len())
        });
        if total.is_none_or(|bytes| bytes > MAX_BUILD_SOURCE_BYTES) {
            return Err(CandidateBuildError::SourceBoundExceeded);
        }
        let mut paths = BTreeSet::new();
        let kinds: BTreeSet<_> = files
            .iter()
            .map(|file| {
                if !paths.insert(file.path.clone()) {
                    return Err(CandidateBuildError::DuplicatePath);
                }
                Ok(file.kind)
            })
            .collect::<Result<_, _>>()?;
        if GeneratedSourceKind::ALL
            .iter()
            .any(|required| !kinds.contains(required))
        {
            return Err(CandidateBuildError::MissingSourceCategory);
        }
        Ok(Self {
            schema_version: CANDIDATE_BUILD_BUNDLE_SCHEMA_V1,
            candidate_id,
            task_id,
            manifest_digest,
            source_gate_receipt_digest,
            migration_spec_digest,
            generation_strategy,
            public_symbol,
            target_architecture,
            files,
        })
    }

    #[must_use]
    pub const fn candidate_id(&self) -> &CandidateId {
        &self.candidate_id
    }

    #[must_use]
    pub const fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    #[must_use]
    pub const fn manifest_digest(&self) -> Sha256Digest {
        self.manifest_digest
    }

    #[must_use]
    pub const fn source_gate_receipt_digest(&self) -> Sha256Digest {
        self.source_gate_receipt_digest
    }

    #[must_use]
    pub fn target_architecture(&self) -> &str {
        &self.target_architecture
    }

    #[must_use]
    pub fn files(&self) -> &[CandidateBuildFile] {
        &self.files
    }

    /// Computes the canonical serialized bundle identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

/// Trusted environment identity returned by an Ascend build worker adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AscendBuildEnvironment {
    pub architecture: String,
    pub cann_version: String,
    pub driver_version: String,
    pub firmware_version: String,
}

impl AscendBuildEnvironment {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        [
            self.architecture.as_str(),
            self.cann_version.as_str(),
            self.driver_version.as_str(),
            self.firmware_version.as_str(),
        ]
        .iter()
        .all(|value| !value.trim().is_empty())
    }
}

/// Terminal facts returned through the build-attempt port after Artifact publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AscendBuildTerminal {
    pub assignment_id: AssignmentId,
    pub attempt_id: AttemptId,
    pub outcome: AttemptOutcome,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub detail: String,
    pub build_completed: bool,
    pub environment: AscendBuildEnvironment,
    pub worker_receipt: Option<ArtifactDescriptor>,
    pub stdout: Option<ArtifactDescriptor>,
    pub stderr: Option<ArtifactDescriptor>,
}

/// Current observation of one stable remote build attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AscendBuildAttemptObservation {
    Pending { diagnostic_digest: Sha256Digest },
    Finished(Box<AscendBuildTerminal>),
}

pub type AscendBuildAttemptFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<AscendBuildAttemptObservation, AscendBuildAttemptError>>
            + Send
            + 'a,
    >,
>;

/// Controller/worker boundary used by the Agent build tool.
pub trait AscendBuildAttemptPort: Debug + Send {
    #[must_use]
    fn dispatch<'a>(
        &'a mut self,
        assignment: &'a AssignmentContract,
    ) -> AscendBuildAttemptFuture<'a>;

    #[must_use]
    fn reconcile<'a>(
        &'a mut self,
        assignment: &'a AssignmentContract,
    ) -> AscendBuildAttemptFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AscendBuildAttemptError {
    Unavailable(String),
    Rejected(String),
    Integrity(String),
}

impl Display for AscendBuildAttemptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(detail) => write!(formatter, "build attempt unavailable: {detail}"),
            Self::Rejected(detail) => write!(formatter, "build attempt rejected: {detail}"),
            Self::Integrity(detail) => write!(formatter, "build attempt integrity: {detail}"),
        }
    }
}

impl Error for AscendBuildAttemptError {}

/// Independently authored Build Gate result over one exact worker attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AscendBuildReceipt {
    schema_version: u16,
    gate_revision: String,
    candidate_id: CandidateId,
    manifest_digest: Sha256Digest,
    source_gate_receipt_digest: Sha256Digest,
    assignment: AssignmentContract,
    passed: bool,
    outcome: AttemptOutcome,
    exit_code: Option<i32>,
    elapsed_ms: u64,
    detail: String,
    environment: AscendBuildEnvironment,
    worker_receipt: Option<ArtifactDescriptor>,
    stdout: Option<ArtifactDescriptor>,
    stderr: Option<ArtifactDescriptor>,
}

impl AscendBuildReceipt {
    /// Authors a receipt only from terminal facts bound to the exact assignment.
    ///
    /// # Errors
    ///
    /// Returns an error for mismatched identities or incomplete trusted environment facts.
    pub fn new(
        bundle: &CandidateBuildBundle,
        assignment: AssignmentContract,
        terminal: AscendBuildTerminal,
    ) -> Result<Self, CandidateBuildError> {
        if assignment.assignment_id != terminal.assignment_id
            || assignment.attempt_id != terminal.attempt_id
            || assignment.candidate_id != *bundle.candidate_id()
            || assignment.task_id != *bundle.task_id()
        {
            return Err(CandidateBuildError::AttemptIdentityMismatch);
        }
        if !terminal.environment.is_complete()
            || terminal.environment.architecture != bundle.target_architecture()
        {
            return Err(CandidateBuildError::IncompleteEnvironment);
        }
        let passed = terminal.outcome == AttemptOutcome::Succeeded
            && terminal.exit_code == Some(0)
            && terminal.build_completed;
        Ok(Self {
            schema_version: ASCEND_BUILD_RECEIPT_SCHEMA_V1,
            gate_revision: ASCEND_BUILD_GATE_REVISION_V1.to_owned(),
            candidate_id: bundle.candidate_id().clone(),
            manifest_digest: bundle.manifest_digest(),
            source_gate_receipt_digest: bundle.source_gate_receipt_digest(),
            assignment,
            passed,
            outcome: terminal.outcome,
            exit_code: terminal.exit_code,
            elapsed_ms: terminal.elapsed_ms,
            detail: terminal.detail,
            environment: terminal.environment,
            worker_receipt: terminal.worker_receipt,
            stdout: terminal.stdout,
            stderr: terminal.stderr,
        })
    }

    #[must_use]
    pub const fn passed(&self) -> bool {
        self.passed
    }

    #[must_use]
    pub const fn outcome(&self) -> AttemptOutcome {
        self.outcome
    }

    /// Computes the canonical serialized receipt identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateBuildError {
    SourceGateDidNotPass,
    SourceGateReceiptMismatch,
    InvalidSourceSet,
    InvalidSourceIdentity,
    IncompleteContext,
    SourceBoundExceeded,
    DuplicatePath,
    MissingSourceCategory,
    AttemptIdentityMismatch,
    IncompleteEnvironment,
}

impl Display for CandidateBuildError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid candidate build contract: {self:?}")
    }
}

impl Error for CandidateBuildError {}
