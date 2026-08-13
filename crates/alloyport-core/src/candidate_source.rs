//! Immutable generated-source manifest and independent structural Source Gate.

use crate::{
    ArtifactDescriptor, BundlePath, CandidateId, GeneratedSourceKind, GenerationStrategy,
    Sha256Digest, TaskId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub const CANDIDATE_SOURCE_MANIFEST_SCHEMA_V1: u16 = 1;
pub const SOURCE_GATE_RECEIPT_SCHEMA_V1: u16 = 1;
pub const SOURCE_GATE_REVISION_V2: &str = "source-gate-v2";

/// One immutable generated source Artifact with its portable candidate path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CandidateSourceFile {
    path: BundlePath,
    kind: GeneratedSourceKind,
    artifact: ArtifactDescriptor,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateSourceFileDocument {
    path: BundlePath,
    kind: GeneratedSourceKind,
    artifact: ArtifactDescriptor,
}

impl<'de> Deserialize<'de> for CandidateSourceFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = CandidateSourceFileDocument::deserialize(deserializer)?;
        Self::new(document.path, document.kind, document.artifact).map_err(serde::de::Error::custom)
    }
}

impl CandidateSourceFile {
    /// Creates one bounded source reference under `generated/`.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid path, empty Artifact, or non-text media type.
    pub fn new(
        path: BundlePath,
        kind: GeneratedSourceKind,
        artifact: ArtifactDescriptor,
    ) -> Result<Self, CandidateSourceError> {
        if !path.as_str().starts_with("generated/") {
            return Err(CandidateSourceError::OutsideGeneratedRoot);
        }
        if artifact.size_bytes == 0 || artifact.media_type != "text/plain; charset=utf-8" {
            return Err(CandidateSourceError::InvalidArtifact);
        }
        Ok(Self {
            path,
            kind,
            artifact,
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
    pub const fn artifact(&self) -> &ArtifactDescriptor {
        &self.artifact
    }
}

/// Content-addressed candidate source set submitted by one stable Agent tool operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CandidateSourceManifest {
    schema_version: u16,
    candidate_id: CandidateId,
    task_id: TaskId,
    parent_candidate_id: Option<CandidateId>,
    migration_spec_digest: Sha256Digest,
    generation_strategy: GenerationStrategy,
    public_symbol: String,
    input_source_paths: BTreeSet<BundlePath>,
    source_bundle_digest: Sha256Digest,
    files: Vec<CandidateSourceFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateSourceManifestDocument {
    schema_version: u16,
    candidate_id: CandidateId,
    task_id: TaskId,
    parent_candidate_id: Option<CandidateId>,
    migration_spec_digest: Sha256Digest,
    generation_strategy: GenerationStrategy,
    public_symbol: String,
    input_source_paths: BTreeSet<BundlePath>,
    source_bundle_digest: Sha256Digest,
    files: Vec<CandidateSourceFile>,
}

impl<'de> Deserialize<'de> for CandidateSourceManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = CandidateSourceManifestDocument::deserialize(deserializer)?;
        if document.schema_version != CANDIDATE_SOURCE_MANIFEST_SCHEMA_V1 {
            return Err(serde::de::Error::custom(
                "unsupported candidate manifest schema",
            ));
        }
        Self::new(CandidateSourceManifestSpec {
            candidate_id: document.candidate_id,
            task_id: document.task_id,
            parent_candidate_id: document.parent_candidate_id,
            migration_spec_digest: document.migration_spec_digest,
            generation_strategy: document.generation_strategy,
            public_symbol: document.public_symbol,
            input_source_paths: document.input_source_paths,
            source_bundle_digest: document.source_bundle_digest,
            files: document.files,
        })
        .map_err(serde::de::Error::custom)
    }
}

/// Trusted facts used to build a candidate manifest around untrusted source bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateSourceManifestSpec {
    pub candidate_id: CandidateId,
    pub task_id: TaskId,
    pub parent_candidate_id: Option<CandidateId>,
    pub migration_spec_digest: Sha256Digest,
    pub generation_strategy: GenerationStrategy,
    pub public_symbol: String,
    pub input_source_paths: BTreeSet<BundlePath>,
    pub source_bundle_digest: Sha256Digest,
    pub files: Vec<CandidateSourceFile>,
}

impl CandidateSourceManifest {
    /// Validates source categories and immutable lineage without assigning a Gate verdict.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete lineage, duplicate paths, or missing source categories.
    pub fn new(spec: CandidateSourceManifestSpec) -> Result<Self, CandidateSourceError> {
        if spec.public_symbol.trim().is_empty() || spec.input_source_paths.is_empty() {
            return Err(CandidateSourceError::IncompleteLineage);
        }
        let mut paths = BTreeSet::new();
        let mut kinds = BTreeSet::new();
        for file in &spec.files {
            if !paths.insert(file.path.clone()) {
                return Err(CandidateSourceError::DuplicatePath);
            }
            kinds.insert(file.kind);
        }
        if GeneratedSourceKind::ALL
            .iter()
            .any(|required| !kinds.contains(required))
        {
            return Err(CandidateSourceError::MissingSourceCategory);
        }
        Ok(Self {
            schema_version: CANDIDATE_SOURCE_MANIFEST_SCHEMA_V1,
            candidate_id: spec.candidate_id,
            task_id: spec.task_id,
            parent_candidate_id: spec.parent_candidate_id,
            migration_spec_digest: spec.migration_spec_digest,
            generation_strategy: spec.generation_strategy,
            public_symbol: spec.public_symbol,
            input_source_paths: spec.input_source_paths,
            source_bundle_digest: spec.source_bundle_digest,
            files: spec.files,
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
    pub const fn migration_spec_digest(&self) -> Sha256Digest {
        self.migration_spec_digest
    }

    #[must_use]
    pub const fn generation_strategy(&self) -> GenerationStrategy {
        self.generation_strategy
    }

    #[must_use]
    pub const fn source_bundle_digest(&self) -> Sha256Digest {
        self.source_bundle_digest
    }

    #[must_use]
    pub fn public_symbol(&self) -> &str {
        &self.public_symbol
    }

    #[must_use]
    pub const fn input_source_paths(&self) -> &BTreeSet<BundlePath> {
        &self.input_source_paths
    }

    #[must_use]
    pub fn files(&self) -> &[CandidateSourceFile] {
        &self.files
    }

    /// Returns whether this manifest is bound to the controller-owned migration context.
    #[must_use]
    pub fn matches_context(
        &self,
        task_id: &TaskId,
        migration_spec_digest: Sha256Digest,
        generation_strategy: GenerationStrategy,
        public_symbol: &str,
        input_source_paths: &BTreeSet<BundlePath>,
    ) -> bool {
        &self.task_id == task_id
            && self.migration_spec_digest == migration_spec_digest
            && self.generation_strategy == generation_strategy
            && self.public_symbol == public_symbol
            && &self.input_source_paths == input_source_paths
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceGateFailureKind {
    ArtifactSetMismatch,
    ArtifactIdentityMismatch,
    NonUtf8Source,
    ForbiddenFallback,
    MissingAscendCKernel,
    MissingHostEntryPoint,
    MissingCorrectnessAbi,
    MissingBuildReference,
    MissingBuildTarget,
    IncompleteComponentMapping,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceGateFailure {
    pub kind: SourceGateFailureKind,
    pub path: Option<BundlePath>,
    pub detail: String,
}

/// Independently authored Source Gate result over exact immutable source Artifacts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceGateReceipt {
    schema_version: u16,
    gate_revision: String,
    candidate_id: CandidateId,
    manifest_digest: Sha256Digest,
    passed: bool,
    inspected_artifacts: Vec<Sha256Digest>,
    failures: Vec<SourceGateFailure>,
}

impl SourceGateReceipt {
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.passed
    }

    #[must_use]
    pub fn failures(&self) -> &[SourceGateFailure] {
        &self.failures
    }

    /// Computes the canonical identity of this receipt.
    ///
    /// # Errors
    ///
    /// Returns an error only when the receipt cannot be serialized.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

/// Evaluates source structure without compiling, executing, or trusting materialized workspace data.
#[must_use]
pub fn evaluate_source_gate(
    manifest: &CandidateSourceManifest,
    manifest_digest: Sha256Digest,
    sources: &BTreeMap<BundlePath, Vec<u8>>,
) -> SourceGateReceipt {
    let mut failures = Vec::new();
    let expected: BTreeSet<_> = manifest
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect();
    let actual: BTreeSet<_> = sources.keys().cloned().collect();
    if expected != actual {
        failures.push(failure(
            SourceGateFailureKind::ArtifactSetMismatch,
            None,
            "source Artifact set does not exactly match the candidate manifest",
        ));
    }
    let mut text = BTreeMap::new();
    for file in &manifest.files {
        let Some(bytes) = sources.get(&file.path) else {
            continue;
        };
        if file.artifact.size_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            || file.artifact.digest != Sha256Digest::digest_bytes(bytes)
        {
            failures.push(failure(
                SourceGateFailureKind::ArtifactIdentityMismatch,
                Some(file.path.clone()),
                "source bytes do not match the immutable Artifact identity",
            ));
            continue;
        }
        match std::str::from_utf8(bytes) {
            Ok(value) => {
                text.insert(file.path.clone(), (file.kind, value));
            }
            Err(_) => failures.push(failure(
                SourceGateFailureKind::NonUtf8Source,
                Some(file.path.clone()),
                "generated source must be UTF-8",
            )),
        }
    }
    inspect_forbidden(&text, &mut failures);
    inspect_device(&text, &mut failures);
    inspect_host(manifest, &text, &mut failures);
    inspect_build(manifest, &text, &mut failures);
    inspect_mapping(manifest, &text, &mut failures);
    failures.sort_by(|left, right| (&left.kind, &left.path).cmp(&(&right.kind, &right.path)));
    SourceGateReceipt {
        schema_version: SOURCE_GATE_RECEIPT_SCHEMA_V1,
        gate_revision: SOURCE_GATE_REVISION_V2.to_owned(),
        candidate_id: manifest.candidate_id.clone(),
        manifest_digest,
        passed: failures.is_empty(),
        inspected_artifacts: manifest
            .files
            .iter()
            .map(|file| file.artifact.digest)
            .collect(),
        failures,
    }
}

fn inspect_forbidden(
    sources: &BTreeMap<BundlePath, (GeneratedSourceKind, &str)>,
    failures: &mut Vec<SourceGateFailure>,
) {
    const FORBIDDEN: [&str; 8] = [
        "torch::",
        "at::",
        "aclnn",
        "aclop",
        "tbe",
        "tvm",
        "fallback(",
        "prebuilt",
    ];
    for (path, (_, contents)) in sources {
        let lowercase = contents.to_ascii_lowercase();
        if FORBIDDEN.iter().any(|token| lowercase.contains(token)) {
            failures.push(failure(
                SourceGateFailureKind::ForbiddenFallback,
                Some(path.clone()),
                "generated source references a forbidden framework, prebuilt operator, or fallback",
            ));
        }
    }
}

fn inspect_device(
    sources: &BTreeMap<BundlePath, (GeneratedSourceKind, &str)>,
    failures: &mut Vec<SourceGateFailure>,
) {
    let valid = sources.values().any(|(kind, contents)| {
        *kind == GeneratedSourceKind::AscendCDevice
            && contents.contains("kernel_operator.h")
            && contents.contains("__aicore__")
            && (contents.contains("GM_ADDR") || contents.contains("GlobalTensor"))
    });
    if !valid {
        failures.push(failure(
            SourceGateFailureKind::MissingAscendCKernel,
            None,
            "device sources contain no structural Ascend C kernel evidence",
        ));
    }
}

fn inspect_host(
    manifest: &CandidateSourceManifest,
    sources: &BTreeMap<BundlePath, (GeneratedSourceKind, &str)>,
    failures: &mut Vec<SourceGateFailure>,
) {
    let valid = sources.values().any(|(kind, contents)| {
        *kind == GeneratedSourceKind::AscendHost
            && contents.contains(manifest.public_symbol())
            && (contents.contains("aclrtlaunch_") || contents.contains("ACLRT_LAUNCH_KERNEL"))
    });
    if !valid {
        failures.push(failure(
            SourceGateFailureKind::MissingHostEntryPoint,
            None,
            "host sources do not preserve the public symbol and an Ascend C launch path",
        ));
    }
    let abi = sources.values().any(|(kind, contents)| {
        *kind == GeneratedSourceKind::AscendHost
            && contents.contains("extern \"C\"")
            && contents.contains("int alloyport_reduce_sum_f32")
            && contents.contains("const float")
            && contents.contains("size_t")
            && contents.contains("float *")
    });
    if !abi {
        failures.push(failure(
            SourceGateFailureKind::MissingCorrectnessAbi,
            None,
            "host sources do not expose int alloyport_reduce_sum_f32(const float *, size_t, float *)",
        ));
    }
}

fn inspect_build(
    manifest: &CandidateSourceManifest,
    sources: &BTreeMap<BundlePath, (GeneratedSourceKind, &str)>,
    failures: &mut Vec<SourceGateFailure>,
) {
    let build = sources
        .values()
        .filter(|(kind, _)| *kind == GeneratedSourceKind::BuildIntegration)
        .map(|(_, contents)| *contents)
        .collect::<Vec<_>>()
        .join("\n");
    let missing = manifest.files.iter().any(|file| {
        matches!(
            file.kind,
            GeneratedSourceKind::AscendCDevice | GeneratedSourceKind::AscendHost
        ) && !build.contains(file.path.as_str().rsplit('/').next().unwrap_or_default())
    });
    if missing {
        failures.push(failure(
            SourceGateFailureKind::MissingBuildReference,
            None,
            "build integration does not reference every generated device and host source",
        ));
    }
    if !build.contains("alloyport_reduction_candidate") {
        failures.push(failure(
            SourceGateFailureKind::MissingBuildTarget,
            None,
            "build integration does not define the fixed alloyport_reduction_candidate target",
        ));
    }
}

fn inspect_mapping(
    manifest: &CandidateSourceManifest,
    sources: &BTreeMap<BundlePath, (GeneratedSourceKind, &str)>,
    failures: &mut Vec<SourceGateFailure>,
) {
    let mapping = sources
        .values()
        .filter(|(kind, _)| *kind == GeneratedSourceKind::ComponentMapping)
        .map(|(_, contents)| *contents)
        .collect::<Vec<_>>()
        .join("\n");
    let maps_inputs = manifest
        .input_source_paths
        .iter()
        .all(|path| mapping.contains(path.as_str()));
    let maps_outputs = manifest
        .files
        .iter()
        .filter(|file| file.kind != GeneratedSourceKind::ComponentMapping)
        .all(|file| mapping.contains(file.path.as_str()));
    if !maps_inputs || !maps_outputs {
        failures.push(failure(
            SourceGateFailureKind::IncompleteComponentMapping,
            None,
            "component mapping does not cover every input and generated implementation source",
        ));
    }
}

fn failure(
    kind: SourceGateFailureKind,
    path: Option<BundlePath>,
    detail: &str,
) -> SourceGateFailure {
    SourceGateFailure {
        kind,
        path,
        detail: detail.to_owned(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateSourceError {
    OutsideGeneratedRoot,
    InvalidArtifact,
    IncompleteLineage,
    DuplicatePath,
    MissingSourceCategory,
}

impl Display for CandidateSourceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid candidate source manifest: {self:?}")
    }
}

impl Error for CandidateSourceError {}

#[cfg(test)]
#[path = "candidate_source_tests.rs"]
mod tests;
