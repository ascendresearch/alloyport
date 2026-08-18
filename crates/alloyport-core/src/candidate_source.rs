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
    build_target: String,
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
    build_target: String,
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
            build_target: document.build_target,
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
    pub build_target: String,
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
        if spec.public_symbol.trim().is_empty()
            || spec.build_target.trim().is_empty()
            || spec.input_source_paths.is_empty()
        {
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
            build_target: spec.build_target,
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

    /// The candidate this one was assembled from, when it inherited rather than started fresh.
    #[must_use]
    pub const fn parent_candidate_id(&self) -> Option<&CandidateId> {
        self.parent_candidate_id.as_ref()
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

    /// Build target the migration declares, carried here so the gate never names a specimen.
    #[must_use]
    pub fn build_target(&self) -> &str {
        &self.build_target
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
        build_target: &str,
        input_source_paths: &BTreeSet<BundlePath>,
    ) -> bool {
        &self.task_id == task_id
            && self.migration_spec_digest == migration_spec_digest
            && self.generation_strategy == generation_strategy
            && self.public_symbol == public_symbol
            && self.build_target == build_target
            && &self.input_source_paths == input_source_paths
    }
}

/// Whether a finding stops the candidate or is reported to the model and moved past.
///
/// The split exists because this gate reads text. Text can prove that a candidate crossed the
/// product boundary — it delegates to a framework, it drops the public symbol, it does not build
/// what the harness will link — and those block. Text cannot prove that a kernel is a good kernel,
/// and a gate that pretends otherwise both hands the model the answer and refuses correct work that
/// is spelled differently. Those findings are advisory: the compiler and the Correctness Gate are
/// the judges of the implementation, and they run downstream.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceGateSeverity {
    Blocking,
    Advisory,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceGateFailureKind {
    ArtifactSetMismatch,
    ArtifactIdentityMismatch,
    NonUtf8Source,
    ForbiddenFallback,
    MissingDeviceSource,
    MissingHostEntryPoint,
    MissingBuildReference,
    MissingBuildTarget,
    IncompleteComponentMapping,
    UnrecognizedKernelStructure,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceGateFailure {
    pub kind: SourceGateFailureKind,
    pub severity: SourceGateSeverity,
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
    /// True when nothing blocking was found. Advisories do not stop a candidate.
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.passed
    }

    /// Every finding, blocking and advisory alike, in a stable order.
    #[must_use]
    pub fn failures(&self) -> &[SourceGateFailure] {
        &self.failures
    }

    /// Findings that stopped the candidate.
    pub fn blocking(&self) -> impl Iterator<Item = &SourceGateFailure> {
        self.failures
            .iter()
            .filter(|item| item.severity == SourceGateSeverity::Blocking)
    }

    /// Findings reported to the model that did not stop the candidate.
    pub fn advisories(&self) -> impl Iterator<Item = &SourceGateFailure> {
        self.failures
            .iter()
            .filter(|item| item.severity == SourceGateSeverity::Advisory)
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
        failures.push(blocking(
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
            failures.push(blocking(
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
            Err(_) => failures.push(blocking(
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
    failures.sort_by(|left, right| {
        (&left.severity, &left.kind, &left.path).cmp(&(&right.severity, &right.kind, &right.path))
    });
    let passed = failures
        .iter()
        .all(|item| item.severity == SourceGateSeverity::Advisory);
    SourceGateReceipt {
        schema_version: SOURCE_GATE_RECEIPT_SCHEMA_V1,
        gate_revision: SOURCE_GATE_REVISION_V2.to_owned(),
        candidate_id: manifest.candidate_id.clone(),
        manifest_digest,
        passed,
        inspected_artifacts: manifest
            .files
            .iter()
            .map(|file| file.artifact.digest)
            .collect(),
        failures,
    }
}

/// Delegating the computation to somebody else's operator is the one thing a migration may not do.
///
/// This is the product boundary — the deliverable is maintainable Ascend C, not a call into a
/// framework or a prebuilt kernel — so it blocks. It deliberately does not say how the kernel must
/// be written; the tokens name other people's libraries, not our preferred spelling of ours.
fn inspect_forbidden(
    sources: &BTreeMap<BundlePath, (GeneratedSourceKind, &str)>,
    failures: &mut Vec<SourceGateFailure>,
) {
    const FORBIDDEN: [&str; 9] = [
        "torch/", "torch::", "at::", "aclnn", "aclop", "tbe/", "tbe::", "tvm/", "tvm::",
    ];
    for (path, (_, contents)) in sources {
        let lowercase = contents.to_ascii_lowercase();
        if let Some(token) = FORBIDDEN.iter().find(|token| lowercase.contains(**token)) {
            failures.push(blocking(
                SourceGateFailureKind::ForbiddenFallback,
                Some(path.clone()),
                format!(
                    "generated source references {token}: the deliverable is Ascend C, not a call \
                     into a framework or a prebuilt operator"
                ),
            ));
        }
    }
}

/// Markers an Ascend C device source usually carries. Their absence is reported, never enforced.
const ASCEND_C_MARKERS: [&str; 6] = [
    "__aicore__",
    "kernel_operator.h",
    "GM_ADDR",
    "GlobalTensor",
    "AscendC::",
    "tensor_api",
];

fn inspect_device(
    sources: &BTreeMap<BundlePath, (GeneratedSourceKind, &str)>,
    failures: &mut Vec<SourceGateFailure>,
) {
    let device: Vec<_> = sources
        .iter()
        .filter(|(_, (kind, _))| *kind == GeneratedSourceKind::AscendCDevice)
        .collect();
    if device
        .iter()
        .all(|(_, (_, contents))| contents.trim().is_empty())
    {
        failures.push(blocking(
            SourceGateFailureKind::MissingDeviceSource,
            None,
            "the candidate declares no non-empty Ascend C device source",
        ));
        return;
    }
    // ADVISORY ONLY. An earlier revision required `kernel_operator.h`, `__aicore__`, and
    // `GM_ADDR`/`GlobalTensor` in the device source and refused the candidate otherwise. That both
    // handed the model the tokens to emit — which any wrong kernel can also emit — and would reject
    // a correct one written against a different Ascend C surface, such as the low-level `Te` tensor
    // API. Whether the source is a valid Ascend C kernel is decided by the compiler on the Build
    // Gate and by the Correctness Gate after it; a substring is not evidence either way.
    for (path, (_, contents)) in device {
        if !ASCEND_C_MARKERS
            .iter()
            .any(|marker| contents.contains(marker))
        {
            failures.push(advisory(
                SourceGateFailureKind::UnrecognizedKernelStructure,
                Some(path.clone()),
                "device source carries none of the usual Ascend C markers; the compiler decides",
            ));
        }
    }
}

/// The migration must still be callable the way its callers call it today.
fn inspect_host(
    manifest: &CandidateSourceManifest,
    sources: &BTreeMap<BundlePath, (GeneratedSourceKind, &str)>,
    failures: &mut Vec<SourceGateFailure>,
) {
    let symbol = manifest.public_symbol();
    let preserved = sources.values().any(|(kind, contents)| {
        *kind == GeneratedSourceKind::AscendHost
            && contents.contains(symbol)
            && contents.contains("extern \"C\"")
    });
    if !preserved {
        failures.push(blocking(
            SourceGateFailureKind::MissingHostEntryPoint,
            None,
            format!("no host source exposes the migration's public symbol {symbol} with C linkage"),
        ));
    }
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or_default()
}

/// Every generated source the build reaches: named by a build file, or included by one it names.
///
/// The earlier rule required each source to appear in the build text itself, which forbids the
/// composition CANN's own `CMake` language package expects — one translation unit listed, the kernel
/// `#include`d into it. This repository's person-written specimen, `fixtures/ascend-add-v1`, is
/// written that way and would have been refused by its own gate. That is what walking the honest
/// path through a gate is for, and nobody had walked this one against a supported build.
///
/// Still text, and still blocking: a source nothing reaches is a source the compiler never sees, and
/// the harness would link a candidate that does not contain it.
fn buildable_names<'a>(
    build: &str,
    sources: &'a BTreeMap<BundlePath, (GeneratedSourceKind, &'a str)>,
) -> BTreeSet<&'a str> {
    let mut reached: BTreeSet<&str> = sources
        .keys()
        .map(|path| file_name(path.as_str()))
        .filter(|name| build.contains(*name))
        .collect();
    // Includes can nest, and the fixed point is reached in at most one pass per file.
    for _ in 0..sources.len() {
        let mut grew = false;
        for (path, (_, contents)) in sources {
            if !reached.contains(file_name(path.as_str())) {
                continue;
            }
            for candidate in sources.keys().map(|other| file_name(other.as_str())) {
                if !reached.contains(candidate) && includes(contents, candidate) {
                    reached.insert(candidate);
                    grew = true;
                }
            }
        }
        if !grew {
            break;
        }
    }
    reached
}

/// Whether `contents` has an `#include` naming `target`, rather than merely mentioning it.
fn includes(contents: &str, target: &str) -> bool {
    contents.lines().any(|line| {
        let line = line.trim_start();
        line.starts_with("#include") && line.contains(target)
    })
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
    // Naming the paths rather than the rule. The check already knows exactly which files are
    // unreferenced; withholding them made the model guess at a format it could not find, and cost
    // two to four turns on every run that reached this gate.
    let reachable = buildable_names(&build, sources);
    let missing: Vec<&str> = manifest
        .files
        .iter()
        .filter(|file| {
            matches!(
                file.kind,
                GeneratedSourceKind::AscendCDevice | GeneratedSourceKind::AscendHost
            ) && !reachable.contains(file_name(file.path.as_str()))
        })
        .map(|file| file.path.as_str())
        .collect();
    if !missing.is_empty() {
        failures.push(blocking(
            SourceGateFailureKind::MissingBuildReference,
            None,
            format!(
                "build integration does not reach these generated sources: {}. A source counts as \
                 reached when the build files name it, or when a source they do name includes it.",
                missing.join(", ")
            ),
        ));
    }
    let target = manifest.build_target();
    if !build.contains(target) {
        failures.push(blocking(
            SourceGateFailureKind::MissingBuildTarget,
            None,
            format!("build integration does not define the migration's {target} target"),
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
    let mut uncovered: Vec<&str> = manifest
        .input_source_paths
        .iter()
        .map(BundlePath::as_str)
        .filter(|path| !mapping.contains(path))
        .collect();
    uncovered.extend(
        manifest
            .files
            .iter()
            .filter(|file| file.kind != GeneratedSourceKind::ComponentMapping)
            .map(|file| file.path.as_str())
            .filter(|path| !mapping.contains(path)),
    );
    if !uncovered.is_empty() {
        failures.push(blocking(
            SourceGateFailureKind::IncompleteComponentMapping,
            None,
            format!(
                "component mapping does not mention these paths: {}. Each input and generated \
                 implementation source must appear somewhere in the mapping document; the gate \
                 looks for the path text and does not require any particular format.",
                uncovered.join(", ")
            ),
        ));
    }
}

fn blocking(
    kind: SourceGateFailureKind,
    path: Option<BundlePath>,
    detail: impl Into<String>,
) -> SourceGateFailure {
    SourceGateFailure {
        kind,
        severity: SourceGateSeverity::Blocking,
        path,
        detail: detail.into(),
    }
}

fn advisory(
    kind: SourceGateFailureKind,
    path: Option<BundlePath>,
    detail: impl Into<String>,
) -> SourceGateFailure {
    SourceGateFailure {
        kind,
        severity: SourceGateSeverity::Advisory,
        path,
        detail: detail.into(),
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
