//! Untrusted candidate-authoring input and output contracts.

use crate::{
    BundlePath, GenerationStrategy, MigrationInspection, MigrationSpec, Sha256Digest,
    inspect_migration_source,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// The only candidate-authoring request schema currently emitted by `AlloyPort`.
pub const AUTHORING_REQUEST_SCHEMA_V1: u16 = 1;
const GENERATED_ROOT: &str = "generated/";
const MAX_GENERATED_FILES: usize = 32;
const MAX_GENERATED_FILE_BYTES: usize = 1024 * 1024;

/// One declared source file sent to a candidate author.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceDocument {
    path: BundlePath,
    contents: String,
}

impl SourceDocument {
    #[must_use]
    pub const fn path(&self) -> &BundlePath {
        &self.path
    }

    #[must_use]
    pub fn contents(&self) -> &str {
        &self.contents
    }
}

/// Immutable input for one untrusted candidate-authoring invocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CandidateAuthoringRequest {
    schema_version: u16,
    migration_spec: MigrationSpec,
    migration_spec_digest: Sha256Digest,
    declared_source_digest: Sha256Digest,
    inspection: MigrationInspection,
    generation_strategy: GenerationStrategy,
    sources: Vec<SourceDocument>,
}

impl CandidateAuthoringRequest {
    /// Constructs a request only from a passing, reproducible intake inspection.
    ///
    /// Undeclared files are intentionally excluded from the model context. Recomputing the
    /// inspection here prevents callers from pairing stale evidence with different source bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the supplied inspection failed or does not exactly describe `spec`
    /// and its declared files.
    pub fn new(
        spec: MigrationSpec,
        inspection: MigrationInspection,
        files: &BTreeMap<BundlePath, String>,
        generation_strategy: GenerationStrategy,
    ) -> Result<Self, CandidateAuthoringError> {
        if !inspection.passed {
            return Err(CandidateAuthoringError::InspectionDidNotPass);
        }
        if inspection.migration_spec_digest != spec.digest() {
            return Err(CandidateAuthoringError::MigrationSpecMismatch);
        }

        let reproduced = inspect_migration_source(&spec, files);
        if !reproduced.passed {
            return Err(CandidateAuthoringError::SourceInspectionDidNotPass);
        }
        if reproduced != inspection {
            return Err(CandidateAuthoringError::InspectionMismatch);
        }

        let declared_paths = spec
            .sources()
            .device_sources()
            .iter()
            .chain(spec.sources().host_sources())
            .chain(spec.sources().build_files());
        let mut sources = Vec::new();
        for path in declared_paths {
            let Some(contents) = files.get(path) else {
                return Err(CandidateAuthoringError::SourceInspectionDidNotPass);
            };
            sources.push(SourceDocument {
                path: path.clone(),
                contents: contents.clone(),
            });
        }

        Ok(Self {
            schema_version: AUTHORING_REQUEST_SCHEMA_V1,
            migration_spec_digest: inspection.migration_spec_digest,
            declared_source_digest: inspection.declared_source_digest,
            migration_spec: spec,
            inspection,
            generation_strategy,
            sources,
        })
    }

    /// Content identity used to bind a proposal to exactly this model-visible request.
    #[must_use]
    pub fn digest(&self) -> Sha256Digest {
        let mut input = b"alloyport-candidate-authoring-request-v1\0".to_vec();
        input.extend_from_slice(&self.schema_version.to_be_bytes());
        input.extend_from_slice(&self.migration_spec_digest.bytes());
        input.extend_from_slice(&self.declared_source_digest.bytes());
        input.push(match self.generation_strategy {
            GenerationStrategy::DirectAscendC => 1,
            GenerationStrategy::AscendSimtBootstrap => 2,
            GenerationStrategy::VerifiedTemplateAdaptation => 3,
            GenerationStrategy::MemoryGuidedSynthesis => 4,
        });
        Sha256Digest::digest_bytes(&input)
    }

    #[must_use]
    pub const fn migration_spec_digest(&self) -> Sha256Digest {
        self.migration_spec_digest
    }

    #[must_use]
    pub const fn declared_source_digest(&self) -> Sha256Digest {
        self.declared_source_digest
    }

    #[must_use]
    pub const fn generation_strategy(&self) -> GenerationStrategy {
        self.generation_strategy
    }

    #[must_use]
    pub fn sources(&self) -> &[SourceDocument] {
        &self.sources
    }
}

/// Stable category for a generated source deliverable.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GeneratedSourceKind {
    AscendCDevice,
    AscendHost,
    BuildIntegration,
    ComponentMapping,
}

impl GeneratedSourceKind {
    pub const ALL: [Self; 4] = [
        Self::AscendCDevice,
        Self::AscendHost,
        Self::BuildIntegration,
        Self::ComponentMapping,
    ];
}

/// One generated source file proposed by an untrusted candidate author.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GeneratedSourceFile {
    path: BundlePath,
    kind: GeneratedSourceKind,
    contents: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GeneratedSourceFileDocument {
    path: BundlePath,
    kind: GeneratedSourceKind,
    contents: String,
}

impl<'de> Deserialize<'de> for GeneratedSourceFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = GeneratedSourceFileDocument::deserialize(deserializer)?;
        Self::new(document.path, document.kind, document.contents).map_err(serde::de::Error::custom)
    }
}

impl GeneratedSourceFile {
    /// Builds one bounded generated file under the isolated `generated/` tree.
    ///
    /// # Errors
    ///
    /// Returns an error for a source-overwriting path or empty/oversized contents.
    pub fn new(
        path: BundlePath,
        kind: GeneratedSourceKind,
        contents: impl Into<String>,
    ) -> Result<Self, GeneratedSourceError> {
        if !path.as_str().starts_with(GENERATED_ROOT) || path.as_str().len() == GENERATED_ROOT.len()
        {
            return Err(GeneratedSourceError::OutsideGeneratedRoot(
                path.as_str().to_owned(),
            ));
        }
        let contents = contents.into();
        if contents.trim().is_empty() {
            return Err(GeneratedSourceError::EmptyFile(path.as_str().to_owned()));
        }
        if contents.len() > MAX_GENERATED_FILE_BYTES {
            return Err(GeneratedSourceError::FileTooLarge(path.as_str().to_owned()));
        }
        Ok(Self {
            path,
            kind,
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
    pub fn contents(&self) -> &str {
        &self.contents
    }
}

/// Complete, still-untrusted source proposal returned by a runtime model.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GeneratedSourceBundle {
    files: Vec<GeneratedSourceFile>,
    author_notes: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GeneratedSourceBundleDocument {
    files: Vec<GeneratedSourceFile>,
    #[serde(default)]
    author_notes: Vec<String>,
}

impl<'de> Deserialize<'de> for GeneratedSourceBundle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = GeneratedSourceBundleDocument::deserialize(deserializer)?;
        Self::new(document.files, document.author_notes).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for GeneratedSourceChange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = GeneratedSourceBundleDocument::deserialize(deserializer)?;
        Self::new(document.files, document.author_notes).map_err(serde::de::Error::custom)
    }
}

impl GeneratedSourceBundle {
    /// Validates completeness without assigning any Gate or release authority.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing deliverable category, duplicate path, excessive file count,
    /// or empty note.
    pub fn new(
        files: Vec<GeneratedSourceFile>,
        author_notes: Vec<String>,
    ) -> Result<Self, GeneratedSourceError> {
        if files.len() > MAX_GENERATED_FILES {
            return Err(GeneratedSourceError::TooManyFiles(files.len()));
        }
        let mut paths = BTreeSet::new();
        let mut kinds = BTreeSet::new();
        for file in &files {
            if !paths.insert(file.path.clone()) {
                return Err(GeneratedSourceError::DuplicatePath(
                    file.path.as_str().to_owned(),
                ));
            }
            kinds.insert(file.kind);
        }
        for required in GeneratedSourceKind::ALL {
            if !kinds.contains(&required) {
                return Err(GeneratedSourceError::MissingKind(required));
            }
        }
        if author_notes.iter().any(|note| note.trim().is_empty()) {
            return Err(GeneratedSourceError::EmptyAuthorNote);
        }
        Ok(Self {
            files,
            author_notes,
        })
    }

    #[must_use]
    pub fn files(&self) -> &[GeneratedSourceFile] {
        &self.files
    }

    #[must_use]
    pub fn author_notes(&self) -> &[String] {
        &self.author_notes
    }

    /// Computes an order-independent identity of the complete untrusted proposal.
    #[must_use]
    pub fn digest(&self) -> Sha256Digest {
        generated_source_digest(&self.files, &self.author_notes)
    }
}

/// One authored change applied onto a parent candidate rather than a whole deliverable.
///
/// [`GeneratedSourceBundle`] means a complete four-part deliverable and must keep meaning that, so
/// a partial submission is a different type rather than a weakened one. Completeness is still
/// required — of the assembled candidate, by `CandidateSourceManifest`, which is where it belongs.
///
/// This exists because a complete bundle costs 90-100% of one model response: on the first
/// migration to reach a compiler, correcting a single `CMake` line meant re-emitting all four files
/// and the JSON truncated mid-string at exactly the output ceiling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedSourceChange {
    files: Vec<GeneratedSourceFile>,
    author_notes: Vec<String>,
}

impl GeneratedSourceChange {
    /// Validates everything a whole bundle validates except that it is whole.
    ///
    /// # Errors
    ///
    /// Returns an error for no files, a duplicate path, an excessive file count, or an empty note.
    pub fn new(
        files: Vec<GeneratedSourceFile>,
        author_notes: Vec<String>,
    ) -> Result<Self, GeneratedSourceError> {
        if files.len() > MAX_GENERATED_FILES {
            return Err(GeneratedSourceError::TooManyFiles(files.len()));
        }
        let mut paths = BTreeSet::new();
        for file in &files {
            if !paths.insert(file.path.clone()) {
                return Err(GeneratedSourceError::DuplicatePath(
                    file.path.as_str().to_owned(),
                ));
            }
        }
        if author_notes.iter().any(|note| note.trim().is_empty()) {
            return Err(GeneratedSourceError::EmptyAuthorNote);
        }
        Ok(Self {
            files,
            author_notes,
        })
    }

    #[must_use]
    pub fn files(&self) -> &[GeneratedSourceFile] {
        &self.files
    }

    #[must_use]
    pub fn author_notes(&self) -> &[String] {
        &self.author_notes
    }

    /// Identical to [`GeneratedSourceBundle::digest`], so a change that happens to be complete and
    /// the same bundle name the same thing.
    #[must_use]
    pub fn digest(&self) -> Sha256Digest {
        generated_source_digest(&self.files, &self.author_notes)
    }

    /// Confirms this change is a whole deliverable, for a submission with nothing to inherit.
    ///
    /// # Errors
    ///
    /// Returns the missing deliverable category.
    pub fn require_complete(&self) -> Result<(), GeneratedSourceError> {
        let kinds: BTreeSet<_> = self.files.iter().map(|file| file.kind).collect();
        for required in GeneratedSourceKind::ALL {
            if !kinds.contains(&required) {
                return Err(GeneratedSourceError::MissingKind(required));
            }
        }
        Ok(())
    }
}

fn generated_source_digest(files: &[GeneratedSourceFile], author_notes: &[String]) -> Sha256Digest {
    {
        let mut files: Vec<_> = files.iter().collect();
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let mut input = b"alloyport-generated-source-bundle-v1\0".to_vec();
        for file in files {
            input.extend_from_slice(&(file.path.as_str().len() as u64).to_be_bytes());
            input.extend_from_slice(file.path.as_str().as_bytes());
            input.push(match file.kind {
                GeneratedSourceKind::AscendCDevice => 1,
                GeneratedSourceKind::AscendHost => 2,
                GeneratedSourceKind::BuildIntegration => 3,
                GeneratedSourceKind::ComponentMapping => 4,
            });
            input.extend_from_slice(&(file.contents.len() as u64).to_be_bytes());
            input.extend_from_slice(file.contents.as_bytes());
        }
        let mut notes: Vec<_> = author_notes.iter().collect();
        notes.sort();
        for note in notes {
            input.extend_from_slice(&(note.len() as u64).to_be_bytes());
            input.extend_from_slice(note.as_bytes());
        }
        Sha256Digest::digest_bytes(&input)
    }
}

/// Adapter-authored facts for one runtime model invocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ModelInvocation {
    provider: String,
    model: String,
    prompt_revision: String,
    max_output_tokens: u32,
    temperature_millis: u16,
}

impl ModelInvocation {
    /// Validates model provenance before it can be attached to a proposal.
    ///
    /// # Errors
    ///
    /// Returns an error for missing identity, zero output budget, or temperature above 2.0.
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        prompt_revision: impl Into<String>,
        max_output_tokens: u32,
        temperature_millis: u16,
    ) -> Result<Self, CandidateAuthoringError> {
        let provider = provider.into();
        let model = model.into();
        let prompt_revision = prompt_revision.into();
        if provider.trim().is_empty()
            || model.trim().is_empty()
            || prompt_revision.trim().is_empty()
        {
            return Err(CandidateAuthoringError::MissingModelFact);
        }
        if max_output_tokens == 0 {
            return Err(CandidateAuthoringError::ZeroOutputBudget);
        }
        if temperature_millis > 2_000 {
            return Err(CandidateAuthoringError::InvalidTemperature(
                temperature_millis,
            ));
        }
        Ok(Self {
            provider,
            model,
            prompt_revision,
            max_output_tokens,
            temperature_millis,
        })
    }

    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    #[must_use]
    pub fn prompt_revision(&self) -> &str {
        &self.prompt_revision
    }

    #[must_use]
    pub const fn max_output_tokens(&self) -> u32 {
        self.max_output_tokens
    }

    #[must_use]
    pub const fn temperature_millis(&self) -> u16 {
        self.temperature_millis
    }
}

/// A model proposal bound to immutable input and adapter-owned provenance.
///
/// This type deliberately has no Gate, verdict, receipt, candidate ID, or release fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CandidateProposal {
    authoring_request_digest: Sha256Digest,
    migration_spec_digest: Sha256Digest,
    declared_source_digest: Sha256Digest,
    generation_strategy: GenerationStrategy,
    model_invocation: ModelInvocation,
    generated_source: GeneratedSourceBundle,
}

impl CandidateProposal {
    /// Attaches trusted request lineage and invocation facts to an untrusted generated bundle.
    #[must_use]
    pub fn from_authoring(
        request: &CandidateAuthoringRequest,
        model_invocation: ModelInvocation,
        generated_source: GeneratedSourceBundle,
    ) -> Self {
        Self {
            authoring_request_digest: request.digest(),
            migration_spec_digest: request.migration_spec_digest(),
            declared_source_digest: request.declared_source_digest(),
            generation_strategy: request.generation_strategy(),
            model_invocation,
            generated_source,
        }
    }

    #[must_use]
    pub const fn authoring_request_digest(&self) -> Sha256Digest {
        self.authoring_request_digest
    }

    #[must_use]
    pub const fn migration_spec_digest(&self) -> Sha256Digest {
        self.migration_spec_digest
    }

    #[must_use]
    pub const fn declared_source_digest(&self) -> Sha256Digest {
        self.declared_source_digest
    }

    #[must_use]
    pub const fn generation_strategy(&self) -> GenerationStrategy {
        self.generation_strategy
    }

    #[must_use]
    pub const fn model_invocation(&self) -> &ModelInvocation {
        &self.model_invocation
    }

    #[must_use]
    pub const fn generated_source(&self) -> &GeneratedSourceBundle {
        &self.generated_source
    }
}

/// Invalid immutable authoring input or adapter-owned invocation metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateAuthoringError {
    InspectionDidNotPass,
    SourceInspectionDidNotPass,
    MigrationSpecMismatch,
    InspectionMismatch,
    MissingModelFact,
    ZeroOutputBudget,
    InvalidTemperature(u16),
}

impl Display for CandidateAuthoringError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InspectionDidNotPass => write!(formatter, "intake inspection did not pass"),
            Self::SourceInspectionDidNotPass => {
                write!(formatter, "authoring source bytes fail intake inspection")
            }
            Self::MigrationSpecMismatch => {
                write!(formatter, "inspection belongs to another MigrationSpec")
            }
            Self::InspectionMismatch => {
                write!(
                    formatter,
                    "inspection does not match the authoring source bytes"
                )
            }
            Self::MissingModelFact => write!(formatter, "model invocation identity is incomplete"),
            Self::ZeroOutputBudget => {
                write!(formatter, "model output token budget must be nonzero")
            }
            Self::InvalidTemperature(value) => {
                write!(
                    formatter,
                    "model temperature {value} milli-units exceeds 2000"
                )
            }
        }
    }
}

impl Error for CandidateAuthoringError {}

/// Invalid model-authored source bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GeneratedSourceError {
    OutsideGeneratedRoot(String),
    EmptyFile(String),
    FileTooLarge(String),
    TooManyFiles(usize),
    DuplicatePath(String),
    MissingKind(GeneratedSourceKind),
    EmptyAuthorNote,
}

impl Display for GeneratedSourceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutsideGeneratedRoot(path) => {
                write!(formatter, "generated file {path:?} is outside generated/")
            }
            Self::EmptyFile(path) => write!(formatter, "generated file {path:?} is empty"),
            Self::FileTooLarge(path) => write!(formatter, "generated file {path:?} exceeds 1 MiB"),
            Self::TooManyFiles(count) => {
                write!(
                    formatter,
                    "generated bundle has {count} files; maximum is 32"
                )
            }
            Self::DuplicatePath(path) => write!(formatter, "duplicate generated path {path:?}"),
            Self::MissingKind(kind) => write!(formatter, "generated bundle is missing {kind:?}"),
            Self::EmptyAuthorNote => write!(formatter, "generated author note is empty"),
        }
    }
}

impl Error for GeneratedSourceError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AscendTarget, CudaSourceSet, PublicEntryPoint, ReferenceWorkload};

    fn path(value: &str) -> BundlePath {
        BundlePath::try_from(value).expect("valid test path")
    }

    fn intake() -> (MigrationSpec, BTreeMap<BundlePath, String>) {
        let spec = MigrationSpec::new_v1(
            "source-sha",
            CudaSourceSet::new(
                [path("src/reduce.cu")],
                [path("include/reduce.h"), path("src/reduce_host.cu")],
                [path("CMakeLists.txt")],
            )
            .expect("source set"),
            PublicEntryPoint::new(
                "reduce_sum",
                "sum contiguous fp32 input",
                "reduce_sum_candidate",
            )
            .expect("public entry"),
            ReferenceWorkload::new(
                path("."),
                ["./build/reference".to_owned()],
                "reference_library",
            )
            .expect("reference"),
            AscendTarget::new("Ascend950PR", "9.1", "ccec", "25.7", "acl-9.1").expect("target"),
            "1 <= elements <= 1024",
            Vec::new(),
            "return unsupported",
        )
        .expect("spec");
        let files = BTreeMap::from([
            (
                path("src/reduce.cu"),
                "__global__ void reduce(float *x) { int i = blockIdx.x * blockDim.x + threadIdx.x; }"
                    .to_owned(),
            ),
            (
                path("include/reduce.h"),
                "int reduce_sum(const float *input, float *output);".to_owned(),
            ),
            (
                path("src/reduce_host.cu"),
                "int reduce_sum(const float *input, float *output) { reduce<<<1, 1>>>(output); return cudaGetLastError(); }"
                    .to_owned(),
            ),
            (
                path("CMakeLists.txt"),
                "project(reduce LANGUAGES CXX CUDA)\nadd_library(reduce src/reduce.cu src/reduce_host.cu)"
                    .to_owned(),
            ),
        ]);
        (spec, files)
    }

    fn complete_bundle_json(extra: &str) -> String {
        format!(
            r#"{{
              "files": [
                {{"path":"generated/device/reduce.cpp","kind":"ascend_c_device","contents":"kernel"}},
                {{"path":"generated/host/reduce.cpp","kind":"ascend_host","contents":"host"}},
                {{"path":"generated/CMakeLists.txt","kind":"build_integration","contents":"build"}},
                {{"path":"generated/component-map.json","kind":"component_mapping","contents":"{{}}"}}
              ],
              "author_notes": ["unverified proposal"]{extra}
            }}"#
        )
    }

    #[test]
    fn request_reproduces_inspection_and_excludes_undeclared_files() {
        let (spec, mut files) = intake();
        let inspection = inspect_migration_source(&spec, &files);
        files.insert(path("secret.txt"), "not model context".to_owned());
        let request = CandidateAuthoringRequest::new(
            spec,
            inspection,
            &files,
            GenerationStrategy::DirectAscendC,
        )
        .expect("reproducible request");
        assert_eq!(request.sources().len(), 4);
        assert!(
            request
                .sources()
                .iter()
                .all(|source| source.path().as_str() != "secret.txt")
        );
    }

    #[test]
    fn stale_inspection_cannot_author_another_source_snapshot() {
        let (spec, mut files) = intake();
        let inspection = inspect_migration_source(&spec, &files);
        files
            .get_mut(&path("src/reduce.cu"))
            .expect("device source")
            .push_str(" // changed");
        assert_eq!(
            CandidateAuthoringRequest::new(
                spec,
                inspection,
                &files,
                GenerationStrategy::DirectAscendC,
            ),
            Err(CandidateAuthoringError::InspectionMismatch)
        );
    }

    #[test]
    fn generated_bundle_requires_every_source_category() {
        let mut document: serde_json::Value =
            serde_json::from_str(&complete_bundle_json("")).expect("complete bundle JSON");
        document["files"].as_array_mut().expect("files array").pop();
        let json = serde_json::to_string(&document).expect("incomplete bundle JSON");
        let error = serde_json::from_str::<GeneratedSourceBundle>(&json)
            .expect_err("component mapping is mandatory");
        assert!(error.to_string().contains("ComponentMapping"));
    }

    #[test]
    fn generated_bundle_rejects_path_escape_and_source_overwrite() {
        let traversal = complete_bundle_json("")
            .replace("generated/device/reduce.cpp", "generated/../src/reduce.cu");
        assert!(serde_json::from_str::<GeneratedSourceBundle>(&traversal).is_err());

        let overwrite =
            complete_bundle_json("").replace("generated/device/reduce.cpp", "src/reduce.cu");
        let error = serde_json::from_str::<GeneratedSourceBundle>(&overwrite)
            .expect_err("model cannot overwrite intake source");
        assert!(error.to_string().contains("outside generated/"));
    }

    #[test]
    fn generated_bundle_rejects_authority_fields() {
        let json = complete_bundle_json(", \"verdict\": {\"passed\": true}");
        let error = serde_json::from_str::<GeneratedSourceBundle>(&json)
            .expect_err("model cannot submit a verdict");
        assert!(error.to_string().contains("unknown field `verdict`"));
    }
}
