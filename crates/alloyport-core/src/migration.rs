//! Immutable contract for one bounded CUDA-to-Ascend-C migration.

use crate::Sha256Digest;
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// The only schema currently accepted by the Phase-1 product path.
pub const MIGRATION_SPEC_SCHEMA_V1: u16 = 1;

/// A portable path inside an immutable source or workload bundle.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BundlePath(String);

impl<'de> Deserialize<'de> for BundlePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

impl BundlePath {
    /// Returns the validated bundle-relative path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for BundlePath {
    type Error = MigrationSpecError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_owned())
    }
}

impl TryFrom<String> for BundlePath {
    type Error = MigrationSpecError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.trim().is_empty()
            || value.starts_with('/')
            || value.contains('\0')
            || value.contains('\\')
            || (value != "."
                && value
                    .split('/')
                    .any(|component| component.is_empty() || matches!(component, "." | "..")))
        {
            return Err(MigrationSpecError::InvalidBundlePath(value));
        }
        Ok(Self(value))
    }
}

/// CUDA source and build files included in the migration boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "CudaSourceSetDocument")]
pub struct CudaSourceSet {
    device_sources: BTreeSet<BundlePath>,
    host_sources: BTreeSet<BundlePath>,
    build_files: BTreeSet<BundlePath>,
}

#[derive(Deserialize)]
struct CudaSourceSetDocument {
    device_sources: BTreeSet<BundlePath>,
    host_sources: BTreeSet<BundlePath>,
    build_files: BTreeSet<BundlePath>,
}

impl TryFrom<CudaSourceSetDocument> for CudaSourceSet {
    type Error = MigrationSpecError;

    fn try_from(document: CudaSourceSetDocument) -> Result<Self, Self::Error> {
        Self::new(
            document.device_sources,
            document.host_sources,
            document.build_files,
        )
    }
}

impl CudaSourceSet {
    /// Creates a complete extension source set.
    ///
    /// # Errors
    ///
    /// Returns an error unless device, host, and build files are all represented.
    pub fn new(
        device_sources: impl IntoIterator<Item = BundlePath>,
        host_sources: impl IntoIterator<Item = BundlePath>,
        build_files: impl IntoIterator<Item = BundlePath>,
    ) -> Result<Self, MigrationSpecError> {
        let sources = Self {
            device_sources: device_sources.into_iter().collect(),
            host_sources: host_sources.into_iter().collect(),
            build_files: build_files.into_iter().collect(),
        };
        if sources.device_sources.is_empty() {
            return Err(MigrationSpecError::MissingDeviceSource);
        }
        if sources.host_sources.is_empty() {
            return Err(MigrationSpecError::MissingHostSource);
        }
        if sources.build_files.is_empty() {
            return Err(MigrationSpecError::MissingBuildFile);
        }
        Ok(sources)
    }

    /// CUDA device files in the migration boundary.
    #[must_use]
    pub const fn device_sources(&self) -> &BTreeSet<BundlePath> {
        &self.device_sources
    }

    /// Host-side launch and runtime files in the migration boundary.
    #[must_use]
    pub const fn host_sources(&self) -> &BTreeSet<BundlePath> {
        &self.host_sources
    }

    /// Build definitions required to compile the extension.
    #[must_use]
    pub const fn build_files(&self) -> &BTreeSet<BundlePath> {
        &self.build_files
    }
}

/// Caller-visible entry point that the migrated extension must preserve.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "PublicEntryPointDocument")]
pub struct PublicEntryPoint {
    symbol: String,
    contract: String,
    build_target: String,
}

#[derive(Deserialize)]
struct PublicEntryPointDocument {
    symbol: String,
    contract: String,
    build_target: String,
}

impl TryFrom<PublicEntryPointDocument> for PublicEntryPoint {
    type Error = MigrationSpecError;

    fn try_from(document: PublicEntryPointDocument) -> Result<Self, Self::Error> {
        Self::new(document.symbol, document.contract, document.build_target)
    }
}

impl PublicEntryPoint {
    /// Creates a public entry-point contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the symbol, caller-visible behavior, or build target is empty.
    pub fn new(
        symbol: impl Into<String>,
        contract: impl Into<String>,
        build_target: impl Into<String>,
    ) -> Result<Self, MigrationSpecError> {
        let symbol = required_text(symbol, MigrationSpecError::MissingPublicSymbol)?;
        let contract = required_text(contract, MigrationSpecError::MissingPublicContract)?;
        let build_target = required_text(build_target, MigrationSpecError::MissingBuildTarget)?;
        Ok(Self {
            symbol,
            contract,
            build_target,
        })
    }

    /// Public symbol or extension entry name.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Caller-visible behavior that must remain true after migration.
    #[must_use]
    pub fn contract(&self) -> &str {
        &self.contract
    }

    /// Build target the migrated extension must define so downstream harnesses can link it.
    ///
    /// This belongs to the migration, not to the factory. A gate that hard-codes it can only ever
    /// gate one specimen.
    #[must_use]
    pub fn build_target(&self) -> &str {
        &self.build_target
    }
}

/// Shell-free command that produces authoritative source-side observations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ReferenceWorkloadDocument")]
pub struct ReferenceWorkload {
    working_directory: BundlePath,
    argv: Vec<String>,
    library_target: String,
}

#[derive(Deserialize)]
struct ReferenceWorkloadDocument {
    working_directory: BundlePath,
    argv: Vec<String>,
    library_target: String,
}

impl TryFrom<ReferenceWorkloadDocument> for ReferenceWorkload {
    type Error = MigrationSpecError;

    fn try_from(document: ReferenceWorkloadDocument) -> Result<Self, Self::Error> {
        Self::new(
            document.working_directory,
            document.argv,
            document.library_target,
        )
    }
}

impl ReferenceWorkload {
    /// Creates a reference command with an explicit executable and arguments.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty command, an empty argv element, or no library target.
    pub fn new(
        working_directory: BundlePath,
        argv: impl IntoIterator<Item = String>,
        library_target: impl Into<String>,
    ) -> Result<Self, MigrationSpecError> {
        let argv: Vec<_> = argv.into_iter().collect();
        let library_target = library_target.into();
        if argv.is_empty()
            || argv.iter().any(|argument| argument.trim().is_empty())
            || library_target.trim().is_empty()
        {
            return Err(MigrationSpecError::InvalidReferenceCommand);
        }
        Ok(Self {
            working_directory,
            argv,
            library_target,
        })
    }

    /// Build target carrying the authority implementation, which a trusted harness links against.
    #[must_use]
    pub fn library_target(&self) -> &str {
        &self.library_target
    }

    /// Working directory relative to the source bundle.
    #[must_use]
    pub const fn working_directory(&self) -> &BundlePath {
        &self.working_directory
    }

    /// Executable and arguments without an intervening shell.
    #[must_use]
    pub fn argv(&self) -> &[String] {
        &self.argv
    }
}

/// Exact Ascend environment against which the migration is released.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "AscendTargetDocument")]
pub struct AscendTarget {
    soc: String,
    cann: String,
    compiler: String,
    driver: String,
    runtime: String,
}

#[derive(Deserialize)]
struct AscendTargetDocument {
    soc: String,
    cann: String,
    compiler: String,
    driver: String,
    runtime: String,
}

impl TryFrom<AscendTargetDocument> for AscendTarget {
    type Error = MigrationSpecError;

    fn try_from(document: AscendTargetDocument) -> Result<Self, Self::Error> {
        Self::new(
            document.soc,
            document.cann,
            document.compiler,
            document.driver,
            document.runtime,
        )
    }
}

impl AscendTarget {
    /// Creates the target identity included in execution receipts.
    ///
    /// # Errors
    ///
    /// Returns an error when any environment component is absent.
    pub fn new(
        soc: impl Into<String>,
        cann: impl Into<String>,
        compiler: impl Into<String>,
        driver: impl Into<String>,
        runtime: impl Into<String>,
    ) -> Result<Self, MigrationSpecError> {
        Ok(Self {
            soc: required_text(soc, MigrationSpecError::MissingTargetFact("soc"))?,
            cann: required_text(cann, MigrationSpecError::MissingTargetFact("cann"))?,
            compiler: required_text(compiler, MigrationSpecError::MissingTargetFact("compiler"))?,
            driver: required_text(driver, MigrationSpecError::MissingTargetFact("driver"))?,
            runtime: required_text(runtime, MigrationSpecError::MissingTargetFact("runtime"))?,
        })
    }

    /// Target system-on-chip identity.
    #[must_use]
    pub fn soc(&self) -> &str {
        &self.soc
    }

    /// Target CANN identity.
    #[must_use]
    pub fn cann(&self) -> &str {
        &self.cann
    }

    /// Target compiler identity.
    #[must_use]
    pub fn compiler(&self) -> &str {
        &self.compiler
    }

    /// Target driver identity.
    #[must_use]
    pub fn driver(&self) -> &str {
        &self.driver
    }

    /// Target runtime identity.
    #[must_use]
    pub fn runtime(&self) -> &str {
        &self.runtime
    }
}

/// Immutable Phase-1 migration contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "MigrationSpecDocument")]
pub struct MigrationSpec {
    schema_version: u16,
    source_revision: String,
    sources: CudaSourceSet,
    public_entry: PublicEntryPoint,
    reference: ReferenceWorkload,
    target: AscendTarget,
    supported_domain: String,
    unsupported_constructs: Vec<String>,
    fallback: String,
}

#[derive(Deserialize)]
struct MigrationSpecDocument {
    schema_version: u16,
    source_revision: String,
    sources: CudaSourceSet,
    public_entry: PublicEntryPoint,
    reference: ReferenceWorkload,
    target: AscendTarget,
    supported_domain: String,
    unsupported_constructs: Vec<String>,
    fallback: String,
}

impl TryFrom<MigrationSpecDocument> for MigrationSpec {
    type Error = MigrationSpecError;

    fn try_from(document: MigrationSpecDocument) -> Result<Self, Self::Error> {
        if document.schema_version != MIGRATION_SPEC_SCHEMA_V1 {
            return Err(MigrationSpecError::UnsupportedSchemaVersion(
                document.schema_version,
            ));
        }
        Self::new_v1(
            document.source_revision,
            document.sources,
            document.public_entry,
            document.reference,
            document.target,
            document.supported_domain,
            document.unsupported_constructs,
            document.fallback,
        )
    }
}

impl MigrationSpec {
    /// Creates a validated `MigrationSpec v1`.
    ///
    /// # Errors
    ///
    /// Returns an error for missing revision, domain, fallback, or malformed unsupported entries.
    #[allow(clippy::too_many_arguments)]
    pub fn new_v1(
        source_revision: impl Into<String>,
        sources: CudaSourceSet,
        public_entry: PublicEntryPoint,
        reference: ReferenceWorkload,
        target: AscendTarget,
        supported_domain: impl Into<String>,
        unsupported_constructs: impl IntoIterator<Item = String>,
        fallback: impl Into<String>,
    ) -> Result<Self, MigrationSpecError> {
        let source_revision =
            required_text(source_revision, MigrationSpecError::MissingSourceRevision)?;
        let supported_domain =
            required_text(supported_domain, MigrationSpecError::MissingSupportedDomain)?;
        let fallback = required_text(fallback, MigrationSpecError::MissingFallback)?;
        let unsupported_constructs: Vec<_> = unsupported_constructs.into_iter().collect();
        if unsupported_constructs
            .iter()
            .any(|construct| construct.trim().is_empty())
        {
            return Err(MigrationSpecError::InvalidUnsupportedConstruct);
        }

        Ok(Self {
            schema_version: MIGRATION_SPEC_SCHEMA_V1,
            source_revision,
            sources,
            public_entry,
            reference,
            target,
            supported_domain,
            unsupported_constructs,
            fallback,
        })
    }

    /// Schema version included in the content-addressed representation.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// Immutable source revision being migrated.
    #[must_use]
    pub fn source_revision(&self) -> &str {
        &self.source_revision
    }

    /// CUDA source and build boundary.
    #[must_use]
    pub const fn sources(&self) -> &CudaSourceSet {
        &self.sources
    }

    /// Caller-visible entry point.
    #[must_use]
    pub const fn public_entry(&self) -> &PublicEntryPoint {
        &self.public_entry
    }

    /// Authoritative source-side workload.
    #[must_use]
    pub const fn reference(&self) -> &ReferenceWorkload {
        &self.reference
    }

    /// Fixed Ascend release environment.
    #[must_use]
    pub const fn target(&self) -> &AscendTarget {
        &self.target
    }

    /// Supported input and semantic domain.
    #[must_use]
    pub fn supported_domain(&self) -> &str {
        &self.supported_domain
    }

    /// Constructs detected in the intake but excluded from this release.
    #[must_use]
    pub fn unsupported_constructs(&self) -> &[String] {
        &self.unsupported_constructs
    }

    /// Required behavior outside the supported domain.
    #[must_use]
    pub fn fallback(&self) -> &str {
        &self.fallback
    }

    /// Computes the canonical identity used to bind tasks, candidates, and inspection reports.
    #[must_use]
    pub fn digest(&self) -> Sha256Digest {
        let mut context = Context::new(&SHA256);
        context.update(b"alloyport-migration-spec-v1\0");
        context.update(&self.schema_version.to_be_bytes());
        hash_text(&mut context, &self.source_revision);
        hash_paths(&mut context, self.sources.device_sources());
        hash_paths(&mut context, self.sources.host_sources());
        hash_paths(&mut context, self.sources.build_files());
        hash_text(&mut context, self.public_entry.symbol());
        hash_text(&mut context, self.public_entry.contract());
        hash_text(&mut context, self.reference.working_directory().as_str());
        hash_strings(&mut context, self.reference.argv());
        hash_text(&mut context, self.target.soc());
        hash_text(&mut context, self.target.cann());
        hash_text(&mut context, self.target.compiler());
        hash_text(&mut context, self.target.driver());
        hash_text(&mut context, self.target.runtime());
        hash_text(&mut context, &self.supported_domain);
        hash_strings(&mut context, &self.unsupported_constructs);
        hash_text(&mut context, &self.fallback);
        let digest = context.finish();
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(digest.as_ref());
        Sha256Digest::from_bytes(bytes)
    }
}

fn hash_paths(context: &mut Context, paths: &BTreeSet<BundlePath>) {
    context.update(&(paths.len() as u64).to_be_bytes());
    for path in paths {
        hash_text(context, path.as_str());
    }
}

fn hash_strings(context: &mut Context, values: &[String]) {
    context.update(&(values.len() as u64).to_be_bytes());
    for value in values {
        hash_text(context, value);
    }
}

fn hash_text(context: &mut Context, value: &str) {
    context.update(&(value.len() as u64).to_be_bytes());
    context.update(value.as_bytes());
}

fn required_text(
    value: impl Into<String>,
    error: MigrationSpecError,
) -> Result<String, MigrationSpecError> {
    let value = value.into();
    if value.trim().is_empty() {
        Err(error)
    } else {
        Ok(value)
    }
}

/// Validation failure for a Phase-1 migration contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationSpecError {
    UnsupportedSchemaVersion(u16),
    InvalidBundlePath(String),
    MissingDeviceSource,
    MissingHostSource,
    MissingBuildFile,
    MissingPublicSymbol,
    MissingPublicContract,
    MissingBuildTarget,
    InvalidReferenceCommand,
    MissingTargetFact(&'static str),
    MissingSourceRevision,
    MissingSupportedDomain,
    InvalidUnsupportedConstruct,
    MissingFallback,
}

impl Display for MigrationSpecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchemaVersion(version) => {
                write!(formatter, "unsupported MigrationSpec schema {version}")
            }
            Self::InvalidBundlePath(path) => {
                write!(formatter, "invalid bundle-relative path {path:?}")
            }
            Self::MissingDeviceSource => write!(formatter, "migration has no CUDA device source"),
            Self::MissingHostSource => write!(formatter, "migration has no CUDA host source"),
            Self::MissingBuildFile => write!(formatter, "migration has no build file"),
            Self::MissingPublicSymbol => write!(formatter, "migration has no public symbol"),
            Self::MissingPublicContract => write!(formatter, "migration has no public contract"),
            Self::MissingBuildTarget => write!(formatter, "migration has no build target"),
            Self::InvalidReferenceCommand => write!(formatter, "reference command argv is empty"),
            Self::MissingTargetFact(fact) => write!(formatter, "Ascend target is missing {fact}"),
            Self::MissingSourceRevision => write!(formatter, "migration has no source revision"),
            Self::MissingSupportedDomain => write!(formatter, "migration has no supported domain"),
            Self::InvalidUnsupportedConstruct => {
                write!(formatter, "unsupported construct entries must be nonempty")
            }
            Self::MissingFallback => write!(formatter, "migration has no fallback behavior"),
        }
    }
}

impl Error for MigrationSpecError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(value: &str) -> BundlePath {
        BundlePath::try_from(value).expect("valid test path")
    }

    fn source_set() -> CudaSourceSet {
        CudaSourceSet::new(
            [path("src/reduce.cu")],
            [path("src/reduce_host.cpp")],
            [path("CMakeLists.txt")],
        )
        .expect("complete source set")
    }

    fn target() -> AscendTarget {
        AscendTarget::new("Ascend950PR", "9.1.0", "ccec", "25.7", "acl-9.1")
            .expect("complete target")
    }

    fn spec() -> MigrationSpec {
        MigrationSpec::new_v1(
            "source-sha",
            source_set(),
            PublicEntryPoint::new(
                "reduce_sum",
                "sum fp32 input into one fp32 output",
                "reduce_sum_candidate",
            )
            .expect("entry point"),
            ReferenceWorkload::new(
                path("."),
                ["./build/reduce_test".to_owned()],
                "reference_library",
            )
            .expect("reference"),
            target(),
            "1 <= elements <= 1048576; contiguous fp32",
            ["cooperative groups".to_owned()],
            "return unsupported-domain status before launch",
        )
        .expect("valid migration spec")
    }

    #[test]
    fn bundle_paths_are_portable_and_cannot_escape() {
        for invalid in [
            "",
            "/tmp/reduce.cu",
            "../reduce.cu",
            "src/../reduce.cu",
            "src//x.cu",
        ] {
            assert!(
                BundlePath::try_from(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
        assert_eq!(path("src/reduce.cu").as_str(), "src/reduce.cu");
    }

    #[test]
    fn extension_intake_requires_device_host_and_build_sources() {
        assert_eq!(
            CudaSourceSet::new([], [path("host.cpp")], [path("CMakeLists.txt")]),
            Err(MigrationSpecError::MissingDeviceSource)
        );
        assert_eq!(
            CudaSourceSet::new([path("kernel.cu")], [], [path("CMakeLists.txt")]),
            Err(MigrationSpecError::MissingHostSource)
        );
        assert_eq!(
            CudaSourceSet::new([path("kernel.cu")], [path("host.cpp")], []),
            Err(MigrationSpecError::MissingBuildFile)
        );
    }

    #[test]
    fn migration_spec_serializes_its_versioned_contract() {
        let spec = spec();
        let value = serde_json::to_value(&spec).expect("serialize spec");
        assert_eq!(value["schema_version"], MIGRATION_SPEC_SCHEMA_V1);
        assert_eq!(value["public_entry"]["symbol"], "reduce_sum");
        assert_eq!(value["target"]["soc"], "Ascend950PR");
        assert_eq!(
            serde_json::from_value::<MigrationSpec>(value).expect("validated round trip"),
            spec
        );
        assert_eq!(spec.digest(), spec.clone().digest());
    }

    #[test]
    fn migration_spec_digest_changes_with_release_semantics() {
        let original = spec();
        let changed = MigrationSpec::new_v1(
            original.source_revision(),
            original.sources().clone(),
            original.public_entry().clone(),
            original.reference().clone(),
            original.target().clone(),
            original.supported_domain(),
            original.unsupported_constructs().iter().cloned(),
            "a different fallback",
        )
        .expect("changed spec");

        assert_ne!(original.digest(), changed.digest());
    }

    #[test]
    fn deserialization_cannot_bypass_contract_validation() {
        let mut value = serde_json::to_value(spec()).expect("serialize spec");
        value["schema_version"] = serde_json::json!(2);
        assert!(serde_json::from_value::<MigrationSpec>(value).is_err());

        let mut value = serde_json::to_value(spec()).expect("serialize spec");
        value["sources"]["host_sources"] = serde_json::json!([]);
        assert!(serde_json::from_value::<MigrationSpec>(value).is_err());

        let mut value = serde_json::to_value(spec()).expect("serialize spec");
        value["sources"]["device_sources"] = serde_json::json!(["../escape.cu"]);
        assert!(serde_json::from_value::<MigrationSpec>(value).is_err());
    }

    #[test]
    fn first_product_fixture_is_a_valid_migration_spec() {
        let document =
            include_str!("../../../fixtures/migrations/cuda-reduction-v1/migration-spec-v1.json");
        let spec: MigrationSpec = serde_json::from_str(document).expect("valid product fixture");

        assert_eq!(spec.public_entry().symbol(), "alloyport_reduce_sum_f32");
        assert_eq!(spec.sources().device_sources().len(), 1);
        assert_eq!(spec.sources().host_sources().len(), 3);
        assert_eq!(spec.target().soc(), "Ascend950PR");
    }

    #[test]
    fn migration_spec_rejects_undeclared_domain_and_fallback() {
        let entry = PublicEntryPoint::new("reduce_sum", "sum input", "reduce_sum_candidate")
            .expect("entry point");
        let reference =
            ReferenceWorkload::new(path("."), ["./run".to_owned()], "reference_library")
                .expect("reference");
        assert_eq!(
            MigrationSpec::new_v1(
                "source-sha",
                source_set(),
                entry.clone(),
                reference.clone(),
                target(),
                "",
                [],
                "reject",
            ),
            Err(MigrationSpecError::MissingSupportedDomain)
        );
        assert_eq!(
            MigrationSpec::new_v1(
                "source-sha",
                source_set(),
                entry,
                reference,
                target(),
                "contiguous fp32",
                [],
                "",
            ),
            Err(MigrationSpecError::MissingFallback)
        );
    }
}
