use alloyport_artifacts::{ArtifactStore, ArtifactStoreError};
use alloyport_core::{BundlePath, CandidateSourceManifest, Sha256Digest};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// Verified create-only materialization rooted at one content-derived candidate identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateMaterialization {
    pub candidate_root: PathBuf,
    pub manifest_digest: Sha256Digest,
}

impl CandidateMaterialization {
    /// Materializes missing files from CAS and then independently verifies the complete tree.
    ///
    /// # Errors
    ///
    /// Returns an error for Artifact failure, symlinks, conflicting bytes, or extra/missing files.
    pub fn materialize(
        workspace_root: &Path,
        artifacts: &dyn ArtifactStore,
        manifest: &CandidateSourceManifest,
        manifest_digest: Sha256Digest,
    ) -> Result<Self, CandidateMaterializationError> {
        verify_workspace_root(workspace_root)?;
        if !is_safe_candidate_directory_name(manifest.candidate_id().as_str()) {
            return Err(CandidateMaterializationError::UnsafeCandidateId);
        }
        let candidate_root = workspace_root.join(manifest.candidate_id().as_str());
        create_candidate_root(&candidate_root)?;
        for source in manifest.files() {
            let target = candidate_root.join(source.path().as_str());
            create_safe_parents(&candidate_root, &target)?;
            match fs::symlink_metadata(&target) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    return Err(CandidateMaterializationError::UnsafeEntry(
                        source.path().clone(),
                    ));
                }
                Ok(_) => verify_file(
                    &target,
                    source.artifact().digest,
                    source.artifact().size_bytes,
                )?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    write_artifact_create_only(
                        artifacts,
                        source.artifact().digest,
                        source.artifact().size_bytes,
                        &target,
                    )?;
                }
                Err(source) => {
                    return Err(CandidateMaterializationError::Io {
                        operation: "inspect candidate target",
                        source,
                    });
                }
            }
        }
        verify_complete_tree(&candidate_root, manifest)?;
        Ok(Self {
            candidate_root,
            manifest_digest,
        })
    }

    /// Independently rereads the exact materialized tree and returns verified source bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the tree or any file no longer matches the immutable manifest.
    pub fn read_verified_sources(
        &self,
        manifest: &CandidateSourceManifest,
    ) -> Result<BTreeMap<BundlePath, Vec<u8>>, CandidateMaterializationError> {
        verify_complete_tree(&self.candidate_root, manifest)?;
        manifest
            .files()
            .iter()
            .map(|file| {
                let bytes = read_verified_file(
                    &self.candidate_root.join(file.path().as_str()),
                    file.artifact().digest,
                    file.artifact().size_bytes,
                )?;
                Ok((file.path().clone(), bytes))
            })
            .collect()
    }
}

fn is_safe_candidate_directory_name(value: &str) -> bool {
    value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn verify_workspace_root(root: &Path) -> Result<(), CandidateMaterializationError> {
    let metadata =
        fs::symlink_metadata(root).map_err(|source| CandidateMaterializationError::Io {
            operation: "inspect candidate workspace",
            source,
        })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CandidateMaterializationError::UnsafeWorkspace);
    }
    Ok(())
}

fn create_candidate_root(root: &Path) -> Result<(), CandidateMaterializationError> {
    match fs::create_dir(root) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata =
                fs::symlink_metadata(root).map_err(|source| CandidateMaterializationError::Io {
                    operation: "inspect existing candidate root",
                    source,
                })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                Err(CandidateMaterializationError::UnsafeWorkspace)
            } else {
                Ok(())
            }
        }
        Err(source) => Err(CandidateMaterializationError::Io {
            operation: "create candidate root",
            source,
        }),
    }
}

fn create_safe_parents(
    candidate_root: &Path,
    target: &Path,
) -> Result<(), CandidateMaterializationError> {
    let parent = target
        .parent()
        .ok_or(CandidateMaterializationError::UnsafeWorkspace)?;
    let relative = parent
        .strip_prefix(candidate_root)
        .map_err(|_| CandidateMaterializationError::UnsafeWorkspace)?;
    let mut current = candidate_root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        match fs::create_dir(&current) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&current).map_err(|source| {
                    CandidateMaterializationError::Io {
                        operation: "inspect candidate directory",
                        source,
                    }
                })?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(CandidateMaterializationError::UnsafeWorkspace);
                }
            }
            Err(source) => {
                return Err(CandidateMaterializationError::Io {
                    operation: "create candidate directory",
                    source,
                });
            }
        }
    }
    Ok(())
}

fn write_artifact_create_only(
    artifacts: &dyn ArtifactStore,
    digest: Sha256Digest,
    size: u64,
    target: &Path,
) -> Result<(), CandidateMaterializationError> {
    let mut source = artifacts.open(digest)?;
    if source.identity().size_bytes != size {
        return Err(CandidateMaterializationError::ArtifactIdentity);
    }
    let parent = target
        .parent()
        .ok_or(CandidateMaterializationError::UnsafeWorkspace)?;
    let mut destination = tempfile::NamedTempFile::new_in(parent).map_err(|source| {
        CandidateMaterializationError::Io {
            operation: "create candidate source staging file",
            source,
        }
    })?;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read =
            source
                .read(&mut buffer)
                .map_err(|source| CandidateMaterializationError::Io {
                    operation: "read candidate Artifact",
                    source,
                })?;
        if read == 0 {
            break;
        }
        destination.write_all(&buffer[..read]).map_err(|source| {
            CandidateMaterializationError::Io {
                operation: "write candidate source",
                source,
            }
        })?;
    }
    destination
        .as_file()
        .sync_all()
        .map_err(|source| CandidateMaterializationError::Io {
            operation: "sync candidate source",
            source,
        })?;
    verify_file(destination.path(), digest, size)?;
    match destination.persist_noclobber(target) {
        Ok(file) => {
            let mut permissions = file
                .metadata()
                .map_err(|source| CandidateMaterializationError::Io {
                    operation: "inspect materialized candidate source",
                    source,
                })?
                .permissions();
            permissions.set_readonly(true);
            file.set_permissions(permissions).map_err(|source| {
                CandidateMaterializationError::Io {
                    operation: "seal materialized candidate source",
                    source,
                }
            })?;
            sync_directory(parent)?;
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_file(target, digest, size)?;
        }
        Err(error) => {
            return Err(CandidateMaterializationError::Io {
                operation: "publish candidate source",
                source: error.error,
            });
        }
    }
    verify_file(target, digest, size)
}

fn sync_directory(path: &Path) -> Result<(), CandidateMaterializationError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| CandidateMaterializationError::Io {
            operation: "sync candidate directory",
            source,
        })
}

fn verify_file(
    path: &Path,
    expected_digest: Sha256Digest,
    expected_size: u64,
) -> Result<(), CandidateMaterializationError> {
    read_verified_file(path, expected_digest, expected_size).map(|_| ())
}

fn read_verified_file(
    path: &Path,
    expected_digest: Sha256Digest,
    expected_size: u64,
) -> Result<Vec<u8>, CandidateMaterializationError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| CandidateMaterializationError::Io {
            operation: "inspect candidate source",
            source,
        })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CandidateMaterializationError::UnsafeWorkspace);
    }
    let bytes = fs::read(path).map_err(|source| CandidateMaterializationError::Io {
        operation: "reread candidate source",
        source,
    })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected_size
        || Sha256Digest::digest_bytes(&bytes) != expected_digest
    {
        return Err(CandidateMaterializationError::ArtifactIdentity);
    }
    Ok(bytes)
}

fn verify_complete_tree(
    candidate_root: &Path,
    manifest: &CandidateSourceManifest,
) -> Result<(), CandidateMaterializationError> {
    let expected: BTreeSet<_> = manifest
        .files()
        .iter()
        .map(|file| file.path().clone())
        .collect();
    let mut actual = BTreeSet::new();
    scan_tree(candidate_root, candidate_root, &mut actual)?;
    if expected != actual {
        return Err(CandidateMaterializationError::FileSetMismatch);
    }
    for file in manifest.files() {
        verify_file(
            &candidate_root.join(file.path().as_str()),
            file.artifact().digest,
            file.artifact().size_bytes,
        )?;
    }
    Ok(())
}

fn scan_tree(
    root: &Path,
    directory: &Path,
    files: &mut BTreeSet<BundlePath>,
) -> Result<(), CandidateMaterializationError> {
    for entry in fs::read_dir(directory).map_err(|source| CandidateMaterializationError::Io {
        operation: "scan candidate tree",
        source,
    })? {
        let entry = entry.map_err(|source| CandidateMaterializationError::Io {
            operation: "read candidate tree entry",
            source,
        })?;
        let kind = entry
            .file_type()
            .map_err(|source| CandidateMaterializationError::Io {
                operation: "inspect candidate tree entry",
                source,
            })?;
        if kind.is_symlink() {
            return Err(CandidateMaterializationError::UnsafeWorkspace);
        }
        if kind.is_dir() {
            scan_tree(root, &entry.path(), files)?;
        } else if kind.is_file() {
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| CandidateMaterializationError::UnsafeWorkspace)?
                .to_string_lossy()
                .into_owned();
            files.insert(
                BundlePath::try_from(relative)
                    .map_err(|_| CandidateMaterializationError::UnsafeWorkspace)?,
            );
        } else {
            return Err(CandidateMaterializationError::UnsafeWorkspace);
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum CandidateMaterializationError {
    UnsafeWorkspace,
    UnsafeCandidateId,
    UnsafeEntry(BundlePath),
    FileSetMismatch,
    ArtifactIdentity,
    Artifact(ArtifactStoreError),
    Io {
        operation: &'static str,
        source: std::io::Error,
    },
}

impl Display for CandidateMaterializationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsafeWorkspace => write!(formatter, "candidate workspace is unsafe"),
            Self::UnsafeCandidateId => write!(formatter, "candidate ID is not a safe path segment"),
            Self::UnsafeEntry(path) => write!(formatter, "candidate entry {path:?} is unsafe"),
            Self::FileSetMismatch => {
                write!(formatter, "candidate file set does not match manifest")
            }
            Self::ArtifactIdentity => write!(formatter, "candidate source identity mismatch"),
            Self::Artifact(error) => Display::fmt(error, formatter),
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
        }
    }
}

impl Error for CandidateMaterializationError {}

impl From<ArtifactStoreError> for CandidateMaterializationError {
    fn from(error: ArtifactStoreError) -> Self {
        Self::Artifact(error)
    }
}
