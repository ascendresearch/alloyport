//! Materializes the candidate record into a real git repository, then reads it back to check it.
//!
//! The stream in [`crate::record_stream`] is where every decision lives; this module only runs git
//! and then refuses to believe it. After the import it re-reads every blob through `git cat-file`,
//! rehashes it, and compares against the manifest digest the Episode recorded — because a projection
//! that quietly disagreed with the manifest would be a wrong record that survives being rebuilt.
//! That is the same shape as `verify_source_gate_receipt`: re-evaluate, do not trust the writer.
//!
//! Git is invoked with the host's configuration switched off. A global `core.autocrlf`, a template
//! directory with hooks, or a signing key would each make the same history produce a different
//! repository on a different machine, and this record's whole claim is that it can be rebuilt.

use crate::record::CandidateRecordError;
use crate::record_stream::{RECORD_BRANCH, RecordedCandidate, candidate_ref, fast_import_stream};
use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Where the record was written and what each candidate is reachable by.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateRecord {
    pub root: PathBuf,
    pub commits: Vec<RecordedCommit>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedCommit {
    pub candidate_id: String,
    pub reference: String,
    pub commit: String,
}

/// Builds the repository for one task's candidates and verifies it against their manifests.
///
/// The directory must not already exist or must be empty. The record is a projection and may be
/// rebuilt at any time, so refusing to write into an existing tree costs nothing and removes the
/// chance of leaving a half-replaced history behind.
///
/// # Errors
///
/// Returns an error when the destination is occupied, when `git` is absent or fails, or when the
/// imported repository disagrees with the manifests in any file, path set, or parent link.
pub fn write_candidate_record(
    root: &Path,
    candidates: &[RecordedCandidate],
) -> Result<CandidateRecord, CandidateRecordError> {
    let stream = fast_import_stream(candidates)?;
    prepare_directory(root)?;
    // `--template=` keeps the host's hooks and excludes out, and the initial branch is stated
    // because `init.defaultBranch` is a host setting and the record names its own branch.
    git(
        root,
        &["init", "--quiet", "--template=", "--initial-branch=main"],
    )?;
    import(root, &stream)?;
    // A checkout is the point of a non-bare repository: the last candidate's files are readable
    // without knowing any git plumbing.
    git(root, &["reset", "--quiet", "--hard", RECORD_BRANCH])?;
    let commits = verify(root, candidates)?;
    Ok(CandidateRecord {
        root: root.to_path_buf(),
        commits,
    })
}

fn prepare_directory(root: &Path) -> Result<(), CandidateRecordError> {
    match std::fs::read_dir(root) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(CandidateRecordError::Occupied(root.to_path_buf()));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => std::fs::create_dir_all(root)
            .map_err(|error| CandidateRecordError::Io {
                operation: "create record directory",
                source: error,
            }),
        Err(error) => Err(CandidateRecordError::Io {
            operation: "inspect record directory",
            source: error,
        }),
    }
}

fn import(root: &Path, stream: &[u8]) -> Result<(), CandidateRecordError> {
    let mut child = command(root)
        .args(["fast-import", "--done", "--quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| CandidateRecordError::GitUnavailable(error.to_string()))?;
    let written = child
        .stdin
        .as_mut()
        .ok_or_else(|| CandidateRecordError::Git {
            operation: "fast-import",
            message: "git accepted no standard input".to_owned(),
        })
        .and_then(|stdin| {
            stdin
                .write_all(stream)
                .map_err(|error| CandidateRecordError::Git {
                    operation: "fast-import",
                    message: error.to_string(),
                })
        });
    drop(child.stdin.take());
    let output = child
        .wait_with_output()
        .map_err(|error| CandidateRecordError::Git {
            operation: "fast-import",
            message: error.to_string(),
        })?;
    // git's own complaint first: a write that failed with a broken pipe is a symptom of git having
    // already rejected the stream, and reporting the pipe would hide the reason.
    if !output.status.success() {
        return Err(CandidateRecordError::Git {
            operation: "fast-import",
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    written
}

/// Re-reads the imported repository and proves it says what the manifests say.
fn verify(
    root: &Path,
    candidates: &[RecordedCandidate],
) -> Result<Vec<RecordedCommit>, CandidateRecordError> {
    let mut commits: Vec<RecordedCommit> = Vec::new();
    for candidate in candidates {
        let reference = candidate_ref(candidate);
        let commit = git(root, &["rev-parse", &reference])?.trim().to_owned();
        let expected: BTreeSet<&str> = candidate
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect();
        let listing = git_bytes(root, &["ls-tree", "-r", "-z", "--name-only", &reference])?;
        let actual: BTreeSet<&str> = listing
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
            .map(|entry| std::str::from_utf8(entry).unwrap_or_default())
            .collect();
        if expected != actual {
            return Err(CandidateRecordError::Projection(format!(
                "{reference} holds {} paths where the manifest declares {}",
                actual.len(),
                expected.len()
            )));
        }
        for file in &candidate.files {
            let bytes = git_bytes(
                root,
                &[
                    "cat-file",
                    "blob",
                    &format!("{reference}:{}", file.path.as_str()),
                ],
            )?;
            if alloyport_core::Sha256Digest::digest_bytes(&bytes) != file.digest {
                return Err(CandidateRecordError::Projection(format!(
                    "{reference} stores {} as bytes the manifest does not name",
                    file.path.as_str()
                )));
            }
        }
        let lineage = git(root, &["rev-list", "--parents", "-n", "1", &reference])?;
        let parents: Vec<&str> = lineage.split_whitespace().skip(1).collect();
        // A parent the record does not hold is a root commit whose message says the parent is
        // elsewhere, so the expectation is what the record can reach, not what the manifest names.
        let expected: Vec<&str> = candidate
            .parent_candidate_id
            .as_ref()
            .and_then(|parent| {
                commits
                    .iter()
                    .find(|recorded| recorded.candidate_id == parent.as_str())
            })
            .map(|recorded| recorded.commit.as_str())
            .into_iter()
            .collect();
        if parents != expected {
            return Err(CandidateRecordError::Projection(format!(
                "{reference} has parents {parents:?} where the record implies {expected:?}"
            )));
        }
        commits.push(RecordedCommit {
            candidate_id: candidate.candidate_id.as_str().to_owned(),
            reference,
            commit,
        });
    }
    Ok(commits)
}

fn command(root: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .current_dir(root)
        // The host's configuration must not reach a repository whose claim is reproducibility.
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE");
    command
}

fn git(root: &Path, arguments: &[&str]) -> Result<String, CandidateRecordError> {
    let bytes = git_bytes(root, arguments)?;
    String::from_utf8(bytes).map_err(|_| CandidateRecordError::Git {
        operation: "read git output",
        message: "git returned output that is not UTF-8".to_owned(),
    })
}

fn git_bytes(root: &Path, arguments: &[&str]) -> Result<Vec<u8>, CandidateRecordError> {
    let output = command(root)
        .args(arguments)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| CandidateRecordError::GitUnavailable(error.to_string()))?;
    if output.status.success() {
        return Ok(output.stdout);
    }
    Err(CandidateRecordError::Git {
        operation: "git",
        message: format!(
            "{} failed: {}",
            arguments.first().copied().unwrap_or("git"),
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    })
}
