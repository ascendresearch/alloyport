//! Offline projection of one task's candidate lineage into a readable git repository.
//!
//! [Design 0044](../../../../docs/design/0044-git-as-the-candidate-record.md) adopts git as the
//! record. This command is the whole entry point, and where it runs is a decision worth stating.
//!
//! **It is offline, not part of the migration.** 0044's text says the controller commits as a
//! submission is assembled. That cannot carry the gate outcome it also asks for — no gate has run
//! when a candidate is submitted — and it would put a new step, with new ways to fail, inside a paid
//! run. Seven migrations on 2026-08-16 died in the harness and none died from a bad kernel; adding
//! anything to that path needs a better reason than convenience. Built afterwards the projection
//! knows every verdict, costs the run nothing, and matches 0044's own consequence: the repository is
//! a projection and may be rebuilt from manifests at any time.
//!
//! **It is a server subcommand, not a CLI one.** The record reads the Episode database and the CAS
//! directly, which the CLI reaches only over gRPC; exposing it there would mean a new management RPC
//! — control-plane growth the product plan freezes — and the CLI is deliberately kept clear of gate
//! documents by an architecture check.

use super::config::ServerConfig;
use crate::adapters::sqlite::SqliteEpisodeRepository;
use alloyport_artifacts::FilesystemArtifactStore;
use alloyport_candidate_tools::{collect_candidate_record, write_candidate_record};
use alloyport_core::{EpisodeId, EpisodeRepository};
use std::error::Error;
use std::path::{Path, PathBuf};

/// Builds the record for one task and reports where it is and what it holds.
///
/// # Errors
///
/// Returns an error when the server configuration has no migration runtime, when the task has no
/// Episode database, when a digest the Episode recorded cannot be read, or when the written
/// repository disagrees with the manifests it projects.
pub(super) fn run(config: &ServerConfig, task_id: &str, into: &Path) -> Result<(), Box<dyn Error>> {
    let runtime = config
        .migration_runtime
        .as_ref()
        .ok_or("this server configuration has no migration_runtime, so it owns no task Episodes")?;
    let database = episode_database(&runtime.root, task_id);
    if !database.is_file() {
        return Err(format!(
            "no Episode database at {}; candidate-record reads what a run already wrote",
            database.display()
        )
        .into());
    }
    // The same identity `load_for_task` derives. Stated here rather than searched for, so a missing
    // Episode is a clear refusal instead of an empty record.
    let episode_id = EpisodeId::try_from(format!("episode-{task_id}"))?;
    let repository = SqliteEpisodeRepository::open(&database)?;
    let state = repository.load(&episode_id)?.state;
    let artifacts = FilesystemArtifactStore::open(
        config.artifact.root.join("cas"),
        config.artifact.max_artifact_bytes,
    )?;
    let candidates = collect_candidate_record(&state, &artifacts)?;
    if candidates.is_empty() {
        return Err(format!(
            "{episode_id} recorded {} tool operations and no candidate submission",
            state.tool_operation_count()
        )
        .into());
    }
    let record = write_candidate_record(into, &candidates)?;
    println!("candidate record for {task_id}: {}", record.root.display());
    for (candidate, commit) in candidates.iter().zip(&record.commits) {
        println!(
            "  {} {} {} file(s){}",
            &commit.commit[..commit.commit.len().min(12)],
            commit.reference.trim_start_matches("refs/tags/"),
            candidate.files.len(),
            summary(candidate)
        );
    }
    println!(
        "read it with: git -C {} log --all --graph --oneline",
        record.root.display()
    );
    Ok(())
}

fn summary(candidate: &alloyport_candidate_tools::RecordedCandidate) -> String {
    use std::fmt::Write as _;

    if candidate.outcomes.is_empty() {
        return ", no gate ran".to_owned();
    }
    candidate
        .outcomes
        .iter()
        .fold(String::new(), |mut line, outcome| {
            let _ = write!(line, ", {} {}", outcome.gate.label(), outcome.verdict);
            line
        })
}

/// The per-task runtime layout the migration dispatcher writes.
fn episode_database(root: &Path, task_id: &str) -> PathBuf {
    root.join(task_id).join("episode.sqlite3")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_episode_database_is_the_one_a_dispatched_task_writes() {
        assert_eq!(
            episode_database(Path::new("/var/lib/alloyport/migrations"), "task-abc"),
            Path::new("/var/lib/alloyport/migrations/task-abc/episode.sqlite3")
        );
    }
}
