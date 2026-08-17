//! Durable migration intake owned by the persistent server process: model and port only.
//!
//! The `SQLite` implementation lives in [`crate::adapters::sqlite`]. It was here, which put `rusqlite`
//! and nine SQL statements outside the adapter boundary and made the server's task lifecycle depend
//! on a concrete store — both of which `scripts/check_sql_boundaries.sh` and
//! `scripts/check_architecture_boundaries.sh` had been reporting since 2026-08-14, to nobody
//! ([`boundary-gates-red-20260817.md`](../../../docs/evidence/boundary-gates-red-20260817.md)).

use alloyport_core::Sha256Digest;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum MigrationTaskState {
    Captured = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
    Cancelled = 5,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationTaskRecord {
    pub owner_id: String,
    pub task_id: String,
    pub project_name: String,
    pub project_digest: Sha256Digest,
    pub project_size_bytes: u64,
    pub file_count: u64,
    pub state: MigrationTaskState,
    pub created_at_ms: u64,
}

/// One intake request, so a store's own identity fields cannot be reordered by accident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationTaskSubmission<'a> {
    pub owner_id: &'a str,
    pub request_id: &'a str,
    pub task_id: &'a str,
    pub project_name: &'a str,
    pub project_digest: Sha256Digest,
    pub project_size_bytes: u64,
    pub file_count: u64,
    pub created_at_ms: u64,
}

#[derive(Debug)]
pub enum MigrationTaskError {
    Storage(Box<dyn Error + Send + Sync>),
    Conflict,
    NotFound,
    Corrupt(String),
}

impl Display for MigrationTaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => Display::fmt(error, formatter),
            Self::Conflict => formatter.write_str("migration request identity was reused"),
            Self::NotFound => formatter.write_str("migration task was not found"),
            Self::Corrupt(detail) => write!(formatter, "corrupt migration task: {detail}"),
        }
    }
}

impl Error for MigrationTaskError {}

/// Durable intake port. Every method is idempotent or explicitly refuses a repeat.
///
/// `Debug` is required because the services holding one derive it, and `Send + Sync` because intake
/// work runs on blocking tasks.
#[allow(clippy::missing_errors_doc)]
pub trait MigrationTaskStore: Debug + Send + Sync {
    /// Creates a task idempotently for one owner/request identity; conflicts on changed retry bytes.
    fn submit(
        &self,
        submission: MigrationTaskSubmission<'_>,
    ) -> Result<MigrationTaskRecord, MigrationTaskError>;

    /// Reads one owner-scoped task.
    fn get(&self, owner_id: &str, task_id: &str)
    -> Result<MigrationTaskRecord, MigrationTaskError>;

    /// Lists recent tasks belonging to one owner.
    fn list(
        &self,
        owner_id: &str,
        limit: usize,
    ) -> Result<Vec<MigrationTaskRecord>, MigrationTaskError>;

    /// Marks a captured or running task cancelled, idempotently.
    fn cancel(
        &self,
        owner_id: &str,
        task_id: &str,
    ) -> Result<MigrationTaskRecord, MigrationTaskError>;

    /// Atomically claims the oldest captured task, or resumes a running task after restart.
    fn claim_next(&self) -> Result<Option<MigrationTaskRecord>, MigrationTaskError>;

    /// Moves a running task to one terminal state unless it was cancelled concurrently.
    fn finish(&self, task_id: &str, state: MigrationTaskState) -> Result<(), MigrationTaskError>;

    /// Returns a failed task to the queue so its Episode continues instead of starting over.
    ///
    /// Only from `Failed`. A completed task has nothing to continue and a cancelled one was stopped
    /// on purpose; both would be a new decision rather than a resumption.
    fn resume(
        &self,
        owner_id: &str,
        task_id: &str,
    ) -> Result<MigrationTaskRecord, MigrationTaskError>;

    /// Reports whether a task has been cancelled by its owner.
    fn is_cancelled(&self, task_id: &str) -> Result<bool, MigrationTaskError>;
}
