//! Shared behavioral contract for server assignment dispatch persistence.

use crate::adapters::sqlite::SqliteControlRepository;
use crate::storage::{
    ArtifactIdentity, AssignmentContract, AssignmentDeliveryPreparation, AssignmentReadRepository,
    AssignmentRecord, AssignmentWriteRepository, AttemptLifecycleRepository, AttemptState,
    ConnectionRegistration, ExecutionContract, FinishedObservation, LeaseRecord,
    ReassignmentRecord, RepositoryError, ServerFrameKind, ServerOutboxFrame,
    ServerOutboxRepository, StoreAssignmentOutcome, WorkerCapabilities, WorkerConnectionRepository,
    WorkerRegistration,
};
use alloyport_core::{AssignmentId, AttemptId, CandidateId, ExecutionKind, TaskId};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::{Mutex, MutexGuard};

const WORKER_ID: &str = "worker-1";
const CONNECTION_ID: &str = "assignment-contract-connection";

#[test]
fn sqlite_assignment_store_satisfies_shared_dispatch_contract() -> Result<(), Box<dyn Error>> {
    assignment_dispatch_port_contract(&SqliteControlRepository::in_memory()?)
}

#[test]
fn memory_assignment_store_satisfies_shared_dispatch_contract() -> Result<(), Box<dyn Error>> {
    assignment_dispatch_port_contract(&MemoryAssignmentRepository::default())
}

fn assignment_dispatch_port_contract(
    repository: &impl AssignmentDispatchHarness,
) -> Result<(), Box<dyn Error>> {
    repository.initialize_connection()?;
    admission_and_preparation_contract(repository)?;
    atomic_dispatch_contract(repository)?;
    reassignment_contract(repository)?;
    Ok(())
}

fn admission_and_preparation_contract(
    repository: &impl AssignmentDispatchHarness,
) -> Result<(), Box<dyn Error>> {
    let first = contract("attempt-first", 1)?;
    assert_eq!(
        repository.store_assignment(WORKER_ID, &first, 100)?,
        StoreAssignmentOutcome::Inserted
    );
    assert_eq!(
        repository.store_assignment(WORKER_ID, &first, 101)?,
        StoreAssignmentOutcome::Duplicate
    );
    let mut changed = first.clone();
    changed.execution.argv = vec!["different".to_owned()];
    assert!(matches!(
        repository.store_assignment(WORKER_ID, &changed, 102),
        Err(RepositoryError::ConflictingAttempt(attempt)) if attempt == "attempt-first"
    ));

    let second = contract("attempt-second", 1)?;
    repository.store_assignment(WORKER_ID, &second, 110)?;
    assert_eq!(repository.preparing_assignment_count()?, 2);
    assert!(repository.preparing_assignments(0)?.is_empty());
    assert_eq!(
        repository.preparing_assignments(1)?[0].contract.attempt_id,
        first.attempt_id
    );
    assert!(repository.defer_assignment_preparation(first.attempt_id.as_str(), WORKER_ID, 200,)?);
    assert_eq!(
        repository.preparing_assignments(1)?[0].contract.attempt_id,
        second.attempt_id
    );
    assert!(!repository.defer_assignment_preparation(
        first.attempt_id.as_str(),
        "worker-other",
        201,
    )?);
    assert!(repository.replayable_assignments(WORKER_ID)?.is_empty());
    assert!(repository.mark_assignment_dispatchable(second.attempt_id.as_str(), WORKER_ID, 120,)?);
    assert!(!repository.mark_assignment_dispatchable(
        second.attempt_id.as_str(),
        WORKER_ID,
        121,
    )?);
    assert!(matches!(
        repository.mark_assignment_dispatchable(first.attempt_id.as_str(), "worker-other", 202),
        Err(RepositoryError::IdentityMismatch(attempt)) if attempt == "attempt-first"
    ));
    assert_eq!(repository.replayable_assignments(WORKER_ID)?.len(), 1);
    Ok(())
}

fn atomic_dispatch_contract(
    repository: &impl AssignmentDispatchHarness,
) -> Result<(), Box<dyn Error>> {
    let second = contract("attempt-second", 1)?;
    let first_delivery = delivery(&second, 2, 130, 100);
    assert_eq!(
        repository.prepare_assignment_delivery(&first_delivery)?,
        second
    );
    assert_eq!(
        repository
            .assignment("attempt-second")?
            .expect("dispatched assignment remains readable")
            .state,
        AttemptState::Sent
    );
    let original_lease = repository
        .contract_lease("attempt-second")?
        .expect("successful dispatch grants a lease");
    assert_eq!(original_lease.expires_at_ms, 230);
    assert_eq!(repository.contract_outbox_len()?, 1);

    assert!(
        repository
            .prepare_assignment_delivery(&delivery(&second, 2, 140, 500))
            .is_err()
    );
    assert_eq!(
        repository
            .contract_lease("attempt-second")?
            .expect("failed retry preserves the original lease"),
        original_lease
    );
    assert_eq!(repository.contract_outbox_len()?, 1);

    let third = contract("attempt-third", 1)?;
    repository.store_assignment(WORKER_ID, &third, 140)?;
    repository.mark_assignment_dispatchable(third.attempt_id.as_str(), WORKER_ID, 141)?;
    assert!(
        repository
            .prepare_assignment_delivery(&delivery(&third, 2, 142, 100))
            .is_err()
    );
    assert_eq!(
        repository
            .assignment("attempt-third")?
            .expect("failed dispatch keeps assignment")
            .state,
        AttemptState::Dispatchable
    );
    assert!(repository.contract_lease("attempt-third")?.is_none());
    assert_eq!(repository.contract_outbox_len()?, 1);

    repository.prepare_assignment_delivery(&delivery(&third, 3, 150, 100))?;
    assert_eq!(
        repository
            .assignment("attempt-third")?
            .expect("successful retry sends assignment")
            .state,
        AttemptState::Sent
    );
    assert!(repository.contract_lease("attempt-third")?.is_some());
    assert_eq!(repository.contract_outbox_len()?, 2);
    Ok(())
}

fn reassignment_contract(
    repository: &impl AssignmentDispatchHarness,
) -> Result<(), Box<dyn Error>> {
    let source = contract("attempt-source", 1)?;
    repository.store_assignment(WORKER_ID, &source, 300)?;
    repository.mark_assignment_dispatchable(source.attempt_id.as_str(), WORKER_ID, 301)?;
    repository.prepare_assignment_delivery(&delivery(&source, 4, 310, 100))?;
    assert!(matches!(
        repository.reassign_expired("attempt-source", "worker-2", "attempt-replacement", 350),
        Err(RepositoryError::InvalidTransition {
            from: AttemptState::Sent,
            to: AttemptState::Dispatchable,
        })
    ));
    repository.expire_contract_leases(410)?;
    assert_eq!(
        repository
            .assignment("attempt-source")?
            .expect("source remains auditable")
            .state,
        AttemptState::LeaseExpired
    );
    assert!(matches!(
        repository.reassign_expired("attempt-source", "worker-2", "  ", 411),
        Err(RepositoryError::InvalidIdentity(_))
    ));

    let replacement =
        repository.reassign_expired("attempt-source", "worker-2", "attempt-replacement", 412)?;
    assert_eq!(replacement.outcome, StoreAssignmentOutcome::Inserted);
    assert_eq!(replacement.assignment.worker_id, "worker-2");
    assert_eq!(
        replacement.assignment.contract.attempt_id.as_str(),
        "attempt-replacement"
    );
    assert_eq!(replacement.assignment.contract.attempt_number, 2);
    assert_eq!(replacement.assignment.state, AttemptState::Preparing);
    assert_eq!(
        repository
            .reassign_expired("attempt-source", "worker-2", "attempt-replacement", 413,)?
            .outcome,
        StoreAssignmentOutcome::Duplicate
    );
    assert!(matches!(
        repository.reassign_expired("attempt-source", "worker-2", "attempt-other", 414),
        Err(RepositoryError::ConflictingAttempt(attempt)) if attempt == "attempt-source"
    ));
    Ok(())
}

trait AssignmentDispatchHarness: AssignmentReadRepository + AssignmentWriteRepository {
    fn initialize_connection(&self) -> Result<(), RepositoryError>;
    fn contract_lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError>;
    fn contract_outbox_len(&self) -> Result<usize, RepositoryError>;
    fn expire_contract_leases(&self, now_ms: u64) -> Result<(), RepositoryError>;
}

impl AssignmentDispatchHarness for SqliteControlRepository {
    fn initialize_connection(&self) -> Result<(), RepositoryError> {
        self.register_worker(&worker_registration(), &connection_registration())
    }

    fn contract_lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError> {
        self.lease(attempt_id)
    }

    fn contract_outbox_len(&self) -> Result<usize, RepositoryError> {
        self.server_outbox_len(CONNECTION_ID)
    }

    fn expire_contract_leases(&self, now_ms: u64) -> Result<(), RepositoryError> {
        self.expire_leases(now_ms).map(|_| ())
    }
}

#[derive(Debug, Default)]
struct MemoryAssignmentRepository {
    state: Mutex<MemoryAssignmentState>,
}

#[derive(Debug, Default)]
struct MemoryAssignmentState {
    connected: bool,
    assignments: BTreeMap<String, AssignmentRecord>,
    leases: BTreeMap<String, LeaseRecord>,
    outbox_sequences: BTreeSet<u64>,
    reassignments: BTreeMap<String, (String, String)>,
}

impl MemoryAssignmentRepository {
    fn state(&self) -> Result<MutexGuard<'_, MemoryAssignmentState>, RepositoryError> {
        self.state.lock().map_err(|_| RepositoryError::LockPoisoned)
    }
}

impl AssignmentReadRepository for MemoryAssignmentRepository {
    fn assignment(&self, attempt_id: &str) -> Result<Option<AssignmentRecord>, RepositoryError> {
        Ok(self.state()?.assignments.get(attempt_id).cloned())
    }

    fn finished_observation(
        &self,
        _attempt_id: &str,
    ) -> Result<Option<FinishedObservation>, RepositoryError> {
        Ok(None)
    }

    fn preparing_assignments(
        &self,
        limit: usize,
    ) -> Result<Vec<AssignmentRecord>, RepositoryError> {
        let state = self.state()?;
        let mut assignments = state
            .assignments
            .values()
            .filter(|assignment| assignment.state == AttemptState::Preparing)
            .cloned()
            .collect::<Vec<_>>();
        assignments.sort_by(|left, right| {
            (left.updated_at_ms, &left.contract.attempt_id)
                .cmp(&(right.updated_at_ms, &right.contract.attempt_id))
        });
        assignments.truncate(limit);
        Ok(assignments)
    }

    fn preparing_assignment_count(&self) -> Result<usize, RepositoryError> {
        Ok(self
            .state()?
            .assignments
            .values()
            .filter(|assignment| assignment.state == AttemptState::Preparing)
            .count())
    }

    fn replayable_assignments(
        &self,
        worker_id: &str,
    ) -> Result<Vec<AssignmentRecord>, RepositoryError> {
        let state = self.state()?;
        let mut assignments = state
            .assignments
            .values()
            .filter(|assignment| {
                assignment.worker_id == worker_id && assignment.state.is_replayable()
            })
            .cloned()
            .collect::<Vec<_>>();
        assignments.sort_by(|left, right| {
            (left.created_at_ms, &left.contract.attempt_id)
                .cmp(&(right.created_at_ms, &right.contract.attempt_id))
        });
        Ok(assignments)
    }
}

impl AssignmentWriteRepository for MemoryAssignmentRepository {
    fn store_assignment(
        &self,
        worker_id: &str,
        contract: &AssignmentContract,
        at_ms: u64,
    ) -> Result<StoreAssignmentOutcome, RepositoryError> {
        let mut state = self.state()?;
        if let Some(existing) = state.assignments.get(contract.attempt_id.as_str()) {
            return if existing.worker_id == worker_id && existing.contract == *contract {
                Ok(StoreAssignmentOutcome::Duplicate)
            } else {
                Err(RepositoryError::ConflictingAttempt(
                    contract.attempt_id.to_string(),
                ))
            };
        }
        state.assignments.insert(
            contract.attempt_id.to_string(),
            AssignmentRecord {
                worker_id: worker_id.to_owned(),
                contract: contract.clone(),
                state: AttemptState::Preparing,
                created_at_ms: at_ms,
                updated_at_ms: at_ms,
                cancellation_reason: None,
            },
        );
        Ok(StoreAssignmentOutcome::Inserted)
    }

    fn mark_assignment_dispatchable(
        &self,
        attempt_id: &str,
        worker_id: &str,
        at_ms: u64,
    ) -> Result<bool, RepositoryError> {
        let mut state = self.state()?;
        let assignment = assignment_mut(&mut state, attempt_id, worker_id)?;
        if assignment.state == AttemptState::Preparing {
            assignment.state = AttemptState::Dispatchable;
            assignment.updated_at_ms = at_ms;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn defer_assignment_preparation(
        &self,
        attempt_id: &str,
        worker_id: &str,
        retry_at_ms: u64,
    ) -> Result<bool, RepositoryError> {
        let mut state = self.state()?;
        let Some(assignment) = state.assignments.get_mut(attempt_id) else {
            return Ok(false);
        };
        if assignment.worker_id != worker_id || assignment.state != AttemptState::Preparing {
            return Ok(false);
        }
        assignment.updated_at_ms = retry_at_ms;
        Ok(true)
    }

    fn reassign_expired(
        &self,
        expired_attempt_id: &str,
        replacement_worker_id: &str,
        replacement_attempt_id: &str,
        at_ms: u64,
    ) -> Result<ReassignmentRecord, RepositoryError> {
        let mut state = self.state()?;
        if let Some((existing_attempt, existing_worker)) =
            state.reassignments.get(expired_attempt_id)
        {
            if existing_attempt != replacement_attempt_id
                || existing_worker != replacement_worker_id
            {
                return Err(RepositoryError::ConflictingAttempt(
                    expired_attempt_id.to_owned(),
                ));
            }
            let assignment = state
                .assignments
                .get(replacement_attempt_id)
                .cloned()
                .ok_or_else(|| RepositoryError::NotFound(replacement_attempt_id.to_owned()))?;
            return Ok(ReassignmentRecord {
                outcome: StoreAssignmentOutcome::Duplicate,
                assignment,
            });
        }
        let source = state
            .assignments
            .get(expired_attempt_id)
            .cloned()
            .ok_or_else(|| RepositoryError::NotFound(expired_attempt_id.to_owned()))?;
        if source.state != AttemptState::LeaseExpired {
            return Err(RepositoryError::InvalidTransition {
                from: source.state,
                to: AttemptState::Dispatchable,
            });
        }
        if replacement_attempt_id.is_empty() || replacement_attempt_id == expired_attempt_id {
            return Err(RepositoryError::ConflictingAttempt(
                replacement_attempt_id.to_owned(),
            ));
        }
        let replacement_id = AttemptId::try_from(replacement_attempt_id)
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?;
        if state.assignments.contains_key(replacement_attempt_id) {
            return Err(RepositoryError::ConflictingAttempt(
                replacement_attempt_id.to_owned(),
            ));
        }
        let mut replacement = source;
        replacement_worker_id.clone_into(&mut replacement.worker_id);
        replacement.contract.attempt_id = replacement_id;
        replacement.contract.attempt_number = replacement.contract.attempt_number.saturating_add(1);
        replacement.state = AttemptState::Preparing;
        replacement.created_at_ms = at_ms;
        replacement.updated_at_ms = at_ms;
        replacement.cancellation_reason = None;
        state
            .assignments
            .insert(replacement_attempt_id.to_owned(), replacement.clone());
        state.reassignments.insert(
            expired_attempt_id.to_owned(),
            (
                replacement_attempt_id.to_owned(),
                replacement_worker_id.to_owned(),
            ),
        );
        Ok(ReassignmentRecord {
            outcome: StoreAssignmentOutcome::Inserted,
            assignment: replacement,
        })
    }

    fn prepare_assignment_delivery(
        &self,
        preparation: &AssignmentDeliveryPreparation,
    ) -> Result<AssignmentContract, RepositoryError> {
        let attempt_id =
            preparation.frame.attempt_id.as_deref().ok_or_else(|| {
                RepositoryError::Corrupt("assignment frame has no attempt ID".into())
            })?;
        if preparation.frame.kind != ServerFrameKind::Assignment {
            return Err(RepositoryError::Corrupt(
                "assignment delivery preparation received a non-assignment frame".into(),
            ));
        }
        let mut state = self.state()?;
        let assignment = state
            .assignments
            .get(attempt_id)
            .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))?;
        if assignment.worker_id != preparation.frame.worker_id {
            return Err(RepositoryError::IdentityMismatch(attempt_id.to_owned()));
        }
        if !assignment.state.is_replayable() {
            return Err(RepositoryError::InvalidTransition {
                from: assignment.state,
                to: AttemptState::Sent,
            });
        }
        if state
            .leases
            .get(attempt_id)
            .is_some_and(|lease| lease.expires_at_ms <= preparation.now_ms)
        {
            expire_memory_assignment(&mut state, attempt_id, preparation.now_ms);
            return Err(RepositoryError::InvalidTransition {
                from: AttemptState::LeaseExpired,
                to: AttemptState::Sent,
            });
        }
        if !state.connected {
            return Err(RepositoryError::Corrupt(format!(
                "active connection {} for worker {} is missing",
                preparation.frame.connection_id, preparation.frame.worker_id
            )));
        }
        if state.outbox_sequences.contains(&preparation.frame.sequence) {
            return Err(RepositoryError::Corrupt(format!(
                "outbox sequence {} conflicts",
                preparation.frame.sequence
            )));
        }
        let contract = assignment.contract.clone();
        let assignment = state
            .assignments
            .get_mut(attempt_id)
            .expect("assignment checked above");
        if assignment.state == AttemptState::Dispatchable {
            assignment.state = AttemptState::Sent;
        }
        assignment.updated_at_ms = preparation.now_ms;
        let existing_grant = state
            .leases
            .get(attempt_id)
            .map_or(preparation.now_ms, |lease| lease.granted_at_ms);
        state.leases.insert(
            attempt_id.to_owned(),
            LeaseRecord {
                attempt_id: attempt_id.to_owned(),
                lease_id: preparation.lease_id.clone(),
                worker_id: preparation.frame.worker_id.clone(),
                granted_at_ms: existing_grant,
                renewed_at_ms: preparation.now_ms,
                expires_at_ms: preparation
                    .now_ms
                    .saturating_add(preparation.lease_duration_ms),
                expired_at_ms: None,
            },
        );
        state.outbox_sequences.insert(preparation.frame.sequence);
        Ok(contract)
    }
}

impl AssignmentDispatchHarness for MemoryAssignmentRepository {
    fn initialize_connection(&self) -> Result<(), RepositoryError> {
        self.state()?.connected = true;
        Ok(())
    }

    fn contract_lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError> {
        Ok(self.state()?.leases.get(attempt_id).cloned())
    }

    fn contract_outbox_len(&self) -> Result<usize, RepositoryError> {
        Ok(self.state()?.outbox_sequences.len())
    }

    fn expire_contract_leases(&self, now_ms: u64) -> Result<(), RepositoryError> {
        let mut state = self.state()?;
        let due = state
            .leases
            .iter()
            .filter(|(attempt_id, lease)| {
                lease.expired_at_ms.is_none()
                    && lease.expires_at_ms <= now_ms
                    && state
                        .assignments
                        .get(*attempt_id)
                        .is_some_and(|assignment| assignment.state.is_replayable())
            })
            .map(|(attempt_id, _)| attempt_id.clone())
            .collect::<Vec<_>>();
        for attempt_id in due {
            expire_memory_assignment(&mut state, &attempt_id, now_ms);
        }
        Ok(())
    }
}

fn assignment_mut<'a>(
    state: &'a mut MemoryAssignmentState,
    attempt_id: &str,
    worker_id: &str,
) -> Result<&'a mut AssignmentRecord, RepositoryError> {
    let assignment = state
        .assignments
        .get_mut(attempt_id)
        .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))?;
    if assignment.worker_id != worker_id {
        return Err(RepositoryError::IdentityMismatch(attempt_id.to_owned()));
    }
    Ok(assignment)
}

fn expire_memory_assignment(state: &mut MemoryAssignmentState, attempt_id: &str, now_ms: u64) {
    if let Some(lease) = state.leases.get_mut(attempt_id)
        && lease.expired_at_ms.is_none()
    {
        lease.expired_at_ms = Some(now_ms);
    }
    if let Some(assignment) = state.assignments.get_mut(attempt_id)
        && assignment.state.is_replayable()
    {
        assignment.state = AttemptState::LeaseExpired;
        assignment.updated_at_ms = now_ms;
    }
}

fn delivery(
    contract: &AssignmentContract,
    sequence: u64,
    now_ms: u64,
    lease_duration_ms: u64,
) -> AssignmentDeliveryPreparation {
    AssignmentDeliveryPreparation {
        frame: ServerOutboxFrame {
            connection_id: CONNECTION_ID.to_owned(),
            sequence,
            message_id: format!("assignment:{}:{sequence}", contract.attempt_id),
            worker_id: WORKER_ID.to_owned(),
            kind: ServerFrameKind::Assignment,
            attempt_id: Some(contract.attempt_id.to_string()),
        },
        lease_id: format!("lease:{}", contract.attempt_id),
        last_worker_sequence: 1,
        last_server_acknowledged_by_worker: 0,
        now_ms,
        lease_duration_ms,
    }
}

fn contract(attempt_id: &str, attempt_number: u32) -> Result<AssignmentContract, RepositoryError> {
    Ok(AssignmentContract {
        assignment_id: AssignmentId::try_from(format!("assignment-{attempt_id}"))
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        attempt_id: AttemptId::try_from(attempt_id)
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        attempt_number,
        idempotency_key: format!("key-{attempt_id}"),
        task_id: TaskId::try_from(format!("task-{attempt_id}"))
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        candidate_id: CandidateId::try_from("candidate-assignment-contract")
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        execution: ExecutionContract {
            executor_kind: ExecutionKind::Container,
            argv: vec!["true".to_owned()],
            working_directory: "source".to_owned(),
            environment: Vec::new(),
            timeout_ms: 1_000,
            bundle: artifact('a'),
            image: artifact('b'),
            limits: None,
        },
        required_features: Vec::new(),
    })
}

fn artifact(byte: char) -> ArtifactIdentity {
    ArtifactIdentity {
        digest: format!("sha256:{}", byte.to_string().repeat(64))
            .parse()
            .expect("valid fixture digest"),
        size_bytes: 1,
        media_type: "application/octet-stream".to_owned(),
    }
}

fn worker_registration() -> WorkerRegistration {
    WorkerRegistration {
        protocol_major: 1,
        protocol_minor: 0,
        worker_id: WORKER_ID.to_owned(),
        instance_id: "assignment-contract-instance".to_owned(),
        worker_version: "contract".to_owned(),
        features: Vec::new(),
        capabilities: WorkerCapabilities {
            backend: 1,
            architecture: "contract".to_owned(),
            device_count: 1,
            max_concurrency: 1,
            driver_version: "contract".to_owned(),
            toolkit_version: "contract".to_owned(),
            container_runtime: "contract".to_owned(),
            devices: Vec::new(),
        },
    }
}

fn connection_registration() -> ConnectionRegistration {
    ConnectionRegistration {
        connection_id: CONNECTION_ID.to_owned(),
        worker_id: WORKER_ID.to_owned(),
        instance_id: "assignment-contract-instance".to_owned(),
        connected_at_ms: 1,
    }
}
