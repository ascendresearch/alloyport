//! Reusable behavioral contracts for server control persistence ports.

use crate::adapters::sqlite::SqliteControlRepository;
use crate::storage::{
    ArtifactIdentity, AssignmentContract, AssignmentDeliveryPreparation, AssignmentReadRepository,
    AssignmentWriteRepository, AttemptLifecycleRepository, AttemptObservation, AttemptState,
    CancellationRecord, CancellationStoreOutcome, ConnectionRegistration, ExecutionContract,
    FinishedObservation, LeaseRecord, ObservationDisposition, ObservedAttempt, RepositoryError,
    ServerFrameKind, ServerOutboxFrame, WorkerCapabilities, WorkerConnectionRepository,
    WorkerRegistration,
};
use alloyport_core::{AssignmentId, AttemptId, AttemptOutcome, CandidateId, ExecutionKind, TaskId};
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::{Mutex, MutexGuard};

const WORKER_ID: &str = "worker-1";
const CONNECTION_ID: &str = "contract-connection";

#[test]
fn sqlite_attempt_lifecycle_satisfies_shared_port_contract() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    attempt_lifecycle_port_contract(&repository)
}

#[test]
fn memory_attempt_lifecycle_satisfies_shared_port_contract() -> Result<(), Box<dyn Error>> {
    attempt_lifecycle_port_contract(&MemoryAttemptLifecycleRepository::default())
}

fn attempt_lifecycle_port_contract(
    repository: &impl AttemptLifecycleHarness,
) -> Result<(), Box<dyn Error>> {
    repository.populate_contract_fixtures()?;
    cancellation_before_send_contract(repository)?;
    lease_renewal_and_expiry_contract(repository)?;
    observation_transition_contract(repository)?;
    stale_observation_contract(repository)?;
    cancellation_acknowledgement_contract(repository)?;
    identity_contract(repository)?;
    Ok(())
}

fn cancellation_before_send_contract(
    repository: &impl AttemptLifecycleHarness,
) -> Result<(), Box<dyn Error>> {
    assert!(repository.lease("attempt-missing")?.is_none());
    assert!(matches!(
        repository.request_cancellation("attempt-missing", "missing", 1),
        Err(RepositoryError::NotFound(attempt)) if attempt == "attempt-missing"
    ));

    let queued = repository.request_cancellation("attempt-queued", "cancel queued", 10)?;
    assert_eq!(queued.worker_id, WORKER_ID);
    assert_eq!(
        queued.outcome,
        CancellationStoreOutcome::CancelledBeforeSend
    );
    assert_eq!(
        repository.attempt_state("attempt-queued")?,
        AttemptState::Cancelled
    );
    assert_eq!(
        repository
            .request_cancellation("attempt-queued", "duplicate", 11)?
            .outcome,
        CancellationStoreOutcome::AlreadyTerminal
    );
    Ok(())
}

fn lease_renewal_and_expiry_contract(
    repository: &impl AttemptLifecycleHarness,
) -> Result<(), Box<dyn Error>> {
    repository.renew_active_leases(WORKER_ID, &["attempt-renew".to_owned()], 1_050, 100)?;
    let renewed = repository
        .lease("attempt-renew")?
        .expect("active lease is observable");
    assert_eq!(renewed.renewed_at_ms, 1_050);
    assert_eq!(renewed.expires_at_ms, 1_150);
    assert!(repository.expire_leases(1_100)?.is_empty());
    assert_eq!(repository.expire_leases(1_150)?, vec!["attempt-renew"]);
    assert_eq!(
        repository.attempt_state("attempt-renew")?,
        AttemptState::LeaseExpired
    );

    repository.renew_active_leases(WORKER_ID, &["attempt-late".to_owned()], 2_101, 100)?;
    let late = repository
        .lease("attempt-late")?
        .expect("expired lease remains auditable");
    assert_eq!(late.expired_at_ms, Some(2_101));
    assert_eq!(
        repository.attempt_state("attempt-late")?,
        AttemptState::LeaseExpired
    );
    repository.renew_active_leases(WORKER_ID, &["attempt-late".to_owned()], 2_102, 100)?;
    assert_eq!(
        repository
            .lease("attempt-late")?
            .expect("expired lease remains auditable")
            .expired_at_ms,
        Some(2_101),
        "a later heartbeat cannot resurrect or rewrite an expired lease"
    );
    Ok(())
}

fn observation_transition_contract(
    repository: &impl AttemptLifecycleHarness,
) -> Result<(), Box<dyn Error>> {
    let accepted = observation(
        "attempt-observe",
        3_001,
        AttemptObservation::Accepted {
            already_known: false,
        },
    )?;
    assert_eq!(
        repository.observe_attempt(&accepted)?,
        ObservationDisposition::Applied
    );
    assert_eq!(
        repository.observe_attempt(&accepted)?,
        ObservationDisposition::Duplicate
    );
    assert_eq!(
        repository.observe_attempt(&observation(
            "attempt-observe",
            3_002,
            AttemptObservation::Started,
        )?)?,
        ObservationDisposition::Applied
    );
    assert_eq!(
        repository.observe_attempt(&accepted)?,
        ObservationDisposition::Duplicate
    );
    let completed = observation(
        "attempt-observe",
        3_003,
        successful_observation("completed"),
    )?;
    assert_eq!(
        repository.observe_attempt(&completed)?,
        ObservationDisposition::Applied
    );
    assert_eq!(
        repository.observe_attempt(&completed)?,
        ObservationDisposition::Duplicate
    );
    assert_eq!(
        repository.attempt_state("attempt-observe")?,
        AttemptState::Finished
    );
    let finished = repository
        .finished_observation("attempt-observe")?
        .expect("terminal observation remains queryable");
    assert_eq!(finished.outcome, AttemptOutcome::Succeeded);
    assert_eq!(finished.detail, "completed");
    assert_eq!(
        repository
            .request_cancellation("attempt-observe", "too late", 3_004)?
            .outcome,
        CancellationStoreOutcome::AlreadyTerminal
    );
    Ok(())
}

fn stale_observation_contract(
    repository: &impl AttemptLifecycleHarness,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(repository.expire_leases(4_100)?, vec!["attempt-stale"]);
    assert_eq!(
        repository.observe_attempt(&observation(
            "attempt-stale",
            4_101,
            successful_observation("late success"),
        )?)?,
        ObservationDisposition::Stale
    );
    assert_eq!(
        repository.attempt_state("attempt-stale")?,
        AttemptState::LeaseExpired
    );
    Ok(())
}

fn cancellation_acknowledgement_contract(
    repository: &impl AttemptLifecycleHarness,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        repository
            .request_cancellation("attempt-cancel", "operator request", 5_001)?
            .outcome,
        CancellationStoreOutcome::Requested
    );
    assert_eq!(
        repository
            .request_cancellation("attempt-cancel", "operator request", 5_002)?
            .outcome,
        CancellationStoreOutcome::Duplicate
    );
    assert_eq!(
        repository.observe_attempt(&observation(
            "attempt-cancel",
            5_003,
            AttemptObservation::CancellationAcknowledged {
                already_terminal: false,
            },
        )?)?,
        ObservationDisposition::Applied
    );
    assert_eq!(
        repository.attempt_state("attempt-cancel")?,
        AttemptState::CancelRequested,
        "control acknowledgement is not execution termination"
    );
    assert_eq!(
        repository.observe_attempt(&observation(
            "attempt-cancel",
            5_004,
            successful_observation("cancelled execution terminated"),
        )?)?,
        ObservationDisposition::Applied
    );
    assert_eq!(
        repository.attempt_state("attempt-cancel")?,
        AttemptState::Finished
    );
    Ok(())
}

fn identity_contract(repository: &impl AttemptLifecycleHarness) -> Result<(), Box<dyn Error>> {
    let mut wrong_worker = observation(
        "attempt-identity",
        6_001,
        AttemptObservation::Accepted {
            already_known: false,
        },
    )?;
    wrong_worker.worker_id = "worker-other".to_owned();
    assert!(matches!(
        repository.observe_attempt(&wrong_worker),
        Err(RepositoryError::IdentityMismatch(attempt)) if attempt == "attempt-identity"
    ));
    Ok(())
}

trait AttemptLifecycleHarness: AttemptLifecycleRepository {
    fn populate_contract_fixtures(&self) -> Result<(), RepositoryError>;
    fn attempt_state(&self, attempt_id: &str) -> Result<AttemptState, RepositoryError>;
    fn finished_observation(
        &self,
        attempt_id: &str,
    ) -> Result<Option<FinishedObservation>, RepositoryError>;
}

impl AttemptLifecycleHarness for SqliteControlRepository {
    fn populate_contract_fixtures(&self) -> Result<(), RepositoryError> {
        self.register_worker(&worker_registration(), &connection_registration())?;
        store_preparing(self, "attempt-queued", 10)?;
        for (attempt_id, granted_at_ms) in [
            ("attempt-renew", 1_000),
            ("attempt-late", 2_000),
            ("attempt-observe", 3_000),
            ("attempt-stale", 4_000),
            ("attempt-cancel", 5_000),
            ("attempt-identity", 6_000),
        ] {
            store_leased(self, attempt_id, granted_at_ms, 100)?;
        }
        Ok(())
    }

    fn attempt_state(&self, attempt_id: &str) -> Result<AttemptState, RepositoryError> {
        self.assignment(attempt_id)?
            .map(|assignment| assignment.state)
            .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))
    }

    fn finished_observation(
        &self,
        attempt_id: &str,
    ) -> Result<Option<FinishedObservation>, RepositoryError> {
        AssignmentReadRepository::finished_observation(self, attempt_id)
    }
}

#[derive(Debug, Default)]
struct MemoryAttemptLifecycleRepository {
    attempts: Mutex<BTreeMap<String, MemoryAttempt>>,
}

#[derive(Debug)]
struct MemoryAttempt {
    assignment_id: AssignmentId,
    worker_id: String,
    state: AttemptState,
    lease: Option<LeaseRecord>,
    cancellation_reason: Option<String>,
    finished: Option<FinishedObservation>,
}

impl MemoryAttemptLifecycleRepository {
    fn attempts(&self) -> Result<MutexGuard<'_, BTreeMap<String, MemoryAttempt>>, RepositoryError> {
        self.attempts
            .lock()
            .map_err(|_| RepositoryError::LockPoisoned)
    }
}

impl AttemptLifecycleHarness for MemoryAttemptLifecycleRepository {
    fn populate_contract_fixtures(&self) -> Result<(), RepositoryError> {
        let mut attempts = self.attempts()?;
        attempts.insert(
            "attempt-queued".to_owned(),
            memory_attempt("attempt-queued", None)?,
        );
        for (attempt_id, granted_at_ms) in [
            ("attempt-renew", 1_000),
            ("attempt-late", 2_000),
            ("attempt-observe", 3_000),
            ("attempt-stale", 4_000),
            ("attempt-cancel", 5_000),
            ("attempt-identity", 6_000),
        ] {
            attempts.insert(
                attempt_id.to_owned(),
                memory_attempt(attempt_id, Some((granted_at_ms, 100)))?,
            );
        }
        Ok(())
    }

    fn attempt_state(&self, attempt_id: &str) -> Result<AttemptState, RepositoryError> {
        self.attempts()?
            .get(attempt_id)
            .map(|attempt| attempt.state)
            .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))
    }

    fn finished_observation(
        &self,
        attempt_id: &str,
    ) -> Result<Option<FinishedObservation>, RepositoryError> {
        Ok(self
            .attempts()?
            .get(attempt_id)
            .and_then(|attempt| attempt.finished.clone()))
    }
}

impl AttemptLifecycleRepository for MemoryAttemptLifecycleRepository {
    fn observe_attempt(
        &self,
        observation: &ObservedAttempt,
    ) -> Result<ObservationDisposition, RepositoryError> {
        let mut attempts = self.attempts()?;
        let attempt = attempts
            .get_mut(observation.attempt_id.as_str())
            .ok_or_else(|| RepositoryError::NotFound(observation.attempt_id.to_string()))?;
        if attempt.worker_id != observation.worker_id
            || attempt.assignment_id != observation.assignment_id
        {
            return Err(RepositoryError::IdentityMismatch(
                observation.attempt_id.to_string(),
            ));
        }
        let target = observation.observation.target_state();
        let disposition = match target {
            None => match attempt.state {
                AttemptState::CancelRequested => ObservationDisposition::Applied,
                AttemptState::Finished | AttemptState::Cancelled => {
                    ObservationDisposition::Duplicate
                }
                AttemptState::LeaseExpired => ObservationDisposition::Stale,
                current => {
                    return Err(RepositoryError::InvalidTransition {
                        from: current,
                        to: AttemptState::CancelRequested,
                    });
                }
            },
            Some(target) => {
                let expired = attempt
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.expires_at_ms <= observation.observed_at_ms);
                let stale = attempt.state == AttemptState::LeaseExpired
                    || (attempt.state == AttemptState::Rejected
                        && target == AttemptState::Finished)
                    || (target == AttemptState::Finished
                        && attempt.state != AttemptState::Finished
                        && expired);
                if stale {
                    expire_memory_attempt(attempt, observation.observed_at_ms);
                    ObservationDisposition::Stale
                } else if attempt.state == target
                    || attempt.state == AttemptState::Finished
                    || (attempt.state == AttemptState::Running && target == AttemptState::Accepted)
                    || (attempt.state == AttemptState::CancelRequested
                        && target == AttemptState::Accepted)
                {
                    ObservationDisposition::Duplicate
                } else if transition_allowed(attempt.state, target) {
                    attempt.state = target;
                    ObservationDisposition::Applied
                } else {
                    return Err(RepositoryError::InvalidTransition {
                        from: attempt.state,
                        to: target,
                    });
                }
            }
        };
        if disposition == ObservationDisposition::Applied
            && let AttemptObservation::Finished(finished) = &observation.observation
        {
            attempt.finished = Some((**finished).clone());
        }
        Ok(disposition)
    }

    fn renew_active_leases(
        &self,
        worker_id: &str,
        attempt_ids: &[String],
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<(), RepositoryError> {
        let mut attempts = self.attempts()?;
        for attempt_id in attempt_ids {
            let attempt = attempts
                .get_mut(attempt_id)
                .ok_or_else(|| RepositoryError::NotFound(attempt_id.clone()))?;
            if attempt.worker_id != worker_id {
                return Err(RepositoryError::IdentityMismatch(attempt_id.clone()));
            }
            if attempt.state.is_replayable()
                && let Some(lease) = attempt.lease.as_mut()
            {
                if lease.expired_at_ms.is_some() || lease.expires_at_ms <= now_ms {
                    expire_memory_attempt(attempt, now_ms);
                } else {
                    lease.renewed_at_ms = now_ms;
                    lease.expires_at_ms = now_ms.saturating_add(lease_duration_ms);
                }
            }
        }
        Ok(())
    }

    fn expire_leases(&self, now_ms: u64) -> Result<Vec<String>, RepositoryError> {
        let mut attempts = self.attempts()?;
        let mut expired = attempts
            .iter()
            .filter(|(_, attempt)| {
                attempt.state.is_replayable()
                    && attempt.lease.as_ref().is_some_and(|lease| {
                        lease.expired_at_ms.is_none() && lease.expires_at_ms <= now_ms
                    })
            })
            .map(|(attempt_id, _)| attempt_id.clone())
            .collect::<Vec<_>>();
        expired.sort();
        for attempt_id in &expired {
            if let Some(attempt) = attempts.get_mut(attempt_id) {
                expire_memory_attempt(attempt, now_ms);
            }
        }
        Ok(expired)
    }

    fn request_cancellation(
        &self,
        attempt_id: &str,
        reason: &str,
        _now_ms: u64,
    ) -> Result<CancellationRecord, RepositoryError> {
        let mut attempts = self.attempts()?;
        let attempt = attempts
            .get_mut(attempt_id)
            .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))?;
        let (next, outcome) = match attempt.state {
            AttemptState::Preparing | AttemptState::Dispatchable => (
                AttemptState::Cancelled,
                CancellationStoreOutcome::CancelledBeforeSend,
            ),
            AttemptState::Sent | AttemptState::Accepted | AttemptState::Running => (
                AttemptState::CancelRequested,
                CancellationStoreOutcome::Requested,
            ),
            AttemptState::CancelRequested => (
                AttemptState::CancelRequested,
                CancellationStoreOutcome::Duplicate,
            ),
            AttemptState::Finished
            | AttemptState::Rejected
            | AttemptState::LeaseExpired
            | AttemptState::Cancelled => (attempt.state, CancellationStoreOutcome::AlreadyTerminal),
        };
        if outcome != CancellationStoreOutcome::AlreadyTerminal {
            attempt.state = next;
            attempt.cancellation_reason = Some(reason.to_owned());
        }
        Ok(CancellationRecord {
            worker_id: attempt.worker_id.clone(),
            outcome,
        })
    }

    fn lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError> {
        Ok(self
            .attempts()?
            .get(attempt_id)
            .and_then(|attempt| attempt.lease.clone()))
    }
}

fn store_preparing(
    repository: &SqliteControlRepository,
    attempt_id: &str,
    at_ms: u64,
) -> Result<(), RepositoryError> {
    repository.store_assignment(WORKER_ID, &contract(attempt_id)?, at_ms)?;
    Ok(())
}

fn store_leased(
    repository: &SqliteControlRepository,
    attempt_id: &str,
    granted_at_ms: u64,
    duration_ms: u64,
) -> Result<(), RepositoryError> {
    store_preparing(repository, attempt_id, granted_at_ms)?;
    repository.mark_assignment_dispatchable(attempt_id, WORKER_ID, granted_at_ms)?;
    repository.prepare_assignment_delivery(&AssignmentDeliveryPreparation {
        frame: ServerOutboxFrame {
            connection_id: CONNECTION_ID.to_owned(),
            sequence: granted_at_ms,
            message_id: format!("assignment:{attempt_id}"),
            worker_id: WORKER_ID.to_owned(),
            kind: ServerFrameKind::Assignment,
            attempt_id: Some(attempt_id.to_owned()),
        },
        lease_id: format!("lease:{attempt_id}"),
        last_worker_sequence: 1,
        last_server_acknowledged_by_worker: 0,
        now_ms: granted_at_ms,
        lease_duration_ms: duration_ms,
    })?;
    Ok(())
}

fn memory_attempt(
    attempt_id: &str,
    lease: Option<(u64, u64)>,
) -> Result<MemoryAttempt, RepositoryError> {
    Ok(MemoryAttempt {
        assignment_id: AssignmentId::try_from(format!("assignment-{attempt_id}"))
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        worker_id: WORKER_ID.to_owned(),
        state: if lease.is_some() {
            AttemptState::Sent
        } else {
            AttemptState::Preparing
        },
        lease: lease.map(|(granted_at_ms, duration_ms)| LeaseRecord {
            attempt_id: attempt_id.to_owned(),
            lease_id: format!("lease:{attempt_id}"),
            worker_id: WORKER_ID.to_owned(),
            granted_at_ms,
            renewed_at_ms: granted_at_ms,
            expires_at_ms: granted_at_ms.saturating_add(duration_ms),
            expired_at_ms: None,
        }),
        cancellation_reason: None,
        finished: None,
    })
}

fn expire_memory_attempt(attempt: &mut MemoryAttempt, now_ms: u64) {
    if let Some(lease) = attempt.lease.as_mut()
        && lease.expired_at_ms.is_none()
    {
        lease.expired_at_ms = Some(now_ms);
    }
    if attempt.state.is_replayable() {
        attempt.state = AttemptState::LeaseExpired;
    }
}

const fn transition_allowed(from: AttemptState, to: AttemptState) -> bool {
    matches!(
        (from, to),
        (
            AttemptState::Sent,
            AttemptState::Accepted | AttemptState::Rejected
        ) | (
            AttemptState::Accepted,
            AttemptState::Running | AttemptState::Finished
        ) | (
            AttemptState::Running | AttemptState::CancelRequested,
            AttemptState::Finished
        ) | (AttemptState::CancelRequested, AttemptState::Rejected)
    )
}

fn observation(
    attempt_id: &str,
    observed_at_ms: u64,
    observation: AttemptObservation,
) -> Result<ObservedAttempt, RepositoryError> {
    Ok(ObservedAttempt {
        assignment_id: AssignmentId::try_from(format!("assignment-{attempt_id}"))
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        attempt_id: AttemptId::try_from(attempt_id)
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        worker_id: WORKER_ID.to_owned(),
        observed_at_ms,
        observation,
    })
}

fn successful_observation(detail: &str) -> AttemptObservation {
    AttemptObservation::Finished(Box::new(FinishedObservation {
        outcome: AttemptOutcome::Succeeded,
        exit_code: Some(0),
        elapsed_ms: 1,
        receipt: None,
        stdout: None,
        stderr: None,
        detail: detail.to_owned(),
    }))
}

fn contract(attempt_id: &str) -> Result<AssignmentContract, RepositoryError> {
    Ok(AssignmentContract {
        assignment_id: AssignmentId::try_from(format!("assignment-{attempt_id}"))
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        attempt_id: AttemptId::try_from(attempt_id)
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        attempt_number: 1,
        idempotency_key: format!("key-{attempt_id}"),
        task_id: TaskId::try_from(format!("task-{attempt_id}"))
            .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?,
        candidate_id: CandidateId::try_from("candidate-contract")
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
        instance_id: "contract-instance".to_owned(),
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
        instance_id: "contract-instance".to_owned(),
        connected_at_ms: 1,
    }
}
