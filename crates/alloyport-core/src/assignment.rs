//! Immutable execution assignment contract shared by controller and worker applications.

use crate::{
    ArtifactDescriptor, AssignmentId, AttemptId, CandidateId, ExecutionKind, NetworkPolicy, TaskId,
};
use serde::{Deserialize, Serialize};

/// Transport-independent assignment accepted by application and persistence layers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AssignmentContract {
    pub assignment_id: AssignmentId,
    pub attempt_id: AttemptId,
    pub attempt_number: u32,
    pub idempotency_key: String,
    pub task_id: TaskId,
    pub candidate_id: CandidateId,
    pub execution: ExecutionContract,
    pub required_features: Vec<String>,
}

/// Immutable process and sandbox requirements for one assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecutionContract {
    pub executor_kind: ExecutionKind,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub environment: Vec<EnvironmentEntry>,
    pub timeout_ms: u64,
    pub bundle: ArtifactDescriptor,
    pub image: ArtifactDescriptor,
    pub limits: Option<ResourceContract>,
}

/// One explicit environment value passed without shell interpolation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EnvironmentEntry {
    pub name: String,
    pub value: String,
}

/// Resource ceilings and network policy applied by an execution backend.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceContract {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub process_count: u32,
    pub output_bytes: u64,
    pub device_count: u32,
    pub network: NetworkPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn assignment_contract_keeps_existing_json_shape() -> Result<(), Box<dyn Error>> {
        let descriptor = ArtifactDescriptor {
            digest: crate::Sha256Digest::digest_bytes(b"artifact"),
            size_bytes: 8,
            media_type: "application/octet-stream".to_owned(),
        };
        let contract = AssignmentContract {
            assignment_id: AssignmentId::try_from("assignment-1")?,
            attempt_id: AttemptId::try_from("attempt-1")?,
            attempt_number: 1,
            idempotency_key: "task-1:candidate-1".to_owned(),
            task_id: TaskId::try_from("task-1")?,
            candidate_id: CandidateId::try_from("candidate-1")?,
            execution: ExecutionContract {
                executor_kind: ExecutionKind::Container,
                argv: vec!["true".to_owned()],
                working_directory: ".".to_owned(),
                environment: Vec::new(),
                timeout_ms: 1_000,
                bundle: descriptor.clone(),
                image: descriptor,
                limits: None,
            },
            required_features: Vec::new(),
        };

        let encoded = serde_json::to_string(&contract)?;
        assert!(encoded.contains(r#""assignment_id":"assignment-1""#));
        assert_eq!(
            serde_json::from_str::<AssignmentContract>(&encoded)?,
            contract
        );
        Ok(())
    }
}
