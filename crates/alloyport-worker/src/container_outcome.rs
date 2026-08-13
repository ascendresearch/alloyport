//! Shared terminal outcome policy for fixed accelerator container executions.

use crate::container_engine::{ContainerExit, ContainerLogs};
use crate::executor::ExecutorResult;
use alloyport_core::{AttemptOutcome, ReductionRunReceipt};

#[derive(Clone, Copy)]
pub(crate) enum ContainerTermination {
    Exited(ContainerExit),
    Cancelled(ContainerExit),
    TimedOut(ContainerExit),
    OutputLimitExceeded(ContainerExit),
}

#[derive(Clone, Copy)]
pub(crate) struct FixtureOutcomePolicy {
    pub fixture_id: &'static str,
    pub exited_detail: &'static str,
    pub nonzero_detail: &'static str,
    pub missing_marker_detail: &'static str,
}

#[derive(Clone, Copy)]
pub(crate) struct CorrectnessOutcomePolicy {
    pub exited: &'static str,
    pub nonzero: &'static str,
    pub invalid_receipt: &'static str,
}

pub(crate) fn enforce_output_limit(mut logs: ContainerLogs, limit: u64) -> ContainerLogs {
    let stdout_len = u64::try_from(logs.stdout.len()).unwrap_or(u64::MAX);
    let stderr_len = u64::try_from(logs.stderr.len()).unwrap_or(u64::MAX);
    if stdout_len.saturating_add(stderr_len) <= limit {
        return logs;
    }
    logs.output_limit_exceeded = true;
    let kept_stdout = usize::try_from(limit.min(stdout_len)).unwrap_or(usize::MAX);
    logs.stdout.truncate(kept_stdout);
    let remaining = limit.saturating_sub(u64::try_from(logs.stdout.len()).unwrap_or(u64::MAX));
    logs.stderr
        .truncate(usize::try_from(remaining).unwrap_or(usize::MAX));
    logs
}

pub(crate) fn classify_fixture_outcome(
    termination: ContainerTermination,
    logs: ContainerLogs,
    timeout_ms: u64,
    policy: FixtureOutcomePolicy,
) -> ExecutorResult {
    classify_verified_outcome(
        termination,
        logs,
        timeout_ms,
        policy.exited_detail,
        policy.nonzero_detail,
        policy.missing_marker_detail,
        |stdout| has_verification_marker(stdout, policy.fixture_id),
    )
}

pub(crate) fn classify_correctness_outcome(
    termination: ContainerTermination,
    logs: ContainerLogs,
    timeout_ms: u64,
    policy: CorrectnessOutcomePolicy,
) -> ExecutorResult {
    classify_verified_outcome(
        termination,
        logs,
        timeout_ms,
        policy.exited,
        policy.nonzero,
        policy.invalid_receipt,
        |stdout| serde_json::from_slice::<ReductionRunReceipt>(stdout).is_ok(),
    )
}

fn classify_verified_outcome(
    termination: ContainerTermination,
    logs: ContainerLogs,
    timeout_ms: u64,
    exited_detail: &'static str,
    nonzero_detail: &'static str,
    missing_evidence_detail: &'static str,
    has_success_evidence: impl FnOnce(&[u8]) -> bool,
) -> ExecutorResult {
    let (exit, forced_outcome, detail) = match termination {
        ContainerTermination::Exited(exit) => (exit, None, exited_detail),
        ContainerTermination::Cancelled(exit) => {
            (exit, Some(AttemptOutcome::Cancelled), "execution cancelled")
        }
        ContainerTermination::TimedOut(exit) => {
            (exit, Some(AttemptOutcome::TimedOut), "execution timed out")
        }
        ContainerTermination::OutputLimitExceeded(exit) => (
            exit,
            Some(AttemptOutcome::InfraError),
            "execution output limit exceeded",
        ),
    };
    let (outcome, exit_code, elapsed_ms, detail) = if logs.output_limit_exceeded {
        (
            AttemptOutcome::InfraError,
            None,
            exit.elapsed_ms,
            "execution output limit exceeded",
        )
    } else if let Some(outcome) = forced_outcome {
        (
            outcome,
            None,
            if outcome == AttemptOutcome::TimedOut {
                timeout_ms
            } else {
                exit.elapsed_ms
            },
            detail,
        )
    } else if exit.exit_code != 0 {
        (
            AttemptOutcome::CandidateFailed,
            Some(exit.exit_code),
            exit.elapsed_ms,
            nonzero_detail,
        )
    } else if !has_success_evidence(&logs.stdout) {
        (
            AttemptOutcome::IntegrityViolation,
            Some(0),
            exit.elapsed_ms,
            missing_evidence_detail,
        )
    } else {
        (AttemptOutcome::Succeeded, Some(0), exit.elapsed_ms, detail)
    };
    ExecutorResult {
        outcome,
        exit_code,
        elapsed_ms,
        stdout: logs.stdout,
        stderr: logs.stderr,
        detail: detail.into(),
    }
}

fn has_verification_marker(stdout: &[u8], fixture_id: &str) -> bool {
    let prefix = format!("PASS fixture={fixture_id} ");
    String::from_utf8_lossy(stdout)
        .lines()
        .any(|line| line.starts_with(&prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_core::{CandidateId, ReductionObservation, ReductionRunRole, Sha256Digest};

    const POLICIES: [FixtureOutcomePolicy; 2] = [
        FixtureOutcomePolicy {
            fixture_id: crate::cuda::VECTOR_ADD_FIXTURE_ID,
            exited_detail: "CUDA fixture exited",
            nonzero_detail: "CUDA fixture returned a nonzero exit code",
            missing_marker_detail: "CUDA fixture exited zero without its verification marker",
        },
        FixtureOutcomePolicy {
            fixture_id: crate::ascend::ASCEND_ADD_FIXTURE_ID,
            exited_detail: "Ascend fixture exited",
            nonzero_detail: "Ascend fixture returned a nonzero exit code",
            missing_marker_detail: "Ascend fixture exited zero without its verification marker",
        },
    ];

    #[test]
    fn cuda_and_ascend_share_terminal_outcome_contract() {
        for policy in POLICIES {
            fixture_terminal_outcome_contract(policy);
        }
    }

    #[test]
    fn correctness_requires_one_valid_structured_receipt() {
        let digest = Sha256Digest::digest_bytes(b"correctness");
        let receipt = ReductionRunReceipt::new(
            digest,
            ReductionRunRole::AscendCandidate,
            Some(CandidateId::try_from("candidate-1").expect("candidate ID")),
            digest,
            digest,
            digest,
            true,
            true,
            vec![ReductionObservation {
                case_id: "zero".into(),
                repetition: 1,
                elements: 0,
                input_digest: digest,
                status: 0,
                output_bits: Some(0),
            }],
        )
        .expect("valid run receipt");
        let policy = CorrectnessOutcomePolicy {
            exited: "correctness exited",
            nonzero: "correctness failed",
            invalid_receipt: "invalid receipt",
        };
        let valid = classify_correctness_outcome(
            ContainerTermination::Exited(ContainerExit {
                exit_code: 0,
                elapsed_ms: 8,
            }),
            logs(serde_json::to_vec(&receipt).expect("serialize receipt")),
            100,
            policy,
        );
        assert_eq!(valid.outcome, AttemptOutcome::Succeeded);

        let invalid = classify_correctness_outcome(
            ContainerTermination::Exited(ContainerExit {
                exit_code: 0,
                elapsed_ms: 8,
            }),
            logs(br#"{"schema_version":1}"#.to_vec()),
            100,
            policy,
        );
        assert_eq!(invalid.outcome, AttemptOutcome::IntegrityViolation);
    }

    fn fixture_terminal_outcome_contract(policy: FixtureOutcomePolicy) {
        let exit = ContainerExit {
            exit_code: 0,
            elapsed_ms: 9,
        };
        let succeeded = classify_fixture_outcome(
            ContainerTermination::Exited(exit),
            logs(format!("PASS fixture={} verified\n", policy.fixture_id).into_bytes()),
            100,
            policy,
        );
        assert_eq!(succeeded.outcome, AttemptOutcome::Succeeded);
        assert_eq!(succeeded.exit_code, Some(0));
        assert_eq!(succeeded.elapsed_ms, 9);

        let cancelled = classify_fixture_outcome(
            ContainerTermination::Cancelled(exit),
            logs(Vec::new()),
            100,
            policy,
        );
        assert_eq!(cancelled.outcome, AttemptOutcome::Cancelled);
        assert_eq!(cancelled.exit_code, None);

        let timed_out = classify_fixture_outcome(
            ContainerTermination::TimedOut(exit),
            logs(Vec::new()),
            100,
            policy,
        );
        assert_eq!(timed_out.outcome, AttemptOutcome::TimedOut);
        assert_eq!(timed_out.elapsed_ms, 100);

        let failed = classify_fixture_outcome(
            ContainerTermination::Exited(ContainerExit {
                exit_code: 17,
                elapsed_ms: 9,
            }),
            logs(Vec::new()),
            100,
            policy,
        );
        assert_eq!(failed.outcome, AttemptOutcome::CandidateFailed);
        assert_eq!(failed.exit_code, Some(17));

        let unverified = classify_fixture_outcome(
            ContainerTermination::Exited(exit),
            logs(b"not verified\n".to_vec()),
            100,
            policy,
        );
        assert_eq!(unverified.outcome, AttemptOutcome::IntegrityViolation);

        let exhausted = classify_fixture_outcome(
            ContainerTermination::OutputLimitExceeded(exit),
            enforce_output_limit(
                ContainerLogs {
                    stdout: b"1234".to_vec(),
                    stderr: b"5678".to_vec(),
                    output_limit_exceeded: false,
                },
                5,
            ),
            100,
            policy,
        );
        assert_eq!(exhausted.outcome, AttemptOutcome::InfraError);
        assert_eq!(exhausted.stdout, b"1234");
        assert_eq!(exhausted.stderr, b"5");
    }

    fn logs(stdout: Vec<u8>) -> ContainerLogs {
        ContainerLogs {
            stdout,
            stderr: Vec::new(),
            output_limit_exceeded: false,
        }
    }
}
