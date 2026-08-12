//! Shared terminal outcome policy for fixed accelerator container executions.

use crate::container_engine::{ContainerExit, ContainerLogs};
use crate::executor::ExecutorResult;
use alloyport_core::AttemptOutcome;

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
    let (exit, forced_outcome, detail) = match termination {
        ContainerTermination::Exited(exit) => (exit, None, policy.exited_detail),
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
            policy.nonzero_detail,
        )
    } else if !has_verification_marker(&logs.stdout, policy.fixture_id) {
        (
            AttemptOutcome::IntegrityViolation,
            Some(0),
            exit.elapsed_ms,
            policy.missing_marker_detail,
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
