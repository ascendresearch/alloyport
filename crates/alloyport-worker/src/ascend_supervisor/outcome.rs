use super::engine::{ContainerExit, ContainerLogs};
use crate::ascend::ASCEND_ADD_FIXTURE_ID;
use crate::executor::ExecutorResult;
use alloyport_core::AttemptOutcome;

#[derive(Clone, Copy)]
pub(super) enum Termination {
    Exited(ContainerExit),
    Cancelled(ContainerExit),
    TimedOut(ContainerExit),
    OutputLimitExceeded(ContainerExit),
}

pub(super) fn enforce_output_limit(mut logs: ContainerLogs, limit: u64) -> ContainerLogs {
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

pub(super) fn classify(
    termination: Termination,
    logs: ContainerLogs,
    timeout_ms: u64,
) -> ExecutorResult {
    let (exit, forced_outcome, detail) = match termination {
        Termination::Exited(exit) => (exit, None, "Ascend fixture exited"),
        Termination::Cancelled(exit) => {
            (exit, Some(AttemptOutcome::Cancelled), "execution cancelled")
        }
        Termination::TimedOut(exit) => {
            (exit, Some(AttemptOutcome::TimedOut), "execution timed out")
        }
        Termination::OutputLimitExceeded(exit) => (
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
            "Ascend fixture returned a nonzero exit code",
        )
    } else if !String::from_utf8_lossy(&logs.stdout)
        .lines()
        .any(|line| line.starts_with(&format!("PASS fixture={ASCEND_ADD_FIXTURE_ID} ")))
    {
        (
            AttemptOutcome::IntegrityViolation,
            Some(0),
            exit.elapsed_ms,
            "Ascend fixture exited zero without its verification marker",
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
