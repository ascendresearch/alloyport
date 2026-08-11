//! Plain-text presentation adapter for canonical interaction events.

use crate::{Event, EventEnvelope, FileChange, MessageRole, OutputStream};
use std::fmt::Write as _;

#[must_use]
pub fn render_plain(envelope: &EventEnvelope) -> String {
    match &envelope.event {
        Event::RunStarted { task } => format!("AlloyPort · {task}\n"),
        Event::TurnStarted { .. } | Event::TurnCompleted { .. } => String::new(),
        Event::TurnFailed { turn, error } => format!("! turn {turn} failed: {error}\n"),
        Event::RunCompleted { .. } => "\n✓ run completed\n".to_owned(),
        Event::RunFailed { error } => format!("\n✗ run failed: {error}\n"),
        Event::MessageStarted { role } => format!("\n{}\n", message_role_label(*role)),
        Event::MessageDelta { text } => text.clone(),
        Event::MessageCompleted {} => "\n".to_owned(),
        Event::PlanUpdated { entries } => format!("plan: {entries}\n"),
        Event::ToolStarted { name, arguments } => format!("\n→ {name} {arguments}\n"),
        Event::ToolCompleted { name, output } => format!("← {name} completed\n{output}\n"),
        Event::ToolFailed {
            name,
            error,
            output,
        } => format!(
            "← {name} failed: {error}{}\n",
            output
                .as_ref()
                .map_or_else(String::new, |value| format!("\n{value}"))
        ),
        Event::CommandStarted {
            command,
            cwd,
            execution_site,
            description,
        } => {
            let description = description
                .as_ref()
                .map_or_else(String::new, |value| format!(" · {value}"));
            let cwd = cwd
                .as_ref()
                .map_or_else(String::new, |value| format!(" · cwd {value}"));
            format!("\n$ {command}\n  @ {execution_site}{cwd}{description}\n")
        }
        Event::CommandOutput { stream, text, .. } => match stream {
            OutputStream::Stdout => text.clone(),
            OutputStream::Stderr => format!("[stderr]\n{text}"),
        },
        Event::CommandCompleted {
            exit_code,
            elapsed_ms,
            timed_out,
            ..
        } => format!(
            "\n  exit {exit_code} · {elapsed_ms} ms{}\n",
            if *timed_out { " · timed out" } else { "" }
        ),
        Event::WorkspaceDelta {
            changes,
            diff,
            commit,
        } => render_workspace_delta(changes, diff.as_deref(), commit.as_deref()),
        Event::ApprovalRequested {
            action,
            reason,
            risk,
        } => format!("approval required [{risk}]: {action}\n  {reason}\n"),
        Event::ApprovalResolved { decision } => format!("approval: {decision}\n"),
        Event::GateStarted { gate } => format!("gate {gate}: running\n"),
        Event::GateCompleted { gate, passed, .. } => format!(
            "{} gate {gate}: {}\n",
            if *passed { "✓" } else { "✗" },
            if *passed { "PASS" } else { "FAIL" }
        ),
        Event::ArtifactProduced { artifact } => {
            format!("artifact {} ({})\n", artifact.reference, artifact.digest)
        }
        Event::Warning { message } => format!("! {message}\n"),
        Event::Error { message } => format!("✗ {message}\n"),
    }
}

fn render_workspace_delta(
    changes: &[FileChange],
    diff: Option<&str>,
    commit: Option<&str>,
) -> String {
    let mut rendered = String::from("\nworkspace changes");
    if let Some(commit) = commit {
        let _ = write!(rendered, " · commit {commit}");
    }
    rendered.push('\n');
    for change in changes {
        let additions = change
            .additions
            .map_or_else(|| "?".to_owned(), |value| value.to_string());
        let deletions = change
            .deletions
            .map_or_else(|| "?".to_owned(), |value| value.to_string());
        let _ = writeln!(
            rendered,
            "  {:?} {} +{additions}/-{deletions}",
            change.kind, change.path
        );
    }
    if let Some(diff) = diff {
        rendered.push_str(diff);
        if !diff.ends_with('\n') {
            rendered.push('\n');
        }
    }
    rendered
}

const fn message_role_label(role: MessageRole) -> &'static str {
    match role {
        MessageRole::Assistant => "assistant",
        MessageRole::User => "user",
        MessageRole::System => "system",
    }
}
