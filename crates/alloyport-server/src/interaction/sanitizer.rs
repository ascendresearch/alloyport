//! Fail-closed display sanitization at the worker event boundary.

use alloyport_events::Event;

/// Applies the controller's fail-closed display policy before an observed worker event is persisted.
pub(crate) fn redact_worker_event(event: &mut Event) {
    match event {
        Event::CommandStarted {
            command,
            cwd,
            description,
            ..
        } => {
            *command = sanitize_display_text(command);
            if let Some(cwd) = cwd {
                *cwd = strip_terminal_sequences(cwd);
            }
            if let Some(description) = description {
                *description = sanitize_display_text(description);
            }
        }
        Event::CommandOutput {
            text,
            display_sanitized,
            ..
        } => {
            *text = sanitize_display_text(text);
            *display_sanitized = true;
        }
        Event::Warning { message } | Event::Error { message } => {
            *message = sanitize_display_text(message);
        }
        _ => {}
    }
}

fn sanitize_display_text(input: &str) -> String {
    let stripped = strip_terminal_sequences(input);
    let mut output = String::with_capacity(stripped.len());
    let mut redact_next = false;
    for segment in stripped.split_inclusive(char::is_whitespace) {
        let word = segment.trim_end_matches(char::is_whitespace);
        let whitespace = &segment[word.len()..];
        if word.is_empty() {
            output.push_str(segment);
            continue;
        }
        if redact_next {
            output.push_str("[REDACTED]");
            redact_next = false;
        } else if word.eq_ignore_ascii_case("bearer") {
            output.push_str(word);
            redact_next = true;
        } else if let Some((key, _)) = word.split_once('=') {
            if is_sensitive_key(key) {
                output.push_str(key);
                output.push_str("=[REDACTED]");
            } else {
                output.push_str(word);
            }
        } else {
            output.push_str(word);
            redact_next = looks_like_sensitive_label(word);
        }
        output.push_str(whitespace);
    }
    output
}

fn looks_like_sensitive_label(value: &str) -> bool {
    is_sensitive_key(value)
        && (value.starts_with('-')
            || value.ends_with(':')
            || value
                .chars()
                .all(|character| character.is_ascii_uppercase() || character == '_'))
}

fn is_sensitive_key(value: &str) -> bool {
    let normalized = value
        .trim_matches(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .to_ascii_lowercase();
    [
        "password",
        "passwd",
        "token",
        "secret",
        "api_key",
        "apikey",
        "access_key",
        "private_key",
        "authorization",
        "cookie",
    ]
    .iter()
    .any(|sensitive| normalized.contains(sensitive))
}

fn strip_terminal_sequences(input: &str) -> String {
    #[derive(Clone, Copy)]
    enum State {
        Text,
        Escape,
        ControlSequence,
        OperatingSystemCommand,
        OperatingSystemCommandEscape,
    }

    let mut state = State::Text;
    let mut output = String::with_capacity(input.len());
    for character in input.chars() {
        state = match state {
            State::Text if character == '\u{1b}' => State::Escape,
            State::Text => {
                if !character.is_control() || matches!(character, '\n' | '\t') {
                    output.push(character);
                }
                State::Text
            }
            State::Escape if character == '[' => State::ControlSequence,
            State::Escape if character == ']' => State::OperatingSystemCommand,
            State::Escape => State::Text,
            State::ControlSequence if ('@'..='~').contains(&character) => State::Text,
            State::ControlSequence => State::ControlSequence,
            State::OperatingSystemCommand if character == '\u{7}' => State::Text,
            State::OperatingSystemCommandEscape if character == '\\' => State::Text,
            State::OperatingSystemCommand | State::OperatingSystemCommandEscape
                if character == '\u{1b}' =>
            {
                State::OperatingSystemCommandEscape
            }
            State::OperatingSystemCommand | State::OperatingSystemCommandEscape => {
                State::OperatingSystemCommand
            }
        };
    }
    output
}
