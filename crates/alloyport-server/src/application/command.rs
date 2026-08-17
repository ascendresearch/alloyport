//! Strict process-command parsing shared by serving and offline administration.

use std::error::Error;
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug)]
pub(super) enum ServerCommand {
    Bootstrap {
        directory: PathBuf,
    },
    Serve {
        config_path: Option<PathBuf>,
    },
    Identity {
        config_path: Option<PathBuf>,
        action: IdentityAction,
    },
    CandidateEpisode {
        config_path: Option<PathBuf>,
        candidate_config_path: PathBuf,
        action: CandidateEpisodeAction,
    },
    /// Offline projection of one task's candidate lineage into a readable git repository.
    CandidateRecord {
        config_path: Option<PathBuf>,
        task_id: String,
        into: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CandidateEpisodeAction {
    Validate,
    RunAuthorized,
}

#[derive(Debug)]
pub(super) enum IdentityAction {
    Enroll {
        owner: String,
        certificate: PathBuf,
    },
    Rotate {
        owner: String,
        old_certificate: PathBuf,
        new_certificate: PathBuf,
    },
    Revoke {
        certificate: PathBuf,
    },
}

impl ServerCommand {
    pub(super) fn from_process_args() -> Result<Self, Box<dyn Error>> {
        Self::parse(std::env::args_os().skip(1))
    }

    fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Self, Box<dyn Error>> {
        let mut arguments = arguments.into_iter();
        let first = arguments.next();
        let (config_path, command) = if first.as_deref() == Some(std::ffi::OsStr::new("--config")) {
            let path = required_argument(&mut arguments, "server configuration path")?;
            (Some(PathBuf::from(path)), arguments.next())
        } else {
            (None, first)
        };
        match command.as_deref() {
            None => Ok(Self::Serve { config_path }),
            Some(command) if command == "bootstrap" && config_path.is_none() => {
                let directory =
                    PathBuf::from(required_argument(&mut arguments, "bootstrap directory")?);
                if arguments.next().is_some() {
                    return Err(usage().into());
                }
                Ok(Self::Bootstrap { directory })
            }
            Some(command) if command == "identity" => Ok(Self::Identity {
                config_path,
                action: parse_identity_action(&mut arguments)?,
            }),
            Some(command) if command == "candidate-episode" => {
                let action = required_argument(&mut arguments, "candidate Episode action")?;
                let candidate_config_path = PathBuf::from(required_argument(
                    &mut arguments,
                    "candidate Episode configuration path",
                )?);
                let action = match action.to_str() {
                    Some("validate") => CandidateEpisodeAction::Validate,
                    Some("run") => {
                        if required_argument(&mut arguments, "provider dispatch authorization")?
                            != "--authorize-provider-dispatch"
                        {
                            return Err(usage().into());
                        }
                        CandidateEpisodeAction::RunAuthorized
                    }
                    _ => return Err(usage().into()),
                };
                if arguments.next().is_some() {
                    return Err(usage().into());
                }
                Ok(Self::CandidateEpisode {
                    config_path,
                    candidate_config_path,
                    action,
                })
            }
            Some(command) if command == "candidate-record" => {
                let task_id = required_utf8_argument(&mut arguments, "task ID")?;
                if required_argument(&mut arguments, "record destination flag")? != "--into" {
                    return Err(usage().into());
                }
                let into = PathBuf::from(required_argument(&mut arguments, "record directory")?);
                if arguments.next().is_some() {
                    return Err(usage().into());
                }
                Ok(Self::CandidateRecord {
                    config_path,
                    task_id,
                    into,
                })
            }
            _ => Err(usage().into()),
        }
    }
}

fn parse_identity_action(
    arguments: &mut impl Iterator<Item = OsString>,
) -> Result<IdentityAction, Box<dyn Error>> {
    let action = required_argument(arguments, "identity action")?;
    let parsed = match action.to_str() {
        Some("enroll") => IdentityAction::Enroll {
            owner: required_utf8_argument(arguments, "owner ID")?,
            certificate: required_argument(arguments, "certificate PEM path")?.into(),
        },
        Some("rotate") => IdentityAction::Rotate {
            owner: required_utf8_argument(arguments, "owner ID")?,
            old_certificate: required_argument(arguments, "old certificate PEM path")?.into(),
            new_certificate: required_argument(arguments, "new certificate PEM path")?.into(),
        },
        Some("revoke") => IdentityAction::Revoke {
            certificate: required_argument(arguments, "certificate PEM path")?.into(),
        },
        _ => return Err(usage().into()),
    };
    if arguments.next().is_some() {
        return Err(usage().into());
    }
    Ok(parsed)
}

fn required_argument(
    arguments: &mut impl Iterator<Item = OsString>,
    name: &str,
) -> Result<OsString, Box<dyn Error>> {
    arguments
        .next()
        .ok_or_else(|| format!("missing {name}; {}", usage()).into())
}

fn required_utf8_argument(
    arguments: &mut impl Iterator<Item = OsString>,
    name: &str,
) -> Result<String, Box<dyn Error>> {
    required_argument(arguments, name)?
        .into_string()
        .map_err(|_| format!("{name} must be UTF-8").into())
}

const fn usage() -> &'static str {
    "usage: alloyport-server bootstrap DIRECTORY | alloyport-server [--config PATH] [identity enroll OWNER CERT | identity rotate OWNER OLD_CERT NEW_CERT | identity revoke CERT | candidate-episode validate CANDIDATE_CONFIG | candidate-episode run CANDIDATE_CONFIG --authorize-provider-dispatch | candidate-record TASK_ID --into DIRECTORY]"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_locator_applies_to_serve_and_identity_commands() -> Result<(), Box<dyn Error>> {
        let serve = ServerCommand::parse(["--config", "server.json"].map(OsString::from))?;
        assert!(matches!(
            serve,
            ServerCommand::Serve {
                config_path: Some(path)
            } if path.as_os_str() == std::ffi::OsStr::new("server.json")
        ));
        let identity = ServerCommand::parse(
            [
                "--config",
                "server.json",
                "identity",
                "revoke",
                "client.pem",
            ]
            .map(OsString::from),
        )?;
        assert!(matches!(
            identity,
            ServerCommand::Identity {
                config_path: Some(path),
                action: IdentityAction::Revoke { certificate }
            } if path.as_os_str() == std::ffi::OsStr::new("server.json")
                && certificate.as_os_str() == std::ffi::OsStr::new("client.pem")
        ));
        Ok(())
    }

    #[test]
    fn unknown_or_partial_commands_fail_instead_of_starting_the_server() {
        assert!(ServerCommand::parse([OsString::from("unknown")]).is_err());
        assert!(ServerCommand::parse(["identity", "enroll", "owner"].map(OsString::from)).is_err());
    }

    #[test]
    fn candidate_run_requires_the_exact_dispatch_authorization() -> Result<(), Box<dyn Error>> {
        let validate = ServerCommand::parse(
            ["candidate-episode", "validate", "candidate.json"].map(OsString::from),
        )?;
        assert!(matches!(
            validate,
            ServerCommand::CandidateEpisode {
                action: CandidateEpisodeAction::Validate,
                candidate_config_path,
                ..
            } if candidate_config_path == std::path::Path::new("candidate.json")
        ));
        assert!(
            ServerCommand::parse(
                ["candidate-episode", "run", "candidate.json"].map(OsString::from)
            )
            .is_err()
        );
        let run = ServerCommand::parse(
            [
                "candidate-episode",
                "run",
                "candidate.json",
                "--authorize-provider-dispatch",
            ]
            .map(OsString::from),
        )?;
        assert!(matches!(
            run,
            ServerCommand::CandidateEpisode {
                action: CandidateEpisodeAction::RunAuthorized,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn a_candidate_record_names_one_task_and_one_empty_destination() -> Result<(), Box<dyn Error>> {
        let command = ServerCommand::parse(
            [
                "--config",
                "server.json",
                "candidate-record",
                "task-abc",
                "--into",
                "/srv/record",
            ]
            .map(OsString::from),
        )?;
        assert!(matches!(
            command,
            ServerCommand::CandidateRecord { task_id, into, .. }
                if task_id == "task-abc" && into == std::path::Path::new("/srv/record")
        ));
        // A destination that is merely positional would be easy to give by accident, and this
        // command writes a directory tree.
        assert!(
            ServerCommand::parse(
                ["candidate-record", "task-abc", "/srv/record"].map(OsString::from)
            )
            .is_err()
        );
        assert!(
            ServerCommand::parse(["candidate-record", "task-abc"].map(OsString::from)).is_err()
        );
        Ok(())
    }

    #[test]
    fn bootstrap_requires_exactly_one_directory() -> Result<(), Box<dyn Error>> {
        let command = ServerCommand::parse(["bootstrap", "/srv/alloyport"].map(OsString::from))?;
        assert!(matches!(
            command,
            ServerCommand::Bootstrap { directory }
                if directory == std::path::Path::new("/srv/alloyport")
        ));
        assert!(ServerCommand::parse(["bootstrap"].map(OsString::from)).is_err());
        Ok(())
    }
}
