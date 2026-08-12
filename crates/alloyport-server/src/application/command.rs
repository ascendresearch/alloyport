//! Strict process-command parsing shared by serving and offline administration.

use std::error::Error;
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug)]
pub(super) enum ServerCommand {
    Serve {
        config_path: Option<PathBuf>,
    },
    Identity {
        config_path: Option<PathBuf>,
        action: IdentityAction,
    },
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
            Some(command) if command == "identity" => Ok(Self::Identity {
                config_path,
                action: parse_identity_action(&mut arguments)?,
            }),
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
    "usage: alloyport-server [--config PATH] [identity enroll OWNER CERT | identity rotate OWNER OLD_CERT NEW_CERT | identity revoke CERT]"
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
}
