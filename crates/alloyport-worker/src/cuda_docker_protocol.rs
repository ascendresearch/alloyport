//! Docker CLI JSON and scalar response parsing.

use crate::cuda_supervisor::{
    ContainerEngineError, ContainerExit, ContainerIdentity, ContainerPhase, ContainerSnapshot,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const ATTEMPT_LABEL: &str = "alloyport.attempt";
const BUNDLE_LABEL: &str = "alloyport.bundle";
const IMAGE_LABEL: &str = "alloyport.image";

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerImageInspect {
    id: String,
}

pub(super) fn parse_image_id(bytes: &[u8]) -> Result<String, ContainerEngineError> {
    let mut images: Vec<DockerImageInspect> = serde_json::from_slice(bytes).map_err(|error| {
        ContainerEngineError::InvalidResponse(format!("invalid image inspect JSON: {error}"))
    })?;
    if images.len() != 1 {
        return Err(ContainerEngineError::InvalidResponse(format!(
            "image inspect returned {} objects instead of one",
            images.len()
        )));
    }
    let image = images.pop().expect("length checked above");
    if image.id.is_empty() {
        return Err(ContainerEngineError::InvalidResponse(
            "image inspect returned an empty image ID".into(),
        ));
    }
    Ok(image.id)
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerInspect {
    name: String,
    image: String,
    config: DockerContainerConfig,
    state: DockerContainerState,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerConfig {
    #[serde(default)]
    labels: Option<BTreeMap<String, String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerState {
    status: String,
    exit_code: i32,
    started_at: String,
    finished_at: String,
}

pub(super) struct DockerContainerDetail {
    pub(super) snapshot: ContainerSnapshot,
    pub(super) exit: Option<ContainerExit>,
}

pub(super) fn parse_container_inspect(
    bytes: &[u8],
) -> Result<DockerContainerDetail, ContainerEngineError> {
    let mut containers: Vec<DockerContainerInspect> =
        serde_json::from_slice(bytes).map_err(|error| {
            ContainerEngineError::InvalidResponse(format!(
                "invalid container inspect JSON: {error}"
            ))
        })?;
    if containers.len() != 1 {
        return Err(ContainerEngineError::InvalidResponse(format!(
            "container inspect returned {} objects instead of one",
            containers.len()
        )));
    }
    let container = containers.pop().expect("length checked above");
    let phase = match container.state.status.as_str() {
        "created" => ContainerPhase::Created,
        "running" => ContainerPhase::Running,
        "exited" => ContainerPhase::Exited,
        status => {
            return Err(ContainerEngineError::InvalidResponse(format!(
                "unsupported Docker container state {status:?}"
            )));
        }
    };
    let exit = if phase == ContainerPhase::Exited {
        Some(ContainerExit {
            exit_code: container.state.exit_code,
            elapsed_ms: elapsed_ms(&container.state.started_at, &container.state.finished_at)?,
        })
    } else {
        None
    };
    Ok(DockerContainerDetail {
        snapshot: ContainerSnapshot {
            identity: ContainerIdentity {
                name: container
                    .name
                    .strip_prefix('/')
                    .unwrap_or(&container.name)
                    .into(),
                attempt_id: label(container.config.labels.as_ref(), ATTEMPT_LABEL),
                bundle_digest: label(container.config.labels.as_ref(), BUNDLE_LABEL),
                image_manifest_digest: label(container.config.labels.as_ref(), IMAGE_LABEL),
                image_id: container.image,
            },
            phase,
        },
        exit,
    })
}

fn label(labels: Option<&BTreeMap<String, String>>, name: &str) -> String {
    labels
        .and_then(|labels| labels.get(name))
        .cloned()
        .unwrap_or_default()
}

pub(super) fn elapsed_ms(started_at: &str, finished_at: &str) -> Result<u64, ContainerEngineError> {
    let started = OffsetDateTime::parse(started_at, &Rfc3339).map_err(|error| {
        ContainerEngineError::InvalidResponse(format!("invalid Docker start time: {error}"))
    })?;
    let finished = OffsetDateTime::parse(finished_at, &Rfc3339).map_err(|error| {
        ContainerEngineError::InvalidResponse(format!("invalid Docker finish time: {error}"))
    })?;
    u64::try_from((finished - started).whole_milliseconds()).map_err(|_| {
        ContainerEngineError::InvalidResponse("Docker finish time precedes its start time".into())
    })
}

pub(super) fn parse_wait_exit_code(bytes: &[u8]) -> Result<i32, ContainerEngineError> {
    let text = String::from_utf8_lossy(bytes);
    let mut lines = text.lines();
    let code = lines
        .next()
        .ok_or_else(|| {
            ContainerEngineError::InvalidResponse("Docker wait returned no exit code".into())
        })?
        .trim()
        .parse::<i32>()
        .map_err(|error| {
            ContainerEngineError::InvalidResponse(format!("invalid Docker wait exit code: {error}"))
        })?;
    if lines.any(|line| !line.trim().is_empty()) {
        return Err(ContainerEngineError::InvalidResponse(
            "Docker wait returned multiple exit codes".into(),
        ));
    }
    Ok(code)
}
