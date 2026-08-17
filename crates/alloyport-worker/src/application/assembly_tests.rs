//! Worker composition: every backend assembled only from one explicit local policy.

use super::*;
use crate::ascend::ASCEND_ADD_FIXTURE_ID;
use crate::cuda::VECTOR_ADD_FIXTURE_ID;
use alloyport_artifacts::Sha256Digest;

#[test]
fn cuda_config_is_complete_pinned_and_rejects_unknown_fields() -> Result<(), Box<dyn Error>> {
    let manifest = Sha256Digest::digest_bytes(b"manifest");
    let image_id = Sha256Digest::digest_bytes(b"image");
    let bundle = Sha256Digest::digest_bytes(b"bundle");
    let config = format!(
        r#"{{
            "schema_version": 1,
            "fixture_id": "cuda-vectoradd-v1",
            "bundle_digest": "{bundle}",
            "image_manifest_digest": "{manifest}",
            "image_reference": "example.invalid/cuda@{manifest}",
            "image_id": "{image_id}",
            "device_selection": {{
                "allowed_device_ids": ["0"],
                "preferred_device_id": "0"
            }},
            "sandbox_root": "/var/lib/alloyport/cuda-sandboxes",
            "ceilings": {{
                "cpu_millis": 2000,
                "memory_bytes": 2147483648,
                "disk_bytes": 536870912,
                "process_count": 64,
                "output_bytes": 65536
            }},
            "local_artifact_root": "/var/lib/alloyport/cuda-cas",
            "local_artifact_max_bytes": 8388608,
            "max_input_bytes": 8388608,
            "upload_chunk_bytes": 1048576,
            "upload_ttl_ms": 3600000,
            "docker_binary": "/usr/bin/docker",
            "docker_stop_timeout_seconds": 10,
            "nvidia_smi_binary": "/usr/bin/nvidia-smi"
        }}"#
    );

    let parsed = CudaWorkerConfig::parse(config.as_bytes())?;
    assert_eq!(parsed.fixture_id, VECTOR_ADD_FIXTURE_ID);

    let unknown = config.replacen(
        "\"schema_version\": 1,",
        "\"schema_version\": 1, \"allow_shell\": true,",
        1,
    );
    assert!(CudaWorkerConfig::parse(unknown.as_bytes()).is_err());

    let partial = config.replacen("\"upload_ttl_ms\": 3600000,", "", 1);
    assert!(CudaWorkerConfig::parse(partial.as_bytes()).is_err());

    let unpinned = config.replace(
        &format!("example.invalid/cuda@{manifest}"),
        "example.invalid/cuda:latest",
    );
    assert!(CudaWorkerConfig::parse(unpinned.as_bytes()).is_err());

    let overlapping = config.replace(
        "/var/lib/alloyport/cuda-cas",
        "/var/lib/alloyport/cuda-sandboxes/cas",
    );
    assert!(CudaWorkerConfig::parse(overlapping.as_bytes()).is_err());
    Ok(())
}

#[test]
fn ascend_config_is_complete_pinned_and_default_deny() -> Result<(), Box<dyn Error>> {
    let manifest = Sha256Digest::digest_bytes(b"manifest");
    let image_id = Sha256Digest::digest_bytes(b"image");
    let bundle = Sha256Digest::digest_bytes(b"bundle");
    let config = format!(
        r#"{{
            "schema_version": 1,
            "fixture_id": "ascend-add-v1",
            "bundle_digest": "{bundle}",
            "image_manifest_digest": "{manifest}",
            "image_reference": "example.invalid/ascend@{manifest}",
            "image_id": "{image_id}",
            "device": {{
                "device_id": "3",
                "product_name": "Ascend950PR",
                "serial_number": "serial-3",
                "firmware_version": "9.0.0.105.229"
            }},
            "device_nodes": [
                "/dev/davinci3", "/dev/davinci_manager", "/dev/hisi_hdc"
            ],
            "driver_path": "/usr/local/Ascend/driver",
            "sandbox_root": "/var/lib/alloyport/ascend-sandboxes",
            "environment": {{
                "architecture": "Ascend950PR",
                "cann_version": "9.1.0-beta.1",
                "driver_version": "25.7.rc1.6",
                "firmware_version": "9.0.0.105.229"
            }},
            "ceilings": {{
                "timeout_ms": 60000,
                "cpu_millis": 4000,
                "memory_bytes": 8589934592,
                "disk_bytes": 1073741824,
                "process_count": 128,
                "output_bytes": 1048576
            }},
            "local_artifact_root": "/var/lib/alloyport/ascend-cas",
            "local_artifact_max_bytes": 16777216,
            "max_input_bytes": 16777216,
            "upload_chunk_bytes": 1048576,
            "upload_ttl_ms": 3600000,
            "docker_binary": "/usr/bin/docker",
            "docker_stop_timeout_seconds": 10,
            "npu_smi_binary": "/usr/local/bin/npu-smi"
        }}"#
    );

    let parsed = AscendWorkerConfig::parse(config.as_bytes())?;
    assert_eq!(parsed.fixture_id, ASCEND_ADD_FIXTURE_ID);
    assert_eq!(parsed.wire_device().device_id, "3");

    let unknown = config.replacen(
        "\"schema_version\": 1,",
        "\"schema_version\": 1, \"allow_shell\": true,",
        1,
    );
    assert!(AscendWorkerConfig::parse(unknown.as_bytes()).is_err());
    let mutable_image = config.replace(
        &format!("example.invalid/ascend@{manifest}"),
        "example.invalid/ascend:latest",
    );
    assert!(AscendWorkerConfig::parse(mutable_image.as_bytes()).is_err());
    let relative_probe = config.replace("/usr/local/bin/npu-smi", "npu-smi");
    assert!(AscendWorkerConfig::parse(relative_probe.as_bytes()).is_err());
    let mismatched_firmware = config.replacen(
        "\"firmware_version\": \"9.0.0.105.229\"",
        "\"firmware_version\": \"other\"",
        1,
    );
    assert!(AscendWorkerConfig::parse(mismatched_firmware.as_bytes()).is_err());
    Ok(())
}

#[test]
fn ascend_startup_exposes_only_the_selected_host_device() {
    let discovered = vec![
        PathBuf::from("/dev/davinci0"),
        PathBuf::from("/dev/davinci1"),
        PathBuf::from("/dev/davinci_manager"),
        PathBuf::from("/dev/hisi_hdc"),
    ];
    let selected = vec![
        PathBuf::from("/dev/davinci1"),
        PathBuf::from("/dev/davinci_manager"),
        PathBuf::from("/dev/hisi_hdc"),
    ];
    assert!(require_selected_ascend_device_nodes("1", &selected, &discovered).is_ok());
    assert!(
        require_selected_ascend_device_nodes("1", &discovered, &discovered)
            .expect_err("other host devices must not be exposed")
            .to_string()
            .contains("/dev/davinci0")
    );
    assert!(
        require_selected_ascend_device_nodes(
            "2",
            &[
                PathBuf::from("/dev/davinci2"),
                PathBuf::from("/dev/davinci_manager"),
                PathBuf::from("/dev/hisi_hdc"),
            ],
            &discovered,
        )
        .expect_err("unavailable selected device must fail")
        .to_string()
        .contains("/dev/davinci2")
    );
    assert!(is_ascend_device_node_name("davinci12"));
    assert!(!is_ascend_device_node_name("davinci"));
    assert!(!is_ascend_device_node_name("davinci3.backup"));
}
