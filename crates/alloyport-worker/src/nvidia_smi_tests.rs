use super::*;
use std::collections::VecDeque;
use std::sync::Mutex;

const GPU_ROWS: &[u8] = b"0, NVIDIA GB10, GPU-0000, 96.00.5E.00.01, 0, 128, 24576, 41, 23.500, None\n1, NVIDIA GB10, GPU-1111, 96.00.5E.00.02, 87, 4096, 24576, 72, 181.25, None\n";
const PROCESS_ROWS: &[u8] = b"GPU-1111, 4321\nGPU-1111, 4322\n";

#[tokio::test]
async fn fixed_queries_produce_static_inventory_and_dynamic_occupancy()
-> Result<(), Box<dyn std::error::Error>> {
    let runner = Arc::new(FakeRunner::new(vec![
        success(GPU_ROWS),
        success(PROCESS_ROWS),
    ]));
    let manager = NvidiaSmi::with_runner(runner.clone(), 4096, Duration::from_secs(1))?;
    let inventory = manager.inventory().await?;
    assert_eq!(inventory.len(), 2);
    assert_eq!(inventory[0].serial_number, "GPU-0000");
    assert_eq!(inventory[1].firmware_version, "96.00.5E.00.02");
    let calls = runner.calls.lock().expect("calls lock");
    assert_eq!(calls.as_slice(), [GPU_QUERY, PROCESS_QUERY]);

    let (_, snapshot) = parse_discovery(GPU_ROWS, PROCESS_ROWS, 7)?;
    assert_eq!(snapshot.devices[0].process_count, 0);
    assert_eq!(snapshot.devices[0].power_milliwatts, 23_500);
    assert_eq!(snapshot.devices[1].process_count, 2);
    assert_eq!(snapshot.devices[1].utilization_percent, 87);
    assert_eq!(snapshot.devices[1].observed_at_ms, 7);
    assert_eq!(snapshot.devices[0].health, DeviceHealth::Ready);
    assert_eq!(snapshot.devices[0].detail, "gpu_recovery_action=None");
    Ok(())
}

#[test]
fn malformed_metrics_and_unknown_process_rows_fail_closed() {
    assert!(parse_discovery(b"0, GPU\n", b"", 1).is_err());
    assert!(parse_discovery(GPU_ROWS, b"GPU-1111, not-a-pid\n", 1).is_err());
    let excessive = b"0, NVIDIA GB10, GPU-0000, vbios, 101, 0, 1, 1, 1, None\n";
    assert!(parse_discovery(excessive, b"", 1).is_err());
}

#[test]
fn recovery_action_is_explicit_health_evidence() -> Result<(), Box<dyn std::error::Error>> {
    let reset = b"0, NVIDIA GB10, GPU-0000, vbios, 0, 0, 1, 40, 1, Reset\n";
    let (_, snapshot) = parse_discovery(reset, b"", 1)?;
    assert_eq!(snapshot.devices[0].health, DeviceHealth::Unhealthy);

    let unsupported = b"0, NVIDIA GB10, GPU-0000, vbios, 0, 0, 1, 40, 1, N/A\n";
    let (_, snapshot) = parse_discovery(unsupported, b"", 1)?;
    assert_eq!(snapshot.devices[0].health, DeviceHealth::Degraded);

    let unknown = b"0, NVIDIA GB10, GPU-0000, vbios, 0, 0, 1, 40, 1, surprise\n";
    assert!(parse_discovery(unknown, b"", 1).is_err());
    Ok(())
}

#[derive(Debug)]
struct FakeRunner {
    responses: Mutex<VecDeque<BoundedCommandOutput>>,
    calls: Mutex<Vec<Vec<String>>>,
}

impl FakeRunner {
    fn new(responses: Vec<BoundedCommandOutput>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl NvidiaSmiCommandRunner for FakeRunner {
    fn binary(&self) -> &Path {
        Path::new("/usr/bin/nvidia-smi")
    }

    fn run(
        &self,
        arguments: &[&str],
        _output_limit: u64,
        _timeout: Duration,
    ) -> Result<BoundedCommandOutput, DeviceStatusError> {
        self.calls
            .lock()
            .map_err(|_| DeviceStatusError::Internal("calls lock".into()))?
            .push(arguments.iter().map(|value| (*value).to_owned()).collect());
        self.responses
            .lock()
            .map_err(|_| DeviceStatusError::Internal("responses lock".into()))?
            .pop_front()
            .ok_or_else(|| DeviceStatusError::Internal("missing response".into()))
    }
}

fn success(stdout: &[u8]) -> BoundedCommandOutput {
    BoundedCommandOutput {
        success: true,
        exit_code: Some(0),
        stdout: stdout.to_vec(),
        stderr: Vec::new(),
        output_limit_exceeded: false,
    }
}
