use super::*;
use std::collections::VecDeque;
use std::sync::Mutex;

const LISTING: &[u8] = br"
	Total Count                    : 2

	NPU ID                         : 0
	Product Name                   : A310-50-C00MM304A1
	Serial Number                  : serial-0

	NPU ID                         : 3
	Product Name                   : A310-50-C00MM304A1
	Serial Number                  : serial-3
";

const INFO: &[u8] = br"
+-------------------------------------------------------------------------------------------------+
| NPU ID | Name             | Health        | Power(W)    Temp(C)           Hugepages-Usage(page) |
|        |                  | Bus-Id        | NPU Util(%) Memory-Usage(MB)  HBM-Usage(MB)         |
+========+==================+===============+=====================================================+
| 0      | Ascend950PR      | Alarm         | 207.6       65                0     / 0             |
|        |                  | 0000:71:00.0  | 7           0    / 0          5249  / 131072        |
+========+==================+===============+=====================================================+
| 3      | Ascend950PR      | OK            | 191.3       56                0     / 0             |
|        |                  | 0000:F1:00.0  | 0           0    / 0          5255  / 131072        |
+---------------------------+---------------+-----------------------------------------------------+
| NPU ID                    | Process id    | Process name             | Process memory(MB)       |
+===========================+===============+=====================================================+
| 0                         | 123            | python3                  | 100                     |
+===========================+===============+=====================================================+
| No running processes found in NPU 3                                                             |
+===========================+===============+=====================================================+
";

#[test]
fn captured_fixed_driver_table_yields_complete_observations() -> Result<(), DeviceStatusError> {
    let snapshot = parse_snapshot(INFO, 42)?;
    assert_eq!(snapshot.devices.len(), 2);

    let alarm = &snapshot.devices[0];
    assert_eq!(alarm.device_id, "0");
    assert_eq!(alarm.health, DeviceHealth::Unhealthy);
    assert_eq!(alarm.process_count, 1);
    assert_eq!(alarm.utilization_percent, 7);
    assert_eq!(alarm.memory_used_bytes, 5_249 * 1024 * 1024);
    assert_eq!(alarm.memory_total_bytes, 131_072 * 1024 * 1024);
    assert_eq!(alarm.temperature_millicelsius, 65_000);
    assert_eq!(alarm.power_milliwatts, 207_600);
    assert_eq!(alarm.observed_at_ms, 42);
    assert_eq!(alarm.detail, "npu-smi health=Alarm");

    let ready = &snapshot.devices[1];
    assert_eq!(ready.device_id, "3");
    assert_eq!(ready.health, DeviceHealth::Ready);
    assert_eq!(ready.process_count, 0);
    assert_eq!(ready.memory_used_bytes, 5_255 * 1024 * 1024);
    assert!(ready.detail.is_empty());
    Ok(())
}

#[test]
fn static_inventory_binds_serial_product_and_firmware() -> Result<(), DeviceStatusError> {
    let inventory = parse_inventory(LISTING, INFO, "9.0.0.105.229")?;
    assert_eq!(inventory.len(), 2);
    assert_eq!(inventory[0].device_id, "0");
    assert_eq!(inventory[0].serial_number, "serial-0");
    assert_eq!(inventory[0].product_name, "Ascend950PR");
    assert_eq!(inventory[0].firmware_version, "9.0.0.105.229");
    assert_eq!(inventory[1].device_id, "3");
    assert_eq!(inventory[1].serial_number, "serial-3");
    Ok(())
}

#[test]
fn inventory_mismatch_and_incomplete_rows_fail_closed() {
    assert!(matches!(
        parse_inventory(LISTING, &INFO[..INFO.len() / 2], "firmware"),
        Err(DeviceStatusError::InvalidResponse(_))
    ));
    assert!(matches!(
        parse_snapshot(b"not an npu-smi table", 1),
        Err(DeviceStatusError::InvalidResponse(_))
    ));
}

#[derive(Debug)]
struct FakeRunner {
    binary: PathBuf,
    responses: Mutex<VecDeque<NpuSmiCommandOutput>>,
    calls: Mutex<Vec<Vec<String>>>,
}

impl FakeRunner {
    fn new(responses: Vec<NpuSmiCommandOutput>) -> Self {
        Self {
            binary: PathBuf::from("/usr/local/bin/npu-smi"),
            responses: Mutex::new(responses.into()),
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl NpuSmiCommandRunner for FakeRunner {
    fn binary(&self) -> &Path {
        &self.binary
    }

    fn run(
        &self,
        arguments: &[&str],
        _output_limit: u64,
        _timeout: Duration,
    ) -> Result<NpuSmiCommandOutput, DeviceStatusError> {
        self.calls
            .lock()
            .expect("calls mutex")
            .push(arguments.iter().map(ToString::to_string).collect());
        self.responses
            .lock()
            .expect("responses mutex")
            .pop_front()
            .ok_or_else(|| DeviceStatusError::Internal("missing fake response".to_owned()))
    }
}

fn success(stdout: &[u8]) -> NpuSmiCommandOutput {
    NpuSmiCommandOutput {
        success: true,
        exit_code: Some(0),
        stdout: stdout.to_vec(),
        stderr: Vec::new(),
        output_limit_exceeded: false,
    }
}

#[tokio::test]
async fn adapter_uses_only_fixed_shell_free_inventory_queries() -> Result<(), DeviceStatusError> {
    let runner = Arc::new(FakeRunner::new(vec![success(LISTING), success(INFO)]));
    let adapter = NpuSmi::with_runner(
        runner.clone(),
        "9.0.0.105.229",
        1024 * 1024,
        Duration::from_secs(1),
    )?;

    let inventory = adapter.inventory().await?;
    assert_eq!(inventory.len(), 2);
    assert_eq!(
        *runner.calls.lock().expect("calls mutex"),
        vec![vec!["info", "-l"], vec!["info"]]
    );
    Ok(())
}

#[tokio::test]
async fn adapter_rejects_truncated_output_before_parsing() -> Result<(), DeviceStatusError> {
    let mut output = success(INFO);
    output.output_limit_exceeded = true;
    let runner = Arc::new(FakeRunner::new(vec![output]));
    let adapter = NpuSmi::with_runner(runner, "9.0.0.105.229", 16, Duration::from_secs(1))?;

    assert!(matches!(
        adapter.snapshot().await,
        Err(DeviceStatusError::InvalidResponse(_))
    ));
    Ok(())
}
