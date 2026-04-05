use benchmark_core::{
    BenchmarkConfig, ControlMessage, DurabilityPreset, EngineKind, LoadConfig, OperationMix,
    ScenarioConfig, WorkerEvent,
};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use tempfile::tempdir;

#[test]
fn worker_process_streams_samples_and_finishes_with_live_updates() {
    let run_dir = tempdir().expect("tempdir");
    let config_path = run_dir.path().join("config.json");
    let config = BenchmarkConfig {
        run_name: "process-test".to_string(),
        engine: EngineKind::Sqlite,
        scenario: ScenarioConfig {
            initial_rows: 100,
            payload_size_bytes: 64,
            category_count: 8,
            range_scan_size: 4,
        },
        load: LoadConfig {
            concurrency: 2,
            batch_size: 1,
            duration_secs: 1,
            sample_interval_ms: 250,
            mix: OperationMix::default(),
        },
        ramp_schedule: vec![],
        durability: DurabilityPreset::Balanced,
    };
    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).expect("serialize config"),
    )
    .expect("write config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_benchmark-worker"))
        .arg("--run-id")
        .arg("integration-run")
        .arg("--run-dir")
        .arg(run_dir.path().join("run"))
        .arg("--config")
        .arg(&config_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn worker");

    let mut stdin = child.stdin.take().expect("stdin");
    let stdout = child.stdout.take().expect("stdout");
    let reader = BufReader::new(stdout);
    let mut saw_sample = false;
    let mut saw_finished = false;

    for line in reader.lines() {
        let line = line.expect("stdout line");
        let event: WorkerEvent = serde_json::from_str(&line).expect("worker event");
        match event {
            WorkerEvent::Ready { .. } => {
                let message =
                    serde_json::to_string(&ControlMessage::UpdateConcurrency { concurrency: 1 })
                        .expect("serialize control");
                writeln!(stdin, "{message}").expect("write control");
                stdin.flush().expect("flush control");
            }
            WorkerEvent::Sample { .. } => saw_sample = true,
            WorkerEvent::Finished { .. } => {
                saw_finished = true;
                break;
            }
            WorkerEvent::Failed { message, .. } => panic!("worker failed: {message}"),
        }
    }

    let status = child.wait().expect("wait child");
    assert!(status.success());
    assert!(saw_sample, "expected at least one metric sample");
    assert!(saw_finished, "expected worker to emit a finished event");
}
