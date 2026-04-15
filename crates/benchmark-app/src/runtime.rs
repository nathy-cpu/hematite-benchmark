use crate::engine::{EngineAdapter, execute_operation, logical_bytes_for_operation, open_engine};
use crate::metrics::{IoCounters, current_io_counters, current_rss_bytes, dir_size_bytes};
use anyhow::{Context, Result, bail};
use benchmark_core::{
    AppliedControlEvent, ArtifactPaths, BenchmarkConfig, ControlMessage, ControlSource, EngineKind,
    MetricSample, OperationKind, OperationMix, RunAggregate, RunLogEntry, RunLogLevel,
    RunLogSource, RunStatus, RunSummary, SampleAccumulator, WorkerEvent,
};
use rand::Rng;
use serde_json::Deserializer;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const LOCK_RETRY_TIMEOUT_MS: u64 = 500;
const LOCK_RETRY_SLEEP_MS: u64 = 10;
const MAX_ERROR_MESSAGES: usize = 8;

#[derive(Clone)]
struct RuntimeControl {
    paused: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    manual_stop: Arc<AtomicBool>,
    concurrency: Arc<AtomicUsize>,
    mix: Arc<Mutex<OperationMix>>,
    effective_config: Arc<Mutex<BenchmarkConfig>>,
    control_events: Arc<Mutex<Vec<AppliedControlEvent>>>,
}

impl RuntimeControl {
    fn new(initial_config: BenchmarkConfig) -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            manual_stop: Arc::new(AtomicBool::new(false)),
            concurrency: Arc::new(AtomicUsize::new(initial_config.load.concurrency)),
            mix: Arc::new(Mutex::new(initial_config.load.mix.clone())),
            effective_config: Arc::new(Mutex::new(initial_config)),
            control_events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn apply(
        &self,
        message: ControlMessage,
        source: ControlSource,
    ) -> Result<Option<(AppliedControlEvent, BenchmarkConfig)>> {
        match &message {
            ControlMessage::Pause => {
                self.paused.store(true, Ordering::Relaxed);
                Ok(None)
            }
            ControlMessage::Resume => {
                self.paused.store(false, Ordering::Relaxed);
                Ok(None)
            }
            ControlMessage::Stop => {
                if source == ControlSource::Interactive {
                    self.manual_stop.store(true, Ordering::Relaxed);
                }
                self.stop.store(true, Ordering::Relaxed);
                Ok(None)
            }
            ControlMessage::UpdateConcurrency { concurrency } => {
                if *concurrency == 0 {
                    bail!("concurrency must be greater than zero");
                }
                self.concurrency.store(*concurrency, Ordering::Relaxed);
                let mut effective_config = self
                    .effective_config
                    .lock()
                    .expect("effective config lock poisoned");
                effective_config.load.concurrency = *concurrency;
                Ok(Some(self.record_control(
                    message,
                    source,
                    effective_config.clone(),
                )))
            }
            ControlMessage::UpdateMix {
                point_reads,
                range_scans,
                inserts,
                updates,
            } => {
                let mix = OperationMix {
                    point_reads: *point_reads,
                    range_scans: *range_scans,
                    inserts: *inserts,
                    updates: *updates,
                };
                mix.validate().map_err(anyhow::Error::msg)?;
                *self.mix.lock().expect("mix lock poisoned") = mix.clone();
                let mut effective_config = self
                    .effective_config
                    .lock()
                    .expect("effective config lock poisoned");
                effective_config.load.mix = mix;
                Ok(Some(self.record_control(
                    message,
                    source,
                    effective_config.clone(),
                )))
            }
            ControlMessage::ApplyPhase { phase } => {
                if let Some(concurrency) = phase.concurrency {
                    if concurrency == 0 {
                        bail!("concurrency must be greater than zero");
                    }
                    self.concurrency.store(concurrency, Ordering::Relaxed);
                }
                if let Some(mix) = &phase.mix {
                    mix.validate().map_err(anyhow::Error::msg)?;
                    *self.mix.lock().expect("mix lock poisoned") = mix.clone();
                }
                let mut effective_config = self
                    .effective_config
                    .lock()
                    .expect("effective config lock poisoned");
                effective_config.apply_phase(phase);
                Ok(Some(self.record_control(
                    message,
                    source,
                    effective_config.clone(),
                )))
            }
        }
    }

    fn current_mix(&self) -> OperationMix {
        self.mix.lock().expect("mix lock poisoned").clone()
    }

    fn effective_config(&self) -> BenchmarkConfig {
        self.effective_config
            .lock()
            .expect("effective config lock poisoned")
            .clone()
    }

    fn control_events(&self) -> Vec<AppliedControlEvent> {
        self.control_events
            .lock()
            .expect("control events lock poisoned")
            .clone()
    }

    fn manual_stop_requested(&self) -> bool {
        self.manual_stop.load(Ordering::Relaxed)
    }

    fn record_control(
        &self,
        control: ControlMessage,
        source: ControlSource,
        effective_config: BenchmarkConfig,
    ) -> (AppliedControlEvent, BenchmarkConfig) {
        let event = AppliedControlEvent {
            timestamp_ms: now_ms(),
            source,
            control,
        };
        self.control_events
            .lock()
            .expect("control events lock poisoned")
            .push(event.clone());
        (event, effective_config)
    }
}

#[derive(Clone, Default)]
struct RuntimeErrors {
    total: Arc<AtomicU64>,
    messages: Arc<Mutex<Vec<String>>>,
}

impl RuntimeErrors {
    fn record(&self, message: impl Into<String>) -> Option<String> {
        let message = message.into();
        self.total.fetch_add(1, Ordering::Relaxed);
        let mut messages = self.messages.lock().expect("error messages lock poisoned");
        if messages.len() >= MAX_ERROR_MESSAGES
            || messages.iter().any(|existing| existing == &message)
        {
            return None;
        }
        messages.push(message.clone());
        Some(message)
    }

    fn count(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    fn messages(&self) -> Vec<String> {
        self.messages
            .lock()
            .expect("error messages lock poisoned")
            .clone()
    }
}

pub fn run_worker_from_args() -> Result<()> {
    let args = WorkerArgs::parse(env::args().skip(1).collect())?;
    let result = (|| -> Result<()> {
        let config_text = fs::read_to_string(&args.config)
            .with_context(|| format!("failed to read config file {}", args.config.display()))?;
        let config: BenchmarkConfig = serde_json::from_str(&config_text)?;
        config.validate().map_err(anyhow::Error::msg)?;

        let runtime = WorkerRuntime::new(args.run_id.clone(), args.run_dir.clone(), config)?;
        runtime.run()
    })();

    if let Err(error) = &result {
        let _ = emit_event(&WorkerEvent::Failed {
            run_id: args.run_id.clone(),
            message: error.to_string(),
        });
    }

    result
}

struct WorkerArgs {
    run_id: String,
    run_dir: PathBuf,
    config: PathBuf,
}

impl WorkerArgs {
    fn parse(args: Vec<String>) -> Result<Self> {
        let mut run_id = None;
        let mut run_dir = None;
        let mut config = None;

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--run-id" => run_id = iter.next(),
                "--run-dir" => run_dir = iter.next().map(PathBuf::from),
                "--config" => config = iter.next().map(PathBuf::from),
                other => bail!("unexpected argument {other}"),
            }
        }

        Ok(Self {
            run_id: run_id.context("missing --run-id")?,
            run_dir: run_dir.context("missing --run-dir")?,
            config: config.context("missing --config")?,
        })
    }
}

struct WorkerRuntime {
    run_id: String,
    run_dir: PathBuf,
    data_dir: PathBuf,
    config: BenchmarkConfig,
}

impl WorkerRuntime {
    fn new(run_id: String, run_dir: PathBuf, config: BenchmarkConfig) -> Result<Self> {
        let data_dir = run_dir.join("data");
        fs::create_dir_all(&data_dir)?;
        Ok(Self {
            run_id,
            run_dir,
            data_dir,
            config,
        })
    }

    fn run(self) -> Result<()> {
        let started_at_ms = now_ms();
        emit_runtime_log(
            &self.run_id,
            RunLogLevel::Info,
            format!(
                "Worker startup: engine={}, storage={}",
                self.config.engine.as_str(),
                describe_storage(&self.config)
            ),
        )?;
        emit_runtime_log(
            &self.run_id,
            RunLogLevel::Info,
            format!(
                "Preparing dataset with {} seed rows",
                self.config.scenario.initial_rows
            ),
        )?;
        let mut setup_engine = open_engine(&self.config, &self.data_dir)?;
        setup_engine.prepare_dataset(&self.config)?;
        emit_runtime_log(
            &self.run_id,
            RunLogLevel::Info,
            "Dataset preparation finished",
        )?;
        emit_runtime_log(
            &self.run_id,
            RunLogLevel::Debug,
            "Running startup flush/checkpoint",
        )?;
        setup_engine.flush()?;
        emit_runtime_log(
            &self.run_id,
            RunLogLevel::Info,
            "Startup flush/checkpoint finished",
        )?;
        let mut warnings = setup_engine.warnings().to_vec();
        drop(setup_engine);

        emit_event(&WorkerEvent::Ready {
            run_id: self.run_id.clone(),
            engine: self.config.engine,
            pid: std::process::id(),
            warnings: warnings.clone(),
        })?;

        let control = RuntimeControl::new(self.config.clone());
        let next_id = Arc::new(AtomicU64::new(self.config.scenario.initial_rows + 1));
        let accumulator = Arc::new(Mutex::new(SampleAccumulator::default()));
        let aggregate = Arc::new(Mutex::new(RunAggregate::default()));
        let errors = RuntimeErrors::default();

        let _control_reader = spawn_control_reader(self.run_id.clone(), control.clone());
        let scheduler = spawn_scheduler(self.run_id.clone(), control.clone(), self.config.clone());
        let workers = spawn_workers(
            self.run_id.clone(),
            self.data_dir.clone(),
            self.config.clone(),
            control.clone(),
            next_id.clone(),
            accumulator.clone(),
            errors.clone(),
        );
        let sampler = spawn_sampler(
            self.run_id.clone(),
            self.config.clone(),
            self.data_dir.clone(),
            control.clone(),
            accumulator.clone(),
            aggregate.clone(),
        );

        let duration = Duration::from_secs(self.config.load.duration_secs);
        let start = Instant::now();
        while start.elapsed() < duration && !control.stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));
        }
        control.stop.store(true, Ordering::Relaxed);

        scheduler.join().expect("scheduler panicked");
        for handle in workers {
            handle.join().expect("worker thread panicked");
        }
        sampler.join().expect("sampler thread panicked")?;

        let mut flush_engine = open_engine(&self.config, &self.data_dir)?;
        flush_engine.flush()?;

        let aggregate = aggregate.lock().expect("aggregate lock poisoned").clone();
        let ended_at_ms = now_ms();
        let error_count = errors.count();
        let error_messages = errors.messages();
        if error_count > 0 {
            warnings.push(format!(
                "{error_count} operation or worker errors were recorded during the run."
            ));
        }

        let status = if error_count > 0 {
            RunStatus::Failed
        } else if control.manual_stop_requested() {
            RunStatus::Interrupted
        } else {
            RunStatus::Completed
        };

        let summary = RunSummary {
            run_id: self.run_id.clone(),
            engine: self.config.engine,
            config: self.config.clone(),
            final_config: control.effective_config(),
            started_at_ms,
            ended_at_ms,
            status,
            warnings,
            error_messages,
            control_events: control.control_events(),
            artifact_paths: artifact_paths(&self.run_dir, &self.data_dir),
            avg_writes_per_sec: aggregate.avg_writes_per_sec(),
            avg_reads_per_sec: aggregate.avg_reads_per_sec(),
            peak_rss_bytes: aggregate.peak_rss_bytes(),
            peak_disk_usage_bytes: aggregate.peak_disk_usage_bytes(),
            log_count: 0,
            recent_logs: Vec::new(),
        };
        emit_event(&WorkerEvent::Finished { summary })?;
        Ok(())
    }
}

fn spawn_control_reader(run_id: String, control: RuntimeControl) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let stdin = std::io::stdin();
        let reader = BufReader::new(stdin.lock());
        for line in reader.lines() {
            let Ok(line) = line else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<ControlMessage>(&line) {
                Ok(message) => {
                    apply_control_message(&run_id, &control, message, ControlSource::Interactive)
                }
                Err(_) => {
                    for message in Deserializer::from_str(&line).into_iter::<ControlMessage>() {
                        if let Ok(message) = message {
                            apply_control_message(
                                &run_id,
                                &control,
                                message,
                                ControlSource::Interactive,
                            );
                        }
                    }
                }
            }
            if control.stop.load(Ordering::Relaxed) {
                break;
            }
        }
    })
}

fn spawn_scheduler(
    run_id: String,
    control: RuntimeControl,
    config: BenchmarkConfig,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let start = Instant::now();
        let mut phases = config.ramp_schedule.into_iter().peekable();
        while !control.stop.load(Ordering::Relaxed) {
            let elapsed = start.elapsed().as_secs();
            while let Some(phase) = phases.peek() {
                if phase.at_second > elapsed {
                    break;
                }
                let phase = phases.next().expect("phase existed");
                apply_control_message(
                    &run_id,
                    &control,
                    ControlMessage::ApplyPhase { phase },
                    ControlSource::Schedule,
                );
            }
            thread::sleep(Duration::from_millis(100));
        }
    })
}

fn spawn_workers(
    run_id: String,
    data_dir: PathBuf,
    config: BenchmarkConfig,
    control: RuntimeControl,
    next_id: Arc<AtomicU64>,
    accumulator: Arc<Mutex<SampleAccumulator>>,
    errors: RuntimeErrors,
) -> Vec<thread::JoinHandle<()>> {
    let max_concurrency = max_runtime_concurrency(&config);
    (0..max_concurrency)
        .map(|index| {
            let run_id = run_id.clone();
            let data_dir = data_dir.clone();
            let config = config.clone();
            let control = control.clone();
            let next_id = next_id.clone();
            let accumulator = accumulator.clone();
            let errors = errors.clone();
            thread::spawn(move || {
                let engine = match open_engine(&config, &data_dir) {
                    Ok(engine) => engine,
                    Err(error) => {
                        let message = format!(
                            "worker {index} failed to open {} engine: {error}",
                            config.engine.as_str()
                        );
                        if let Some(message) = errors.record(message) {
                            let _ = emit_runtime_log(&run_id, RunLogLevel::Error, message);
                        }
                        control.stop.store(true, Ordering::Relaxed);
                        return;
                    }
                };
                worker_loop(
                    &run_id,
                    index,
                    engine,
                    config,
                    control,
                    next_id,
                    accumulator,
                    errors,
                );
            })
        })
        .collect()
}

fn max_runtime_concurrency(config: &BenchmarkConfig) -> usize {
    let scheduled_max = config
        .ramp_schedule
        .iter()
        .filter_map(|phase| phase.concurrency)
        .max()
        .unwrap_or(config.load.concurrency);
    scheduled_max.max(config.load.concurrency).max(1)
}

fn worker_loop(
    run_id: &str,
    index: usize,
    mut engine: Box<dyn EngineAdapter>,
    config: BenchmarkConfig,
    control: RuntimeControl,
    next_id: Arc<AtomicU64>,
    accumulator: Arc<Mutex<SampleAccumulator>>,
    errors: RuntimeErrors,
) {
    let mut rng = rand::rng();
    while !control.stop.load(Ordering::Relaxed) {
        if control.paused.load(Ordering::Relaxed)
            || index >= control.concurrency.load(Ordering::Relaxed)
        {
            thread::sleep(Duration::from_millis(20));
            continue;
        }

        let mix = control.current_mix();
        for _ in 0..config.load.batch_size {
            if control.stop.load(Ordering::Relaxed)
                || control.paused.load(Ordering::Relaxed)
                || index >= control.concurrency.load(Ordering::Relaxed)
            {
                break;
            }

            let op = mix.choose(rng.random_range(0..100));
            let candidate_id = if matches!(op, OperationKind::Insert) {
                next_id.fetch_add(1, Ordering::Relaxed)
            } else {
                next_id.load(Ordering::Relaxed)
            };

            let start = Instant::now();
            let result = execute_operation_with_retry(engine.as_mut(), &config, op, candidate_id);
            let elapsed = start.elapsed().as_micros() as u64;

            let mut stats = accumulator.lock().expect("accumulator lock poisoned");
            match result {
                Ok(rows) => {
                    let (logical_read_bytes, logical_write_bytes) =
                        logical_bytes_for_operation(&config, op, rows.max(1));
                    stats.record(
                        matches!(op, OperationKind::Insert | OperationKind::Update),
                        elapsed,
                        logical_read_bytes,
                        logical_write_bytes,
                    );
                }
                Err(error) => {
                    stats.record_error();
                    let message = format!("worker {index} {op:?} failed: {error}");
                    if let Some(message) = errors.record(message) {
                        let _ = emit_runtime_log(run_id, RunLogLevel::Error, message);
                    }
                }
            }
        }
    }
}

fn spawn_sampler(
    run_id: String,
    config: BenchmarkConfig,
    data_dir: PathBuf,
    control: RuntimeControl,
    accumulator: Arc<Mutex<SampleAccumulator>>,
    aggregate: Arc<Mutex<RunAggregate>>,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        let target_interval = Duration::from_millis(config.load.sample_interval_ms);
        let mut previous_io = current_io_counters().0;
        let mut last_sample_at = Instant::now();

        loop {
            let elapsed = wait_for_sample_tick(last_sample_at, target_interval, &control);
            let snapshot = accumulator
                .lock()
                .expect("accumulator lock poisoned")
                .snapshot_and_reset();
            let should_emit = !control.stop.load(Ordering::Relaxed)
                || snapshot.reads > 0
                || snapshot.writes > 0
                || snapshot.errors > 0;
            if !should_emit {
                break;
            }

            let sample_interval = elapsed.max(Duration::from_millis(1));
            let (current_io, io_precision) = current_io_counters();
            let (disk_read_bytes_per_sec, disk_write_bytes_per_sec) =
                io_per_second(previous_io, current_io, sample_interval, &snapshot);
            previous_io = current_io;

            let sample = MetricSample {
                timestamp_ms: now_ms(),
                sample_duration_ms: sample_interval.as_millis() as u64,
                run_id: run_id.clone(),
                engine: config.engine,
                writes_per_sec: snapshot.writes as f64 / sample_interval.as_secs_f64(),
                reads_per_sec: snapshot.reads as f64 / sample_interval.as_secs_f64(),
                p50_latency_ms: snapshot.p50_latency_ms,
                p95_latency_ms: snapshot.p95_latency_ms,
                rss_bytes: current_rss_bytes(),
                disk_read_bytes_per_sec,
                disk_write_bytes_per_sec,
                disk_usage_bytes: dir_size_bytes(&data_dir),
                error_count: snapshot.errors,
                io_precision,
            };
            aggregate
                .lock()
                .expect("aggregate lock poisoned")
                .update(&sample);
            emit_event(&WorkerEvent::Sample { sample })?;

            if control.stop.load(Ordering::Relaxed) {
                break;
            }
            last_sample_at = Instant::now();
        }

        Ok(())
    })
}

fn execute_operation_with_retry(
    adapter: &mut dyn EngineAdapter,
    config: &BenchmarkConfig,
    op: OperationKind,
    next_id: u64,
) -> Result<usize> {
    let start = Instant::now();
    loop {
        match execute_operation(adapter, config, op, next_id) {
            Ok(rows) => return Ok(rows),
            Err(error)
                if is_lock_error(&error)
                    && start.elapsed() < Duration::from_millis(LOCK_RETRY_TIMEOUT_MS) =>
            {
                thread::sleep(Duration::from_millis(LOCK_RETRY_SLEEP_MS));
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_lock_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("locked") || message.contains("busy")
}

fn apply_control_message(
    run_id: &str,
    control: &RuntimeControl,
    message: ControlMessage,
    source: ControlSource,
) {
    if let Ok(Some((event, effective_config))) = control.apply(message, source) {
        let _ = emit_event(&WorkerEvent::ControlApplied {
            run_id: run_id.to_string(),
            event,
            effective_config,
        });
    }
}

fn wait_for_sample_tick(
    last_sample_at: Instant,
    target_interval: Duration,
    control: &RuntimeControl,
) -> Duration {
    loop {
        let elapsed = last_sample_at.elapsed();
        if elapsed >= target_interval || control.stop.load(Ordering::Relaxed) {
            return elapsed;
        }
        let sleep_for = target_interval
            .saturating_sub(elapsed)
            .min(Duration::from_millis(20));
        thread::sleep(sleep_for);
    }
}

fn io_per_second(
    previous: Option<IoCounters>,
    current: Option<IoCounters>,
    interval: Duration,
    snapshot: &benchmark_core::SampleSnapshot,
) -> (f64, f64) {
    if let (Some(previous), Some(current)) = (previous, current) {
        let seconds = interval.as_secs_f64();
        let read = current.read_bytes.saturating_sub(previous.read_bytes) as f64 / seconds;
        let write = current.write_bytes.saturating_sub(previous.write_bytes) as f64 / seconds;
        (read, write)
    } else {
        (
            snapshot.logical_read_bytes as f64 / interval.as_secs_f64(),
            snapshot.logical_write_bytes as f64 / interval.as_secs_f64(),
        )
    }
}

fn artifact_paths(run_dir: &Path, data_dir: &Path) -> ArtifactPaths {
    ArtifactPaths {
        config_path: run_dir.join("config.json").display().to_string(),
        metrics_path: run_dir.join("metrics.jsonl").display().to_string(),
        summary_path: run_dir.join("summary.json").display().to_string(),
        control_events_path: run_dir.join("control-events.jsonl").display().to_string(),
        data_dir: data_dir.display().to_string(),
        logs_path: run_dir.join("logs.jsonl").display().to_string(),
    }
}

fn emit_event(event: &WorkerEvent) -> Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, event)?;
    writeln!(&mut handle)?;
    handle.flush()?;
    Ok(())
}

fn emit_runtime_log(run_id: &str, level: RunLogLevel, message: impl Into<String>) -> Result<()> {
    emit_event(&WorkerEvent::Log {
        run_id: run_id.to_string(),
        entry: RunLogEntry {
            timestamp_ms: now_ms(),
            level,
            source: RunLogSource::WorkerEvent,
            message: message.into(),
        },
    })
}

fn describe_storage(config: &BenchmarkConfig) -> String {
    let storage = config.resolved_storage();
    match config.engine {
        EngineKind::Sqlite => format!(
            "sqlite journal_mode={:?}, synchronous={:?}",
            storage.sqlite.journal_mode, storage.sqlite.synchronous
        ),
        EngineKind::Hematite => {
            format!("hematite journal_mode={:?}", storage.hematite.journal_mode)
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use benchmark_core::{EngineKind, LoadConfig, ScenarioConfig, StorageConfig};

    fn config() -> BenchmarkConfig {
        BenchmarkConfig {
            run_name: "worker".to_string(),
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
                sample_interval_ms: 100,
                mix: OperationMix::default(),
            },
            ramp_schedule: vec![],
            storage: StorageConfig::default(),
            durability: None,
        }
    }

    #[test]
    fn runtime_control_updates_mix() -> Result<()> {
        let control = RuntimeControl::new(config());
        control.apply(
            ControlMessage::UpdateMix {
                point_reads: 25,
                range_scans: 25,
                inserts: 25,
                updates: 25,
            },
            ControlSource::Interactive,
        )?;
        assert_eq!(control.current_mix().point_reads, 25);
        assert_eq!(control.control_events().len(), 1);
        Ok(())
    }

    #[test]
    fn scheduled_concurrency_uses_peak_value() {
        let mut benchmark = config();
        benchmark.ramp_schedule = vec![benchmark_core::RampPhase {
            at_second: 1,
            concurrency: Some(8),
            mix: None,
        }];
        assert_eq!(max_runtime_concurrency(&benchmark), 8);
    }
}
