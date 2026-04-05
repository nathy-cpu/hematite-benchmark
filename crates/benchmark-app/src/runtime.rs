use crate::engine::{EngineAdapter, execute_operation, logical_bytes_for_operation, open_engine};
use crate::metrics::{IoCounters, current_io_counters, current_rss_bytes, dir_size_bytes};
use anyhow::{Context, Result, bail};
use benchmark_core::{
    ArtifactPaths, BenchmarkConfig, ControlMessage, MetricSample, OperationKind, OperationMix,
    RunAggregate, RunStatus, RunSummary, SampleAccumulator, WorkerEvent,
};
use rand::Rng;
use serde_json::Deserializer;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct RuntimeControl {
    paused: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    concurrency: Arc<AtomicUsize>,
    mix: Arc<Mutex<OperationMix>>,
}

impl RuntimeControl {
    fn new(initial_concurrency: usize, mix: OperationMix) -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            concurrency: Arc::new(AtomicUsize::new(initial_concurrency)),
            mix: Arc::new(Mutex::new(mix)),
        }
    }

    fn apply(&self, message: ControlMessage) -> Result<()> {
        match message {
            ControlMessage::Pause => self.paused.store(true, Ordering::Relaxed),
            ControlMessage::Resume => self.paused.store(false, Ordering::Relaxed),
            ControlMessage::Stop => self.stop.store(true, Ordering::Relaxed),
            ControlMessage::UpdateConcurrency { concurrency } => {
                if concurrency == 0 {
                    bail!("concurrency must be greater than zero");
                }
                self.concurrency.store(concurrency, Ordering::Relaxed);
            }
            ControlMessage::UpdateMix {
                point_reads,
                range_scans,
                inserts,
                updates,
            } => {
                let mix = OperationMix {
                    point_reads,
                    range_scans,
                    inserts,
                    updates,
                };
                mix.validate().map_err(anyhow::Error::msg)?;
                *self.mix.lock().expect("mix lock poisoned") = mix;
            }
            ControlMessage::ApplyPhase { phase } => {
                if let Some(concurrency) = phase.concurrency {
                    self.apply(ControlMessage::UpdateConcurrency { concurrency })?;
                }
                if let Some(mix) = phase.mix {
                    self.apply(ControlMessage::UpdateMix {
                        point_reads: mix.point_reads,
                        range_scans: mix.range_scans,
                        inserts: mix.inserts,
                        updates: mix.updates,
                    })?;
                }
            }
        }
        Ok(())
    }
}

pub fn run_worker_from_args() -> Result<()> {
    let args = WorkerArgs::parse(env::args().skip(1).collect())?;
    let config_text = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read config file {}", args.config.display()))?;
    let config: BenchmarkConfig = serde_json::from_str(&config_text)?;
    config.validate().map_err(anyhow::Error::msg)?;

    let runtime = WorkerRuntime::new(args.run_id, args.run_dir, config)?;
    runtime.run()
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
        let mut engine = open_engine(self.config.engine, &self.data_dir, self.config.durability)?;
        engine.prepare_dataset(&self.config)?;
        let warnings = engine.warnings().to_vec();

        emit_event(&WorkerEvent::Ready {
            run_id: self.run_id.clone(),
            engine: self.config.engine,
            pid: std::process::id(),
            warnings: warnings.clone(),
        })?;

        let engine = Arc::new(Mutex::new(engine));
        let control =
            RuntimeControl::new(self.config.load.concurrency, self.config.load.mix.clone());
        let next_id = Arc::new(AtomicU64::new(self.config.scenario.initial_rows + 1));
        let accumulator = Arc::new(Mutex::new(SampleAccumulator::default()));
        let aggregate = Arc::new(Mutex::new(RunAggregate::default()));

        let _control_reader = spawn_control_reader(control.clone());
        let scheduler = spawn_scheduler(control.clone(), self.config.clone());
        let workers = spawn_workers(
            engine.clone(),
            self.config.clone(),
            control.clone(),
            next_id.clone(),
            accumulator.clone(),
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

        engine.lock().expect("engine lock poisoned").flush()?;

        let aggregate = aggregate.lock().expect("aggregate lock poisoned").clone();
        let ended_at_ms = now_ms();
        let summary = RunSummary {
            run_id: self.run_id.clone(),
            engine: self.config.engine,
            config: self.config.clone(),
            started_at_ms,
            ended_at_ms,
            status: RunStatus::Completed,
            warnings,
            artifact_paths: ArtifactPaths {
                config_path: self.run_dir.join("config.json").display().to_string(),
                metrics_path: self.run_dir.join("metrics.jsonl").display().to_string(),
                summary_path: self.run_dir.join("summary.json").display().to_string(),
                data_dir: self.data_dir.display().to_string(),
            },
            avg_writes_per_sec: aggregate.avg_writes_per_sec(),
            avg_reads_per_sec: aggregate.avg_reads_per_sec(),
            peak_rss_bytes: aggregate.peak_rss_bytes(),
            peak_disk_usage_bytes: aggregate.peak_disk_usage_bytes(),
        };
        emit_event(&WorkerEvent::Finished { summary })?;
        Ok(())
    }
}

fn spawn_control_reader(control: RuntimeControl) -> thread::JoinHandle<()> {
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
                    let _ = control.apply(message);
                }
                Err(_) => {
                    for message in Deserializer::from_str(&line).into_iter::<ControlMessage>() {
                        if let Ok(message) = message {
                            let _ = control.apply(message);
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

fn spawn_scheduler(control: RuntimeControl, config: BenchmarkConfig) -> thread::JoinHandle<()> {
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
                let _ = control.apply(ControlMessage::ApplyPhase { phase });
            }
            thread::sleep(Duration::from_millis(100));
        }
    })
}

fn spawn_workers(
    engine: Arc<Mutex<Box<dyn EngineAdapter>>>,
    config: BenchmarkConfig,
    control: RuntimeControl,
    next_id: Arc<AtomicU64>,
    accumulator: Arc<Mutex<SampleAccumulator>>,
) -> Vec<thread::JoinHandle<()>> {
    let max_concurrency = max_runtime_concurrency(&config);
    (0..max_concurrency)
        .map(|index| {
            let engine = engine.clone();
            let config = config.clone();
            let control = control.clone();
            let next_id = next_id.clone();
            let accumulator = accumulator.clone();
            thread::spawn(move || worker_loop(index, engine, config, control, next_id, accumulator))
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
    index: usize,
    engine: Arc<Mutex<Box<dyn EngineAdapter>>>,
    config: BenchmarkConfig,
    control: RuntimeControl,
    next_id: Arc<AtomicU64>,
    accumulator: Arc<Mutex<SampleAccumulator>>,
) {
    let mut rng = rand::rng();
    while !control.stop.load(Ordering::Relaxed) {
        if control.paused.load(Ordering::Relaxed)
            || index >= control.concurrency.load(Ordering::Relaxed)
        {
            thread::sleep(Duration::from_millis(20));
            continue;
        }

        let mix = control.mix.lock().expect("mix lock poisoned").clone();
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
            let result = {
                let mut engine = engine.lock().expect("engine lock poisoned");
                execute_operation(engine.as_mut(), &config, op, candidate_id)
            };
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
                Err(_) => stats.record_error(),
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
        let interval = Duration::from_millis(config.load.sample_interval_ms);
        let mut previous_io = current_io_counters().0;

        loop {
            thread::sleep(interval);
            let snapshot = accumulator
                .lock()
                .expect("accumulator lock poisoned")
                .snapshot_and_reset();

            let (current_io, io_precision) = current_io_counters();
            let (disk_read_bytes_per_sec, disk_write_bytes_per_sec) =
                io_per_second(previous_io, current_io, interval, &snapshot);
            previous_io = current_io;

            let sample = MetricSample {
                timestamp_ms: now_ms(),
                run_id: run_id.clone(),
                engine: config.engine,
                writes_per_sec: snapshot.writes as f64 / interval.as_secs_f64(),
                reads_per_sec: snapshot.reads as f64 / interval.as_secs_f64(),
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
        }

        Ok(())
    })
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

fn emit_event(event: &WorkerEvent) -> Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, event)?;
    writeln!(&mut handle)?;
    handle.flush()?;
    Ok(())
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
    use benchmark_core::{DurabilityPreset, EngineKind, LoadConfig, ScenarioConfig};

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
            durability: DurabilityPreset::Balanced,
        }
    }

    #[test]
    fn runtime_control_updates_mix() -> Result<()> {
        let control = RuntimeControl::new(2, OperationMix::default());
        control.apply(ControlMessage::UpdateMix {
            point_reads: 25,
            range_scans: 25,
            inserts: 25,
            updates: 25,
        })?;
        assert_eq!(control.mix.lock().expect("mix").point_reads, 25);
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
