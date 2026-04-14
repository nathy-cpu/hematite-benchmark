use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use benchmark_core::{
    AppliedControlEvent, ArtifactPaths, BenchmarkConfig, ControlMessage, MetricSample,
    RunAggregate, RunDetail, RunListItem, RunLogEntry, RunLogLevel, RunLogSource, RunStatus,
    RunSummary, WorkerEvent,
};
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path as FsPath, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const INDEX_HTML: &str = include_str!("../static/index.html");
const APP_JS: &str = include_str!("../static/app.js");
const STYLES_CSS: &str = include_str!("../static/styles.css");
const MAX_RECENT_LOGS: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerVerbosity {
    Quiet,
    Normal,
    Verbose,
    Trace,
}

impl ServerVerbosity {
    pub fn default_filter(self) -> &'static str {
        match self {
            Self::Quiet => "warn",
            Self::Normal => "info,benchmark_app=info",
            Self::Verbose => "info,benchmark_app=debug",
            Self::Trace => "info,benchmark_app=trace",
        }
    }
}

impl Default for ServerVerbosity {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Clone)]
struct AppState {
    runs_dir: PathBuf,
    runs: Arc<RwLock<HashMap<String, StoredRun>>>,
}

struct StoredRun {
    run_id: String,
    config: BenchmarkConfig,
    effective_config: BenchmarkConfig,
    status: RunStatus,
    started_at_ms: u64,
    ended_at_ms: Option<u64>,
    warnings: Vec<String>,
    error_messages: Vec<String>,
    control_events: Vec<AppliedControlEvent>,
    latest_sample: Option<MetricSample>,
    recent_logs: Vec<RunLogEntry>,
    log_count: usize,
    summary: Option<RunSummary>,
    run_dir: PathBuf,
    active: Option<ActiveRun>,
}

struct ActiveRun {
    stdin: Arc<Mutex<ChildStdin>>,
    tx: broadcast::Sender<WorkerEvent>,
}

struct SpawnedWorker {
    child: Child,
    launcher: &'static str,
}

pub async fn run_server() -> Result<()> {
    run_server_with_verbosity(ServerVerbosity::default()).await
}

pub async fn run_server_with_verbosity(verbosity: ServerVerbosity) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| verbosity.default_filter().into()),
        )
        .init();

    let runs_dir = std::env::current_dir()?.join("runs");
    fs::create_dir_all(&runs_dir).await?;
    let state = AppState {
        runs_dir: runs_dir.clone(),
        runs: Arc::new(RwLock::new(load_existing_runs(&runs_dir).await?)),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/setup", get(index))
        .route("/dashboard", get(index))
        .route("/history", get(index))
        .route("/app.js", get(app_js))
        .route("/styles.css", get(styles_css))
        .route("/api/runs", get(list_runs).post(start_run))
        .route("/api/runs/{run_id}", get(get_run))
        .route("/api/runs/{run_id}/control", post(control_run))
        .route("/api/runs/{run_id}/stream", get(stream_run))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 3000)).await?;
    info!("benchmark dashboard listening on http://127.0.0.1:3000");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_js() -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        APP_JS,
    )
        .into_response()
}

async fn styles_css() -> Response {
    ([(axum::http::header::CONTENT_TYPE, "text/css")], STYLES_CSS).into_response()
}

async fn list_runs(State(state): State<AppState>) -> Json<Vec<RunListItem>> {
    let runs = state.runs.read().await;
    let mut items = runs
        .values()
        .map(|run| RunListItem {
            run_id: run.run_id.clone(),
            engine: run.config.engine,
            run_name: run.config.run_name.clone(),
            status: run.status.clone(),
            started_at_ms: run.started_at_ms,
            ended_at_ms: run.ended_at_ms,
            latest_sample: run.latest_sample.clone(),
            summary: run.summary.clone(),
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| right.started_at_ms.cmp(&left.started_at_ms));
    Json(items)
}

async fn get_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<RunDetail>, ApiError> {
    let runs = state.runs.read().await;
    let run = runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let samples = load_samples(&run.run_dir.join("metrics.jsonl")).await?;
    let logs = load_logs(&run.run_dir.join("logs.jsonl")).await?;
    Ok(Json(RunDetail {
        run_id: run.run_id.clone(),
        status: run.status.clone(),
        config: run.config.clone(),
        effective_config: run.effective_config.clone(),
        warnings: run.warnings.clone(),
        error_messages: run.error_messages.clone(),
        control_events: run.control_events.clone(),
        samples,
        logs,
        summary: run.summary.clone(),
    }))
}

async fn start_run(
    State(state): State<AppState>,
    Json(config): Json<BenchmarkConfig>,
) -> Result<Json<RunListItem>, ApiError> {
    config.validate().map_err(ApiError::bad_request)?;

    let run_id = Uuid::new_v4().to_string();
    let run_dir = state.runs_dir.join(&run_id);
    let config_path = run_dir.join("config.json");
    let metrics_path = run_dir.join("metrics.jsonl");
    let control_events_path = run_dir.join("control-events.jsonl");
    let logs_path = run_dir.join("logs.jsonl");
    fs::create_dir_all(run_dir.join("data")).await?;
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?).await?;
    fs::write(&metrics_path, &[]).await?;
    fs::write(&control_events_path, &[]).await?;
    fs::write(&logs_path, &[]).await?;

    let started_at_ms = now_ms();
    let (tx, _) = broadcast::channel(200);
    {
        let mut runs = state.runs.write().await;
        runs.insert(
            run_id.clone(),
            StoredRun {
                run_id: run_id.clone(),
                config: config.clone(),
                effective_config: config.clone(),
                status: RunStatus::Pending,
                started_at_ms,
                ended_at_ms: None,
                warnings: Vec::new(),
                error_messages: Vec::new(),
                control_events: Vec::new(),
                latest_sample: None,
                recent_logs: Vec::new(),
                log_count: 0,
                summary: None,
                run_dir: run_dir.clone(),
                active: None,
            },
        );
    }

    record_run_log(
        &state,
        &run_id,
        RunLogEntry {
            timestamp_ms: now_ms(),
            level: RunLogLevel::Info,
            source: RunLogSource::Server,
            message: format!(
                "Run queued: engine={}, rows={}, concurrency={}, duration={}s",
                config.engine.as_str(),
                config.scenario.initial_rows,
                config.load.concurrency,
                config.load.duration_secs
            ),
        },
    )
    .await?;

    let spawned = match spawn_worker_process(&run_id, &run_dir, &config_path).await {
        Ok(child) => child,
        Err(error) => {
            state.runs.write().await.remove(&run_id);
            return Err(error);
        }
    };
    record_run_log(
        &state,
        &run_id,
        RunLogEntry {
            timestamp_ms: now_ms(),
            level: RunLogLevel::Info,
            source: RunLogSource::Server,
            message: format!(
                "Launching worker via {}{}",
                spawned.launcher,
                spawned
                    .child
                    .id()
                    .map(|pid| format!(" (pid={pid})"))
                    .unwrap_or_default()
            ),
        },
    )
    .await?;
    let active = match register_worker(
        state.clone(),
        run_id.clone(),
        run_dir.clone(),
        spawned.child,
        tx.clone(),
    )
    .await
    {
        Ok(active) => active,
        Err(error) => {
            state.runs.write().await.remove(&run_id);
            return Err(error);
        }
    };

    {
        let mut runs = state.runs.write().await;
        if let Some(run) = runs.get_mut(&run_id) {
            run.active = Some(active);
        }
    }

    Ok(Json(RunListItem {
        run_id,
        engine: config.engine,
        run_name: config.run_name,
        status: RunStatus::Pending,
        started_at_ms,
        ended_at_ms: None,
        latest_sample: None,
        summary: None,
    }))
}

async fn control_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    Json(message): Json<ControlMessage>,
) -> Result<StatusCode, ApiError> {
    let stdin = {
        let runs = state.runs.read().await;
        let run = runs
            .get(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let active = run
            .active
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("run is not active"))?;
        active.stdin.clone()
    };

    let mut stdin = stdin.lock().await;
    let payload = serde_json::to_vec(&message)?;
    stdin.write_all(&payload).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(StatusCode::ACCEPTED)
}

async fn stream_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let rx = {
        let runs = state.runs.read().await;
        let run = runs
            .get(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let active = run
            .active
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("run is not active"))?;
        active.tx.subscribe()
    };

    let stream = BroadcastStream::new(rx).filter_map(|event| async move {
        match event {
            Ok(event) => Some(Ok(Event::default().json_data(event).expect("serializable"))),
            Err(_) => None,
        }
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn register_worker(
    state: AppState,
    run_id: String,
    run_dir: PathBuf,
    mut child: Child,
    tx: broadcast::Sender<WorkerEvent>,
) -> Result<ActiveRun, ApiError> {
    let stdout = child.stdout.take().context("worker stdout missing")?;
    let stderr = child.stderr.take().context("worker stderr missing")?;
    let stdin = child.stdin.take().context("worker stdin missing")?;
    let stdin = Arc::new(Mutex::new(stdin));
    let metrics_path = run_dir.join("metrics.jsonl");
    let control_events_path = run_dir.join("control-events.jsonl");
    let summary_path = run_dir.join("summary.json");

    let stdout_state = state.clone();
    let stdout_run_id = run_id.clone();
    let stdout_tx = tx.clone();
    tokio::spawn(async move {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&metrics_path)
            .await;
        let mut file = match file {
            Ok(file) => file,
            Err(error) => {
                error!(?error, "failed to open metrics file");
                return;
            }
        };
        let control_events_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&control_events_path)
            .await;
        let mut control_events_file = match control_events_file {
            Ok(file) => file,
            Err(error) => {
                error!(?error, "failed to open control events file");
                return;
            }
        };
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            match serde_json::from_str::<WorkerEvent>(&line) {
                Ok(event) => {
                    if let Err(error) = handle_worker_event(
                        &stdout_state,
                        &stdout_run_id,
                        &summary_path,
                        &mut file,
                        &mut control_events_file,
                        event.clone(),
                    )
                    .await
                    {
                        error!(?error, "failed to handle worker event");
                    }
                    if !matches!(event, WorkerEvent::Log { .. }) {
                        let _ = stdout_tx.send(event);
                    }
                }
                Err(_) => {
                    if !line.trim().is_empty() {
                        warn!("ignoring non-json worker line: {line}");
                    }
                }
            }
        }
    });

    let stderr_state = state.clone();
    let stderr_run_id = run_id.clone();
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if !line.trim().is_empty() {
                warn!("worker stderr: {line}");
                if let Err(error) = record_run_log(
                    &stderr_state,
                    &stderr_run_id,
                    RunLogEntry {
                        timestamp_ms: now_ms(),
                        level: RunLogLevel::Warn,
                        source: RunLogSource::WorkerStderr,
                        message: line.clone(),
                    },
                )
                .await
                {
                    error!(?error, "failed to record worker stderr log");
                }
            }
        }
    });

    let wait_state = state.clone();
    let wait_run_id = run_id.clone();
    tokio::spawn(async move {
        match child.wait().await {
            Ok(status) if !status.success() => {
                let message = format!("worker exited with status {status}");
                if let Err(error) = record_run_log(
                    &wait_state,
                    &wait_run_id,
                    RunLogEntry {
                        timestamp_ms: now_ms(),
                        level: RunLogLevel::Error,
                        source: RunLogSource::Server,
                        message: message.clone(),
                    },
                )
                .await
                {
                    error!(?error, "failed to record worker exit log");
                }
                if let Err(error) = finalize_failed_run(&wait_state, &wait_run_id, message).await {
                    error!(?error, "failed to finalize failed run");
                }
            }
            Ok(_) => {}
            Err(error) => {
                error!(?error, "failed to wait for worker");
                if let Err(log_error) = record_run_log(
                    &wait_state,
                    &wait_run_id,
                    RunLogEntry {
                        timestamp_ms: now_ms(),
                        level: RunLogLevel::Error,
                        source: RunLogSource::Server,
                        message: format!("failed to wait for worker: {error}"),
                    },
                )
                .await
                {
                    error!(?log_error, "failed to record wait error log");
                }
                if let Err(finalize_error) =
                    finalize_failed_run(&wait_state, &wait_run_id, error.to_string()).await
                {
                    error!(?finalize_error, "failed to persist wait error");
                }
            }
        }
    });

    Ok(ActiveRun { stdin, tx })
}

async fn handle_worker_event(
    state: &AppState,
    run_id: &str,
    summary_path: &FsPath,
    metrics_file: &mut tokio::fs::File,
    control_events_file: &mut tokio::fs::File,
    event: WorkerEvent,
) -> Result<()> {
    match event {
        WorkerEvent::Ready { pid, warnings, .. } => {
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.status = RunStatus::Running;
                run.warnings = warnings;
            }
            drop(runs);
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: now_ms(),
                    level: RunLogLevel::Info,
                    source: RunLogSource::WorkerEvent,
                    message: format!("Worker ready (pid={pid}) and dataset prepared"),
                },
            )
            .await?;
        }
        WorkerEvent::ControlApplied {
            event,
            effective_config,
            ..
        } => {
            let message = format!(
                "Control applied: {}",
                describe_control_event(&effective_config)
            );
            control_events_file
                .write_all(format!("{}\n", serde_json::to_string(&event)?).as_bytes())
                .await?;
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.control_events.push(event);
                run.effective_config = effective_config;
            }
            drop(runs);
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: now_ms(),
                    level: RunLogLevel::Info,
                    source: RunLogSource::WorkerEvent,
                    message,
                },
            )
            .await?;
        }
        WorkerEvent::Sample { sample } => {
            metrics_file
                .write_all(format!("{}\n", serde_json::to_string(&sample)?).as_bytes())
                .await?;
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.latest_sample = Some(sample.clone());
                run.status = RunStatus::Running;
            }
            drop(runs);
            let message = format!(
                "Sample: writes/s={:.1}, reads/s={:.1}, p95={:.2} ms, rss={}, errors={}",
                sample.writes_per_sec,
                sample.reads_per_sec,
                sample.p95_latency_ms,
                sample.rss_bytes,
                sample.error_count
            );
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: sample.timestamp_ms,
                    level: RunLogLevel::Debug,
                    source: RunLogSource::WorkerEvent,
                    message,
                },
            )
            .await?;
        }
        WorkerEvent::Finished { mut summary } => {
            let status = summary.status.clone();
            let avg_writes = summary.avg_writes_per_sec;
            let avg_reads = summary.avg_reads_per_sec;
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: now_ms(),
                    level: RunLogLevel::Info,
                    source: RunLogSource::Server,
                    message: format!(
                        "Run finished: status={status:?}, avg writes/s={avg_writes:.1}, avg reads/s={avg_reads:.1}"
                    ),
                },
            )
            .await?;
            {
                let runs = state.runs.read().await;
                if let Some(run) = runs.get(run_id) {
                    summary.log_count = run.log_count;
                    summary.recent_logs = run.recent_logs.clone();
                }
            }
            fs::write(summary_path, serde_json::to_vec_pretty(&summary)?).await?;
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.ended_at_ms = Some(summary.ended_at_ms);
                run.status = summary.status.clone();
                run.warnings = summary.warnings.clone();
                run.error_messages = summary.error_messages.clone();
                run.control_events = summary.control_events.clone();
                run.effective_config = summary.final_config.clone();
                run.summary = Some(summary);
                run.active = None;
            }
        }
        WorkerEvent::Log { entry, .. } => {
            record_run_log(state, run_id, entry).await?;
        }
        WorkerEvent::Failed { message, .. } => {
            warn!("worker failed: {message}");
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: now_ms(),
                    level: RunLogLevel::Error,
                    source: RunLogSource::Server,
                    message: format!("Worker failed: {message}"),
                },
            )
            .await?;
            finalize_failed_run(state, run_id, message).await?;
        }
    }
    Ok(())
}

async fn spawn_worker_process(
    run_id: &str,
    run_dir: &FsPath,
    config_path: &FsPath,
) -> Result<SpawnedWorker, ApiError> {
    let current_exe = std::env::current_exe()?;
    let worker_binary = current_exe.with_file_name(worker_binary_name());

    let (launcher, mut command) = if should_reuse_worker_binary(&current_exe, &worker_binary) {
        ("worker binary", Command::new(worker_binary))
    } else {
        if worker_binary.exists() {
            info!(
                "worker binary is older than the running server binary; spawning via cargo to avoid config/schema drift"
            );
        }
        let mut command = Command::new("cargo");
        command.args([
            "run",
            "--quiet",
            "-p",
            "benchmark-app",
            "--bin",
            "benchmark-worker",
            "--",
        ]);
        ("cargo run", command)
    };

    command
        .arg("--run-id")
        .arg(run_id)
        .arg("--run-dir")
        .arg(run_dir)
        .arg("--config")
        .arg(config_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let child = command.spawn().map_err(ApiError::from)?;
    Ok(SpawnedWorker { child, launcher })
}

fn should_reuse_worker_binary(current_exe: &FsPath, worker_binary: &FsPath) -> bool {
    if !worker_binary.exists() {
        return false;
    }

    let Ok(worker_meta) = std::fs::metadata(worker_binary) else {
        return true;
    };
    let Ok(server_meta) = std::fs::metadata(current_exe) else {
        return true;
    };
    let Ok(worker_modified) = worker_meta.modified() else {
        return true;
    };
    let Ok(server_modified) = server_meta.modified() else {
        return true;
    };

    worker_modified >= server_modified
}

fn worker_binary_name() -> &'static OsStr {
    #[cfg(target_os = "windows")]
    {
        OsStr::new("benchmark-worker.exe")
    }
    #[cfg(not(target_os = "windows"))]
    {
        OsStr::new("benchmark-worker")
    }
}

async fn load_existing_runs(runs_dir: &FsPath) -> Result<HashMap<String, StoredRun>> {
    let mut runs = HashMap::new();
    let mut entries = fs::read_dir(runs_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_dir() {
            continue;
        }
        let run_dir = entry.path();
        let run_id = entry.file_name().to_string_lossy().to_string();
        let config_path = run_dir.join("config.json");
        if !config_path.exists() {
            continue;
        }
        let config: BenchmarkConfig = serde_json::from_slice(&fs::read(&config_path).await?)?;
        let control_events = load_control_events(&run_dir.join("control-events.jsonl"))
            .await
            .unwrap_or_default();
        let summary_path = run_dir.join("summary.json");
        let summary = if summary_path.exists() {
            Some(serde_json::from_slice::<RunSummary>(
                &fs::read(&summary_path).await?,
            )?)
        } else {
            None
        };
        let latest_sample = latest_sample(&run_dir.join("metrics.jsonl")).await?;
        let status = summary
            .as_ref()
            .map(|summary| summary.status.clone())
            .unwrap_or(RunStatus::Interrupted);
        let started_at_ms = summary
            .as_ref()
            .map(|summary| summary.started_at_ms)
            .unwrap_or_else(now_ms);
        let ended_at_ms = summary.as_ref().map(|summary| summary.ended_at_ms);
        let warnings = summary
            .as_ref()
            .map(|summary| summary.warnings.clone())
            .unwrap_or_default();
        let error_messages = summary
            .as_ref()
            .map(|summary| summary.error_messages.clone())
            .unwrap_or_default();
        let recent_logs = summary
            .as_ref()
            .map(|summary| summary.recent_logs.clone())
            .unwrap_or_default();
        let log_count = summary
            .as_ref()
            .map(|summary| summary.log_count)
            .unwrap_or_else(|| {
                load_logs_sync(&run_dir.join("logs.jsonl"))
                    .map(|logs| logs.len())
                    .unwrap_or(0)
            });
        let effective_config = summary
            .as_ref()
            .map(|summary| summary.final_config.clone())
            .unwrap_or_else(|| apply_control_events(&config, &control_events));

        runs.insert(
            run_id.clone(),
            StoredRun {
                run_id,
                config,
                effective_config,
                status,
                started_at_ms,
                ended_at_ms,
                warnings,
                error_messages,
                control_events,
                latest_sample,
                recent_logs,
                log_count,
                summary,
                run_dir,
                active: None,
            },
        );
    }
    Ok(runs)
}

async fn load_samples(path: &FsPath) -> Result<Vec<MetricSample>, ApiError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path).await?;
    let mut samples = Vec::new();
    for line in String::from_utf8_lossy(&bytes).lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(sample) = serde_json::from_str::<MetricSample>(line) {
            samples.push(sample);
        }
    }
    Ok(samples)
}

async fn latest_sample(path: &FsPath) -> Result<Option<MetricSample>> {
    let samples = load_samples(path)
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    Ok(samples.last().cloned())
}

async fn load_logs(path: &FsPath) -> Result<Vec<RunLogEntry>, ApiError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path).await?;
    Ok(parse_logs(&bytes))
}

fn load_logs_sync(path: &FsPath) -> Result<Vec<RunLogEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    Ok(parse_logs(&std::fs::read(path)?))
}

fn parse_logs(bytes: &[u8]) -> Vec<RunLogEntry> {
    let mut logs = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<RunLogEntry>(line) {
            logs.push(entry);
        }
    }
    logs
}

async fn load_control_events(path: &FsPath) -> Result<Vec<AppliedControlEvent>, ApiError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path).await?;
    let mut events = Vec::new();
    for line in String::from_utf8_lossy(&bytes).lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str::<AppliedControlEvent>(line) {
            events.push(event);
        }
    }
    Ok(events)
}

fn apply_control_events(
    initial_config: &BenchmarkConfig,
    events: &[AppliedControlEvent],
) -> BenchmarkConfig {
    let mut config = initial_config.clone();
    for event in events {
        match &event.control {
            ControlMessage::UpdateConcurrency { concurrency } => {
                config.load.concurrency = *concurrency;
            }
            ControlMessage::UpdateMix {
                point_reads,
                range_scans,
                inserts,
                updates,
            } => {
                config.load.mix = benchmark_core::OperationMix {
                    point_reads: *point_reads,
                    range_scans: *range_scans,
                    inserts: *inserts,
                    updates: *updates,
                };
            }
            ControlMessage::ApplyPhase { phase } => config.apply_phase(phase),
            ControlMessage::Pause | ControlMessage::Resume | ControlMessage::Stop => {}
        }
    }
    config
}

fn summarize_samples(samples: &[MetricSample]) -> RunAggregate {
    let mut aggregate = RunAggregate::default();
    for sample in samples {
        aggregate.update(sample);
    }
    aggregate
}

fn artifact_paths(run_dir: &FsPath) -> ArtifactPaths {
    ArtifactPaths {
        config_path: run_dir.join("config.json").display().to_string(),
        metrics_path: run_dir.join("metrics.jsonl").display().to_string(),
        summary_path: run_dir.join("summary.json").display().to_string(),
        control_events_path: run_dir.join("control-events.jsonl").display().to_string(),
        data_dir: run_dir.join("data").display().to_string(),
        logs_path: run_dir.join("logs.jsonl").display().to_string(),
    }
}

async fn record_run_log(state: &AppState, run_id: &str, entry: RunLogEntry) -> Result<()> {
    match entry.level {
        RunLogLevel::Debug => debug!(run_id, source = ?entry.source, "{}", entry.message),
        RunLogLevel::Info => info!(run_id, source = ?entry.source, "{}", entry.message),
        RunLogLevel::Warn => warn!(run_id, source = ?entry.source, "{}", entry.message),
        RunLogLevel::Error => error!(run_id, source = ?entry.source, "{}", entry.message),
    }

    let (run_dir, tx) = {
        let runs = state.runs.read().await;
        let Some(run) = runs.get(run_id) else {
            return Ok(());
        };
        (
            run.run_dir.clone(),
            run.active.as_ref().map(|active| active.tx.clone()),
        )
    };

    let log_path = run_dir.join("logs.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await?;
    file.write_all(format!("{}\n", serde_json::to_string(&entry)?).as_bytes())
        .await?;

    {
        let mut runs = state.runs.write().await;
        if let Some(run) = runs.get_mut(run_id) {
            run.log_count += 1;
            run.recent_logs.push(entry.clone());
            trim_recent_logs(&mut run.recent_logs);
        }
    }

    if let Some(tx) = tx {
        let _ = tx.send(WorkerEvent::Log {
            run_id: run_id.to_string(),
            entry,
        });
    }
    Ok(())
}

fn trim_recent_logs(logs: &mut Vec<RunLogEntry>) {
    if logs.len() > MAX_RECENT_LOGS {
        let extra = logs.len() - MAX_RECENT_LOGS;
        logs.drain(0..extra);
    }
}

fn describe_control_event(config: &BenchmarkConfig) -> String {
    format!(
        "effective concurrency={} mix={}/{}/{}/{}",
        config.load.concurrency,
        config.load.mix.point_reads,
        config.load.mix.range_scans,
        config.load.mix.inserts,
        config.load.mix.updates
    )
}

async fn finalize_failed_run(state: &AppState, run_id: &str, message: String) -> Result<()> {
    let (
        run_dir,
        config,
        effective_config,
        started_at_ms,
        ended_at_ms,
        warnings,
        error_messages,
        control_events,
        recent_logs,
        log_count,
        has_summary,
    ) = {
        let mut runs = state.runs.write().await;
        let Some(run) = runs.get_mut(run_id) else {
            return Ok(());
        };
        if !run
            .error_messages
            .iter()
            .any(|existing| existing == &message)
            && run.error_messages.len() < 8
        {
            run.error_messages.push(message.clone());
        }
        run.status = RunStatus::Failed;
        run.active = None;
        let ended_at_ms = *run.ended_at_ms.get_or_insert_with(now_ms);
        (
            run.run_dir.clone(),
            run.config.clone(),
            run.effective_config.clone(),
            run.started_at_ms,
            ended_at_ms,
            run.warnings.clone(),
            run.error_messages.clone(),
            run.control_events.clone(),
            run.recent_logs.clone(),
            run.log_count,
            run.summary.is_some(),
        )
    };

    if has_summary {
        return Ok(());
    }

    let samples = load_samples(&run_dir.join("metrics.jsonl"))
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let aggregate = summarize_samples(&samples);
    let summary = RunSummary {
        run_id: run_id.to_string(),
        engine: config.engine,
        config,
        final_config: effective_config,
        started_at_ms,
        ended_at_ms,
        status: RunStatus::Failed,
        warnings,
        error_messages,
        control_events,
        artifact_paths: artifact_paths(&run_dir),
        avg_writes_per_sec: aggregate.avg_writes_per_sec(),
        avg_reads_per_sec: aggregate.avg_reads_per_sec(),
        peak_rss_bytes: aggregate.peak_rss_bytes(),
        peak_disk_usage_bytes: aggregate.peak_disk_usage_bytes(),
        log_count,
        recent_logs,
    };
    let summary_path = run_dir.join("summary.json");
    fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?).await?;

    let mut runs = state.runs.write().await;
    if let Some(run) = runs.get_mut(run_id) {
        run.summary = Some(summary);
        run.ended_at_ms = Some(ended_at_ms);
        run.status = RunStatus::Failed;
        run.active = None;
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(error: std::io::Error) -> Self {
        anyhow::Error::from(error).into()
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(error: serde_json::Error) -> Self {
        anyhow::Error::from(error).into()
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        (
            self.status,
            headers,
            serde_json::json!({ "error": self.message }).to_string(),
        )
            .into_response()
    }
}
