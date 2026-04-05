use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use benchmark_core::{
    BenchmarkConfig, ControlMessage, MetricSample, RunDetail, RunListItem, RunStatus, RunSummary,
    WorkerEvent,
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
use tracing::{error, info, warn};
use uuid::Uuid;

const INDEX_HTML: &str = include_str!("../static/index.html");
const APP_JS: &str = include_str!("../static/app.js");
const STYLES_CSS: &str = include_str!("../static/styles.css");

#[derive(Clone)]
struct AppState {
    runs_dir: PathBuf,
    runs: Arc<RwLock<HashMap<String, StoredRun>>>,
}

struct StoredRun {
    run_id: String,
    config: BenchmarkConfig,
    status: RunStatus,
    started_at_ms: u64,
    ended_at_ms: Option<u64>,
    warnings: Vec<String>,
    latest_sample: Option<MetricSample>,
    summary: Option<RunSummary>,
    run_dir: PathBuf,
    active: Option<ActiveRun>,
}

struct ActiveRun {
    stdin: Arc<Mutex<ChildStdin>>,
    tx: broadcast::Sender<WorkerEvent>,
}

pub async fn run_server() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,benchmark_app=debug".into()),
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
    Ok(Json(RunDetail {
        run_id: run.run_id.clone(),
        status: run.status.clone(),
        config: run.config.clone(),
        warnings: run.warnings.clone(),
        samples,
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
    fs::create_dir_all(run_dir.join("data")).await?;
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?).await?;
    fs::write(&metrics_path, &[]).await?;

    let started_at_ms = now_ms();
    let (tx, _) = broadcast::channel(200);
    let child = spawn_worker_process(&run_id, &run_dir, &config_path).await?;
    let active = register_worker(
        state.clone(),
        run_id.clone(),
        run_dir.clone(),
        child,
        tx.clone(),
    )
    .await?;

    {
        let mut runs = state.runs.write().await;
        runs.insert(
            run_id.clone(),
            StoredRun {
                run_id: run_id.clone(),
                config: config.clone(),
                status: RunStatus::Pending,
                started_at_ms,
                ended_at_ms: None,
                warnings: Vec::new(),
                latest_sample: None,
                summary: None,
                run_dir: run_dir.clone(),
                active: Some(active),
            },
        );
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
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            match serde_json::from_str::<WorkerEvent>(&line) {
                Ok(event) => {
                    if let Err(error) = handle_worker_event(
                        &stdout_state,
                        &stdout_run_id,
                        &summary_path,
                        &mut file,
                        event.clone(),
                    )
                    .await
                    {
                        error!(?error, "failed to handle worker event");
                    }
                    let _ = stdout_tx.send(event);
                }
                Err(_) => {
                    if !line.trim().is_empty() {
                        warn!("ignoring non-json worker line: {line}");
                    }
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if !line.trim().is_empty() {
                warn!("worker stderr: {line}");
            }
        }
    });

    let wait_state = state.clone();
    let wait_run_id = run_id.clone();
    tokio::spawn(async move {
        match child.wait().await {
            Ok(status) if !status.success() => {
                let mut runs = wait_state.runs.write().await;
                if let Some(run) = runs.get_mut(&wait_run_id)
                    && matches!(run.status, RunStatus::Pending | RunStatus::Running)
                {
                    run.status = RunStatus::Failed;
                    run.active = None;
                }
            }
            Ok(_) => {}
            Err(error) => error!(?error, "failed to wait for worker"),
        }
    });

    Ok(ActiveRun { stdin, tx })
}

async fn handle_worker_event(
    state: &AppState,
    run_id: &str,
    summary_path: &FsPath,
    metrics_file: &mut tokio::fs::File,
    event: WorkerEvent,
) -> Result<()> {
    match event {
        WorkerEvent::Ready { warnings, .. } => {
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.status = RunStatus::Running;
                run.warnings = warnings;
            }
        }
        WorkerEvent::Sample { sample } => {
            metrics_file
                .write_all(format!("{}\n", serde_json::to_string(&sample)?).as_bytes())
                .await?;
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.latest_sample = Some(sample);
                run.status = RunStatus::Running;
            }
        }
        WorkerEvent::Finished { summary } => {
            fs::write(summary_path, serde_json::to_vec_pretty(&summary)?).await?;
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.ended_at_ms = Some(summary.ended_at_ms);
                run.status = summary.status.clone();
                run.warnings = summary.warnings.clone();
                run.summary = Some(summary);
                run.active = None;
            }
        }
        WorkerEvent::Failed { message, .. } => {
            warn!("worker failed: {message}");
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.status = RunStatus::Failed;
                run.active = None;
            }
        }
    }
    Ok(())
}

async fn spawn_worker_process(
    run_id: &str,
    run_dir: &FsPath,
    config_path: &FsPath,
) -> Result<Child, ApiError> {
    let current_exe = std::env::current_exe()?;
    let worker_binary = current_exe.with_file_name(worker_binary_name());

    let mut command = if worker_binary.exists() {
        Command::new(worker_binary)
    } else {
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
        command
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

    command.spawn().map_err(ApiError::from)
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

        runs.insert(
            run_id.clone(),
            StoredRun {
                run_id,
                config,
                status,
                started_at_ms,
                ended_at_ms,
                warnings,
                latest_sample,
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
