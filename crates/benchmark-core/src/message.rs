use crate::{BenchmarkConfig, EngineKind, IoPrecision, RampPhase};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPaths {
    pub config_path: String,
    pub metrics_path: String,
    pub summary_path: String,
    pub control_events_path: String,
    pub data_dir: String,
    #[serde(default)]
    pub logs_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunLogSource {
    Server,
    WorkerEvent,
    WorkerStderr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLogEntry {
    pub timestamp_ms: u64,
    pub level: RunLogLevel,
    pub source: RunLogSource,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlSource {
    Interactive,
    Schedule,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedControlEvent {
    pub timestamp_ms: u64,
    pub source: ControlSource,
    pub control: ControlMessage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    pub timestamp_ms: u64,
    pub sample_duration_ms: u64,
    pub run_id: String,
    pub engine: EngineKind,
    pub writes_per_sec: f64,
    pub reads_per_sec: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub rss_bytes: u64,
    pub disk_read_bytes_per_sec: f64,
    pub disk_write_bytes_per_sec: f64,
    pub disk_usage_bytes: u64,
    pub error_count: u64,
    pub io_precision: IoPrecision,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSummary {
    pub run_id: String,
    pub engine: EngineKind,
    pub config: BenchmarkConfig,
    pub final_config: BenchmarkConfig,
    pub started_at_ms: u64,
    pub ended_at_ms: u64,
    pub status: RunStatus,
    pub warnings: Vec<String>,
    pub error_messages: Vec<String>,
    pub control_events: Vec<AppliedControlEvent>,
    pub artifact_paths: ArtifactPaths,
    pub avg_writes_per_sec: f64,
    pub avg_reads_per_sec: f64,
    pub peak_rss_bytes: u64,
    pub peak_disk_usage_bytes: u64,
    #[serde(default)]
    pub log_count: usize,
    #[serde(default)]
    pub recent_logs: Vec<RunLogEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunListItem {
    pub run_id: String,
    pub engine: EngineKind,
    pub run_name: String,
    pub status: RunStatus,
    pub started_at_ms: u64,
    #[serde(default)]
    pub ended_at_ms: Option<u64>,
    #[serde(default)]
    pub latest_sample: Option<MetricSample>,
    #[serde(default)]
    pub summary: Option<RunSummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunDetail {
    pub run_id: String,
    pub status: RunStatus,
    pub config: BenchmarkConfig,
    pub effective_config: BenchmarkConfig,
    pub warnings: Vec<String>,
    pub error_messages: Vec<String>,
    pub control_events: Vec<AppliedControlEvent>,
    pub samples: Vec<MetricSample>,
    #[serde(default)]
    pub logs: Vec<RunLogEntry>,
    #[serde(default)]
    pub summary: Option<RunSummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlMessage {
    Pause,
    Resume,
    Stop,
    UpdateConcurrency {
        concurrency: usize,
    },
    UpdateMix {
        point_reads: u8,
        range_scans: u8,
        inserts: u8,
        updates: u8,
    },
    ApplyPhase {
        phase: RampPhase,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerEvent {
    Ready {
        run_id: String,
        engine: EngineKind,
        pid: u32,
        warnings: Vec<String>,
    },
    ControlApplied {
        run_id: String,
        event: AppliedControlEvent,
        effective_config: BenchmarkConfig,
    },
    Sample {
        sample: MetricSample,
    },
    Log {
        run_id: String,
        entry: RunLogEntry,
    },
    Finished {
        summary: RunSummary,
    },
    Failed {
        run_id: String,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LoadConfig, ScenarioConfig, StorageConfig};

    #[test]
    fn metric_sample_round_trip() {
        let sample = MetricSample {
            timestamp_ms: 1,
            sample_duration_ms: 1_000,
            run_id: "run-1".to_string(),
            engine: EngineKind::Sqlite,
            writes_per_sec: 12.5,
            reads_per_sec: 9.5,
            p50_latency_ms: 1.2,
            p95_latency_ms: 2.4,
            rss_bytes: 128,
            disk_read_bytes_per_sec: 64.0,
            disk_write_bytes_per_sec: 32.0,
            disk_usage_bytes: 512,
            error_count: 0,
            io_precision: IoPrecision::Exact,
        };

        let json = serde_json::to_string(&sample).unwrap();
        let decoded: MetricSample = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.run_id, "run-1");
        assert_eq!(decoded.io_precision, IoPrecision::Exact);
    }

    #[test]
    fn control_message_round_trip() {
        let message = ControlMessage::ApplyPhase {
            phase: RampPhase {
                at_second: 10,
                concurrency: Some(8),
                mix: None,
            },
        };
        let json = serde_json::to_string(&message).unwrap();
        let decoded: ControlMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn summary_embeds_config() {
        let config = BenchmarkConfig {
            run_name: "summary".to_string(),
            engine: EngineKind::Hematite,
            scenario: ScenarioConfig::default(),
            load: LoadConfig::default(),
            ramp_schedule: vec![],
            storage: StorageConfig::default(),
            durability: None,
        };
        let summary = RunSummary {
            run_id: "run".to_string(),
            engine: EngineKind::Hematite,
            config: config.clone(),
            final_config: config,
            started_at_ms: 1,
            ended_at_ms: 2,
            status: RunStatus::Completed,
            warnings: vec![],
            error_messages: vec![],
            control_events: vec![],
            artifact_paths: ArtifactPaths {
                config_path: "config".to_string(),
                metrics_path: "metrics".to_string(),
                summary_path: "summary".to_string(),
                control_events_path: "controls".to_string(),
                data_dir: "data".to_string(),
                logs_path: "logs".to_string(),
            },
            avg_writes_per_sec: 1.0,
            avg_reads_per_sec: 2.0,
            peak_rss_bytes: 3,
            peak_disk_usage_bytes: 4,
            log_count: 1,
            recent_logs: vec![RunLogEntry {
                timestamp_ms: 3,
                level: RunLogLevel::Info,
                source: RunLogSource::Server,
                message: "summary ready".to_string(),
            }],
        };

        let json = serde_json::to_string(&summary).unwrap();
        let decoded: RunSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.engine, EngineKind::Hematite);
        assert_eq!(decoded.config.run_name, "summary");
    }
}
