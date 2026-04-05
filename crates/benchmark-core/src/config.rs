use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineKind {
    Sqlite,
    Hematite,
}

impl EngineKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Hematite => "hematite",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityPreset {
    Safe,
    Balanced,
    Fast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum IoPrecision {
    #[default]
    Exact,
    Approximate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    PointRead,
    RangeScan,
    Insert,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationMix {
    pub point_reads: u8,
    pub range_scans: u8,
    pub inserts: u8,
    pub updates: u8,
}

impl OperationMix {
    pub fn validate(&self) -> Result<(), String> {
        if self.total() != 100 {
            return Err(format!(
                "operation mix must add up to 100, got {}",
                self.total()
            ));
        }
        Ok(())
    }

    pub fn total(&self) -> u16 {
        self.point_reads as u16
            + self.range_scans as u16
            + self.inserts as u16
            + self.updates as u16
    }

    pub fn choose(&self, slot: u8) -> OperationKind {
        let point_end = self.point_reads;
        let range_end = point_end.saturating_add(self.range_scans);
        let insert_end = range_end.saturating_add(self.inserts);

        if slot < point_end {
            OperationKind::PointRead
        } else if slot < range_end {
            OperationKind::RangeScan
        } else if slot < insert_end {
            OperationKind::Insert
        } else {
            OperationKind::Update
        }
    }
}

impl Default for OperationMix {
    fn default() -> Self {
        Self {
            point_reads: 50,
            range_scans: 10,
            inserts: 20,
            updates: 20,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub initial_rows: u64,
    pub payload_size_bytes: usize,
    pub category_count: u32,
    pub range_scan_size: usize,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            initial_rows: 5_000,
            payload_size_bytes: 256,
            category_count: 32,
            range_scan_size: 25,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadConfig {
    pub concurrency: usize,
    pub batch_size: usize,
    pub duration_secs: u64,
    pub sample_interval_ms: u64,
    pub mix: OperationMix,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            batch_size: 1,
            duration_secs: 30,
            sample_interval_ms: 1_000,
            mix: OperationMix::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RampPhase {
    pub at_second: u64,
    #[serde(default)]
    pub concurrency: Option<usize>,
    #[serde(default)]
    pub mix: Option<OperationMix>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub run_name: String,
    pub engine: EngineKind,
    pub scenario: ScenarioConfig,
    pub load: LoadConfig,
    #[serde(default)]
    pub ramp_schedule: Vec<RampPhase>,
    pub durability: DurabilityPreset,
}

impl BenchmarkConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.run_name.trim().is_empty() {
            return Err("run name cannot be empty".to_string());
        }
        if self.scenario.initial_rows == 0 {
            return Err("initial_rows must be greater than 0".to_string());
        }
        if self.scenario.payload_size_bytes == 0 {
            return Err("payload_size_bytes must be greater than 0".to_string());
        }
        if self.scenario.category_count == 0 {
            return Err("category_count must be greater than 0".to_string());
        }
        if self.scenario.range_scan_size == 0 {
            return Err("range_scan_size must be greater than 0".to_string());
        }
        if self.load.concurrency == 0 {
            return Err("concurrency must be greater than 0".to_string());
        }
        if self.load.batch_size == 0 {
            return Err("batch_size must be greater than 0".to_string());
        }
        if self.load.duration_secs == 0 {
            return Err("duration_secs must be greater than 0".to_string());
        }
        if self.load.sample_interval_ms == 0 {
            return Err("sample_interval_ms must be greater than 0".to_string());
        }
        self.load.mix.validate()?;

        let mut last_at = None;
        for phase in &self.ramp_schedule {
            if let Some(previous) = last_at
                && phase.at_second < previous
            {
                return Err("ramp schedule must be sorted by at_second".to_string());
            }
            if let Some(mix) = &phase.mix {
                mix.validate()?;
            }
            last_at = Some(phase.at_second);
        }

        Ok(())
    }

    pub fn apply_phase(&mut self, phase: &RampPhase) {
        if let Some(concurrency) = phase.concurrency {
            self.load.concurrency = concurrency;
        }
        if let Some(mix) = &phase.mix {
            self.load.mix = mix.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> BenchmarkConfig {
        BenchmarkConfig {
            run_name: "baseline".to_string(),
            engine: EngineKind::Sqlite,
            scenario: ScenarioConfig::default(),
            load: LoadConfig::default(),
            ramp_schedule: vec![],
            durability: DurabilityPreset::Balanced,
        }
    }

    #[test]
    fn rejects_invalid_mix() {
        let mut config = valid_config();
        config.load.mix.point_reads = 70;
        assert!(config.validate().is_err());
    }

    #[test]
    fn applies_ramp_phase() {
        let mut config = valid_config();
        let phase = RampPhase {
            at_second: 5,
            concurrency: Some(12),
            mix: Some(OperationMix {
                point_reads: 25,
                range_scans: 25,
                inserts: 25,
                updates: 25,
            }),
        };

        config.apply_phase(&phase);

        assert_eq!(config.load.concurrency, 12);
        assert_eq!(config.load.mix.point_reads, 25);
        assert_eq!(config.load.mix.updates, 25);
    }

    #[test]
    fn requires_sorted_ramp_schedule() {
        let mut config = valid_config();
        config.ramp_schedule = vec![
            RampPhase {
                at_second: 5,
                concurrency: Some(6),
                mix: None,
            },
            RampPhase {
                at_second: 4,
                concurrency: Some(8),
                mix: None,
            },
        ];

        assert!(config.validate().is_err());
    }
}
