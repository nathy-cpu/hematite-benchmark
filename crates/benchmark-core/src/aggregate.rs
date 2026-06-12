use crate::{IoPrecision, MetricSample};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct SampleAccumulator {
    pub reads: u64,
    pub writes: u64,
    pub errors: u64,
    pub logical_read_bytes: u64,
    pub logical_write_bytes: u64,
    pub latencies: hdrhistogram::Histogram<u64>,
}

impl Default for SampleAccumulator {
    fn default() -> Self {
        let mut latencies = hdrhistogram::Histogram::new_with_bounds(1, 10_000_000, 3)
            .expect("failed to create latency histogram");
        latencies.auto(true);
        Self {
            reads: 0,
            writes: 0,
            errors: 0,
            logical_read_bytes: 0,
            logical_write_bytes: 0,
            latencies,
        }
    }
}

impl SampleAccumulator {
    pub fn record(
        &mut self,
        is_write: bool,
        latency_micros: u64,
        logical_read_bytes: u64,
        logical_write_bytes: u64,
    ) {
        if is_write {
            self.writes += 1;
        } else {
            self.reads += 1;
        }
        self.logical_read_bytes += logical_read_bytes;
        self.logical_write_bytes += logical_write_bytes;
        let _ = self.latencies.record(latency_micros.max(1));
    }

    pub fn record_error(&mut self) {
        self.errors += 1;
    }

    pub fn snapshot_and_reset(&mut self) -> SampleSnapshot {
        let p50 = self.latencies.value_at_quantile(0.50);
        let p95 = self.latencies.value_at_quantile(0.95);

        let snapshot = SampleSnapshot {
            reads: self.reads,
            writes: self.writes,
            errors: self.errors,
            logical_read_bytes: self.logical_read_bytes,
            logical_write_bytes: self.logical_write_bytes,
            p50_latency_ms: micros_to_ms(p50),
            p95_latency_ms: micros_to_ms(p95),
        };

        self.reads = 0;
        self.writes = 0;
        self.errors = 0;
        self.logical_read_bytes = 0;
        self.logical_write_bytes = 0;
        self.latencies.reset();

        snapshot
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SampleSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub errors: u64,
    pub logical_read_bytes: u64,
    pub logical_write_bytes: u64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RunAggregate {
    total_sample_duration_ms: u64,
    weighted_writes: f64,
    weighted_reads: f64,
    peak_rss_bytes: u64,
    peak_total_rss_bytes: u64,
    peak_disk_usage_bytes: u64,
    io_precision: IoPrecision,
}

impl RunAggregate {
    pub fn update(&mut self, sample: &MetricSample) {
        let duration_secs = sample.sample_duration_ms as f64 / 1_000.0;
        self.total_sample_duration_ms += sample.sample_duration_ms;
        self.weighted_writes += sample.writes_per_sec * duration_secs;
        self.weighted_reads += sample.reads_per_sec * duration_secs;
        self.peak_rss_bytes = self.peak_rss_bytes.max(sample.rss_bytes);
        self.peak_total_rss_bytes = self.peak_total_rss_bytes.max(sample.total_rss_bytes);
        self.peak_disk_usage_bytes = self.peak_disk_usage_bytes.max(sample.disk_usage_bytes);
        if sample.io_precision == IoPrecision::Approximate {
            self.io_precision = IoPrecision::Approximate;
        }
    }

    pub fn avg_writes_per_sec(&self) -> f64 {
        if self.total_sample_duration_ms == 0 {
            0.0
        } else {
            self.weighted_writes / (self.total_sample_duration_ms as f64 / 1_000.0)
        }
    }

    pub fn avg_reads_per_sec(&self) -> f64 {
        if self.total_sample_duration_ms == 0 {
            0.0
        } else {
            self.weighted_reads / (self.total_sample_duration_ms as f64 / 1_000.0)
        }
    }

    pub fn peak_rss_bytes(&self) -> u64 {
        self.peak_rss_bytes
    }

    pub fn peak_total_rss_bytes(&self) -> u64 {
        self.peak_total_rss_bytes
    }

    pub fn peak_disk_usage_bytes(&self) -> u64 {
        self.peak_disk_usage_bytes
    }

    pub fn io_precision(&self) -> IoPrecision {
        self.io_precision
    }
}


fn micros_to_ms(value: u64) -> f64 {
    value as f64 / 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulator_resets_after_snapshot() {
        let mut accumulator = SampleAccumulator::default();
        accumulator.record(false, 1_000, 100, 0);
        accumulator.record(true, 2_000, 0, 200);
        accumulator.record_error();

        let snapshot = accumulator.snapshot_and_reset();

        assert_eq!(snapshot.reads, 1);
        assert_eq!(snapshot.writes, 1);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(accumulator.reads, 0);
        assert_eq!(accumulator.latencies.len(), 0);
    }
}
