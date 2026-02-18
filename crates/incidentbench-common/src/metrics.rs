// Copyright 2025 Mach5 Software, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::proto::worker::{LatencyDistribution, TDigestCentroid};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Pre-computed percentile summary for display and reporting.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PercentileSummary {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub count: u64,
}

/// Aggregated 1-second snapshot across all workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMetricPoint {
    /// Unix timestamp in seconds for this bucket.
    pub timestamp_s: u64,
    /// Current benchmark phase.
    pub phase: String,

    // Ingest metrics.
    pub ingest_events_produced: u64,
    pub ingest_events_acknowledged: u64,
    pub ingest_events_failed: u64,
    pub ingest_target_eps: u64,
    pub ingest_kafka_produce_latency: PercentileSummary,

    // Query metrics.
    pub query_executed: u64,
    pub query_errors: u64,
    pub query_target_qps: f64,
    pub query_latency: PercentileSummary,

    // Kafka consumer lag.
    pub kafka_consumer_lag: u64,

    // Worker counts.
    pub ingest_workers_reporting: u32,
    pub query_workers_reporting: u32,

    // Harness health.
    pub harness_saturated: bool,
    pub max_worker_cpu: f64,

    /// Per-query-group metrics (populated when query_groups are configured).
    /// Key is the group name.
    #[serde(default)]
    pub query_group_metrics: HashMap<String, QueryGroupMetrics>,
}

/// Per-query-group metrics within a 1-second aggregation bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryGroupMetrics {
    pub executed: u64,
    pub errors: u64,
    pub target_qps: f64,
    pub latency: PercentileSummary,
    /// Warehouse name this group is querying through.
    pub warehouse_name: String,
}

/// The full time-series output written to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeries {
    pub resolution_s: u32,
    pub points: Vec<AggregatedMetricPoint>,
}

/// Derived metrics computed after the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedMetrics {
    // Query derived
    pub query_baseline_p50: f64,
    pub query_baseline_p99: f64,
    pub query_overlap_p50: f64,
    pub query_overlap_p99: f64,
    pub query_p99_ratio: f64,
    pub query_error_rate_overlap: f64,
    pub query_throughput_ratio: f64,

    // Ingest derived
    pub ingest_baseline_eps: f64,
    pub ingest_peak_eps: f64,
    pub ingest_throughput_ratio: f64,
    pub ingest_peak_backlog: u64,
    pub ingest_backlog_drain_time_s: f64,

    // Isolation derived
    pub recovery_time_to_baseline_s: f64,
}

/// Primary scorecard — the GTM artifact.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Scorecard {
    pub baseline_p99_ms: f64,
    pub overlap_p99_ms: f64,
    pub p99_degradation_ratio: f64,
    pub query_error_rate_overlap: f64,
    pub peak_backlog: u64,
    pub backlog_drain_time_s: f64,
    pub recovery_time_s: f64,
}

impl Scorecard {
    pub fn from_derived(d: &DerivedMetrics) -> Self {
        Self {
            baseline_p99_ms: d.query_baseline_p99,
            overlap_p99_ms: d.query_overlap_p99,
            p99_degradation_ratio: d.query_p99_ratio,
            query_error_rate_overlap: d.query_error_rate_overlap,
            peak_backlog: d.ingest_peak_backlog,
            backlog_drain_time_s: d.ingest_backlog_drain_time_s,
            recovery_time_s: d.recovery_time_to_baseline_s,
        }
    }
}

/// Compute derived metrics from the raw time-series.
pub fn compute_derived(timeseries: &TimeSeries) -> DerivedMetrics {
    let baseline_points: Vec<_> = timeseries
        .points
        .iter()
        .filter(|p| p.phase == "baseline")
        .collect();
    let overlap_points: Vec<_> = timeseries
        .points
        .iter()
        .filter(|p| p.phase == "overlap")
        .collect();
    let surge_overlap_points: Vec<_> = timeseries
        .points
        .iter()
        .filter(|p| p.phase == "ingestion_surge" || p.phase == "overlap")
        .collect();
    let recovery_points: Vec<_> = timeseries
        .points
        .iter()
        .filter(|p| p.phase == "recovery" || p.phase == "post_incident")
        .collect();

    // Query baseline metrics
    let query_baseline_p50 = mean_of(baseline_points.iter().map(|p| p.query_latency.p50));
    let query_baseline_p99 = mean_of(baseline_points.iter().map(|p| p.query_latency.p99));

    // Query overlap metrics
    let query_overlap_p50 = mean_of(overlap_points.iter().map(|p| p.query_latency.p50));
    let query_overlap_p99 = mean_of(overlap_points.iter().map(|p| p.query_latency.p99));

    let query_p99_ratio = if query_baseline_p99 > 0.0 {
        query_overlap_p99 / query_baseline_p99
    } else {
        0.0
    };

    // Query error rate during overlap
    let overlap_total_queries: u64 = overlap_points.iter().map(|p| p.query_executed).sum();
    let overlap_total_errors: u64 = overlap_points.iter().map(|p| p.query_errors).sum();
    let query_error_rate_overlap = if overlap_total_queries > 0 {
        overlap_total_errors as f64 / overlap_total_queries as f64
    } else {
        0.0
    };

    // Query throughput ratio during overlap
    let overlap_achieved_qps = if !overlap_points.is_empty() {
        overlap_total_queries as f64 / overlap_points.len() as f64
    } else {
        0.0
    };
    let overlap_target_qps = overlap_points
        .first()
        .map(|p| p.query_target_qps)
        .unwrap_or(0.0);
    let query_throughput_ratio = if overlap_target_qps > 0.0 {
        overlap_achieved_qps / overlap_target_qps
    } else {
        0.0
    };

    // Ingest baseline EPS
    let ingest_baseline_eps = mean_of(
        baseline_points
            .iter()
            .map(|p| p.ingest_events_produced as f64),
    );

    // Ingest peak EPS
    let ingest_peak_eps = surge_overlap_points
        .iter()
        .map(|p| p.ingest_events_produced as f64)
        .fold(0.0f64, f64::max);

    // Ingest throughput ratio
    let overlap_achieved_eps = if !overlap_points.is_empty() {
        overlap_points
            .iter()
            .map(|p| p.ingest_events_produced)
            .sum::<u64>() as f64
            / overlap_points.len() as f64
    } else {
        0.0
    };
    let overlap_target_eps = overlap_points
        .first()
        .map(|p| p.ingest_target_eps as f64)
        .unwrap_or(0.0);
    let ingest_throughput_ratio = if overlap_target_eps > 0.0 {
        overlap_achieved_eps / overlap_target_eps
    } else {
        0.0
    };

    // Peak backlog (Kafka consumer lag)
    let ingest_peak_backlog = timeseries
        .points
        .iter()
        .map(|p| p.kafka_consumer_lag)
        .max()
        .unwrap_or(0);

    // Backlog drain time: seconds from peak lag to lag returning to baseline level
    let baseline_lag = mean_of(baseline_points.iter().map(|p| p.kafka_consumer_lag as f64)) as u64;
    let peak_lag_time = timeseries
        .points
        .iter()
        .filter(|p| p.kafka_consumer_lag == ingest_peak_backlog)
        .map(|p| p.timestamp_s)
        .next()
        .unwrap_or(0);
    let drain_time = timeseries
        .points
        .iter()
        .filter(|p| p.timestamp_s > peak_lag_time && p.kafka_consumer_lag <= baseline_lag.max(100))
        .map(|p| p.timestamp_s)
        .next()
        .unwrap_or(peak_lag_time);
    let ingest_backlog_drain_time_s = (drain_time - peak_lag_time) as f64;

    // Recovery time: time from recovery phase start until p99 returns to 1.2x baseline
    let recovery_start = recovery_points.first().map(|p| p.timestamp_s).unwrap_or(0);
    let recovery_threshold = query_baseline_p99 * 1.2;
    let recovery_end = recovery_points
        .iter()
        .filter(|p| p.query_latency.p99 <= recovery_threshold && p.query_latency.p99 > 0.0)
        .map(|p| p.timestamp_s)
        .next()
        .unwrap_or(recovery_start);
    let recovery_time_to_baseline_s = (recovery_end - recovery_start) as f64;

    DerivedMetrics {
        query_baseline_p50,
        query_baseline_p99,
        query_overlap_p50,
        query_overlap_p99,
        query_p99_ratio,
        query_error_rate_overlap,
        query_throughput_ratio,
        ingest_baseline_eps,
        ingest_peak_eps,
        ingest_throughput_ratio,
        ingest_peak_backlog,
        ingest_backlog_drain_time_s,
        recovery_time_to_baseline_s,
    }
}

fn mean_of(iter: impl Iterator<Item = f64>) -> f64 {
    let mut count = 0u64;
    let mut sum = 0.0;
    for v in iter {
        sum += v;
        count += 1;
    }
    if count > 0 {
        sum / count as f64
    } else {
        0.0
    }
}

/// Merge multiple t-digest centroid lists into a single PercentileSummary.
/// This uses a simplified merge — for production accuracy, use the tdigest crate.
pub fn merge_latency_distributions(dists: &[LatencyDistribution]) -> PercentileSummary {
    if dists.is_empty() {
        return PercentileSummary::default();
    }

    // Collect all centroids and sort by mean.
    let mut all_centroids: Vec<(f64, i64)> = dists
        .iter()
        .flat_map(|d| d.centroids.iter().map(|c| (c.mean, c.count)))
        .collect();
    all_centroids.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let total_count: i64 = all_centroids.iter().map(|(_, c)| c).sum();
    if total_count == 0 {
        return PercentileSummary::default();
    }

    let global_max = dists.iter().map(|d| d.max).fold(f64::MIN, f64::max);

    let p50 = percentile_from_centroids(&all_centroids, total_count, 0.50);
    let p95 = percentile_from_centroids(&all_centroids, total_count, 0.95);
    let p99 = percentile_from_centroids(&all_centroids, total_count, 0.99);

    PercentileSummary {
        p50,
        p95,
        p99,
        max: global_max,
        count: total_count as u64,
    }
}

fn percentile_from_centroids(centroids: &[(f64, i64)], total: i64, quantile: f64) -> f64 {
    let target = (total as f64 * quantile) as i64;
    let mut cumulative = 0i64;
    for (mean, count) in centroids {
        cumulative += count;
        if cumulative >= target {
            return *mean;
        }
    }
    centroids.last().map(|(m, _)| *m).unwrap_or(0.0)
}

/// Create a LatencyDistribution from a set of observed latency values.
/// Uses a simple centroid compression for t-digest-like behavior.
pub fn latency_distribution_from_values(values: &[f64]) -> LatencyDistribution {
    if values.is_empty() {
        return LatencyDistribution {
            centroids: vec![],
            count: 0,
            min: 0.0,
            max: 0.0,
        };
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let min = sorted[0];
    let max = sorted[sorted.len() - 1];

    // Simple compression: group into ~100 centroids.
    let bucket_size = (sorted.len() / 100).max(1);
    let mut centroids = Vec::new();
    for chunk in sorted.chunks(bucket_size) {
        let mean = chunk.iter().sum::<f64>() / chunk.len() as f64;
        centroids.push(TDigestCentroid {
            mean,
            count: chunk.len() as i64,
        });
    }

    LatencyDistribution {
        centroids,
        count: values.len() as i64,
        min,
        max,
    }
}
