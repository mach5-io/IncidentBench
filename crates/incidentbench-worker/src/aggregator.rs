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

use incidentbench_common::metrics::{
    compute_derived, merge_latency_distributions, AggregatedMetricPoint, PerQueryTimeSeries,
    PerQueryTimeSeriesPoint, QueryGroupMetrics, Scorecard, TimeSeries,
};
use incidentbench_common::proto::aggregator::QueryLatencySummary;
use incidentbench_common::proto::aggregator::{
    metrics_service_server::{MetricsService, MetricsServiceServer},
    AggregatedSnapshot, AggregationStatusRequest, AggregationStatusResponse,
    GetLatestSnapshotRequest, GetResultsRequest, GetResultsResponse, ReportMetricsResponse,
    StreamMetricsRequest,
};
use incidentbench_common::proto::worker::{
    QueryErrorRecord, TimedOutQueryRecord, WorkerMetricSnapshot,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::TopicPartitionList;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, warn};

const MAX_QUERY_ERROR_RECORDS: usize = 1000;

/// Configuration for a single data stream's lag polling.
#[derive(Debug, Clone, Deserialize)]
pub struct AggregatorStreamConfig {
    pub name: String,
    pub kafka_topic: String,
    pub consumer_group: String,
}

#[derive(Debug, Deserialize)]
pub struct AggregatorConfig {
    /// Empty string or absent in query-only mode (no Kafka lag polling).
    #[serde(default)]
    pub kafka_bootstrap_servers: String,
    /// Multi-stream lag polling configuration.
    /// Empty in query-only mode.
    #[serde(default)]
    pub streams: Vec<AggregatorStreamConfig>,
    #[allow(dead_code)]
    pub results_path: String,
}

/// Per-second bucket of worker snapshots, keyed by second timestamp.
struct SecondBucket {
    ingest_snapshots: Vec<WorkerMetricSnapshot>,
    query_snapshots: Vec<WorkerMetricSnapshot>,
}

struct AggregatorState {
    /// Snapshots collected per 1-second bucket (keyed by timestamp in seconds).
    buckets: HashMap<u64, SecondBucket>,
    /// Aggregated timeline (ordered).
    timeline: Vec<AggregatedMetricPoint>,
    /// Latest total Kafka consumer lag (sum across all streams).
    kafka_consumer_lag: u64,
    /// Per-stream Kafka consumer lag.
    per_stream_lag: HashMap<String, u64>,
    /// Number of connected workers.
    connected_workers: u32,
    /// Whether any worker has ever connected.
    had_workers: bool,
    /// Total snapshots received.
    total_snapshots: u64,
    /// Aggregation state.
    state: AggState,
    /// Accumulated timed-out query records from all session workers.
    timed_out_queries: Vec<TimedOutQueryRecord>,
    /// Sampled query error records from all query workers.
    query_error_records: Vec<QueryErrorRecord>,
    /// Per-query latency accumulator: query_name → (latency_values, error_count, timeout_count).
    per_query_latency: HashMap<String, PerQueryAccumulator>,
    /// Per-query latency timeline derived from 1-second buckets.
    per_query_timeseries: HashMap<String, Vec<PerQueryTimeSeriesPoint>>,
    /// Directory to write results files (metrics.json, timed_out_queries.json).
    results_path: String,
}

/// Running accumulator for a single query's latency across the full run.
struct PerQueryAccumulator {
    latencies: Vec<f64>,
    error_count: u64,
    timeout_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AggState {
    Collecting,
    Complete,
}

pub struct MetricsAggregatorService {
    state: Arc<Mutex<AggregatorState>>,
    /// Broadcast channel for live metrics subscribers.
    live_tx: broadcast::Sender<AggregatedSnapshot>,
}

impl MetricsAggregatorService {
    fn new(results_path: String) -> Self {
        let (live_tx, _) = broadcast::channel(128);
        Self {
            state: Arc::new(Mutex::new(AggregatorState {
                buckets: HashMap::new(),
                timeline: Vec::new(),
                kafka_consumer_lag: 0,
                per_stream_lag: HashMap::new(),
                connected_workers: 0,
                had_workers: false,
                total_snapshots: 0,
                state: AggState::Collecting,
                timed_out_queries: Vec::new(),
                query_error_records: Vec::new(),
                per_query_latency: HashMap::new(),
                per_query_timeseries: HashMap::new(),
                results_path,
            })),
            live_tx,
        }
    }
}

fn complete_aggregation(s: &mut AggregatorState, reason: &str) {
    if s.state == AggState::Complete {
        return;
    }

    info!(
        reason,
        connected_workers = s.connected_workers,
        total_snapshots = s.total_snapshots,
        seconds_aggregated = s.timeline.len(),
        "Transitioning aggregation to Complete"
    );
    s.state = AggState::Complete;

    if !s.results_path.is_empty() {
        let path = s.results_path.clone();
        let timeouts = s.timed_out_queries.clone();
        let query_errors = s.query_error_records.clone();
        let per_query = s
            .per_query_latency
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    (v.latencies.clone(), v.error_count, v.timeout_count),
                )
            })
            .collect::<Vec<_>>();
        let per_query_timeseries = s
            .per_query_timeseries
            .iter()
            .map(|(query_name, points)| PerQueryTimeSeries {
                query_name: query_name.clone(),
                points: {
                    let mut points = points.clone();
                    points.sort_by_key(|p| p.timestamp_s);
                    points
                },
            })
            .collect::<Vec<_>>();
        let mut timeline = s.timeline.clone();
        timeline.sort_by_key(|p| p.timestamp_s);
        tokio::spawn(async move {
            persist_results(
                &path,
                &timeline,
                &timeouts,
                &query_errors,
                &per_query,
                &per_query_timeseries,
            )
            .await;
        });
    }
}

#[tonic::async_trait]
impl MetricsService for MetricsAggregatorService {
    async fn report_metrics(
        &self,
        request: Request<Streaming<WorkerMetricSnapshot>>,
    ) -> Result<Response<ReportMetricsResponse>, Status> {
        let mut stream = request.into_inner();
        let state = self.state.clone();
        let live_tx = self.live_tx.clone();

        {
            let mut s = state.lock().await;
            s.connected_workers += 1;
            s.had_workers = true;
        }

        while let Ok(Some(snapshot)) = stream.message().await {
            let timestamp_s = (snapshot.timestamp_ns / 1_000_000_000) as u64;

            let mut s = state.lock().await;
            s.total_snapshots += 1;

            // Extract timeout records and per-query latency before snapshot moves into bucket.
            let timeout_records: Vec<_> = snapshot.timed_out_queries.clone();
            let query_error_records: Vec<_> = snapshot.query_error_records.clone();

            // Accumulate per-query latency from query_latency_by_type.
            for qt in &snapshot.query_latency_by_type {
                if let Some(dist) = &qt.latency {
                    let acc = s
                        .per_query_latency
                        .entry(qt.query_name.clone())
                        .or_insert_with(|| PerQueryAccumulator {
                            latencies: Vec::new(),
                            error_count: 0,
                            timeout_count: 0,
                        });
                    // Expand centroids into approximate individual latency values for percentile calc.
                    for centroid in &dist.centroids {
                        for _ in 0..centroid.count.max(1) {
                            acc.latencies.push(centroid.mean);
                        }
                    }
                    acc.error_count += qt.errors as u64;
                }
            }
            // Count timeouts against the individual query that timed out.
            for r in &timeout_records {
                s.per_query_latency
                    .entry(r.query_name.clone())
                    .or_insert_with(|| PerQueryAccumulator {
                        latencies: Vec::new(),
                        error_count: 0,
                        timeout_count: 0,
                    })
                    .timeout_count += 1;
            }

            {
                let bucket = s
                    .buckets
                    .entry(timestamp_s)
                    .or_insert_with(|| SecondBucket {
                        ingest_snapshots: Vec::new(),
                        query_snapshots: Vec::new(),
                    });

                match snapshot.worker_mode.as_str() {
                    "ingest" => bucket.ingest_snapshots.push(snapshot),
                    // Both rate-controlled and session query workers contribute to query metrics.
                    "query" | "query-session" => bucket.query_snapshots.push(snapshot),
                    _ => {}
                }
            } // bucket borrow released here

            if !timeout_records.is_empty() {
                s.timed_out_queries.extend(timeout_records);
            }
            if !query_error_records.is_empty()
                && s.query_error_records.len() < MAX_QUERY_ERROR_RECORDS
            {
                let remaining = MAX_QUERY_ERROR_RECORDS - s.query_error_records.len();
                s.query_error_records
                    .extend(query_error_records.into_iter().take(remaining));
            }

            // Try to aggregate this bucket if it has enough data.
            // (In practice, we'd wait for the watermark. Simplified here.)
            if let Some(agg) = try_aggregate_bucket(&s.buckets, timestamp_s, s.kafka_consumer_lag) {
                let per_query_points = build_per_query_timeseries_points(&s.buckets, timestamp_s);
                for (query_name, point) in per_query_points {
                    let series = s.per_query_timeseries.entry(query_name).or_default();
                    if let Some(existing) = series
                        .iter_mut()
                        .find(|p| p.timestamp_s == point.timestamp_s)
                    {
                        *existing = point;
                    } else {
                        series.push(point);
                    }
                }
                if let Some(existing) = s
                    .timeline
                    .iter_mut()
                    .find(|point| point.timestamp_s == agg.timestamp_s)
                {
                    *existing = agg.clone();
                } else {
                    s.timeline.push(agg.clone());
                }

                // Broadcast to live subscribers.
                let proto_snapshot = metric_point_to_proto(&agg);
                let _ = live_tx.send(proto_snapshot);
            }
        }

        {
            let mut s = state.lock().await;
            s.connected_workers = s.connected_workers.saturating_sub(1);
            if s.connected_workers == 0 && s.had_workers {
                complete_aggregation(&mut s, "all workers disconnected");
            }
        }

        Ok(Response::new(ReportMetricsResponse {}))
    }

    type StreamMetricsStream =
        tokio_stream::wrappers::ReceiverStream<Result<AggregatedSnapshot, Status>>;

    async fn stream_metrics(
        &self,
        request: Request<StreamMetricsRequest>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let mut live_rx = self.live_tx.subscribe();
        let state = self.state.clone();

        tokio::spawn(async move {
            // If requested, send historical data first.
            if req.include_history {
                let s = state.lock().await;
                for point in &s.timeline {
                    let proto = metric_point_to_proto(point);
                    if tx.send(Ok(proto)).await.is_err() {
                        return;
                    }
                }
            }

            // Then stream live.
            while let Ok(snapshot) = live_rx.recv().await {
                if tx.send(Ok(snapshot)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn get_aggregation_status(
        &self,
        _request: Request<AggregationStatusRequest>,
    ) -> Result<Response<AggregationStatusResponse>, Status> {
        let s = self.state.lock().await;
        Ok(Response::new(AggregationStatusResponse {
            state: match s.state {
                AggState::Collecting => "collecting",
                AggState::Complete => "complete",
            }
            .to_string(),
            snapshots_received: s.total_snapshots as i64,
            seconds_aggregated: s.timeline.len() as i64,
            connected_workers: s.connected_workers as i32,
        }))
    }

    async fn get_latest_snapshot(
        &self,
        _request: Request<GetLatestSnapshotRequest>,
    ) -> Result<Response<AggregatedSnapshot>, Status> {
        let s = self.state.lock().await;
        if let Some(point) = s.timeline.last() {
            Ok(Response::new(metric_point_to_proto(point)))
        } else {
            // No data yet — return an empty snapshot.
            Ok(Response::new(AggregatedSnapshot::default()))
        }
    }

    async fn get_results(
        &self,
        _request: Request<GetResultsRequest>,
    ) -> Result<Response<GetResultsResponse>, Status> {
        let mut s = self.state.lock().await;

        if s.state != AggState::Complete {
            if s.had_workers && s.total_snapshots > 0 {
                warn!(
                    connected_workers = s.connected_workers,
                    total_snapshots = s.total_snapshots,
                    seconds_aggregated = s.timeline.len(),
                    "GetResults requested before all worker streams closed; force-completing aggregation with collected snapshots"
                );
                complete_aggregation(&mut s, "forced by GetResults");
            } else {
                return Err(Status::failed_precondition(
                    "Aggregation not yet complete; call GetAggregationStatus to check state",
                ));
            }
        }

        let timeseries = TimeSeries {
            resolution_s: 1,
            points: s.timeline.clone(),
        };
        let derived = compute_derived(&timeseries);
        let scorecard = Scorecard::from_derived(&derived);

        // Check harness saturation across all points.
        let harness_saturated = s.timeline.iter().any(|p| p.harness_saturated);

        // Validity checks.
        let mut violations = Vec::new();
        if s.timeline.len() < 10 {
            violations.push(format!(
                "Timeline too short: {} seconds (minimum 10)",
                s.timeline.len()
            ));
        }
        let has_baseline = s.timeline.iter().any(|p| p.phase == "baseline");
        let has_overlap = s.timeline.iter().any(|p| p.phase == "overlap");
        if !has_baseline {
            violations.push("Missing 'baseline' phase data".to_string());
        }
        if !has_overlap {
            violations.push("Missing 'overlap' phase data".to_string());
        }

        let mut warnings = Vec::new();
        if harness_saturated {
            warnings.push("Harness CPU saturation detected (>90%) — results may undercount achieved throughput".to_string());
        }

        let valid = violations.is_empty();

        let per_query_latency = compute_per_query_summaries(&s.per_query_latency);

        Ok(Response::new(GetResultsResponse {
            valid,
            validity_violations: violations,
            warnings,
            harness_saturated,
            baseline_p99_ms: scorecard.baseline_p99_ms,
            overlap_p99_ms: scorecard.overlap_p99_ms,
            p99_degradation_ratio: scorecard.p99_degradation_ratio,
            query_error_rate_overlap: scorecard.query_error_rate_overlap,
            peak_backlog: scorecard.peak_backlog,
            backlog_drain_time_s: scorecard.backlog_drain_time_s,
            recovery_time_s: scorecard.recovery_time_s,
            timed_out_queries: s.timed_out_queries.clone(),
            per_query_latency,
        }))
    }
}

/// Try to aggregate a 1-second bucket.
fn try_aggregate_bucket(
    buckets: &HashMap<u64, SecondBucket>,
    timestamp_s: u64,
    kafka_lag: u64,
) -> Option<AggregatedMetricPoint> {
    let bucket = buckets.get(&timestamp_s)?;

    // Get phase from first snapshot.
    let phase = bucket
        .ingest_snapshots
        .first()
        .or(bucket.query_snapshots.first())
        .map(|s| s.phase.clone())
        .unwrap_or_default();

    // Aggregate ingest metrics.
    let ingest_events_produced: u64 = bucket
        .ingest_snapshots
        .iter()
        .map(|s| s.events_produced as u64)
        .sum();
    let ingest_events_acked: u64 = bucket
        .ingest_snapshots
        .iter()
        .map(|s| s.events_acknowledged as u64)
        .sum();
    let ingest_events_failed: u64 = bucket
        .ingest_snapshots
        .iter()
        .map(|s| s.events_failed as u64)
        .sum();
    let ingest_target_eps: u64 = bucket
        .ingest_snapshots
        .iter()
        .map(|s| s.target_rate as u64)
        .sum();

    // Merge ingest produce latencies.
    let ingest_latency_dists: Vec<_> = bucket
        .ingest_snapshots
        .iter()
        .filter_map(|s| s.kafka_produce_latency.as_ref())
        .cloned()
        .collect();
    let ingest_produce_latency = merge_latency_distributions(&ingest_latency_dists);

    // Aggregate query metrics.
    let query_executed: u64 = bucket
        .query_snapshots
        .iter()
        .map(|s| s.queries_executed as u64)
        .sum();
    let query_errors: u64 = bucket
        .query_snapshots
        .iter()
        .map(|s| s.query_errors as u64)
        .sum();
    let query_target_mqps: u64 = bucket
        .query_snapshots
        .iter()
        .map(|s| s.target_rate as u64)
        .sum();
    let concurrent_sessions: u32 = bucket
        .query_snapshots
        .iter()
        .map(|s| s.concurrent_sessions.max(0) as u32)
        .sum();

    // Merge query latencies.
    let query_latency_dists: Vec<_> = bucket
        .query_snapshots
        .iter()
        .filter_map(|s| s.query_latency.as_ref())
        .cloned()
        .collect();
    let query_latency = merge_latency_distributions(&query_latency_dists);

    // CPU saturation check.
    let max_cpu = bucket
        .ingest_snapshots
        .iter()
        .chain(bucket.query_snapshots.iter())
        .map(|s| s.cpu_utilization)
        .fold(0.0f64, f64::max);

    // Compute per-query-group metrics.
    let mut query_group_metrics = HashMap::new();
    let mut group_snapshots: HashMap<String, Vec<&WorkerMetricSnapshot>> = HashMap::new();
    for snap in &bucket.query_snapshots {
        if !snap.query_group.is_empty() {
            group_snapshots
                .entry(snap.query_group.clone())
                .or_default()
                .push(snap);
        }
    }
    for (group_name, snaps) in &group_snapshots {
        let grp_executed: u64 = snaps.iter().map(|s| s.queries_executed as u64).sum();
        let grp_errors: u64 = snaps.iter().map(|s| s.query_errors as u64).sum();
        let grp_target_mqps: u64 = snaps.iter().map(|s| s.target_rate as u64).sum();
        let grp_latency_dists: Vec<_> = snaps
            .iter()
            .filter_map(|s| s.query_latency.as_ref())
            .cloned()
            .collect();
        let grp_latency = merge_latency_distributions(&grp_latency_dists);
        query_group_metrics.insert(
            group_name.clone(),
            QueryGroupMetrics {
                executed: grp_executed,
                errors: grp_errors,
                target_qps: grp_target_mqps as f64 / 1000.0,
                latency: grp_latency,
                warehouse_name: String::new(), // Populated from config by reporter
            },
        );
    }

    Some(AggregatedMetricPoint {
        timestamp_s,
        phase,
        ingest_events_produced,
        ingest_events_acknowledged: ingest_events_acked,
        ingest_events_failed,
        ingest_target_eps,
        ingest_kafka_produce_latency: ingest_produce_latency,
        query_executed,
        query_errors,
        query_target_qps: query_target_mqps as f64 / 1000.0,
        query_latency,
        kafka_consumer_lag: kafka_lag,
        ingest_workers_reporting: bucket.ingest_snapshots.len() as u32,
        query_workers_reporting: bucket.query_snapshots.len() as u32,
        concurrent_sessions,
        harness_saturated: max_cpu > 0.9,
        max_worker_cpu: max_cpu,
        query_group_metrics,
    })
}

fn metric_point_to_proto(point: &AggregatedMetricPoint) -> AggregatedSnapshot {
    use incidentbench_common::proto::aggregator::PercentileSummary as ProtoPercentile;

    AggregatedSnapshot {
        timestamp_ns: (point.timestamp_s * 1_000_000_000) as i64,
        phase: point.phase.clone(),
        ingest_events_produced: point.ingest_events_produced as i64,
        ingest_events_acknowledged: point.ingest_events_acknowledged as i64,
        ingest_events_failed: point.ingest_events_failed as i64,
        ingest_target_eps: point.ingest_target_eps as i64,
        ingest_kafka_produce_latency: Some(ProtoPercentile {
            p50: point.ingest_kafka_produce_latency.p50,
            p95: point.ingest_kafka_produce_latency.p95,
            p99: point.ingest_kafka_produce_latency.p99,
            max: point.ingest_kafka_produce_latency.max,
            count: point.ingest_kafka_produce_latency.count as i64,
        }),
        query_executed: point.query_executed as i64,
        query_errors: point.query_errors as i64,
        query_target_qps: point.query_target_qps,
        query_latency: Some(ProtoPercentile {
            p50: point.query_latency.p50,
            p95: point.query_latency.p95,
            p99: point.query_latency.p99,
            max: point.query_latency.max,
            count: point.query_latency.count as i64,
        }),
        kafka_consumer_lag: point.kafka_consumer_lag as i64,
        ingest_workers_reporting: point.ingest_workers_reporting as i32,
        query_workers_reporting: point.query_workers_reporting as i32,
        concurrent_sessions: point.concurrent_sessions as i32,
        harness_saturated: point.harness_saturated,
        max_worker_cpu: point.max_worker_cpu,
    }
}

/// Run the MetricsAggregator gRPC server.
pub async fn run(config_path: &str, listen_addr: &str) -> anyhow::Result<()> {
    let config_str = tokio::fs::read_to_string(config_path).await?;
    let config: AggregatorConfig = serde_yaml::from_str(&config_str)?;

    info!(streams = config.streams.len(), "MetricsAggregator starting");

    let service = MetricsAggregatorService::new(config.results_path.clone());
    let state = service.state.clone();

    // Spawn one lag polling task per stream.
    let kafka_servers = config.kafka_bootstrap_servers.clone();
    for stream_cfg in &config.streams {
        let servers = kafka_servers.clone();
        let stream_name = stream_cfg.name.clone();
        let consumer_group = stream_cfg.consumer_group.clone();
        let kafka_topic = stream_cfg.kafka_topic.clone();
        let lag_state = state.clone();

        info!(
            stream = %stream_name,
            topic = %kafka_topic,
            consumer_group = %consumer_group,
            "Starting lag polling for stream"
        );

        tokio::spawn(async move {
            poll_kafka_consumer_lag_stream(
                &servers,
                &stream_name,
                &consumer_group,
                &kafka_topic,
                lag_state,
            )
            .await;
        });
    }

    let addr = listen_addr.parse()?;
    info!("MetricsAggregator listening on {}", listen_addr);

    Server::builder()
        .add_service(MetricsServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use incidentbench_common::metrics::PercentileSummary;

    #[tokio::test]
    async fn get_results_force_completes_with_collected_snapshots() {
        let service = MetricsAggregatorService::new(String::new());

        {
            let mut state = service.state.lock().await;
            state.had_workers = true;
            state.connected_workers = 1;
            state.total_snapshots = 1;
            state.timeline.push(AggregatedMetricPoint {
                timestamp_s: 1,
                phase: "baseline".to_string(),
                ingest_events_produced: 0,
                ingest_events_acknowledged: 0,
                ingest_events_failed: 0,
                ingest_target_eps: 0,
                ingest_kafka_produce_latency: PercentileSummary::default(),
                query_executed: 1,
                query_errors: 0,
                query_target_qps: 1.0,
                query_latency: PercentileSummary::default(),
                kafka_consumer_lag: 0,
                ingest_workers_reporting: 0,
                query_workers_reporting: 1,
                concurrent_sessions: 0,
                harness_saturated: false,
                max_worker_cpu: 0.0,
                query_group_metrics: HashMap::new(),
            });
        }

        service
            .get_results(Request::new(GetResultsRequest {}))
            .await
            .expect("GetResults should force-complete when snapshots exist");

        let state = service.state.lock().await;
        assert_eq!(state.state, AggState::Complete);
        assert_eq!(state.connected_workers, 1);
    }
}

/// Compute percentile summaries from the per-query accumulator map.
fn compute_per_query_summaries(
    acc: &HashMap<String, PerQueryAccumulator>,
) -> Vec<QueryLatencySummary> {
    acc.iter()
        .map(|(name, a)| {
            let mut sorted = a.latencies.clone();
            sorted.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
            let n = sorted.len();
            let percentile = |p: f64| -> f64 {
                if n == 0 {
                    return 0.0;
                }
                let idx = ((p / 100.0) * (n as f64 - 1.0)).round() as usize;
                sorted[idx.min(n - 1)]
            };
            QueryLatencySummary {
                query_name: name.clone(),
                p50: percentile(50.0),
                p95: percentile(95.0),
                p99: percentile(99.0),
                max: sorted.last().copied().unwrap_or(0.0),
                count: n as i64,
                error_count: a.error_count.saturating_sub(a.timeout_count) as i64,
                timeout_count: a.timeout_count as i64,
            }
        })
        .collect()
}

fn build_per_query_timeseries_points(
    buckets: &HashMap<u64, SecondBucket>,
    timestamp_s: u64,
) -> Vec<(String, PerQueryTimeSeriesPoint)> {
    let Some(bucket) = buckets.get(&timestamp_s) else {
        return Vec::new();
    };

    let phase = bucket
        .query_snapshots
        .first()
        .map(|s| s.phase.clone())
        .or_else(|| bucket.ingest_snapshots.first().map(|s| s.phase.clone()))
        .unwrap_or_default();

    let mut per_query_dists: HashMap<
        String,
        Vec<incidentbench_common::proto::worker::LatencyDistribution>,
    > = HashMap::new();
    let mut per_query_errors: HashMap<String, u64> = HashMap::new();

    for snap in &bucket.query_snapshots {
        for qt in &snap.query_latency_by_type {
            if let Some(dist) = &qt.latency {
                per_query_dists
                    .entry(qt.query_name.clone())
                    .or_default()
                    .push(dist.clone());
            }
            *per_query_errors.entry(qt.query_name.clone()).or_default() += qt.errors as u64;
        }
    }

    let mut out = Vec::new();
    for (query_name, dists) in per_query_dists {
        let merged = merge_latency_distributions(&dists);
        let error_count = per_query_errors.get(&query_name).copied().unwrap_or(0);
        out.push((
            query_name,
            PerQueryTimeSeriesPoint {
                timestamp_s,
                phase: phase.clone(),
                p95_ms: merged.p95,
                count: merged.count,
                error_count,
            },
        ));
    }

    out
}

/// Write metrics.json, timed_out_queries.json, and per_query_latency.json on run completion.
async fn persist_results(
    results_path: &str,
    timeline: &[AggregatedMetricPoint],
    timed_out_queries: &[TimedOutQueryRecord],
    query_error_records: &[QueryErrorRecord],
    per_query: &[(String, (Vec<f64>, u64, u64))],
    per_query_timeseries: &[PerQueryTimeSeries],
) {
    if let Err(e) = tokio::fs::create_dir_all(results_path).await {
        error!(results_path, "Failed to create results directory: {}", e);
        return;
    }

    let timeseries = TimeSeries {
        resolution_s: 1,
        points: timeline.iter().map(|p| p.clone()).collect(),
    };
    match serde_json::to_string_pretty(&timeseries) {
        Ok(json) => {
            let path = format!("{}/metrics.json", results_path);
            if let Err(e) = tokio::fs::write(&path, json).await {
                error!(path, "Failed to write metrics.json: {}", e);
            } else {
                info!(path, "metrics.json written");
            }
        }
        Err(e) => error!("Failed to serialize timeseries: {}", e),
    }

    // Serialize timed_out_queries as plain JSON (not proto) for the reporter.
    #[derive(serde::Serialize)]
    struct TimeoutRecord<'a> {
        query_name: &'a str,
        category: &'a str,
        phase: &'a str,
        timestamp_ms: i64,
        duration_ms: f64,
        timeout_ms: i64,
    }
    let records: Vec<TimeoutRecord<'_>> = timed_out_queries
        .iter()
        .map(|r| TimeoutRecord {
            query_name: &r.query_name,
            category: &r.category,
            phase: &r.phase,
            timestamp_ms: r.timestamp_ms,
            duration_ms: r.duration_ms,
            timeout_ms: r.timeout_ms,
        })
        .collect();
    match serde_json::to_string_pretty(&records) {
        Ok(json) => {
            let path = format!("{}/timed_out_queries.json", results_path);
            if let Err(e) = tokio::fs::write(&path, json).await {
                error!(path, "Failed to write timed_out_queries.json: {}", e);
            } else {
                info!(
                    path,
                    count = records.len(),
                    "timed_out_queries.json written"
                );
            }
        }
        Err(e) => error!("Failed to serialize timed_out_queries: {}", e),
    }

    // Serialize sampled query error details as plain JSON for diagnostics.
    #[derive(serde::Serialize)]
    struct QueryErrorSample<'a> {
        query_name: &'a str,
        category: &'a str,
        phase: &'a str,
        worker_id: &'a str,
        worker_index: i32,
        timestamp_ms: i64,
        duration_ms: f64,
        timed_out: bool,
        error_class: &'a str,
        message: &'a str,
    }
    let error_samples: Vec<QueryErrorSample<'_>> = query_error_records
        .iter()
        .map(|r| QueryErrorSample {
            query_name: &r.query_name,
            category: &r.category,
            phase: &r.phase,
            worker_id: &r.worker_id,
            worker_index: r.worker_index,
            timestamp_ms: r.timestamp_ms,
            duration_ms: r.duration_ms,
            timed_out: r.timed_out,
            error_class: &r.error_class,
            message: &r.message,
        })
        .collect();
    match serde_json::to_string_pretty(&error_samples) {
        Ok(json) => {
            let path = format!("{}/query_error_samples.json", results_path);
            if let Err(e) = tokio::fs::write(&path, json).await {
                error!(path, "Failed to write query_error_samples.json: {}", e);
            } else {
                info!(
                    path,
                    count = error_samples.len(),
                    "query_error_samples.json written"
                );
            }
        }
        Err(e) => error!("Failed to serialize query_error_samples: {}", e),
    }

    // Serialize per-query latency summary for the reporter.
    #[derive(serde::Serialize)]
    struct PerQueryRecord {
        query_name: String,
        min: f64,
        p50: f64,
        p95: f64,
        p99: f64,
        max: f64,
        count: u64,
        error_count: u64,
        timeout_count: u64,
    }
    fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
    let mut pq_records: Vec<PerQueryRecord> = per_query
        .iter()
        .map(|(name, (latencies, error_count, timeout_count))| {
            let mut sorted = latencies.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let min = sorted.first().copied().unwrap_or(0.0);
            let max = sorted.last().copied().unwrap_or(0.0);
            PerQueryRecord {
                query_name: name.clone(),
                min,
                p50: percentile(&sorted, 50.0),
                p95: percentile(&sorted, 95.0),
                p99: percentile(&sorted, 99.0),
                max,
                count: sorted.len() as u64,
                error_count: error_count.saturating_sub(*timeout_count),
                timeout_count: *timeout_count,
            }
        })
        .collect();
    pq_records.sort_by(|a, b| a.query_name.cmp(&b.query_name));
    match serde_json::to_string_pretty(&pq_records) {
        Ok(json) => {
            let path = format!("{}/per_query_latency.json", results_path);
            if let Err(e) = tokio::fs::write(&path, json).await {
                error!(path, "Failed to write per_query_latency.json: {}", e);
            } else {
                info!(
                    path,
                    count = pq_records.len(),
                    "per_query_latency.json written"
                );
            }
        }
        Err(e) => error!("Failed to serialize per_query_latency: {}", e),
    }

    match serde_json::to_string_pretty(per_query_timeseries) {
        Ok(json) => {
            let path = format!("{}/per_query_timeseries.json", results_path);
            if let Err(e) = tokio::fs::write(&path, json).await {
                error!(path, "Failed to write per_query_timeseries.json: {}", e);
            } else {
                info!(
                    path,
                    count = per_query_timeseries.len(),
                    "per_query_timeseries.json written"
                );
            }
        }
        Err(e) => error!("Failed to serialize per_query_timeseries: {}", e),
    }
}

/// Poll Kafka consumer group lag for a single stream every second.
/// Updates both per-stream lag and total lag in the shared state.
async fn poll_kafka_consumer_lag_stream(
    bootstrap_servers: &str,
    stream_name: &str,
    consumer_group: &str,
    topic: &str,
    state: Arc<Mutex<AggregatorState>>,
) {
    // Create a consumer client to fetch committed offsets and watermark offsets.
    let consumer: Result<BaseConsumer, _> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", consumer_group)
        .create();

    let consumer = match consumer {
        Ok(c) => c,
        Err(e) => {
            error!(
                stream = %stream_name,
                "Failed to create Kafka consumer for lag polling: {}. Consumer lag will not be tracked.",
                e
            );
            return;
        }
    };

    info!(
        stream = %stream_name,
        topic = %topic,
        consumer_group = %consumer_group,
        "Starting Kafka consumer lag polling"
    );

    let timeout = std::time::Duration::from_secs(5);
    let consumer = Arc::new(consumer);
    let stream_name = stream_name.to_string();

    loop {
        let lag = tokio::task::spawn_blocking({
            let consumer = consumer.clone();
            let topic = topic.to_string();
            move || compute_consumer_lag(&consumer, &topic, timeout)
        })
        .await;

        match lag {
            Ok(Ok(stream_lag)) => {
                let mut s = state.lock().await;
                s.per_stream_lag.insert(stream_name.clone(), stream_lag);
                // Recompute total lag as sum of all stream lags.
                s.kafka_consumer_lag = s.per_stream_lag.values().sum();
            }
            Ok(Err(e)) => {
                tracing::debug!(stream = %stream_name, "Failed to compute consumer lag: {}", e);
            }
            Err(e) => {
                error!(stream = %stream_name, "Lag polling task panicked: {}", e);
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// Compute total consumer lag across all partitions for a topic.
/// This is a blocking function (rdkafka APIs are synchronous).
fn compute_consumer_lag(
    consumer: &BaseConsumer,
    topic: &str,
    timeout: std::time::Duration,
) -> Result<u64, String> {
    // Fetch topic metadata to discover partitions.
    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| format!("fetch_metadata: {}", e))?;

    let topic_metadata = metadata
        .topics()
        .first()
        .ok_or_else(|| "No topic metadata returned".to_string())?;

    let partitions = topic_metadata.partitions();
    if partitions.is_empty() {
        return Ok(0);
    }

    // Build a TopicPartitionList for committed offset queries.
    let mut tpl = TopicPartitionList::new();
    for partition in partitions {
        tpl.add_partition(topic, partition.id());
    }

    // Fetch committed offsets for the consumer group.
    let committed = consumer
        .committed_offsets(tpl, timeout)
        .map_err(|e| format!("committed_offsets: {}", e))?;

    let mut total_lag: u64 = 0;

    for partition in partitions {
        let pid = partition.id();

        // Fetch high watermark (latest offset) for this partition.
        let (_low, high) = consumer
            .fetch_watermarks(topic, pid, timeout)
            .map_err(|e| format!("fetch_watermarks(partition {}): {}", pid, e))?;

        // Get committed offset for this partition.
        let committed_offset = committed
            .find_partition(topic, pid)
            .and_then(|elem| match elem.offset() {
                rdkafka::Offset::Offset(o) => Some(o),
                _ => None,
            })
            .unwrap_or(0);

        // Lag = high watermark - committed offset (clamped to 0).
        if high > committed_offset {
            total_lag += (high - committed_offset) as u64;
        }
    }

    Ok(total_lag)
}
