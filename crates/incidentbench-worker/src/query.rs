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

use crate::barrier::BarrierClient;
use crate::proc_metrics::ProcMetrics;
use anyhow::Context;
use incidentbench_common::adapters;
use incidentbench_common::metrics::latency_distribution_from_values;
use incidentbench_common::proto::worker::{
    QueryErrorRecord, QueryTypeLatency, TimedOutQueryRecord, WorkerMetricSnapshot,
};
use incidentbench_common::ratelimit::TokenBucketRateLimiter;
use incidentbench_common::scenario::{
    IterationMode, QueryDef, QueryMix, QuerySession, WorkerRateTable,
};
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use serde::Deserialize;
use siphasher::sip::SipHasher13;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct QueryWorkerConfig {
    pub worker_index: u32,
    pub run_seed: u64,
    /// Effective query mix (with group overrides already applied by the operator).
    pub query_mix: QueryMix,
    pub rate_table: WorkerRateTable,
    /// Warehouse query endpoint for this worker's group.
    pub query_endpoint: String,
    /// Query group name (empty string if not using multi-warehouse mode).
    #[serde(default)]
    pub query_group: String,
    pub target_adapter: String,
    pub target_config: HashMap<String, serde_json::Value>,
    pub phase_controller_addr: String,
    pub aggregator_addr: String,
    /// When present, workers run session loops instead of the rate-controlled loop.
    #[serde(default)]
    pub query_session: Option<QuerySession>,
    /// Global query timeout used by session mode. Defaults to 10 000 ms.
    #[serde(default = "default_timeout_ms")]
    pub default_timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    10_000
}

/// A timed-out query record collected during a session tick.
/// Propagated to the aggregator once proto field is wired in step 5.
#[derive(Debug, Clone)]
pub struct TimedOutRecord {
    pub query_name: String,
    pub category: String,
    pub phase: String,
    pub timestamp_ms: i64,
    pub duration_ms: f64,
    pub timeout_ms: u64,
}

pub async fn run(config_path: &str) -> anyhow::Result<()> {
    let config_str = tokio::fs::read_to_string(config_path)
        .await
        .context("Failed to read query worker config")?;
    let config: QueryWorkerConfig = serde_yaml::from_str(&config_str)?;

    // Derive unique worker identity from POD_NAME (set via Kubernetes downward API).
    let pod_name =
        std::env::var("POD_NAME").unwrap_or_else(|_| format!("worker-{}", std::process::id()));
    let effective_index: u32 = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        pod_name.hash(&mut h);
        (h.finish() % 10000) as u32
    };
    let worker_id = format!("query-{}", pod_name);
    info!(
        worker_id = %worker_id,
        effective_index = effective_index,
        query_endpoint = %config.query_endpoint,
        query_group = %config.query_group,
        "Query worker starting"
    );

    // Create adapter.
    let adapter = adapters::create_adapter(&config.target_adapter, &config.target_config)?;

    // Create uniform query selector (all queries have equal probability).
    let query_count = config.query_mix.queries.len();
    let query_dist = WeightedIndex::new(vec![1usize; query_count]).context("Empty query mix")?;

    // Deterministic RNG for query selection and variable generation.
    let mut hasher = SipHasher13::new();
    config.run_seed.hash(&mut hasher);
    pod_name.hash(&mut hasher);
    42u64.hash(&mut hasher); // Extra salt for query workers
    let rng_seed = hasher.finish();
    let mut rng = ChaCha8Rng::seed_from_u64(rng_seed);

    // Connect to PhaseController.
    let mut barrier = BarrierClient::connect(
        &config.phase_controller_addr,
        worker_id.clone(),
        "query".to_string(),
        effective_index as i32,
    )
    .await
    .context("Failed to connect to PhaseController")?;

    // Metrics channel.
    let (metrics_tx, metrics_rx) = mpsc::channel::<WorkerMetricSnapshot>(128);

    let agg_addr = config.aggregator_addr.clone();
    let wid = worker_id.clone();
    tokio::spawn(async move {
        if let Err(e) = stream_metrics_to_aggregator(&agg_addr, metrics_rx).await {
            error!(worker_id = %wid, "Metrics streaming failed: {}", e);
        }
    });

    // Wait for first phase.
    info!("Waiting for first phase...");
    let (mut current_phase, mut current_rate_mqps) = match barrier.wait_for_transition().await {
        Some((phase, rate)) => (phase, rate as u64),
        None => {
            info!("Run complete before first phase");
            return Ok(());
        }
    };

    info!(phase = %current_phase, rate_mqps = current_rate_mqps, "First phase started");

    // Branch: session mode vs rate-controlled mode.
    if config.query_session.is_some() {
        return run_session_loop(
            &config,
            &worker_id,
            adapter.as_ref(),
            barrier,
            metrics_tx,
            effective_index,
            current_phase,
        )
        .await;
    }

    // Convert milli-QPS to QPS for rate limiter.
    let mut rate_limiter = TokenBucketRateLimiter::new(current_rate_mqps as f64 / 1000.0);

    let proc_metrics = ProcMetrics::new();

    // Per-second metrics.
    let mut second_start = Instant::now();
    let mut queries_executed: u64 = 0;
    let mut query_errors: u64 = 0;
    let mut query_latencies: Vec<f64> = Vec::with_capacity(1000);
    let mut per_type_latencies: HashMap<String, Vec<f64>> = HashMap::new();
    let mut per_type_executed: HashMap<String, u64> = HashMap::new();
    let mut per_type_errors: HashMap<String, u64> = HashMap::new();
    let mut pending_error_records: Vec<QueryErrorRecord> = Vec::new();

    loop {
        // Emit 1-second snapshot.
        if second_start.elapsed() >= Duration::from_secs(1) {
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;

            let query_latency_by_type: Vec<QueryTypeLatency> = per_type_latencies
                .iter()
                .map(|(name, lats)| QueryTypeLatency {
                    query_name: name.clone(),
                    latency: Some(latency_distribution_from_values(lats)),
                    executed: *per_type_executed.get(name).unwrap_or(&0) as i64,
                    errors: *per_type_errors.get(name).unwrap_or(&0) as i64,
                })
                .collect();

            let (proc_cpu, proc_mem) = proc_metrics.sample();

            let snapshot = WorkerMetricSnapshot {
                worker_id: worker_id.clone(),
                worker_mode: "query".to_string(),
                worker_index: effective_index as i32,
                timestamp_ns: now_ns,
                phase: current_phase.clone(),
                events_produced: 0,
                events_acknowledged: 0,
                events_failed: 0,
                kafka_produce_latency: None,
                queries_executed: queries_executed as i64,
                query_errors: query_errors as i64,
                query_latency: Some(latency_distribution_from_values(&query_latencies)),
                query_latency_by_type,
                query_group: config.query_group.clone(),
                cpu_utilization: proc_cpu,
                memory_bytes: proc_mem as i64,
                target_rate: current_rate_mqps as i64,
                concurrent_sessions: 0,
                timed_out_queries: vec![],
                query_error_records: std::mem::take(&mut pending_error_records),
            };

            if let Err(e) = metrics_tx.try_send(snapshot) {
                warn!("Metrics snapshot dropped (channel full): {}", e);
            }

            // Reset.
            queries_executed = 0;
            query_errors = 0;
            query_latencies.clear();
            per_type_latencies.clear();
            per_type_executed.clear();
            per_type_errors.clear();
            second_start = Instant::now();
        }

        // Check for phase transitions.
        if let Ok(event) =
            tokio::time::timeout(Duration::from_millis(0), barrier.next_event()).await
        {
            match event {
                Some(crate::barrier::PhaseEvent::Transition {
                    to_phase,
                    new_target_rate,
                    ..
                }) => {
                    info!(from = %current_phase, to = %to_phase, rate_mqps = new_target_rate, "Phase transition");
                    current_phase = to_phase;
                    current_rate_mqps = new_target_rate as u64;
                    rate_limiter.set_rate(current_rate_mqps as f64 / 1000.0);
                }
                Some(crate::barrier::PhaseEvent::RunComplete) | None => {
                    info!("Run complete");
                    break;
                }
                _ => {}
            }
        }

        // Execute query at target rate.
        let _ = rate_limiter.acquire().await;

        // Select query based on weighted mix.
        let query_idx = query_dist.sample(&mut rng);
        let query = &config.query_mix.queries[query_idx];

        // Generate variables.
        let variables = generate_query_variables(query, &mut rng);

        // Each query specifies its target index via the `index` field.
        // Enforce a timeout at the worker level (adapter may also have its own).
        let query_timeout = Duration::from_millis(query.timeout_ms.max(5000));
        let query_started = Instant::now();
        let result = match tokio::time::timeout(
            query_timeout,
            adapter.execute_query(query, &query.index, &config.query_endpoint, &variables),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(anyhow::anyhow!(
                "Query timed out after {}ms",
                query_timeout.as_millis()
            )),
        };

        match result {
            Ok(qr) => {
                queries_executed += 1;
                query_latencies.push(qr.duration_ms);
                per_type_latencies
                    .entry(query.name.clone())
                    .or_default()
                    .push(qr.duration_ms);
                *per_type_executed.entry(query.name.clone()).or_default() += 1;
                if qr.error.is_some() {
                    query_errors += 1;
                    *per_type_errors.entry(query.name.clone()).or_default() += 1;
                    pending_error_records.push(query_error_record(
                        query,
                        query.category.as_deref().unwrap_or(""),
                        &current_phase,
                        &worker_id,
                        effective_index,
                        qr.duration_ms,
                        qr.timed_out,
                        qr.error.as_deref().unwrap_or("query failed"),
                    ));
                }
            }
            Err(e) => {
                queries_executed += 1;
                query_errors += 1;
                *per_type_executed.entry(query.name.clone()).or_default() += 1;
                *per_type_errors.entry(query.name.clone()).or_default() += 1;
                let message = e.to_string();
                pending_error_records.push(query_error_record(
                    query,
                    query.category.as_deref().unwrap_or(""),
                    &current_phase,
                    &worker_id,
                    effective_index,
                    query_started.elapsed().as_secs_f64() * 1000.0,
                    is_timeout_error(&message),
                    &message,
                ));
                debug!("Query failed: {}", e);
            }
        }
    }

    barrier.send_done().await?;
    info!("Query worker shutting down");
    Ok(())
}

// --- Session mode ---

struct SessionCategory {
    name: String,
    queries: Vec<QueryDef>,
    iteration: IterationMode,
    parallelism: usize,
    cursor: usize,
}

impl SessionCategory {
    fn from_query_category(cat: incidentbench_common::scenario::QueryCategory) -> Self {
        Self {
            name: cat.name,
            queries: cat.queries,
            iteration: cat.iteration,
            parallelism: cat.parallelism,
            cursor: 0,
        }
    }

    /// Returns indices of the next batch of queries to fire.
    fn next_batch_indices(&mut self, rng: &mut ChaCha8Rng) -> Vec<usize> {
        let len = self.queries.len();
        match self.iteration {
            IterationMode::Sequential => {
                let indices: Vec<usize> = (0..self.parallelism)
                    .map(|i| (self.cursor + i) % len)
                    .collect();
                self.cursor = (self.cursor + self.parallelism) % len;
                indices
            }
            IterationMode::WeightedRandom => {
                let dist = WeightedIndex::new(vec![1usize; len]).expect("non-empty category");
                (0..self.parallelism).map(|_| dist.sample(rng)).collect()
            }
        }
    }
}

async fn run_session_loop(
    config: &QueryWorkerConfig,
    worker_id: &str,
    adapter: &dyn incidentbench_common::adapter::TargetAdapter,
    mut barrier: BarrierClient,
    metrics_tx: mpsc::Sender<WorkerMetricSnapshot>,
    effective_index: u32,
    initial_phase: String,
) -> anyhow::Result<()> {
    use incidentbench_common::metrics::latency_distribution_from_values;

    let session = config.query_session.as_ref().unwrap();
    let configured_concurrent_sessions: i32 = session
        .categories
        .iter()
        .map(|cat| cat.parallelism as i32)
        .sum();

    // Build session categories with cursors.
    let mut categories: Vec<SessionCategory> = session
        .categories
        .iter()
        .cloned()
        .map(SessionCategory::from_query_category)
        .collect();

    let mut hasher = SipHasher13::new();
    config.run_seed.hash(&mut hasher);
    worker_id.hash(&mut hasher);
    99u64.hash(&mut hasher); // salt for session workers
    let mut rng = ChaCha8Rng::seed_from_u64(hasher.finish());

    let proc_metrics = ProcMetrics::new();
    let mut current_phase = initial_phase;
    let mut second_start = Instant::now();

    // Per-second accumulators keyed by individual query name.
    let mut queries_executed: u64 = 0;
    let mut query_errors: u64 = 0;
    let mut per_query_latencies: HashMap<String, Vec<f64>> = HashMap::new();
    let mut per_query_executed: HashMap<String, u64> = HashMap::new();
    let mut per_query_errors: HashMap<String, u64> = HashMap::new();
    let mut pending_timeouts: Vec<TimedOutRecord> = Vec::new();
    let mut pending_error_records: Vec<QueryErrorRecord> = Vec::new();

    info!(worker_id = %worker_id, "Session loop started");

    loop {
        // Emit 1-second snapshot.
        if second_start.elapsed() >= Duration::from_secs(1) {
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;

            let query_latency_by_type: Vec<QueryTypeLatency> = per_query_latencies
                .iter()
                .map(|(name, lats)| QueryTypeLatency {
                    query_name: name.clone(),
                    latency: Some(latency_distribution_from_values(lats)),
                    executed: *per_query_executed.get(name).unwrap_or(&0) as i64,
                    errors: *per_query_errors.get(name).unwrap_or(&0) as i64,
                })
                .collect();

            let all_latencies: Vec<f64> = per_query_latencies.values().flatten().copied().collect();
            let (proc_cpu, proc_mem) = proc_metrics.sample();

            let proto_timeouts: Vec<TimedOutQueryRecord> = pending_timeouts
                .iter()
                .map(|r| TimedOutQueryRecord {
                    query_name: r.query_name.clone(),
                    category: r.category.clone(),
                    phase: r.phase.clone(),
                    timestamp_ms: r.timestamp_ms,
                    duration_ms: r.duration_ms,
                    timeout_ms: r.timeout_ms as i64,
                })
                .collect();

            let snapshot = WorkerMetricSnapshot {
                worker_id: worker_id.to_string(),
                worker_mode: "query-session".to_string(),
                worker_index: effective_index as i32,
                timestamp_ns: now_ns,
                phase: current_phase.clone(),
                events_produced: 0,
                events_acknowledged: 0,
                events_failed: 0,
                kafka_produce_latency: None,
                queries_executed: queries_executed as i64,
                query_errors: query_errors as i64,
                query_latency: Some(latency_distribution_from_values(&all_latencies)),
                query_latency_by_type,
                query_group: config.query_group.clone(),
                cpu_utilization: proc_cpu,
                memory_bytes: proc_mem as i64,
                target_rate: 1, // 1 active user session per worker pod
                concurrent_sessions: 1,
                timed_out_queries: proto_timeouts,
                query_error_records: std::mem::take(&mut pending_error_records),
            };

            if let Err(e) = metrics_tx.try_send(snapshot) {
                warn!("Session metrics snapshot dropped: {}", e);
            }

            pending_timeouts.clear();

            queries_executed = 0;
            query_errors = 0;
            per_query_latencies.clear();
            per_query_executed.clear();
            per_query_errors.clear();
            second_start = Instant::now();
        }

        // Check for phase transitions (non-blocking).
        if let Ok(event) =
            tokio::time::timeout(Duration::from_millis(0), barrier.next_event()).await
        {
            match event {
                Some(crate::barrier::PhaseEvent::Transition { to_phase, .. }) => {
                    info!(from = %current_phase, to = %to_phase, "Session phase transition");
                    current_phase = to_phase;
                }
                Some(crate::barrier::PhaseEvent::RunComplete) | None => {
                    info!("Session run complete");
                    // Flush any metrics collected since the last 1-second snapshot so the
                    // final partial second contributes both latency counts and timeout records.
                    if queries_executed > 0
                        || query_errors > 0
                        || !per_query_latencies.is_empty()
                        || !pending_timeouts.is_empty()
                    {
                        let now_ns = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as i64;
                        let query_latency_by_type: Vec<QueryTypeLatency> = per_query_latencies
                            .iter()
                            .map(|(name, lats)| QueryTypeLatency {
                                query_name: name.clone(),
                                latency: Some(latency_distribution_from_values(lats)),
                                executed: *per_query_executed.get(name).unwrap_or(&0) as i64,
                                errors: *per_query_errors.get(name).unwrap_or(&0) as i64,
                            })
                            .collect();
                        let all_latencies: Vec<f64> =
                            per_query_latencies.values().flatten().copied().collect();
                        let (proc_cpu, proc_mem) = proc_metrics.sample();
                        let proto_timeouts: Vec<TimedOutQueryRecord> = pending_timeouts
                            .iter()
                            .map(|r| TimedOutQueryRecord {
                                query_name: r.query_name.clone(),
                                category: r.category.clone(),
                                phase: r.phase.clone(),
                                timestamp_ms: r.timestamp_ms,
                                duration_ms: r.duration_ms,
                                timeout_ms: r.timeout_ms as i64,
                            })
                            .collect();
                        let proto_errors = std::mem::take(&mut pending_error_records);
                        let flush_snapshot = WorkerMetricSnapshot {
                            worker_id: worker_id.to_string(),
                            worker_mode: "query-session".to_string(),
                            worker_index: effective_index as i32,
                            timestamp_ns: now_ns,
                            phase: current_phase.clone(),
                            queries_executed: queries_executed as i64,
                            query_errors: query_errors as i64,
                            query_latency: Some(latency_distribution_from_values(&all_latencies)),
                            query_latency_by_type,
                            query_group: config.query_group.clone(),
                            cpu_utilization: proc_cpu,
                            memory_bytes: proc_mem as i64,
                            target_rate: 1,
                            concurrent_sessions: configured_concurrent_sessions,
                            timed_out_queries: proto_timeouts,
                            query_error_records: proto_errors,
                            ..Default::default()
                        };
                        let _ = metrics_tx.try_send(flush_snapshot);
                    }
                    break;
                }
                _ => {}
            }
        }

        // Build the batch for this tick: one batch per category, all fired simultaneously.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Collect (category_name, query_def, variables) before building futures so
        // that ownership is clear and references live long enough.
        let work: Vec<(String, QueryDef, HashMap<String, String>)> = {
            let mut items = Vec::new();
            for cat in &mut categories {
                let indices = cat.next_batch_indices(&mut rng);
                for idx in indices {
                    let query = cat.queries[idx].clone();
                    let variables = HashMap::new(); // SQL queries have no template variables
                    items.push((cat.name.clone(), query, variables));
                }
            }
            items
        };

        let results = futures::future::join_all(work.iter().map(|(_, query, variables)| {
            adapter.execute_query(query, &query.index, &config.query_endpoint, variables)
        }))
        .await;

        let futures_with_meta: Vec<(String, String, u64)> = work
            .iter()
            .map(|(cat, q, _)| {
                let q_timeout = if q.timeout_ms > 0 {
                    q.timeout_ms
                } else {
                    config.default_timeout_ms
                };
                (cat.clone(), q.name.clone(), q_timeout)
            })
            .collect();

        for ((category, query_name, q_timeout_ms), result) in
            futures_with_meta.into_iter().zip(results)
        {
            queries_executed += 1;
            *per_query_executed.entry(query_name.clone()).or_default() += 1;

            match result {
                Ok(qr) => {
                    per_query_latencies
                        .entry(query_name.clone())
                        .or_default()
                        .push(qr.duration_ms);

                    if qr.timed_out {
                        query_errors += 1;
                        *per_query_errors.entry(query_name.clone()).or_default() += 1;
                        pending_timeouts.push(TimedOutRecord {
                            query_name: query_name.clone(),
                            category: category.clone(),
                            phase: current_phase.clone(),
                            timestamp_ms: now_ms,
                            duration_ms: qr.duration_ms,
                            timeout_ms: q_timeout_ms,
                        });
                        pending_error_records.push(query_error_record_parts(
                            &query_name,
                            &category,
                            &current_phase,
                            worker_id,
                            effective_index,
                            now_ms,
                            qr.duration_ms,
                            true,
                            qr.error.as_deref().unwrap_or("query timed out"),
                        ));
                    } else if qr.error.is_some() {
                        query_errors += 1;
                        *per_query_errors.entry(query_name.clone()).or_default() += 1;
                        pending_error_records.push(query_error_record_parts(
                            &query_name,
                            &category,
                            &current_phase,
                            worker_id,
                            effective_index,
                            now_ms,
                            qr.duration_ms,
                            false,
                            qr.error.as_deref().unwrap_or("query failed"),
                        ));
                        debug!("Session query error: {:?}", qr.error);
                    }
                }
                Err(e) => {
                    query_errors += 1;
                    *per_query_errors.entry(query_name.clone()).or_default() += 1;
                    let message = e.to_string();
                    pending_error_records.push(query_error_record_parts(
                        &query_name,
                        &category,
                        &current_phase,
                        worker_id,
                        effective_index,
                        now_ms,
                        0.0,
                        is_timeout_error(&message),
                        &message,
                    ));
                    debug!("Session query failed: {}", e);
                }
            }
        }

        // Pause between ticks if configured.
        if session.think_time_ms > 0 {
            tokio::time::sleep(Duration::from_millis(session.think_time_ms)).await;
        }
    }

    barrier.send_done().await?;
    info!("Session worker shutting down");
    Ok(())
}

fn generate_query_variables(query: &QueryDef, rng: &mut ChaCha8Rng) -> HashMap<String, String> {
    let mut vars = HashMap::new();
    for (name, def) in &query.variables {
        match def.source.as_str() {
            "recently_ingested" => {
                // Generate a random hex trace ID (matching the ingest generator format).
                let hex: String = (0..32)
                    .map(|_| {
                        let idx = rng.gen_range(0..16usize);
                        "0123456789abcdef".as_bytes()[idx] as char
                    })
                    .collect();
                vars.insert(name.clone(), hex);
            }
            _ => {
                vars.insert(name.clone(), format!("var_{}", name));
            }
        }
    }
    vars
}

fn query_error_record(
    query: &QueryDef,
    category: &str,
    phase: &str,
    worker_id: &str,
    worker_index: u32,
    duration_ms: f64,
    timed_out: bool,
    message: &str,
) -> QueryErrorRecord {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    query_error_record_parts(
        &query.name,
        category,
        phase,
        worker_id,
        worker_index,
        timestamp_ms,
        duration_ms,
        timed_out,
        message,
    )
}

fn query_error_record_parts(
    query_name: &str,
    category: &str,
    phase: &str,
    worker_id: &str,
    worker_index: u32,
    timestamp_ms: i64,
    duration_ms: f64,
    timed_out: bool,
    message: &str,
) -> QueryErrorRecord {
    QueryErrorRecord {
        query_name: query_name.to_string(),
        category: category.to_string(),
        phase: phase.to_string(),
        worker_id: worker_id.to_string(),
        worker_index: worker_index as i32,
        timestamp_ms,
        duration_ms,
        timed_out,
        error_class: classify_query_error(message, timed_out).to_string(),
        message: truncate_error_message(message),
    }
}

fn classify_query_error(message: &str, timed_out: bool) -> &'static str {
    if timed_out || is_timeout_error(message) {
        return "timeout";
    }
    let lower = message.to_ascii_lowercase();
    if lower.contains("cancel") {
        "cancelled"
    } else if lower.contains("resource")
        || lower.contains("oom")
        || lower.contains("memory")
        || lower.contains("exhaust")
        || lower.contains("enhance_your_calm")
    {
        "resource"
    } else if lower.contains("sql")
        || lower.contains("sqlstate")
        || lower.contains("code=")
        || lower.contains("syntax")
        || lower.contains("parse")
        || lower.contains("relation")
        || lower.contains("column")
    {
        "sql"
    } else if lower.contains("connection")
        || lower.contains("connect")
        || lower.contains("transport")
        || lower.contains("broken pipe")
        || lower.contains("connection reset")
    {
        "connection"
    } else if lower.contains("http 5") || lower.contains("internal") {
        "target_internal"
    } else {
        "unknown"
    }
}

fn is_timeout_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("timed out") || lower.contains("timeout") || lower.contains("deadline")
}

fn truncate_error_message(message: &str) -> String {
    const MAX_LEN: usize = 1024;
    if message.len() <= MAX_LEN {
        message.to_string()
    } else {
        let mut s = message[..MAX_LEN].to_string();
        s.push_str("…");
        s
    }
}

async fn stream_metrics_to_aggregator(
    aggregator_addr: &str,
    rx: mpsc::Receiver<WorkerMetricSnapshot>,
) -> anyhow::Result<()> {
    use incidentbench_common::proto::aggregator::metrics_service_client::MetricsServiceClient;
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;
    use tonic::transport::Channel;

    let rx = Arc::new(TokioMutex::new(rx));
    let max_retries = 30;
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(10);

    for attempt in 1..=max_retries {
        let endpoint = Channel::from_shared(format!("http://{}", aggregator_addr))?;
        match endpoint.connect().await {
            Ok(channel) => {
                if attempt > 1 {
                    info!(attempt, "Connected to aggregator after retries");
                }
                let mut client = MetricsServiceClient::new(channel);
                let rx_clone = rx.clone();
                let stream = async_stream::stream! {
                    let mut rx = rx_clone.lock().await;
                    while let Some(snapshot) = rx.recv().await {
                        yield snapshot;
                    }
                };
                match client.report_metrics(stream).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        if attempt == max_retries {
                            anyhow::bail!(
                                "Metrics streaming failed after {} retries: {}",
                                max_retries,
                                e
                            );
                        }
                        info!(
                            attempt,
                            max_retries, "Metrics streaming failed, retrying: {}", e
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
            Err(e) => {
                if attempt == max_retries {
                    anyhow::bail!(
                        "Failed to connect to aggregator after {} retries: {}",
                        max_retries,
                        e
                    );
                }
                info!(
                    attempt,
                    max_retries, "Aggregator not ready, retrying: {}", e
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }

    Ok(())
}
