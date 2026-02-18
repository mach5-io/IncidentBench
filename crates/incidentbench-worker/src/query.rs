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
use incidentbench_common::proto::worker::{QueryTypeLatency, WorkerMetricSnapshot};
use incidentbench_common::ratelimit::TokenBucketRateLimiter;
use incidentbench_common::scenario::{QueryDef, QueryMix, WorkerRateTable};
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

    // Create weighted query selector.
    let weights: Vec<f64> = config.query_mix.queries.iter().map(|q| q.weight).collect();
    let query_dist = WeightedIndex::new(&weights).context("Invalid query weights")?;

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
                }
            }
            Err(e) => {
                queries_executed += 1;
                query_errors += 1;
                *per_type_executed.entry(query.name.clone()).or_default() += 1;
                *per_type_errors.entry(query.name.clone()).or_default() += 1;
                debug!("Query failed: {}", e);
            }
        }
    }

    barrier.send_done().await?;
    info!("Query worker shutting down");
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
