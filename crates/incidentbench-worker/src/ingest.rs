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
use incidentbench_common::generator::DataGenerator;
use incidentbench_common::metrics::latency_distribution_from_values;
use incidentbench_common::proto::worker::WorkerMetricSnapshot;
use incidentbench_common::ratelimit::TokenBucketRateLimiter;
use incidentbench_common::scenario::{Schema, WorkerRateTable};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Deserialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct IngestWorkerConfig {
    pub worker_index: u32,
    pub run_seed: u64,
    pub schema: Schema,
    pub rate_table: WorkerRateTable,
    pub kafka_bootstrap_servers: String,
    pub kafka_topic: String,
    pub phase_controller_addr: String,
    pub aggregator_addr: String,
}

/// Run the ingest worker.
pub async fn run(config_path: &str) -> anyhow::Result<()> {
    let config_str = tokio::fs::read_to_string(config_path)
        .await
        .context("Failed to read ingest worker config")?;
    let config: IngestWorkerConfig = serde_yaml::from_str(&config_str)?;

    // Derive unique worker identity from POD_NAME (set via Kubernetes downward API).
    // In a Deployment, pod names have random suffixes, so we hash to get a unique index.
    let pod_name =
        std::env::var("POD_NAME").unwrap_or_else(|_| format!("worker-{}", std::process::id()));
    let effective_index: u32 = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        pod_name.hash(&mut h);
        (h.finish() % 10000) as u32
    };
    let worker_id = format!("ingest-{}", pod_name);
    info!(
        worker_id = %worker_id,
        effective_index = effective_index,
        kafka_topic = %config.kafka_topic,
        "Ingest worker starting"
    );

    // Create Kafka producer.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_bootstrap_servers)
        .set("message.timeout.ms", "30000")
        .set("queue.buffering.max.messages", "100000")
        .set("queue.buffering.max.kbytes", "1048576") // 1GB
        .set("batch.num.messages", "1000")
        .set("linger.ms", "5")
        .create()
        .context("Failed to create Kafka producer")?;

    // Create data generator.
    let mut generator = DataGenerator::new(&config.schema, config.run_seed, effective_index);

    // Connect to PhaseController.
    let mut barrier = BarrierClient::connect(
        &config.phase_controller_addr,
        worker_id.clone(),
        "ingest".to_string(),
        effective_index as i32,
    )
    .await
    .context("Failed to connect to PhaseController")?;

    // Create metrics channel for streaming to aggregator.
    let (metrics_tx, metrics_rx) = mpsc::channel::<WorkerMetricSnapshot>(128);

    // Spawn metrics streaming task.
    let agg_addr = config.aggregator_addr.clone();
    let wid = worker_id.clone();
    tokio::spawn(async move {
        if let Err(e) = stream_metrics_to_aggregator(&agg_addr, metrics_rx).await {
            error!(worker_id = %wid, "Metrics streaming failed: {}", e);
        }
    });

    // Wait for first phase transition (start of baseline).
    info!("Waiting for first phase...");
    let (mut current_phase, _broadcast_rate) = match barrier.wait_for_transition().await {
        Some((phase, rate)) => (phase, rate as u64),
        None => {
            info!("Run complete before first phase");
            return Ok(());
        }
    };

    // Use rate from the worker's own rate_table (per-stream EPS) instead of broadcast rate.
    let mut current_rate = config
        .rate_table
        .phases
        .iter()
        .find(|p| p.phase_name == current_phase)
        .map(|p| p.ingest_eps)
        .unwrap_or(0);

    info!(phase = %current_phase, rate = current_rate, "First phase started (rate from rate_table)");

    // Set incident mode based on phase.
    let incident_phases = ["incident_trigger", "ingestion_surge", "overlap"];
    generator.set_incident_mode(incident_phases.contains(&current_phase.as_str()));

    let mut rate_limiter = TokenBucketRateLimiter::new(current_rate as f64);

    let proc_metrics = ProcMetrics::new();

    // Per-second metrics tracking.
    let mut second_start = Instant::now();
    let mut events_produced: u64 = 0;
    let mut events_acked: u64 = 0;
    let mut events_failed: u64 = 0;
    let mut produce_latencies: Vec<f64> = Vec::with_capacity(10000);

    loop {
        // Check if we need to emit a 1-second snapshot.
        if second_start.elapsed() >= Duration::from_secs(1) {
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;

            let (proc_cpu, proc_mem) = proc_metrics.sample();

            let snapshot = WorkerMetricSnapshot {
                worker_id: worker_id.clone(),
                worker_mode: "ingest".to_string(),
                worker_index: effective_index as i32,
                timestamp_ns: now_ns,
                phase: current_phase.clone(),
                events_produced: events_produced as i64,
                events_acknowledged: events_acked as i64,
                events_failed: events_failed as i64,
                kafka_produce_latency: Some(latency_distribution_from_values(&produce_latencies)),
                queries_executed: 0,
                query_errors: 0,
                query_latency: None,
                query_latency_by_type: vec![],
                query_group: String::new(),
                cpu_utilization: proc_cpu,
                memory_bytes: proc_mem as i64,
                target_rate: current_rate as i64,
                concurrent_sessions: 0,
                timed_out_queries: vec![],
            };

            if let Err(e) = metrics_tx.try_send(snapshot) {
                warn!("Metrics snapshot dropped (channel full): {}", e);
            }

            // Reset counters.
            events_produced = 0;
            events_acked = 0;
            events_failed = 0;
            produce_latencies.clear();
            second_start = Instant::now();
        }

        // Check for phase transitions (non-blocking).
        if let Ok(event) =
            tokio::time::timeout(Duration::from_millis(0), barrier.next_event()).await
        {
            match event {
                Some(crate::barrier::PhaseEvent::Transition { to_phase, .. }) => {
                    // Use rate from the worker's own rate_table (per-stream EPS).
                    let new_rate = config
                        .rate_table
                        .phases
                        .iter()
                        .find(|p| p.phase_name == to_phase)
                        .map(|p| p.ingest_eps)
                        .unwrap_or(0);
                    info!(from = %current_phase, to = %to_phase, rate = new_rate, "Phase transition (rate from rate_table)");
                    current_phase = to_phase;
                    current_rate = new_rate;
                    rate_limiter.set_rate(current_rate as f64);
                    generator.set_incident_mode(incident_phases.contains(&current_phase.as_str()));
                }
                Some(crate::barrier::PhaseEvent::RunComplete) | None => {
                    info!("Run complete");
                    break;
                }
                _ => {}
            }
        }

        // Generate and produce events at the target rate.
        let _ = rate_limiter.acquire().await;

        let event = generator.generate_event();
        let payload = serde_json::to_string(&event).unwrap_or_default();

        let produce_start = Instant::now();

        // Retry transient Kafka errors up to 2 times.
        let mut send_ok = false;
        let mut last_err = String::new();
        for attempt in 0..3u32 {
            let record: FutureRecord<'_, str, str> =
                FutureRecord::to(&config.kafka_topic).payload(&payload);
            match producer.send(record, Duration::from_secs(5)).await {
                Ok(_) => {
                    send_ok = true;
                    break;
                }
                Err((err, _)) => {
                    last_err = err.to_string();
                    if attempt < 2 {
                        debug!(
                            "Kafka produce failed (attempt {}), retrying: {}",
                            attempt + 1,
                            err
                        );
                        tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                    }
                }
            }
        }

        let latency_ms = produce_start.elapsed().as_secs_f64() * 1000.0;
        events_produced += 1;
        if send_ok {
            events_acked += 1;
            produce_latencies.push(latency_ms);
        } else {
            events_failed += 1;
            debug!("Kafka produce failed after retries: {}", last_err);
        }
    }

    // Send final metrics.
    barrier.send_done().await?;
    info!("Ingest worker shutting down");
    Ok(())
}

/// Stream worker metrics to the MetricsAggregator via gRPC.
/// Retries connection with exponential backoff.
async fn stream_metrics_to_aggregator(
    aggregator_addr: &str,
    rx: mpsc::Receiver<WorkerMetricSnapshot>,
) -> anyhow::Result<()> {
    use incidentbench_common::proto::aggregator::metrics_service_client::MetricsServiceClient;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tonic::transport::Channel;

    let rx = Arc::new(Mutex::new(rx));
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
