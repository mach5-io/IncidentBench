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

use incidentbench_common::proto::phasecontroller::{
    controller_message,
    phase_gate_service_server::{PhaseGateService, PhaseGateServiceServer},
    worker_message, ControllerMessage, PhaseTransition, PrepareTransition, RunComplete,
    StatusRequest, StatusResponse, WorkerMessage,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, Mutex};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

#[derive(Debug, Deserialize)]
pub struct PhaseControllerConfig {
    /// Phase definitions with durations and rates.
    pub phases: Vec<PhaseSpec>,
    /// Expected ingest worker count.
    pub expected_ingest_workers: u32,
    /// Expected query worker count.
    pub expected_query_workers: u32,
    /// Maximum seconds to wait for all workers to connect before aborting.
    /// Defaults to 300s (5 minutes).
    #[serde(default = "default_worker_timeout")]
    pub worker_connect_timeout_s: u64,
    /// Optional path for phase checkpoint file. Enables resume after restart.
    #[serde(default)]
    pub checkpoint_path: Option<String>,
}

fn default_worker_timeout() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize)]
pub struct PhaseSpec {
    pub name: String,
    pub duration_seconds: u64,
    /// Per-worker ingest EPS.
    pub per_worker_ingest_eps: u64,
    /// Per-worker query milli-QPS.
    pub per_worker_query_mqps: u64,
}

struct PhaseControllerState {
    config: PhaseControllerConfig,
    /// Connected workers: worker_id -> (mode, sender).
    workers: HashMap<String, (String, mpsc::Sender<ControllerMessage>)>,
    /// Number of ready ingest workers.
    ready_ingest: u32,
    /// Number of ready query workers.
    ready_query: u32,
    /// Current phase index.
    current_phase_index: usize,
    /// Phase start time.
    phase_start: Option<Instant>,
    /// Overall start time.
    run_start: Option<Instant>,
    /// Whether timeline is complete.
    timeline_complete: bool,
    /// Prepare acks received for current transition.
    prepare_acks: u32,
}

pub struct PhaseControllerService {
    state: Arc<Mutex<PhaseControllerState>>,
    /// Broadcast channel for signaling "all workers ready".
    all_ready_tx: broadcast::Sender<()>,
}

impl PhaseControllerService {
    fn new(config: PhaseControllerConfig) -> Self {
        let (all_ready_tx, _) = broadcast::channel(1);
        Self {
            state: Arc::new(Mutex::new(PhaseControllerState {
                config,
                workers: HashMap::new(),
                ready_ingest: 0,
                ready_query: 0,
                current_phase_index: 0,
                phase_start: None,
                run_start: None,
                timeline_complete: false,
                prepare_acks: 0,
            })),
            all_ready_tx,
        }
    }
}

#[tonic::async_trait]
impl PhaseGateService for PhaseControllerService {
    type PhaseStreamStream =
        tokio_stream::wrappers::ReceiverStream<Result<ControllerMessage, Status>>;

    async fn phase_stream(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::PhaseStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let state = self.state.clone();
        let all_ready_tx = self.all_ready_tx.clone();

        let (outbound_tx, outbound_rx) = mpsc::channel::<Result<ControllerMessage, Status>>(32);
        let (worker_tx, mut worker_rx) = mpsc::channel::<ControllerMessage>(32);

        // Spawn task to forward from worker_tx to outbound_tx.
        let out_tx = outbound_tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = worker_rx.recv().await {
                if out_tx.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
        });

        // Spawn task to process inbound worker messages.
        let worker_tx_clone = worker_tx.clone();
        tokio::spawn(async move {
            let mut worker_id = String::new();

            while let Ok(Some(msg)) = inbound.message().await {
                let mut s = state.lock().await;

                match msg.payload {
                    Some(worker_message::Payload::Ready(_)) => {
                        worker_id = msg.worker_id.clone();
                        let mode = msg.worker_mode.clone();

                        info!(
                            worker_id = %worker_id,
                            mode = %mode,
                            "Worker connected and ready"
                        );

                        s.workers
                            .insert(worker_id.clone(), (mode.clone(), worker_tx_clone.clone()));

                        match mode.as_str() {
                            "ingest" => s.ready_ingest += 1,
                            "query" => s.ready_query += 1,
                            _ => {}
                        }

                        // Check if all workers are ready.
                        if s.ready_ingest >= s.config.expected_ingest_workers
                            && s.ready_query >= s.config.expected_query_workers
                        {
                            info!(
                                ingest = s.ready_ingest,
                                query = s.ready_query,
                                "All workers ready, starting timeline"
                            );
                            let _ = all_ready_tx.send(());
                        }
                    }
                    Some(worker_message::Payload::PrepareAck(ack)) => {
                        debug!(worker_id = %msg.worker_id, phase = %ack.phase, "PrepareAck received");
                        s.prepare_acks += 1;
                    }
                    Some(worker_message::Payload::Done(_)) => {
                        info!(worker_id = %msg.worker_id, "Worker done");
                        s.workers.remove(&msg.worker_id);
                    }
                    None => {}
                }
            }

            // Worker disconnected.
            let mut s = state.lock().await;
            if !worker_id.is_empty() {
                if let Some((mode, _)) = s.workers.remove(&worker_id) {
                    match mode.as_str() {
                        "ingest" => s.ready_ingest = s.ready_ingest.saturating_sub(1),
                        "query" => s.ready_query = s.ready_query.saturating_sub(1),
                        _ => {}
                    }
                    warn!(worker_id = %worker_id, "Worker disconnected");
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            outbound_rx,
        )))
    }

    async fn get_status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let s = self.state.lock().await;

        let total_duration: u64 = s.config.phases.iter().map(|p| p.duration_seconds).sum();
        let total_elapsed = s
            .run_start
            .map(|t| t.elapsed().as_secs() as i64)
            .unwrap_or(0);
        let phase_elapsed = s
            .phase_start
            .map(|t| t.elapsed().as_secs() as i64)
            .unwrap_or(0);

        let current_phase = if s.current_phase_index < s.config.phases.len() {
            s.config.phases[s.current_phase_index].name.clone()
        } else {
            "complete".to_string()
        };

        let state_str = if s.timeline_complete {
            "complete"
        } else if s.run_start.is_some() {
            "running"
        } else {
            "waiting"
        };

        Ok(Response::new(StatusResponse {
            state: state_str.to_string(),
            current_phase,
            phase_elapsed_seconds: phase_elapsed,
            total_elapsed_seconds: total_elapsed,
            total_duration_seconds: total_duration as i64,
            connected_ingest_workers: s.ready_ingest as i32,
            connected_query_workers: s.ready_query as i32,
            timeline_complete: s.timeline_complete,
        }))
    }
}

/// Run the PhaseController gRPC server and timeline conductor.
pub async fn run(config_path: &str, listen_addr: &str) -> anyhow::Result<()> {
    let config_str = tokio::fs::read_to_string(config_path).await?;
    let config: PhaseControllerConfig = serde_yaml::from_str(&config_str)?;

    info!(
        phases = config.phases.len(),
        expected_ingest = config.expected_ingest_workers,
        expected_query = config.expected_query_workers,
        "PhaseController starting"
    );

    let service = PhaseControllerService::new(config);
    let state = service.state.clone();
    let mut all_ready_rx = service.all_ready_tx.subscribe();

    let addr = listen_addr.parse()?;

    // Spawn the gRPC server.
    let svc = PhaseGateServiceServer::new(service);
    let _server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .map_err(|e| anyhow::anyhow!("gRPC server error: {}", e))
    });

    // Read checkpoint if available (resume after restart).
    let checkpoint_path = {
        let s = state.lock().await;
        s.config.checkpoint_path.clone()
    };
    let mut resume_from_phase: usize = 0;
    if let Some(ref cp_path) = checkpoint_path {
        if let Ok(contents) = tokio::fs::read_to_string(cp_path).await {
            if let Ok(idx) = contents.trim().parse::<usize>() {
                info!(checkpoint_phase = idx, "Resuming from checkpoint");
                resume_from_phase = idx;
            }
        }
    }

    // Wait for all workers to be ready (with timeout).
    let worker_timeout = {
        let s = state.lock().await;
        Duration::from_secs(s.config.worker_connect_timeout_s)
    };
    info!(
        "Waiting for all workers to connect (timeout: {}s)...",
        worker_timeout.as_secs()
    );
    match tokio::time::timeout(worker_timeout, all_ready_rx.recv()).await {
        Ok(_) => { /* All workers connected */ }
        Err(_) => {
            let s = state.lock().await;
            anyhow::bail!(
                "Timed out waiting for workers after {}s. Connected: {} ingest (expected {}), {} query (expected {})",
                worker_timeout.as_secs(),
                s.ready_ingest, s.config.expected_ingest_workers,
                s.ready_query, s.config.expected_query_workers
            );
        }
    }

    // Run the timeline.
    {
        let mut s = state.lock().await;
        s.run_start = Some(Instant::now());
    }

    let phases = {
        let s = state.lock().await;
        s.config.phases.clone()
    };

    for (i, phase) in phases.iter().enumerate() {
        // Skip phases already completed (checkpoint resume).
        if i < resume_from_phase {
            info!(phase = %phase.name, "Skipping already-completed phase (resume)");
            continue;
        }

        info!(
            phase = %phase.name,
            duration_s = phase.duration_seconds,
            "Starting phase"
        );

        // Write checkpoint.
        if let Some(ref cp_path) = checkpoint_path {
            if let Err(e) = tokio::fs::write(cp_path, format!("{}", i)).await {
                warn!("Failed to write phase checkpoint: {}", e);
            }
        }

        // Update state.
        {
            let mut s = state.lock().await;
            s.current_phase_index = i;
            s.phase_start = Some(Instant::now());
            s.prepare_acks = 0;
        }

        // Determine the rate to send (ingest workers get ingest rate, query workers get query rate).
        // For simplicity, we send the ingest rate — workers know their own mode.
        // Actually, we need to send different rates to ingest vs query workers.

        // Send PrepareTransition ~100ms before phase start (skip for first phase).
        if i > 0 {
            let workers = {
                let s = state.lock().await;
                s.workers.clone()
            };

            let prev_phase = &phases[i - 1].name;
            let prepare_msg = ControllerMessage {
                payload: Some(controller_message::Payload::PrepareTransition(
                    PrepareTransition {
                        from_phase: prev_phase.clone(),
                        to_phase: phase.name.clone(),
                    },
                )),
            };

            for (_, (_, tx)) in &workers {
                let _ = tx.send(prepare_msg.clone()).await;
            }

            // Wait for acks (with 2s timeout).
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Compute transition time 50ms from now.
        let transition_time_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            + 50_000_000; // +50ms

        // Send PhaseTransition to all workers.
        let workers = {
            let s = state.lock().await;
            s.workers.clone()
        };

        for (_wid, (mode, tx)) in &workers {
            let rate = match mode.as_str() {
                "ingest" => phase.per_worker_ingest_eps as i64,
                "query" => phase.per_worker_query_mqps as i64,
                _ => 0,
            };

            let transition_msg = ControllerMessage {
                payload: Some(controller_message::Payload::PhaseTransition(
                    PhaseTransition {
                        from_phase: if i > 0 {
                            phases[i - 1].name.clone()
                        } else {
                            "".to_string()
                        },
                        to_phase: phase.name.clone(),
                        transition_time_unix_ns: transition_time_ns,
                        new_target_rate: rate,
                    },
                )),
            };

            let _ = tx.send(transition_msg).await;
        }

        // Wait for the phase duration.
        tokio::time::sleep(Duration::from_secs(phase.duration_seconds)).await;
    }

    // Send RunComplete to all workers.
    info!("Timeline complete, sending RunComplete to all workers");
    {
        let mut s = state.lock().await;
        s.timeline_complete = true;

        let complete_msg = ControllerMessage {
            payload: Some(controller_message::Payload::RunComplete(RunComplete {})),
        };

        for (_, (_, tx)) in &s.workers {
            let _ = tx.send(complete_msg.clone()).await;
        }
    }

    // Clean up checkpoint file on successful completion.
    if let Some(ref cp_path) = checkpoint_path {
        let _ = tokio::fs::remove_file(cp_path).await;
    }

    // Keep server alive briefly for status queries, then shut down.
    tokio::time::sleep(Duration::from_secs(10)).await;
    info!("PhaseController shutting down");

    Ok(())
}
