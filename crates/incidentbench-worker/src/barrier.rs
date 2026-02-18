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
    controller_message, phase_gate_service_client::PhaseGateServiceClient, worker_message,
    ControllerMessage, PrepareAck, WorkerDone, WorkerMessage, WorkerReady,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, info};

/// Phase transition event received from the PhaseController.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum PhaseEvent {
    /// Prepare for an upcoming phase transition.
    PrepareTransition {
        from_phase: String,
        to_phase: String,
    },
    /// Execute the phase transition at the given timestamp.
    Transition {
        from_phase: String,
        to_phase: String,
        transition_time_ns: i64,
        new_target_rate: i64,
    },
    /// The entire run is complete.
    RunComplete,
}

/// Client-side barrier participant.
///
/// Connects to the PhaseController via bidirectional gRPC streaming,
/// reports readiness, handles prepare/transition messages, and
/// delivers PhaseEvents to the worker loop.
pub struct BarrierClient {
    worker_id: String,
    worker_mode: String,
    worker_index: i32,
    event_rx: mpsc::Receiver<PhaseEvent>,
    /// Sender for outbound worker messages.
    outbound_tx: mpsc::Sender<WorkerMessage>,
}

impl BarrierClient {
    /// Connect to the PhaseController and start the bidirectional stream.
    /// Retries with exponential backoff if the controller isn't ready yet.
    pub async fn connect(
        phase_controller_addr: &str,
        worker_id: String,
        worker_mode: String,
        worker_index: i32,
    ) -> anyhow::Result<Self> {
        let max_retries = 30;
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(10);
        let mut channel = None;

        for attempt in 1..=max_retries {
            match Channel::from_shared(format!("http://{}", phase_controller_addr))
                .map(|ch| ch.connect_timeout(Duration::from_secs(5)))
            {
                Ok(endpoint) => match endpoint.connect().await {
                    Ok(ch) => {
                        if attempt > 1 {
                            info!(attempt, "Connected to PhaseController after retries");
                        }
                        channel = Some(ch);
                        break;
                    }
                    Err(e) => {
                        if attempt == max_retries {
                            return Err(anyhow::anyhow!(
                                "Failed to connect to PhaseController after {} attempts: {}",
                                max_retries,
                                e
                            ));
                        }
                        info!(
                            attempt,
                            max_retries,
                            backoff_s = backoff.as_secs(),
                            "PhaseController not ready, retrying: {}",
                            e
                        );
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                },
                Err(e) => return Err(e.into()),
            }
        }

        let channel = channel.unwrap();

        let mut client = PhaseGateServiceClient::new(channel);

        let (outbound_tx, outbound_rx) = mpsc::channel::<WorkerMessage>(32);
        let (event_tx, event_rx) = mpsc::channel::<PhaseEvent>(32);

        // Convert outbound_rx into a stream for tonic.
        let outbound_stream = tokio_stream::wrappers::ReceiverStream::new(outbound_rx);

        let response = client.phase_stream(outbound_stream).await?;
        let mut inbound: Streaming<ControllerMessage> = response.into_inner();

        // Send READY message.
        let ready_msg = WorkerMessage {
            worker_id: worker_id.clone(),
            worker_mode: worker_mode.clone(),
            worker_index,
            payload: Some(worker_message::Payload::Ready(WorkerReady {})),
        };
        outbound_tx.send(ready_msg).await?;
        info!("Connected to PhaseController, sent READY");

        // Spawn task to process inbound messages.
        let worker_id_clone = worker_id.clone();
        let worker_mode_clone = worker_mode.clone();
        let outbound_tx_clone = outbound_tx.clone();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.payload {
                    Some(controller_message::Payload::PrepareTransition(pt)) => {
                        debug!(
                            "Received PrepareTransition: {} -> {}",
                            pt.from_phase, pt.to_phase
                        );

                        // Send event to worker loop.
                        let _ = event_tx
                            .send(PhaseEvent::PrepareTransition {
                                from_phase: pt.from_phase.clone(),
                                to_phase: pt.to_phase.clone(),
                            })
                            .await;

                        // Send PrepareAck.
                        let ack = WorkerMessage {
                            worker_id: worker_id_clone.clone(),
                            worker_mode: worker_mode_clone.clone(),
                            worker_index,
                            payload: Some(worker_message::Payload::PrepareAck(PrepareAck {
                                phase: pt.from_phase,
                            })),
                        };
                        let _ = outbound_tx_clone.send(ack).await;
                    }
                    Some(controller_message::Payload::PhaseTransition(pt)) => {
                        info!(
                            "Received PhaseTransition: {} -> {} at ns={}",
                            pt.from_phase, pt.to_phase, pt.transition_time_unix_ns
                        );
                        let _ = event_tx
                            .send(PhaseEvent::Transition {
                                from_phase: pt.from_phase,
                                to_phase: pt.to_phase,
                                transition_time_ns: pt.transition_time_unix_ns,
                                new_target_rate: pt.new_target_rate,
                            })
                            .await;
                    }
                    Some(controller_message::Payload::RunComplete(_)) => {
                        info!("Received RunComplete");
                        let _ = event_tx.send(PhaseEvent::RunComplete).await;
                        break;
                    }
                    None => {}
                }
            }
            debug!("Inbound stream ended");
        });

        Ok(Self {
            worker_id,
            worker_mode,
            worker_index,
            event_rx,
            outbound_tx,
        })
    }

    /// Wait for the next phase event from the controller.
    pub async fn next_event(&mut self) -> Option<PhaseEvent> {
        self.event_rx.recv().await
    }

    /// Wait for a PhaseTransition event, sleeping until the transition time.
    /// Returns the new phase name and target rate.
    pub async fn wait_for_transition(&mut self) -> Option<(String, i64)> {
        loop {
            match self.next_event().await? {
                PhaseEvent::PrepareTransition { .. } => {
                    // PrepareTransition is handled by finishing in-flight work.
                    // The actual transition comes next.
                    continue;
                }
                PhaseEvent::Transition {
                    to_phase,
                    transition_time_ns,
                    new_target_rate,
                    ..
                } => {
                    // Sleep until the transition time.
                    let now_ns = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64;
                    let wait_ns = transition_time_ns - now_ns;
                    if wait_ns > 0 {
                        sleep(Duration::from_nanos(wait_ns as u64)).await;
                    }
                    return Some((to_phase, new_target_rate));
                }
                PhaseEvent::RunComplete => return None,
            }
        }
    }

    /// Signal that this worker is done (all phases complete).
    pub async fn send_done(&self) -> anyhow::Result<()> {
        let msg = WorkerMessage {
            worker_id: self.worker_id.clone(),
            worker_mode: self.worker_mode.clone(),
            worker_index: self.worker_index,
            payload: Some(worker_message::Payload::Done(WorkerDone {})),
        };
        self.outbound_tx.send(msg).await?;
        Ok(())
    }
}
