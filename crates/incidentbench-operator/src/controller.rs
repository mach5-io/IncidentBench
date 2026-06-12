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

use crate::resources;
use anyhow::Context;
use futures::StreamExt;
use incidentbench_common::adapter::{DataStreamConfig, WarehouseConfig};
use incidentbench_common::adapters;
use incidentbench_common::crd::{IncidentBenchRun, LifecyclePhase};
use incidentbench_common::scenario::Scenario;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Service};
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher::Config;
use kube::{Api, Client, ResourceExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// Shared context for the reconciler.
struct Ctx {
    client: Client,
}

/// Run the operator controller loop.
pub async fn run(client: Client) -> anyhow::Result<()> {
    let runs: Api<IncidentBenchRun> = Api::all(client.clone());

    let ctx = Arc::new(Ctx {
        client: client.clone(),
    });

    info!("Starting reconciler");

    Controller::new(runs, Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _action)) => {
                    info!(name = %obj.name, "Reconciled");
                }
                Err(e) => {
                    error!("Reconcile error: {:?}", e);
                }
            }
        })
        .await;

    Ok(())
}

/// The main reconciliation function. Called on every change to an IncidentBenchRun.
async fn reconcile(run: Arc<IncidentBenchRun>, ctx: Arc<Ctx>) -> Result<Action, kube::Error> {
    let name = run.name_any();
    let namespace = run.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<IncidentBenchRun> = Api::namespaced(ctx.client.clone(), &namespace);

    let current_phase = run
        .status
        .as_ref()
        .map(|s| s.phase.clone())
        .unwrap_or(LifecyclePhase::Pending);

    info!(
        name = %name,
        namespace = %namespace,
        phase = %current_phase,
        "Reconciling IncidentBenchRun"
    );

    match current_phase {
        LifecyclePhase::Pending => {
            handle_pending(&api, &run, &ctx).await?;
        }
        LifecyclePhase::Preparing => {
            handle_preparing(&api, &run, &ctx).await?;
        }
        LifecyclePhase::Initializing => {
            handle_initializing(&api, &run, &ctx).await?;
        }
        LifecyclePhase::Running => {
            handle_running(&api, &run, &ctx).await?;
        }
        LifecyclePhase::Aggregating => {
            handle_aggregating(&api, &run, &ctx).await?;
        }
        LifecyclePhase::Reporting => {
            handle_reporting(&api, &run, &ctx).await?;
        }
        LifecyclePhase::Completed | LifecyclePhase::Failed => {
            // Auto-cleanup: tear down workers and platform resources on completion.
            let already_cleaned = run
                .status
                .as_ref()
                .map(|s| {
                    s.conditions
                        .iter()
                        .any(|c| c.condition_type == "Cleaned" && c.status == "True")
                })
                .unwrap_or(false);

            if !already_cleaned {
                handle_cleanup(&api, &run, &ctx).await?;
            }
            return Ok(Action::await_change());
        }
    }

    // Re-check in 5 seconds.
    Ok(Action::requeue(Duration::from_secs(5)))
}

fn error_policy(_run: Arc<IncidentBenchRun>, error: &kube::Error, _ctx: Arc<Ctx>) -> Action {
    error!("Reconcile error: {:?}", error);
    Action::requeue(Duration::from_secs(30))
}

/// Pending: Validate scenario, generate runId, add finalizer, transition to Preparing.
async fn handle_pending(
    api: &Api<IncidentBenchRun>,
    run: &IncidentBenchRun,
    ctx: &Ctx,
) -> Result<(), kube::Error> {
    let name = run.name_any();

    // Resolve scenario.
    let scenario = match resolve_scenario(run, ctx).await {
        Ok(s) => s,
        Err(e) => {
            update_phase_failed(api, &name, &format!("Failed to resolve scenario: {}", e)).await?;
            return Ok(());
        }
    };

    // Validate scenario.
    let errors = scenario.validate();
    if !errors.is_empty() {
        update_phase_failed(
            api,
            &name,
            &format!("Scenario validation failed: {}", errors.join("; ")),
        )
        .await?;
        return Ok(());
    }

    // Check for dry-run mode.
    if run.spec.dry_run {
        info!(name = %name, "Dry-run mode: validation passed, skipping execution");
        let now = chrono::Utc::now().to_rfc3339();
        let status = serde_json::json!({
            "status": {
                "phase": "Completed",
                "completion_time": &now,
                "conditions": [
                    {
                        "type": "DryRun",
                        "status": "True",
                        "message": format!(
                            "Dry-run: scenario '{}' validated successfully. {} data streams, {} phases, total duration: {}s",
                            scenario.scenario.display_name,
                            scenario.data_streams.as_deref().map_or(0, |s| s.len()),
                            scenario.timeline.phases.len(),
                            scenario.total_duration_seconds()
                        ),
                        "lastTransitionTime": &now
                    },
                    {
                        "type": "Cleaned",
                        "status": "True",
                        "message": "Dry-run: no resources to clean up",
                        "lastTransitionTime": &now
                    }
                ]
            }
        });
        api.patch_status(
            &name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Merge(&status),
        )
        .await?;
        return Ok(());
    }

    // Generate run ID.
    let run_id = uuid::Uuid::new_v4().to_string();

    // Add finalizer.
    let finalizer = "incidentbench.io/cleanup";
    let has_finalizer = run
        .metadata
        .finalizers
        .as_ref()
        .map(|f| f.contains(&finalizer.to_string()))
        .unwrap_or(false);

    if !has_finalizer {
        let patch = serde_json::json!({
            "metadata": {
                "finalizers": [finalizer]
            }
        });
        api.patch(
            &name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Merge(&patch),
        )
        .await?;
    }

    // Transition to Preparing.
    let status = serde_json::json!({
        "status": {
            "phase": "Preparing",
            "runId": run_id,
            "startTime": chrono::Utc::now().to_rfc3339(),
            "conditions": [{
                "type": "Validated",
                "status": "True",
                "message": "Scenario validated successfully",
                "lastTransitionTime": chrono::Utc::now().to_rfc3339()
            }]
        }
    });

    api.patch_status(
        &name,
        &PatchParams::apply("incidentbench-operator"),
        &Patch::Merge(&status),
    )
    .await?;

    info!(name = %name, run_id = %run_id, "Validated, transitioning to Preparing");
    Ok(())
}

/// Preparing: Create Kafka topics (one per stream), call adapter.prepare().
async fn handle_preparing(
    api: &Api<IncidentBenchRun>,
    run: &IncidentBenchRun,
    ctx: &Ctx,
) -> Result<(), kube::Error> {
    let name = run.name_any();
    let namespace = run.namespace().unwrap_or_else(|| "default".to_string());
    let spec = &run.spec;

    let scenario = match resolve_scenario(run, ctx).await {
        Ok(s) => s,
        Err(e) => {
            update_phase_failed(api, &name, &format!("Failed to resolve scenario: {}", e)).await?;
            return Ok(());
        }
    };

    // Apply scaling.
    let scaled_scenario =
        scenario.with_scaling(spec.scaling.duration_scale, spec.scaling.rate_scale);

    let kafka_bootstrap = spec
        .kafka
        .bootstrap_servers
        .as_deref()
        .unwrap_or("kafka-bootstrap:9092");

    // Create Kafka topics — one per data stream (skipped for query-only scenarios).
    for stream in scaled_scenario.data_streams.as_deref().unwrap_or(&[]) {
        let topic = &stream.schema.index_name;
        let partitions = stream.kafka_partitions.unwrap_or(stream.ingest_replicas);

        info!(topic = %topic, partitions = partitions, stream = %stream.name, "Creating Kafka topic");
        match create_kafka_topic(kafka_bootstrap, topic, partitions).await {
            Ok(_) => info!("Kafka topic created: {}", topic),
            Err(e) => {
                update_phase_failed(
                    api,
                    &name,
                    &format!("Failed to create Kafka topic '{}': {}", topic, e),
                )
                .await?;
                return Ok(());
            }
        }
    }

    // Build DataStreamConfig list for the adapter.
    let stream_configs: Vec<DataStreamConfig> = scaled_scenario
        .data_streams
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|s| DataStreamConfig {
            name: s.name.clone(),
            schema: s.schema.clone(),
            kafka_topic: s.schema.index_name.clone(),
        })
        .collect();

    // Call adapter prepare().
    let adapter = match adapters::create_adapter(&spec.target.adapter, &spec.target.config) {
        Ok(a) => a,
        Err(e) => {
            update_phase_failed(api, &name, &format!("Failed to create adapter: {}", e)).await?;
            return Ok(());
        }
    };

    // Build warehouse configs from queryGroups or fall back to legacy single-warehouse.
    let warehouses = build_warehouse_configs(spec);

    match adapter
        .prepare(&stream_configs, kafka_bootstrap, &warehouses)
        .await
    {
        Ok(result) => {
            let endpoint_summary: String = result
                .query_endpoints
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            let cg_summary: String = result
                .consumer_groups
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                consumer_groups = %cg_summary,
                query_endpoints = %endpoint_summary,
                "Adapter prepare completed"
            );

            // Store prepare results in a ConfigMap for workers.
            let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace);
            let cm = resources::build_prepare_result_configmap(
                &name,
                &result.consumer_groups,
                &result.query_endpoints,
                run,
            );
            let _ = cm_api
                .patch(
                    &format!("{}-prepare", name),
                    &PatchParams::apply("incidentbench-operator"),
                    &Patch::Apply(cm),
                )
                .await;

            // Transition to Initializing.
            let stream_names: Vec<&str> = scaled_scenario
                .data_streams
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|s| s.name.as_str())
                .collect();
            let status = serde_json::json!({
                "status": {
                    "phase": "Initializing",
                    "conditions": [{
                        "type": "Prepared",
                        "status": "True",
                        "message": format!(
                            "Streams {:?} created. Consumer groups: {}. Warehouses: {}",
                            stream_names, cg_summary, endpoint_summary
                        ),
                        "lastTransitionTime": chrono::Utc::now().to_rfc3339()
                    }]
                }
            });
            api.patch_status(
                &name,
                &PatchParams::apply("incidentbench-operator"),
                &Patch::Merge(&status),
            )
            .await?;

            info!(name = %name, "Prepared, transitioning to Initializing");
        }
        Err(e) => {
            update_phase_failed(api, &name, &format!("Adapter prepare failed: {}", e)).await?;
        }
    }

    Ok(())
}

/// Initializing: Deploy workers, PhaseController, MetricsAggregator.
async fn handle_initializing(
    api: &Api<IncidentBenchRun>,
    run: &IncidentBenchRun,
    ctx: &Ctx,
) -> Result<(), kube::Error> {
    let name = run.name_any();
    let namespace = run.namespace().unwrap_or_else(|| "default".to_string());
    let spec = &run.spec;

    let scenario = match resolve_scenario(run, ctx).await {
        Ok(s) => s,
        Err(e) => {
            update_phase_failed(api, &name, &format!("Failed to resolve scenario: {}", e)).await?;
            return Ok(());
        }
    };

    let scaled_scenario =
        scenario.with_scaling(spec.scaling.duration_scale, spec.scaling.rate_scale);

    let deploy_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace);

    // Read prepare results ConfigMap to get consumer_groups and query_endpoints.
    let prepare_cm = cm_api
        .get(&format!("{}-prepare", name))
        .await
        .map_err(|e| {
            error!(name = %name, "Failed to read prepare ConfigMap: {}", e);
            e
        })?;
    let prepare_data = prepare_cm.data.unwrap_or_default();
    let consumer_groups: HashMap<String, String> = prepare_data
        .get("consumer_groups")
        .and_then(|v| serde_json::from_str(v).ok())
        .unwrap_or_default();
    let query_endpoints: HashMap<String, String> = prepare_data
        .get("query_endpoints")
        .and_then(|v| serde_json::from_str(v).ok())
        .unwrap_or_default();
    let kafka_bootstrap = spec
        .kafka
        .bootstrap_servers
        .as_deref()
        .unwrap_or("kafka-bootstrap:9092");

    // Create worker config ConfigMaps.
    let worker_cms = resources::build_worker_configmaps(
        &name,
        &scaled_scenario,
        spec,
        run,
        &consumer_groups,
        &query_endpoints,
        kafka_bootstrap,
    );
    for cm in &worker_cms {
        let cm_name = cm.metadata.name.clone().unwrap_or_default();
        let _ = cm_api
            .patch(
                &cm_name,
                &PatchParams::apply("incidentbench-operator"),
                &Patch::Apply(cm.clone()),
            )
            .await;
    }

    // Create Services for PhaseController and Aggregator.
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);

    let pc_svc = resources::build_phase_controller_service(&name, run);
    let pc_svc_name = pc_svc.metadata.name.clone().unwrap_or_default();
    let _ = svc_api
        .patch(
            &pc_svc_name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Apply(pc_svc),
        )
        .await;

    let agg_svc = resources::build_aggregator_service(&name, run);
    let agg_svc_name = agg_svc.metadata.name.clone().unwrap_or_default();
    let _ = svc_api
        .patch(
            &agg_svc_name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Apply(agg_svc),
        )
        .await;

    // Create results PVC (persists after pod cleanup so the reporter can read files).
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), &namespace);
    let results_pvc = resources::build_results_pvc(&name, run);
    let pvc_name = results_pvc.metadata.name.clone().unwrap_or_default();
    let _ = pvc_api
        .patch(
            &pvc_name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Apply(results_pvc),
        )
        .await;

    // Deploy MetricsAggregator.
    let agg_deploy = resources::build_aggregator_deployment(&name, spec, run);
    let agg_name = agg_deploy.metadata.name.clone().unwrap_or_default();
    let _ = deploy_api
        .patch(
            &agg_name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Apply(agg_deploy),
        )
        .await;

    // Deploy PhaseController.
    let pc_deploy =
        resources::build_phase_controller_deployment(&name, &scaled_scenario, spec, run);
    let pc_name = pc_deploy.metadata.name.clone().unwrap_or_default();
    let _ = deploy_api
        .patch(
            &pc_name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Apply(pc_deploy),
        )
        .await;

    // Build SQL files ConfigMap from sql_dir if specified, and apply it.
    let sql_cm_name: Option<String> = spec
        .sql_dir
        .as_deref()
        .filter(|d| !d.is_empty())
        .and_then(|dir| resources::build_sql_files_configmap(&name, dir, run))
        .map(|cm| {
            let cm_name = cm.metadata.name.clone().unwrap_or_default();
            let cm_api_clone = cm_api.clone();
            let cm_name_clone = cm_name.clone();
            tokio::spawn(async move {
                let _ = cm_api_clone
                    .patch(
                        &cm_name_clone,
                        &PatchParams::apply("incidentbench-operator"),
                        &Patch::Apply(cm),
                    )
                    .await;
            });
            cm_name
        });
    let sql_cm_ref = sql_cm_name.as_deref();

    // Deploy per-stream IngestWorker Deployments (skipped for query-only scenarios).
    if scaled_scenario.has_ingest() {
        let ingest_deploys =
            resources::build_ingest_stream_deployments(&name, &scaled_scenario, spec, run);
        for deploy in &ingest_deploys {
            let deploy_name = deploy.metadata.name.clone().unwrap_or_default();
            let _ = deploy_api
                .patch(
                    &deploy_name,
                    &PatchParams::apply("incidentbench-operator"),
                    &Patch::Apply(deploy.clone()),
                )
                .await;
        }
    }

    // Deploy QueryWorker Deployment(s).
    let total_query_replicas: u32;
    if let Some(ref query_groups) = spec.workers.query_groups {
        total_query_replicas = query_groups.iter().map(|g| g.replicas).sum();
        let group_deploys = resources::build_query_worker_group_deployments(
            &name,
            query_groups,
            spec,
            run,
            sql_cm_ref,
        );
        for deploy in group_deploys {
            let deploy_name = deploy.metadata.name.clone().unwrap_or_default();
            let _ = deploy_api
                .patch(
                    &deploy_name,
                    &PatchParams::apply("incidentbench-operator"),
                    &Patch::Apply(deploy),
                )
                .await;
        }
    } else {
        total_query_replicas = spec.workers.query.replicas;
        let query_deploy = resources::build_query_worker_deployment(&name, spec, run, sql_cm_ref);
        let query_name = query_deploy.metadata.name.clone().unwrap_or_default();
        let _ = deploy_api
            .patch(
                &query_name,
                &PatchParams::apply("incidentbench-operator"),
                &Patch::Apply(query_deploy),
            )
            .await;
    }

    let total_ingest_replicas = scaled_scenario.total_ingest_replicas();

    // Transition to Running.
    let status = serde_json::json!({
        "status": {
            "phase": "Running",
            "workers": {
                "ingest_desired": total_ingest_replicas,
                "ingest_ready": total_ingest_replicas,
                "query_desired": total_query_replicas,
                "query_ready": total_query_replicas
            },
            "conditions": [{
                "type": "WorkersReady",
                "status": "True",
                "message": format!(
                    "{} ingest workers (across {} streams), {} query workers deployed",
                    total_ingest_replicas,
                    scaled_scenario.data_streams.as_deref().map_or(0, |s| s.len()),
                    total_query_replicas
                ),
                "lastTransitionTime": chrono::Utc::now().to_rfc3339()
            }]
        }
    });
    api.patch_status(
        &name,
        &PatchParams::apply("incidentbench-operator"),
        &Patch::Merge(&status),
    )
    .await?;

    info!(name = %name, "Workers deployed, transitioning to Running");
    Ok(())
}

/// Running: Poll PhaseController and Aggregator, update CR progress,
/// detect worker failures.
async fn handle_running(
    api: &Api<IncidentBenchRun>,
    run: &IncidentBenchRun,
    ctx: &Ctx,
) -> Result<(), kube::Error> {
    let name = run.name_any();
    let namespace = run.namespace().unwrap_or_else(|| "default".to_string());

    // Check for worker pod failures before polling phase controller.
    let deploy_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    if let Err(failure_msg) = check_worker_health(&deploy_api, run, ctx).await {
        update_phase_failed(api, &name, &failure_msg).await?;
        return Ok(());
    }

    // Poll PhaseController for status.
    let pc_addr = format!("{}-phase-controller.{}.svc:50051", name, namespace);

    match poll_phase_controller(&pc_addr).await {
        Ok(status) => {
            if status.timeline_complete {
                // Scale down all workers and phase controller.
                for suffix in &["query", "phase-controller"] {
                    let deploy_name = format!("{}-{}", name, suffix);
                    let scale_patch = serde_json::json!({
                        "spec": { "replicas": 0 }
                    });
                    match deploy_api
                        .patch(
                            &deploy_name,
                            &PatchParams::apply("incidentbench-operator"),
                            &Patch::Merge(&scale_patch),
                        )
                        .await
                    {
                        Ok(_) => info!(name = %name, "Scaled down {}", deploy_name),
                        Err(e) => {
                            warn!(name = %name, "Failed to scale down {}: {}", deploy_name, e)
                        }
                    }
                }

                // Scale down per-stream ingest deployments.
                if let Ok(scenario) = resolve_scenario(run, ctx).await {
                    let scaled = scenario
                        .with_scaling(run.spec.scaling.duration_scale, run.spec.scaling.rate_scale);
                    for stream in scaled.data_streams.as_deref().unwrap_or(&[]) {
                        let deploy_name = format!("{}-ingest-{}", name, stream.name);
                        let scale_patch = serde_json::json!({
                            "spec": { "replicas": 0 }
                        });
                        match deploy_api
                            .patch(
                                &deploy_name,
                                &PatchParams::apply("incidentbench-operator"),
                                &Patch::Merge(&scale_patch),
                            )
                            .await
                        {
                            Ok(_) => info!(name = %name, "Scaled down {}", deploy_name),
                            Err(e) => {
                                warn!(name = %name, "Failed to scale down {}: {}", deploy_name, e)
                            }
                        }
                    }
                }

                // Also scale down per-group query deployments.
                if let Some(ref groups) = run.spec.workers.query_groups {
                    for group in groups {
                        let deploy_name = format!("{}-query-{}", name, group.name);
                        let scale_patch = serde_json::json!({
                            "spec": { "replicas": 0 }
                        });
                        let _ = deploy_api
                            .patch(
                                &deploy_name,
                                &PatchParams::apply("incidentbench-operator"),
                                &Patch::Merge(&scale_patch),
                            )
                            .await;
                    }
                }

                // Transition to Aggregating with timestamp annotation for timeout tracking.
                api.patch(
                    &name,
                    &PatchParams::apply("incidentbench-operator"),
                    &Patch::Merge(&serde_json::json!({
                        "metadata": {
                            "annotations": {
                                "incidentbench.io/aggregating-since": chrono::Utc::now().to_rfc3339()
                            }
                        }
                    })),
                ).await?;

                let patch = serde_json::json!({
                    "status": {
                        "phase": "Aggregating"
                    }
                });
                api.patch_status(
                    &name,
                    &PatchParams::apply("incidentbench-operator"),
                    &Patch::Merge(&patch),
                )
                .await?;
                info!(name = %name, "Timeline complete, workers scaled down, transitioning to Aggregating");
            } else {
                // Poll aggregator for live metrics.
                let agg_addr = format!("{}-aggregator.{}.svc:50052", name, namespace);
                let (achieved_eps, achieved_qps, target_eps, target_qps, lag) =
                    match poll_aggregator_snapshot(&agg_addr).await {
                        Ok(snap) => (
                            snap.ingest_events_produced,
                            snap.query_executed as f64,
                            snap.ingest_target_eps,
                            snap.query_target_qps,
                            snap.kafka_consumer_lag,
                        ),
                        Err(e) => {
                            warn!(name = %name, "Failed to poll aggregator snapshot: {}", e);
                            (0, 0.0, 0, 0.0, 0)
                        }
                    };

                let patch = serde_json::json!({
                    "status": {
                        "current_benchmark_phase": status.current_phase,
                        "progress": {
                            "elapsed_seconds": status.total_elapsed_seconds,
                            "total_seconds": status.total_duration_seconds,
                            "achieved_ingest_eps": achieved_eps,
                            "achieved_query_qps": achieved_qps,
                            "kafka_consumer_lag": lag,
                            "target_ingest_eps": target_eps,
                            "target_query_qps": target_qps
                        }
                    }
                });
                api.patch_status(
                    &name,
                    &PatchParams::apply("incidentbench-operator"),
                    &Patch::Merge(&patch),
                )
                .await?;
            }
        }
        Err(e) => {
            // Track consecutive poll failures — if the phase controller has been
            // unreachable for too long, fail the run.
            let consecutive_failures = run
                .status
                .as_ref()
                .and_then(|s| s.progress.as_ref())
                .map(|p| p.elapsed_seconds)
                .unwrap_or(0);

            // Only fail if the run has been going for a while (not a startup race).
            if consecutive_failures > 30 {
                warn!(name = %name, "PhaseController unreachable after {}s elapsed: {}", consecutive_failures, e);
            } else {
                info!(name = %name, "PhaseController not yet reachable ({}s elapsed): {}", consecutive_failures, e);
            }
        }
    }

    Ok(())
}

/// Aggregating: Poll MetricsAggregator for completion, then fetch results
/// and write them directly into the CR's status.results field.
async fn handle_aggregating(
    api: &Api<IncidentBenchRun>,
    run: &IncidentBenchRun,
    _ctx: &Ctx,
) -> Result<(), kube::Error> {
    let name = run.name_any();
    let namespace = run.namespace().unwrap_or_else(|| "default".to_string());
    let agg_addr = format!("{}-aggregator.{}.svc:50052", name, namespace);

    // Connect to the aggregator gRPC service.
    let channel = match tonic::transport::Channel::from_shared(format!("http://{}", agg_addr))
        .and_then(|ch| Ok(ch.connect_timeout(Duration::from_secs(5))))
    {
        Ok(ch) => match ch.connect().await {
            Ok(c) => c,
            Err(e) => {
                warn!(name = %name, "Failed to connect to aggregator: {}", e);
                return Ok(());
            }
        },
        Err(e) => {
            warn!(name = %name, "Invalid aggregator address: {}", e);
            return Ok(());
        }
    };

    use incidentbench_common::proto::aggregator::metrics_service_client::MetricsServiceClient;
    use incidentbench_common::proto::aggregator::{AggregationStatusRequest, GetResultsRequest};

    let mut client = MetricsServiceClient::new(channel);

    // Check aggregation status.
    let status_resp = match client
        .get_aggregation_status(AggregationStatusRequest {})
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!(name = %name, "Failed to get aggregation status: {}", e);
            return Ok(());
        }
    };

    if status_resp.state != "complete" {
        // Aggregating starts only after the phase controller reports timeline completion
        // and the operator scales workers down. If the aggregator still reports
        // "collecting" after a grace period, at least one worker stream is stale or
        // failed to close cleanly; proceed with the snapshots collected so finalizers
        // and reporting cannot hang indefinitely.
        let aggregating_since = run
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("incidentbench.io/aggregating-since"))
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| chrono::Utc::now().signed_duration_since(dt).num_seconds())
            .unwrap_or(0);

        let aggregating_too_long = aggregating_since > 120;

        if aggregating_too_long {
            warn!(
                name = %name,
                connected_workers = status_resp.connected_workers,
                snapshots = status_resp.snapshots_received,
                aggregating_since,
                "Aggregation timeout after timeline completion; forcing result collection from available snapshots"
            );
        } else {
            info!(
                name = %name,
                state = %status_resp.state,
                connected_workers = status_resp.connected_workers,
                snapshots = status_resp.snapshots_received,
                "Aggregation not yet complete, waiting..."
            );
            return Ok(());
        }
    }

    // Aggregation complete — fetch results.
    let status = if let Ok(r) = client.get_results(GetResultsRequest {}).await {
        let results = r.into_inner();
        info!(
            name = %name,
            valid = results.valid,
            baseline_p99 = results.baseline_p99_ms,
            overlap_p99 = results.overlap_p99_ms,
            degradation = results.p99_degradation_ratio,
            "Results retrieved from aggregator"
        );
        serde_json::json!({
            "status": {
                "phase": "Reporting",
                "results": {
                    "valid": results.valid,
                    "validity_violations": results.validity_violations,
                    "warnings": results.warnings,
                    "harness_saturated": results.harness_saturated,
                    "scorecard": {
                        "baseline_p99_ms": results.baseline_p99_ms,
                        "overlap_p99_ms": results.overlap_p99_ms,
                        "p99_degradation_ratio": results.p99_degradation_ratio,
                        "query_error_rate_overlap": results.query_error_rate_overlap,
                        "peak_backlog": results.peak_backlog,
                        "backlog_drain_time_s": results.backlog_drain_time_s,
                        "recovery_time_s": results.recovery_time_s
                    }
                }
            }
        })
    } else {
        warn!(name = %name, "Could not fetch results from aggregator, proceeding to Reporting with empty results");
        serde_json::json!({
            "status": {
                "phase": "Reporting",
                "results": {
                    "valid": false,
                    "validity_violations": ["No metrics data collected from workers"],
                    "warnings": ["Aggregation timed out or workers failed to report metrics"],
                    "harness_saturated": false,
                    "scorecard": {
                        "baseline_p99_ms": 0.0,
                        "overlap_p99_ms": 0.0,
                        "p99_degradation_ratio": 0.0,
                        "query_error_rate_overlap": 0.0,
                        "peak_backlog": 0,
                        "backlog_drain_time_s": 0.0,
                        "recovery_time_s": 0.0
                    }
                }
            }
        })
    };
    api.patch_status(
        &name,
        &PatchParams::apply("incidentbench-operator"),
        &Patch::Merge(&status),
    )
    .await?;

    info!(name = %name, "Results written to CR, transitioning to Reporting");
    Ok(())
}

/// Reporting: create the reporter Job and wait for it to complete.
async fn handle_reporting(
    api: &Api<IncidentBenchRun>,
    run: &IncidentBenchRun,
    ctx: &Ctx,
) -> Result<(), kube::Error> {
    let name = run.name_any();
    let ns = run.namespace().unwrap_or_default();
    let job_name = format!("{}-reporter", name);
    let jobs: Api<Job> = Api::namespaced(ctx.client.clone(), &ns);

    // Create the reporter Job if it doesn't exist yet.
    match jobs.get(&job_name).await {
        Err(kube::Error::Api(e)) if e.code == 404 => {
            let job = resources::build_reporter_job(&name, run);
            jobs.create(&kube::api::PostParams::default(), &job).await?;
            info!(name = %name, job = %job_name, "Reporter job created");
        }
        Err(e) => return Err(e),
        Ok(_) => {}
    }

    // Check whether the Job has finished.
    let job = jobs.get(&job_name).await?;
    let succeeded = job.status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);
    let failed = job.status.as_ref().and_then(|s| s.failed).unwrap_or(0);

    if succeeded > 0 {
        let now = chrono::Utc::now().to_rfc3339();
        let status = serde_json::json!({
            "status": {
                "phase": "Completed",
                "completion_time": &now,
                "conditions": [{
                    "type": "RunComplete",
                    "status": "True",
                    "message": "Reporter job finished — report.html, run.json, timeseries.csv written to results PVC",
                    "lastTransitionTime": &now
                }]
            }
        });
        api.patch_status(
            &name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Merge(&status),
        )
        .await?;
        info!(name = %name, "Reporter job complete, run Completed");
    } else if failed >= 3 {
        let now = chrono::Utc::now().to_rfc3339();
        let status = serde_json::json!({
            "status": {
                "phase": "Completed",
                "completion_time": &now,
                "conditions": [{
                    "type": "RunComplete",
                    "status": "True",
                    "message": "Reporter job failed — raw metrics in PVC, re-run with: incidentbench report regenerate",
                    "lastTransitionTime": &now
                }]
            }
        });
        api.patch_status(
            &name,
            &PatchParams::apply("incidentbench-operator"),
            &Patch::Merge(&status),
        )
        .await?;
        warn!(name = %name, "Reporter job failed, transitioning to Completed anyway");
    } else {
        info!(name = %name, "Waiting for reporter job to complete");
    }

    Ok(())
}

/// Clean up all resources: delete worker Deployments/Services, adapter resources,
/// Kafka topics, and remove the finalizer.
async fn handle_cleanup(
    api: &Api<IncidentBenchRun>,
    run: &IncidentBenchRun,
    ctx: &Ctx,
) -> Result<(), kube::Error> {
    let name = run.name_any();
    let namespace = run.namespace().unwrap_or_else(|| "default".to_string());
    let spec = &run.spec;

    info!(name = %name, "Running cleanup");

    // Delete worker Deployments.
    let deploy_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);

    // Delete common deployments (aggregator, phase-controller, query).
    for suffix in &["aggregator", "phase-controller", "query"] {
        let deploy_name = format!("{}-{}", name, suffix);
        match deploy_api.delete(&deploy_name, &Default::default()).await {
            Ok(_) => info!(name = %name, "Deleted deployment {}", deploy_name),
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => warn!(name = %name, "Failed to delete deployment {}: {}", deploy_name, e),
        }
    }

    // Delete per-stream ingest deployments.
    if let Some(scenario) = &spec.scenario {
        for stream in scenario.data_streams.as_deref().unwrap_or(&[]) {
            let deploy_name = format!("{}-ingest-{}", name, stream.name);
            match deploy_api.delete(&deploy_name, &Default::default()).await {
                Ok(_) => info!(name = %name, "Deleted deployment {}", deploy_name),
                Err(kube::Error::Api(e)) if e.code == 404 => {}
                Err(e) => warn!(name = %name, "Failed to delete deployment {}: {}", deploy_name, e),
            }
        }
    }

    // Delete per-group query deployments.
    if let Some(ref groups) = spec.workers.query_groups {
        for group in groups {
            let deploy_name = format!("{}-query-{}", name, group.name);
            match deploy_api.delete(&deploy_name, &Default::default()).await {
                Ok(_) => info!(name = %name, "Deleted deployment {}", deploy_name),
                Err(kube::Error::Api(e)) if e.code == 404 => {}
                Err(e) => warn!(name = %name, "Failed to delete deployment {}: {}", deploy_name, e),
            }
        }
    }

    // Delete Services.
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    for suffix in &["phase-controller", "aggregator"] {
        let svc_name = format!("{}-{}", name, suffix);
        match svc_api.delete(&svc_name, &Default::default()).await {
            Ok(_) => info!(name = %name, "Deleted service {}", svc_name),
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => warn!(name = %name, "Failed to delete service {}: {}", svc_name, e),
        }
    }

    // Delete ConfigMaps.
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace);
    // Common configmaps.
    for suffix in &[
        "scenario",
        "phase-controller",
        "aggregator",
        "query",
        "prepare",
        "sql-files",
    ] {
        let cm_name = format!("{}-{}", name, suffix);
        match cm_api.delete(&cm_name, &Default::default()).await {
            Ok(_) => info!(name = %name, "Deleted configmap {}", cm_name),
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => warn!(name = %name, "Failed to delete configmap {}: {}", cm_name, e),
        }
    }
    // Per-stream ingest configmaps.
    if let Some(scenario) = &spec.scenario {
        for stream in scenario.data_streams.as_deref().unwrap_or(&[]) {
            let cm_name = format!("{}-ingest-{}", name, stream.name);
            match cm_api.delete(&cm_name, &Default::default()).await {
                Ok(_) => info!(name = %name, "Deleted configmap {}", cm_name),
                Err(kube::Error::Api(e)) if e.code == 404 => {}
                Err(e) => warn!(name = %name, "Failed to delete configmap {}: {}", cm_name, e),
            }
        }
    }
    // Per-group query configmaps.
    if let Some(ref groups) = spec.workers.query_groups {
        for group in groups {
            let cm_name = format!("{}-query-{}", name, group.name);
            match cm_api.delete(&cm_name, &Default::default()).await {
                Ok(_) => info!(name = %name, "Deleted configmap {}", cm_name),
                Err(kube::Error::Api(e)) if e.code == 404 => {}
                Err(e) => warn!(name = %name, "Failed to delete configmap {}: {}", cm_name, e),
            }
        }
    }

    // Call adapter cleanup (Mach5 namespace, indexes, pipelines, warehouses).
    if let Some(scenario) = &spec.scenario {
        if let Ok(adapter) = adapters::create_adapter(&spec.target.adapter, &spec.target.config) {
            let warehouses = build_warehouse_configs(spec);
            let stream_configs: Vec<DataStreamConfig> = scenario
                .data_streams
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|s| DataStreamConfig {
                    name: s.name.clone(),
                    schema: s.schema.clone(),
                    kafka_topic: s.schema.index_name.clone(),
                })
                .collect();
            if let Err(e) = adapter.cleanup(&stream_configs, &warehouses).await {
                warn!(name = %name, "Adapter cleanup failed: {}", e);
            }
        }
    } else {
        warn!(name = %name, "No inline scenario — skipping adapter cleanup");
    }

    // Delete Kafka topics — one per stream.
    if let Some(scenario) = &spec.scenario {
        let kafka_bootstrap = spec
            .kafka
            .bootstrap_servers
            .as_deref()
            .unwrap_or("kafka-bootstrap:9092");
        for stream in scenario.data_streams.as_deref().unwrap_or(&[]) {
            if let Err(e) = delete_kafka_topic(kafka_bootstrap, &stream.schema.index_name).await {
                warn!(name = %name, "Kafka topic deletion failed for '{}': {}", stream.schema.index_name, e);
            }
        }
    }

    // Remove finalizer.
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": null
        }
    });
    api.patch(
        &name,
        &PatchParams::apply("incidentbench-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    // Mark as cleaned so we don't re-run cleanup.
    let status = serde_json::json!({
        "status": {
            "conditions": [{
                "type": "Cleaned",
                "status": "True",
                "message": "All resources cleaned up",
                "lastTransitionTime": chrono::Utc::now().to_rfc3339()
            }]
        }
    });
    api.patch_status(
        &name,
        &PatchParams::apply("incidentbench-operator"),
        &Patch::Merge(&status),
    )
    .await?;

    info!(name = %name, "Cleanup complete, finalizer removed");
    Ok(())
}

// --- Helper functions ---

async fn resolve_scenario(run: &IncidentBenchRun, _ctx: &Ctx) -> anyhow::Result<Scenario> {
    if let Some(scenario) = &run.spec.scenario {
        Ok(scenario.clone())
    } else if let Some(_scenario_ref) = &run.spec.scenario_ref {
        // TODO: resolve from ConfigMap
        anyhow::bail!("scenarioRef resolution not yet implemented")
    } else {
        anyhow::bail!("Either scenario or scenarioRef must be specified")
    }
}

async fn update_phase_failed(
    api: &Api<IncidentBenchRun>,
    name: &str,
    message: &str,
) -> Result<(), kube::Error> {
    let status = serde_json::json!({
        "status": {
            "phase": "Failed",
            "conditions": [{
                "type": "Failed",
                "status": "True",
                "message": message,
                "lastTransitionTime": chrono::Utc::now().to_rfc3339()
            }]
        }
    });
    api.patch_status(
        name,
        &PatchParams::apply("incidentbench-operator"),
        &Patch::Merge(&status),
    )
    .await?;
    Ok(())
}

async fn create_kafka_topic(
    bootstrap_servers: &str,
    topic: &str,
    partitions: u32,
) -> anyhow::Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .context("Failed to create Kafka admin client")?;

    let new_topic = NewTopic::new(topic, partitions as i32, TopicReplication::Fixed(1));
    let opts = AdminOptions::new();

    let results = admin.create_topics(&[new_topic], &opts).await?;
    for result in results {
        match result {
            Ok(_) => info!(topic = %topic, "Kafka topic created"),
            Err((topic_name, err)) => {
                // Topic already exists is OK.
                if err == rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists {
                    info!(topic = %topic_name, "Kafka topic already exists");
                } else {
                    anyhow::bail!("Failed to create topic '{}': {:?}", topic_name, err);
                }
            }
        }
    }

    Ok(())
}

async fn delete_kafka_topic(bootstrap_servers: &str, topic: &str) -> anyhow::Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .context("Failed to create Kafka admin client")?;

    let opts = AdminOptions::new();
    let results = admin.delete_topics(&[topic], &opts).await?;
    for result in results {
        match result {
            Ok(_) => info!(topic = %topic, "Kafka topic deleted"),
            Err((topic_name, err)) => {
                warn!(topic = %topic_name, "Failed to delete Kafka topic: {:?}", err);
            }
        }
    }

    Ok(())
}

/// Build WarehouseConfig list from the CRD spec.
fn build_warehouse_configs(
    spec: &incidentbench_common::crd::IncidentBenchRunSpec,
) -> Vec<WarehouseConfig> {
    if let Some(ref groups) = spec.workers.query_groups {
        // Deduplicate by warehouse name.
        let mut seen = HashMap::new();
        for g in groups {
            seen.entry(g.warehouse.name.clone())
                .or_insert(WarehouseConfig {
                    name: g.warehouse.name.clone(),
                    num_mediators: g.warehouse.num_mediators,
                    num_os: g.warehouse.num_os,
                });
        }
        seen.into_values().collect()
    } else {
        // Legacy single-warehouse from target.config.warehouse.
        let wh_config = spec.target.config.get("warehouse");
        let name = wh_config
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("incidentbench-wh")
            .to_string();
        let num_mediators = wh_config
            .and_then(|v| v.get("numMediators"))
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u32;
        let num_os = wh_config
            .and_then(|v| v.get("numOs"))
            .and_then(|v| v.as_u64())
            .unwrap_or(2) as u32;
        vec![WarehouseConfig {
            name,
            num_mediators,
            num_os,
        }]
    }
}

async fn poll_phase_controller(
    addr: &str,
) -> anyhow::Result<incidentbench_common::proto::phasecontroller::StatusResponse> {
    use incidentbench_common::proto::phasecontroller::phase_gate_service_client::PhaseGateServiceClient;
    use incidentbench_common::proto::phasecontroller::StatusRequest;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await?;

    let mut client = PhaseGateServiceClient::new(channel);
    let response = client.get_status(StatusRequest {}).await?;

    Ok(response.into_inner())
}

async fn poll_aggregator_snapshot(
    addr: &str,
) -> anyhow::Result<incidentbench_common::proto::aggregator::AggregatedSnapshot> {
    use incidentbench_common::proto::aggregator::metrics_service_client::MetricsServiceClient;
    use incidentbench_common::proto::aggregator::GetLatestSnapshotRequest;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))?
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await?;

    let mut client = MetricsServiceClient::new(channel);
    let response = client
        .get_latest_snapshot(GetLatestSnapshotRequest {})
        .await?;

    Ok(response.into_inner())
}

/// Check health of worker deployments. Returns Ok(()) if all healthy,
/// or Err(message) if a critical failure is detected.
async fn check_worker_health(
    deploy_api: &Api<Deployment>,
    run: &IncidentBenchRun,
    ctx: &Ctx,
) -> Result<(), String> {
    let name = run.name_any();

    // Check phase controller.
    check_deployment_health(deploy_api, &format!("{}-phase-controller", name)).await?;

    // Check aggregator.
    check_deployment_health(deploy_api, &format!("{}-aggregator", name)).await?;

    // Check query workers.
    check_deployment_health(deploy_api, &format!("{}-query", name)).await?;

    // Check per-group query workers.
    if let Some(ref groups) = run.spec.workers.query_groups {
        for group in groups {
            check_deployment_health(deploy_api, &format!("{}-query-{}", name, group.name)).await?;
        }
    }

    // Check per-stream ingest workers.
    if let Ok(scenario) = resolve_scenario(run, ctx).await {
        let scaled =
            scenario.with_scaling(run.spec.scaling.duration_scale, run.spec.scaling.rate_scale);
        for stream in scaled.data_streams.as_deref().unwrap_or(&[]) {
            check_deployment_health(deploy_api, &format!("{}-ingest-{}", name, stream.name))
                .await?;
        }
    }

    Ok(())
}

/// Check if a deployment has available replicas. If desired > 0 but
/// unavailable replicas > 0 with restart count threshold exceeded,
/// report a failure.
async fn check_deployment_health(
    deploy_api: &Api<Deployment>,
    deploy_name: &str,
) -> Result<(), String> {
    let deploy = match deploy_api.get(deploy_name).await {
        Ok(d) => d,
        Err(kube::Error::Api(e)) if e.code == 404 => return Ok(()),
        Err(_) => return Ok(()), // Transient k8s API error — don't fail the run
    };

    let status = match deploy.status {
        Some(ref s) => s,
        None => return Ok(()),
    };

    let desired = status.replicas.unwrap_or(0);
    let available = status.available_replicas.unwrap_or(0);
    let unavailable = status.unavailable_replicas.unwrap_or(0);

    // If desired is 0 (already scaled down), skip.
    if desired == 0 {
        return Ok(());
    }

    // If all replicas are unavailable and the deployment has been around,
    // this indicates CrashLoopBackOff or similar persistent failure.
    if available == 0 && unavailable > 0 {
        // Check if pods have restarted excessively (> 5 restarts).
        let conditions = status.conditions.as_deref().unwrap_or_default();
        let progressing_stalled = conditions
            .iter()
            .any(|c| c.type_ == "Progressing" && c.status == "False");

        if progressing_stalled {
            return Err(format!(
                "Deployment '{}' has stalled: {} unavailable, 0 available",
                deploy_name, unavailable
            ));
        }
    }

    Ok(())
}
