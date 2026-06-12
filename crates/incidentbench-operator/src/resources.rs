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

use incidentbench_common::crd::{
    IncidentBenchRun, IncidentBenchRunSpec, QueryGroupSpec, ResourceSpec,
};
use incidentbench_common::scenario::{IterationMode, QueryCategory, QuerySession, Scenario};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::Resource;
use std::collections::{BTreeMap, HashMap};

/// Default resource requests/limits for worker containers when not user-specified.
fn default_worker_resources() -> serde_json::Value {
    serde_json::json!({
        "requests": {
            "cpu": "100m",
            "memory": "128Mi"
        },
        "limits": {
            "cpu": "2",
            "memory": "2Gi"
        }
    })
}

/// Resolve resource spec: use user-provided if set, otherwise use defaults.
fn resolve_resources(resources: &Option<ResourceSpec>) -> serde_json::Value {
    match resources {
        Some(r) => serde_json::to_value(r).unwrap_or_else(|_| default_worker_resources()),
        None => default_worker_resources(),
    }
}

/// Build an OwnerReference pointing to the IncidentBenchRun CR.
fn owner_reference(run: &IncidentBenchRun) -> OwnerReference {
    OwnerReference {
        api_version: IncidentBenchRun::api_version(&()).to_string(),
        kind: IncidentBenchRun::kind(&()).to_string(),
        name: run.metadata.name.clone().unwrap_or_default(),
        uid: run.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Standard labels for child resources.
fn standard_labels(run_name: &str, component: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/name".to_string(),
        "incidentbench".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/instance".to_string(),
        run_name.to_string(),
    );
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        component.to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "incidentbench-operator".to_string(),
    );
    labels
}

/// ConfigMap storing prepare results (consumer groups, warehouse → query endpoint map).
pub fn build_prepare_result_configmap(
    run_name: &str,
    consumer_groups: &HashMap<String, String>,
    query_endpoints: &HashMap<String, String>,
    run: &IncidentBenchRun,
) -> ConfigMap {
    let mut data = BTreeMap::new();
    // Store consumer_groups as JSON: stream_name → consumer_group.
    let cg_json = serde_json::to_string(consumer_groups).unwrap_or_default();
    data.insert("consumer_groups".to_string(), cg_json);
    // Store endpoints as JSON for easy parsing by workers.
    let endpoints_json = serde_json::to_string(query_endpoints).unwrap_or_default();
    data.insert("query_endpoints".to_string(), endpoints_json);
    // For convenience, also store first endpoint as query_endpoint.
    if let Some(first_endpoint) = query_endpoints.values().next() {
        data.insert("query_endpoint".to_string(), first_endpoint.clone());
    }

    ConfigMap {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(format!("{}-prepare", run_name)),
            namespace: run.metadata.namespace.clone(),
            owner_references: Some(vec![owner_reference(run)]),
            labels: Some(standard_labels(run_name, "prepare")),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

/// Build a ConfigMap containing all SQL files found under `sql_dir`.
///
/// Each file at `{sql_dir}/{category}/{filename}.sql` is stored under the key
/// `{category}_{filename}.sql` (slash replaced with underscore, since ConfigMap
/// keys may not contain `/`). Query workers mount this ConfigMap at `/queries/`
/// and read files by the same key mapping.
///
/// Returns `None` when `sql_dir` is not set or is empty.
pub fn build_sql_files_configmap(
    run_name: &str,
    sql_dir: &str,
    run: &IncidentBenchRun,
) -> Option<ConfigMap> {
    if sql_dir.is_empty() {
        return None;
    }

    let mut data = BTreeMap::new();

    // Walk sql_dir recursively and collect .sql files.
    fn walk(dir: &std::path::Path, root: &std::path::Path, data: &mut BTreeMap<String, String>) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, data);
            } else if path.extension().and_then(|e| e.to_str()) == Some("sql") {
                let relative = path.strip_prefix(root).unwrap_or(&path);
                // Convert "basic-search/01-query.sql" → "basic-search_01-query.sql"
                let key = relative.to_string_lossy().replace(['/', '\\'], "_");
                if let Ok(content) = std::fs::read_to_string(&path) {
                    data.insert(key, content);
                }
            }
        }
    }

    walk(
        std::path::Path::new(sql_dir),
        std::path::Path::new(sql_dir),
        &mut data,
    );

    if data.is_empty() {
        return None;
    }

    Some(ConfigMap {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(format!("{}-sql-files", run_name)),
            namespace: run.metadata.namespace.clone(),
            owner_references: Some(vec![owner_reference(run)]),
            labels: Some(standard_labels(run_name, "sql-files")),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
}

/// Build ConfigMaps for worker configuration.
///
/// Creates per-stream ingest configs, query worker config, phase controller config,
/// aggregator config, and scenario reference ConfigMap.
pub fn build_worker_configmaps(
    run_name: &str,
    scenario: &Scenario,
    spec: &IncidentBenchRunSpec,
    run: &IncidentBenchRun,
    consumer_groups: &HashMap<String, String>,
    query_endpoints: &HashMap<String, String>,
    kafka_bootstrap_servers: &str,
) -> Vec<ConfigMap> {
    let mut cms = Vec::new();
    let ns = run.metadata.namespace.clone();

    // Compute deterministic run seed from run name.
    let run_seed: u64 = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        run_name.hash(&mut hasher);
        hasher.finish()
    };

    let total_ingest_replicas = scenario.total_ingest_replicas();
    let total_query_replicas = if let Some(ref groups) = spec.workers.query_groups {
        groups.iter().map(|g| g.replicas).sum()
    } else {
        spec.workers.query.replicas
    };

    let pc_addr = format!(
        "{}-phase-controller.{}.svc:50051",
        run_name,
        ns.as_deref().unwrap_or("default")
    );
    let agg_addr = format!(
        "{}-aggregator.{}.svc:50052",
        run_name,
        ns.as_deref().unwrap_or("default")
    );

    // Scenario ConfigMap (kept for reference/debugging).
    let scenario_yaml = serde_yaml::to_string(scenario).unwrap_or_default();
    let mut data = BTreeMap::new();
    data.insert("scenario.yaml".to_string(), scenario_yaml);
    cms.push(ConfigMap {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(format!("{}-scenario", run_name)),
            namespace: ns.clone(),
            owner_references: Some(vec![owner_reference(run)]),
            labels: Some(standard_labels(run_name, "scenario")),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    });

    // Phase controller config.
    // Ingest workers use their own rate_table, so per_worker_ingest_eps is 0 in phase controller.
    // Query rate is still broadcast from the phase controller.
    let pc_config = serde_json::json!({
        "expected_ingest_workers": total_ingest_replicas,
        "expected_query_workers": total_query_replicas,
        "phases": scenario.timeline.phases.iter().map(|p| {
            serde_json::json!({
                "name": p.name,
                "duration_seconds": p.duration_seconds,
                "per_worker_ingest_eps": 0,
                "per_worker_query_mqps": (p.query.target_qps * 1000.0) as u64 / total_query_replicas.max(1) as u64
            })
        }).collect::<Vec<_>>()
    });
    let mut data = BTreeMap::new();
    data.insert(
        "phase-controller.yaml".to_string(),
        serde_yaml::to_string(&pc_config).unwrap_or_default(),
    );
    cms.push(ConfigMap {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(format!("{}-phase-controller", run_name)),
            namespace: ns.clone(),
            owner_references: Some(vec![owner_reference(run)]),
            labels: Some(standard_labels(run_name, "phase-controller")),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    });

    // Aggregator config — multi-stream lag polling.
    let agg_streams: Vec<serde_json::Value> = scenario
        .data_streams
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|stream| {
            let cg = consumer_groups
                .get(&stream.name)
                .cloned()
                .unwrap_or_else(|| format!("{}-cg", stream.schema.index_name));
            serde_json::json!({
                "name": stream.name,
                "kafka_topic": stream.schema.index_name,
                "consumer_group": cg
            })
        })
        .collect();
    let agg_config = serde_json::json!({
        "kafka_bootstrap_servers": kafka_bootstrap_servers,
        "streams": agg_streams,
        "results_path": "/results"
    });
    let mut data = BTreeMap::new();
    data.insert(
        "aggregator.yaml".to_string(),
        serde_yaml::to_string(&agg_config).unwrap_or_default(),
    );
    cms.push(ConfigMap {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(format!("{}-aggregator", run_name)),
            namespace: ns.clone(),
            owner_references: Some(vec![owner_reference(run)]),
            labels: Some(standard_labels(run_name, "aggregator")),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    });

    // Per-stream ingest worker configs.
    for stream in scenario.data_streams.as_deref().unwrap_or(&[]) {
        let rate_table = scenario.compute_ingest_rate_table(stream, 0);
        let ingest_config = serde_json::json!({
            "worker_index": 0,
            "run_seed": run_seed,
            "schema": &stream.schema,
            "rate_table": rate_table,
            "kafka_bootstrap_servers": kafka_bootstrap_servers,
            "kafka_topic": &stream.schema.index_name,
            "phase_controller_addr": &pc_addr,
            "aggregator_addr": &agg_addr
        });
        let mut data = BTreeMap::new();
        data.insert(
            "ingest.yaml".to_string(),
            serde_yaml::to_string(&ingest_config).unwrap_or_default(),
        );
        cms.push(ConfigMap {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some(format!("{}-ingest-{}", run_name, stream.name)),
                namespace: ns.clone(),
                owner_references: Some(vec![owner_reference(run)]),
                labels: Some(standard_labels(run_name, "ingest")),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        });
    }

    // Derive effective query_session: either explicit from scenario YAML, or
    // auto-built from query_mix when all queries carry a category field.
    let effective_session = derive_query_session(scenario);

    // Query worker config.
    let first_endpoint = query_endpoints.values().next().cloned().unwrap_or_default();
    let query_rate_table = scenario.compute_query_rate_table(total_query_replicas, 0);
    let query_config = serde_json::json!({
        "worker_index": 0,
        "run_seed": run_seed,
        "query_mix": &scenario.query_mix,
        "rate_table": query_rate_table,
        "query_endpoint": first_endpoint,
        "query_group": "",
        "target_adapter": &spec.target.adapter,
        "target_config": &spec.target.config,
        "phase_controller_addr": &pc_addr,
        "aggregator_addr": &agg_addr,
        "query_session": &effective_session,
        "default_timeout_ms": scenario.default_timeout_ms
    });
    let mut data = BTreeMap::new();
    data.insert(
        "query.yaml".to_string(),
        serde_yaml::to_string(&query_config).unwrap_or_default(),
    );
    cms.push(ConfigMap {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(format!("{}-query", run_name)),
            namespace: ns.clone(),
            owner_references: Some(vec![owner_reference(run)]),
            labels: Some(standard_labels(run_name, "query")),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    });

    // Per-group query worker configs (multi-warehouse mode).
    if let Some(ref crd_groups) = spec.workers.query_groups {
        for crd_group in crd_groups {
            // Resolve query mix for this group (apply mix_override if present).
            let group_mix = scenario
                .query_groups
                .as_ref()
                .and_then(|gs| gs.iter().find(|g| g.name == crd_group.name))
                .map(|g| scenario.resolve_group_query_mix(g))
                .unwrap_or_else(|| scenario.query_mix.clone());

            // Get warehouse endpoint for this group.
            let group_endpoint = query_endpoints
                .get(&crd_group.warehouse.name)
                .cloned()
                .unwrap_or_else(|| first_endpoint.clone());

            let group_rate_table = scenario.compute_query_rate_table(crd_group.replicas, 0);

            let group_config = serde_json::json!({
                "worker_index": 0,
                "run_seed": run_seed,
                "query_mix": &group_mix,
                "rate_table": group_rate_table,
                "query_endpoint": group_endpoint,
                "query_group": &crd_group.name,
                "target_adapter": &spec.target.adapter,
                "target_config": &spec.target.config,
                "phase_controller_addr": &pc_addr,
                "aggregator_addr": &agg_addr,
                "query_session": &effective_session,
                "default_timeout_ms": scenario.default_timeout_ms
            });
            let mut data = BTreeMap::new();
            data.insert(
                format!("query-{}.yaml", crd_group.name),
                serde_yaml::to_string(&group_config).unwrap_or_default(),
            );
            cms.push(ConfigMap {
                metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                    name: Some(format!("{}-query-{}", run_name, crd_group.name)),
                    namespace: ns.clone(),
                    owner_references: Some(vec![owner_reference(run)]),
                    labels: Some(standard_labels(run_name, "query")),
                    ..Default::default()
                },
                data: Some(data),
                ..Default::default()
            });
        }
    }

    cms
}

/// Build the MetricsAggregator Deployment.
/// Build a PVC for results storage. Named `{run_name}-results`.
pub fn build_results_pvc(run_name: &str, run: &IncidentBenchRun) -> PersistentVolumeClaim {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": format!("{}-results", run_name),
            "namespace": run.metadata.namespace,
            "labels": standard_labels(run_name, "results")
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": { "storage": "1Gi" }
            }
        }
    }))
    .expect("valid PVC JSON")
}

pub fn build_aggregator_deployment(
    run_name: &str,
    spec: &IncidentBenchRunSpec,
    run: &IncidentBenchRun,
) -> Deployment {
    let image = spec
        .images
        .as_ref()
        .and_then(|i| i.worker.as_ref())
        .cloned()
        .unwrap_or_else(|| "ghcr.io/mach5-io/incidentbench-worker:v0.1.0".to_string());

    let labels = standard_labels(run_name, "aggregator");

    serde_json::from_value(serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{}-aggregator", run_name),
            "namespace": run.metadata.namespace,
            "ownerReferences": [owner_reference(run)],
            "labels": labels
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/instance": run_name,
                    "app.kubernetes.io/component": "aggregator"
                }
            },
            "template": {
                "metadata": {
                    "labels": labels
                },
                "spec": {
                    "containers": [{
                        "name": "aggregator",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["incidentbench-worker", "aggregator", "--config", "/config/aggregator.yaml"],
                        "ports": [{ "containerPort": 50052 }, { "containerPort": 8080, "name": "health" }],
                        "livenessProbe": {
                            "httpGet": { "path": "/healthz", "port": 8080 },
                            "initialDelaySeconds": 5,
                            "periodSeconds": 10
                        },
                        "readinessProbe": {
                            "httpGet": { "path": "/healthz", "port": 8080 },
                            "initialDelaySeconds": 3,
                            "periodSeconds": 5
                        },
                        "volumeMounts": [
                            { "name": "config", "mountPath": "/config" },
                            { "name": "results", "mountPath": "/results" },
                            { "name": "scenario", "mountPath": "/scenario" }
                        ]
                    }],
                    "initContainers": [{
                        "name": "copy-scenario",
                        "image": "busybox:1.36",
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["sh", "-c", "cp /scenario/scenario.yaml /results/scenario.yaml"],
                        "volumeMounts": [
                            { "name": "scenario", "mountPath": "/scenario" },
                            { "name": "results", "mountPath": "/results" }
                        ]
                    }],
                    "volumes": [
                        {
                            "name": "config",
                            "configMap": { "name": format!("{}-aggregator", run_name) }
                        },
                        {
                            "name": "results",
                            "persistentVolumeClaim": { "claimName": format!("{}-results", run_name) }
                        },
                        {
                            "name": "scenario",
                            "configMap": { "name": format!("{}-scenario", run_name) }
                        }
                    ]
                }
            }
        }
    }))
    .expect("valid deployment JSON")
}

/// Build the PhaseController Service (gRPC on port 50051).
pub fn build_phase_controller_service(run_name: &str, run: &IncidentBenchRun) -> Service {
    let labels = standard_labels(run_name, "phase-controller");

    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": format!("{}-phase-controller", run_name),
            "namespace": run.metadata.namespace,
            "ownerReferences": [owner_reference(run)],
            "labels": labels
        },
        "spec": {
            "selector": {
                "app.kubernetes.io/instance": run_name,
                "app.kubernetes.io/component": "phase-controller"
            },
            "ports": [{
                "name": "grpc",
                "port": 50051,
                "targetPort": 50051
            }]
        }
    }))
    .expect("valid service JSON")
}

/// Build the Aggregator Service (gRPC on port 50052).
pub fn build_aggregator_service(run_name: &str, run: &IncidentBenchRun) -> Service {
    let labels = standard_labels(run_name, "aggregator");

    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": format!("{}-aggregator", run_name),
            "namespace": run.metadata.namespace,
            "ownerReferences": [owner_reference(run)],
            "labels": labels
        },
        "spec": {
            "selector": {
                "app.kubernetes.io/instance": run_name,
                "app.kubernetes.io/component": "aggregator"
            },
            "ports": [{
                "name": "grpc",
                "port": 50052,
                "targetPort": 50052
            }]
        }
    }))
    .expect("valid service JSON")
}

/// Build the PhaseController Deployment.
pub fn build_phase_controller_deployment(
    run_name: &str,
    _scenario: &Scenario,
    spec: &IncidentBenchRunSpec,
    run: &IncidentBenchRun,
) -> Deployment {
    let image = spec
        .images
        .as_ref()
        .and_then(|i| i.worker.as_ref())
        .cloned()
        .unwrap_or_else(|| "ghcr.io/mach5-io/incidentbench-worker:v0.1.0".to_string());

    let labels = standard_labels(run_name, "phase-controller");

    serde_json::from_value(serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{}-phase-controller", run_name),
            "namespace": run.metadata.namespace,
            "ownerReferences": [owner_reference(run)],
            "labels": labels
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/instance": run_name,
                    "app.kubernetes.io/component": "phase-controller"
                }
            },
            "template": {
                "metadata": {
                    "labels": labels
                },
                "spec": {
                    "containers": [{
                        "name": "phase-controller",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["incidentbench-worker", "phase-controller", "--config", "/config/phase-controller.yaml"],
                        "ports": [{ "containerPort": 50051 }, { "containerPort": 8080, "name": "health" }],
                        "livenessProbe": {
                            "httpGet": { "path": "/healthz", "port": 8080 },
                            "initialDelaySeconds": 5,
                            "periodSeconds": 10
                        },
                        "readinessProbe": {
                            "httpGet": { "path": "/healthz", "port": 8080 },
                            "initialDelaySeconds": 3,
                            "periodSeconds": 5
                        },
                        "volumeMounts": [{
                            "name": "config",
                            "mountPath": "/config"
                        }]
                    }],
                    "volumes": [{
                        "name": "config",
                        "configMap": {
                            "name": format!("{}-phase-controller", run_name)
                        }
                    }]
                }
            }
        }
    }))
    .expect("valid deployment JSON")
}

/// Build one IngestWorker Deployment per data stream.
/// Each deployment has `stream.ingest_replicas` replicas and mounts
/// the stream-specific ConfigMap.
pub fn build_ingest_stream_deployments(
    run_name: &str,
    scenario: &Scenario,
    spec: &IncidentBenchRunSpec,
    run: &IncidentBenchRun,
) -> Vec<Deployment> {
    let image = spec
        .images
        .as_ref()
        .and_then(|i| i.worker.as_ref())
        .cloned()
        .unwrap_or_else(|| "ghcr.io/mach5-io/incidentbench-worker:v0.1.0".to_string());

    scenario
        .data_streams
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|stream| {
            let mut labels = standard_labels(run_name, "ingest-worker");
            labels.insert(
                "incidentbench.io/data-stream".to_string(),
                stream.name.clone(),
            );

            serde_json::from_value(serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": format!("{}-ingest-{}", run_name, stream.name),
                    "namespace": run.metadata.namespace,
                    "ownerReferences": [owner_reference(run)],
                    "labels": labels
                },
                "spec": {
                    "replicas": stream.ingest_replicas,
                    "selector": {
                        "matchLabels": {
                            "app.kubernetes.io/instance": run_name,
                            "app.kubernetes.io/component": "ingest-worker",
                            "incidentbench.io/data-stream": stream.name
                        }
                    },
                    "template": {
                        "metadata": {
                            "labels": labels
                        },
                        "spec": {
                            "containers": [{
                                "name": "ingest-worker",
                                "image": image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["incidentbench-worker", "ingest", "--config", "/config/ingest.yaml"],
                                "env": [{
                                    "name": "POD_NAME",
                                    "valueFrom": {
                                        "fieldRef": {
                                            "fieldPath": "metadata.name"
                                        }
                                    }
                                }],
                                "ports": [{ "containerPort": 8080, "name": "health" }],
                                "livenessProbe": {
                                    "httpGet": { "path": "/healthz", "port": 8080 },
                                    "initialDelaySeconds": 5,
                                    "periodSeconds": 10
                                },
                                "readinessProbe": {
                                    "httpGet": { "path": "/healthz", "port": 8080 },
                                    "initialDelaySeconds": 3,
                                    "periodSeconds": 5
                                },
                                "volumeMounts": [{
                                    "name": "config",
                                    "mountPath": "/config"
                                }],
                                "resources": resolve_resources(&spec.workers.ingest.resources)
                            }],
                            "volumes": [{
                                "name": "config",
                                "configMap": {
                                    "name": format!("{}-ingest-{}", run_name, stream.name)
                                }
                            }]
                        }
                    }
                }
            }))
            .expect("valid deployment JSON")
        })
        .collect()
}

/// Build the QueryWorker Deployment.
/// `sql_cm_name`: if Some, adds a `/queries/` volume mount from that ConfigMap.
pub fn build_query_worker_deployment(
    run_name: &str,
    spec: &IncidentBenchRunSpec,
    run: &IncidentBenchRun,
    sql_cm_name: Option<&str>,
) -> Deployment {
    let image = spec
        .images
        .as_ref()
        .and_then(|i| i.worker.as_ref())
        .cloned()
        .unwrap_or_else(|| "ghcr.io/mach5-io/incidentbench-worker:v0.1.0".to_string());

    let labels = standard_labels(run_name, "query-worker");

    let mut volume_mounts = vec![serde_json::json!({
        "name": "config",
        "mountPath": "/config"
    })];
    let mut volumes = vec![serde_json::json!({
        "name": "config",
        "configMap": { "name": format!("{}-query", run_name) }
    })];

    if let Some(cm) = sql_cm_name {
        volume_mounts.push(serde_json::json!({
            "name": "sql-files",
            "mountPath": "/queries"
        }));
        volumes.push(serde_json::json!({
            "name": "sql-files",
            "configMap": { "name": cm }
        }));
    }

    serde_json::from_value(serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{}-query", run_name),
            "namespace": run.metadata.namespace,
            "ownerReferences": [owner_reference(run)],
            "labels": labels
        },
        "spec": {
            "replicas": spec.workers.query.replicas,
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/instance": run_name,
                    "app.kubernetes.io/component": "query-worker"
                }
            },
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "containers": [{
                        "name": "query-worker",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["incidentbench-worker", "query", "--config", "/config/query.yaml"],
                        "env": [{
                            "name": "POD_NAME",
                            "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } }
                        }],
                        "ports": [{ "containerPort": 8080, "name": "health" }],
                        "livenessProbe": {
                            "httpGet": { "path": "/healthz", "port": 8080 },
                            "initialDelaySeconds": 5,
                            "periodSeconds": 10
                        },
                        "readinessProbe": {
                            "httpGet": { "path": "/healthz", "port": 8080 },
                            "initialDelaySeconds": 3,
                            "periodSeconds": 5
                        },
                        "volumeMounts": volume_mounts,
                        "resources": resolve_resources(&spec.workers.query.resources)
                    }],
                    "volumes": volumes
                }
            }
        }
    }))
    .expect("valid deployment JSON")
}

/// Build one QueryWorker Deployment per query group (multi-warehouse mode).
/// `sql_cm_name`: if Some, adds a `/queries/` volume mount from that ConfigMap.
pub fn build_query_worker_group_deployments(
    run_name: &str,
    query_groups: &[QueryGroupSpec],
    spec: &IncidentBenchRunSpec,
    run: &IncidentBenchRun,
    sql_cm_name: Option<&str>,
) -> Vec<Deployment> {
    let image = spec
        .images
        .as_ref()
        .and_then(|i| i.worker.as_ref())
        .cloned()
        .unwrap_or_else(|| "ghcr.io/mach5-io/incidentbench-worker:v0.1.0".to_string());

    query_groups
        .iter()
        .map(|group| {
            let mut labels = standard_labels(run_name, "query-worker");
            labels.insert(
                "incidentbench.io/query-group".to_string(),
                group.name.clone(),
            );

            let mut volume_mounts = vec![serde_json::json!({
                "name": "config",
                "mountPath": "/config"
            })];
            let mut volumes = vec![serde_json::json!({
                "name": "config",
                "configMap": { "name": format!("{}-query-{}", run_name, group.name) }
            })];
            if let Some(cm) = sql_cm_name {
                volume_mounts.push(serde_json::json!({
                    "name": "sql-files",
                    "mountPath": "/queries"
                }));
                volumes.push(serde_json::json!({
                    "name": "sql-files",
                    "configMap": { "name": cm }
                }));
            }

            serde_json::from_value(serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": format!("{}-query-{}", run_name, group.name),
                    "namespace": run.metadata.namespace,
                    "ownerReferences": [owner_reference(run)],
                    "labels": labels
                },
                "spec": {
                    "replicas": group.replicas,
                    "selector": {
                        "matchLabels": {
                            "app.kubernetes.io/instance": run_name,
                            "app.kubernetes.io/component": "query-worker",
                            "incidentbench.io/query-group": group.name
                        }
                    },
                    "template": {
                        "metadata": { "labels": labels },
                        "spec": {
                            "containers": [{
                                "name": "query-worker",
                                "image": image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "incidentbench-worker", "query",
                                    "--config", format!("/config/query-{}.yaml", group.name)
                                ],
                                "env": [{
                                    "name": "POD_NAME",
                                    "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } }
                                }],
                                "ports": [{ "containerPort": 8080, "name": "health" }],
                                "livenessProbe": {
                                    "httpGet": { "path": "/healthz", "port": 8080 },
                                    "initialDelaySeconds": 5,
                                    "periodSeconds": 10
                                },
                                "readinessProbe": {
                                    "httpGet": { "path": "/healthz", "port": 8080 },
                                    "initialDelaySeconds": 3,
                                    "periodSeconds": 5
                                },
                                "volumeMounts": volume_mounts,
                                "resources": resolve_resources(&group.resources)
                            }],
                            "volumes": volumes
                        }
                    }
                }
            }))
            .expect("valid deployment JSON")
        })
        .collect()
}

/// Derive the effective QuerySession for worker config.
///
/// Priority:
///   1. Explicit `scenario.query_session` — used as-is.
///   2. Auto-detect from `query_mix`: when every query has a `category`, group
///      them by category (insertion order preserved) and return a QuerySession
///      with sequential round-robin iteration and parallelism=1.
///   3. Neither — returns None (worker uses rate-controlled weighted-random mode).
fn derive_query_session(scenario: &Scenario) -> Option<QuerySession> {
    if let Some(ref qs) = scenario.query_session {
        return Some(qs.clone());
    }

    if !scenario.is_session_mode() {
        return None;
    }

    // Group query_mix queries by category, preserving first-seen insertion order.
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<incidentbench_common::scenario::QueryDef>> = HashMap::new();
    for q in &scenario.query_mix.queries {
        let cat = q.category.as_deref().unwrap_or("default").to_string();
        if !groups.contains_key(&cat) {
            order.push(cat.clone());
        }
        groups.entry(cat).or_default().push(q.clone());
    }

    let categories: Vec<QueryCategory> = order
        .into_iter()
        .map(|name| QueryCategory {
            queries: groups.remove(&name).unwrap_or_default(),
            name,
            iteration: IterationMode::Sequential,
            parallelism: 1,
        })
        .collect();

    Some(QuerySession {
        categories,
        think_time_ms: 0,
    })
}

/// Build a reporter Job that reads metrics from the results PVC and writes
/// report.html, run.json, and timeseries.csv back to the same PVC.
pub fn build_reporter_job(run_name: &str, run: &IncidentBenchRun) -> Job {
    let spec = &run.spec;
    let image = spec
        .images
        .as_ref()
        .and_then(|i| i.reporter.as_ref())
        .cloned()
        .unwrap_or_else(|| "ghcr.io/mach5-io/incidentbench-reporter:v0.1.0".to_string());

    let labels = standard_labels(run_name, "reporter");

    serde_json::from_value(serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": format!("{}-reporter", run_name),
            "namespace": run.metadata.namespace,
            "ownerReferences": [owner_reference(run)],
            "labels": labels
        },
        "spec": {
            "backoffLimit": 2,
            "ttlSecondsAfterFinished": 600,
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "restartPolicy": "OnFailure",
                    "containers": [{
                        "name": "reporter",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": [
                            "incidentbench-reporter",
                            "--input", "/results",
                            "--output", "/results"
                        ],
                        "volumeMounts": [{
                            "name": "results",
                            "mountPath": "/results"
                        }]
                    }],
                    "volumes": [{
                        "name": "results",
                        "persistentVolumeClaim": {
                            "claimName": format!("{}-results", run_name)
                        }
                    }]
                }
            }
        }
    }))
    .expect("valid reporter Job JSON")
}
