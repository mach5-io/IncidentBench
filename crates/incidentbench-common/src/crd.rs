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

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::metrics::Scorecard;
use crate::scenario::Scenario;

fn free_form_object_schema(
    _gen: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    schemars::schema::Schema::Object(schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::InstanceType::Object.into()),
        extensions: [(
            "x-kubernetes-preserve-unknown-fields".to_string(),
            serde_json::json!(true),
        )]
        .into_iter()
        .collect(),
        ..Default::default()
    })
}

/// IncidentBenchRun CRD spec.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "incidentbench.io",
    version = "v1alpha1",
    kind = "IncidentBenchRun",
    plural = "incidentbenchruns",
    shortname = "ibrun",
    status = "IncidentBenchRunStatus",
    namespaced
)]
pub struct IncidentBenchRunSpec {
    /// Inline scenario definition.
    #[serde(default)]
    pub scenario: Option<Scenario>,

    /// Reference to a ConfigMap containing the scenario YAML.
    #[serde(default)]
    pub scenario_ref: Option<ScenarioRef>,

    /// Target platform configuration.
    pub target: TargetSpec,

    /// Kafka configuration.
    pub kafka: KafkaSpec,

    /// Scaling factors.
    #[serde(default)]
    pub scaling: ScalingSpec,

    /// Worker configuration.
    #[serde(default)]
    pub workers: WorkerSpec,

    /// Results storage configuration.
    #[serde(default)]
    pub results: ResultsSpec,

    /// Container image overrides.
    #[serde(default)]
    pub images: Option<ImageSpec>,

    /// Dry-run mode — validate and print execution plan without running.
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ScenarioRef {
    pub config_map: ConfigMapRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConfigMapRef {
    pub name: String,
    #[serde(default = "default_scenario_key")]
    pub key: String,
}

fn default_scenario_key() -> String {
    "scenario.yaml".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TargetSpec {
    pub adapter: String,
    #[serde(default)]
    #[schemars(schema_with = "free_form_object_schema")]
    pub config: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KafkaSpec {
    /// Bootstrap servers for an external Kafka cluster.
    #[serde(default)]
    pub bootstrap_servers: Option<String>,

    /// Let the operator deploy a managed Kafka cluster.
    #[serde(default)]
    pub managed: bool,

    /// Configuration for managed Kafka.
    #[serde(default)]
    pub managed_config: Option<ManagedKafkaConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ManagedKafkaConfig {
    #[serde(default = "default_kafka_replicas")]
    pub replicas: u32,
    #[serde(default = "default_kafka_storage")]
    pub storage: String,
}

fn default_kafka_replicas() -> u32 {
    3
}

fn default_kafka_storage() -> String {
    "10Gi".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ScalingSpec {
    #[serde(default = "default_scale")]
    pub duration_scale: f64,
    #[serde(default = "default_scale")]
    pub rate_scale: f64,
}

impl Default for ScalingSpec {
    fn default() -> Self {
        Self {
            duration_scale: 1.0,
            rate_scale: 1.0,
        }
    }
}

fn default_scale() -> f64 {
    1.0
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkerSpec {
    #[serde(default)]
    pub ingest: WorkerReplicaSpec,
    /// Default query worker config (used when `query_groups` is absent).
    #[serde(default)]
    pub query: WorkerReplicaSpec,
    /// Multi-warehouse query groups. Each group maps a scenario query group
    /// to a warehouse and replica count. When present, `query` is ignored.
    #[serde(default, rename = "queryGroups")]
    pub query_groups: Option<Vec<QueryGroupSpec>>,
}

/// Maps a scenario query group to a warehouse and replica count.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryGroupSpec {
    /// References a query group name from the scenario's `query_groups`.
    pub name: String,
    /// Warehouse configuration for this group.
    pub warehouse: WarehouseSpec,
    /// Number of query worker replicas for this group.
    #[serde(default = "default_query_group_replicas")]
    pub replicas: u32,
    /// Resource requests/limits for this group's query workers.
    #[serde(default)]
    pub resources: Option<ResourceSpec>,
}

/// Warehouse configuration for a query group.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WarehouseSpec {
    /// Warehouse name. Multiple groups can share the same warehouse name
    /// to test contention, or use different names for isolation testing.
    pub name: String,
    /// Number of mediator (coordinator) nodes.
    #[serde(default = "default_num_mediators", rename = "numMediators")]
    pub num_mediators: u32,
    /// Number of OpenSearch (query execution) nodes.
    #[serde(default = "default_num_os", rename = "numOs")]
    pub num_os: u32,
}

fn default_query_group_replicas() -> u32 {
    4
}

fn default_num_mediators() -> u32 {
    1
}

fn default_num_os() -> u32 {
    2
}

impl Default for WorkerSpec {
    fn default() -> Self {
        Self {
            ingest: WorkerReplicaSpec::default_ingest(),
            query: WorkerReplicaSpec::default_query(),
            query_groups: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkerReplicaSpec {
    #[serde(default = "default_ingest_replicas")]
    pub replicas: u32,
    #[serde(default)]
    pub resources: Option<ResourceSpec>,
}

impl Default for WorkerReplicaSpec {
    fn default() -> Self {
        Self {
            replicas: default_ingest_replicas(),
            resources: None,
        }
    }
}

impl WorkerReplicaSpec {
    fn default_ingest() -> Self {
        Self {
            replicas: 10,
            resources: None,
        }
    }

    fn default_query() -> Self {
        Self {
            replicas: 4,
            resources: None,
        }
    }
}

fn default_ingest_replicas() -> u32 {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ResourceSpec {
    #[serde(default)]
    pub requests: Option<HashMap<String, String>>,
    #[serde(default)]
    pub limits: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ResultsSpec {
    #[serde(default)]
    pub storage: ResultsStorageSpec,
}

impl Default for ResultsSpec {
    fn default() -> Self {
        Self {
            storage: ResultsStorageSpec::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ResultsStorageSpec {
    #[serde(default = "default_storage_type")]
    pub storage_type: String,
    #[serde(default)]
    pub pvc: Option<PvcSpec>,
    #[serde(default)]
    pub s3: Option<S3Spec>,
}

impl Default for ResultsStorageSpec {
    fn default() -> Self {
        Self {
            storage_type: "pvc".to_string(),
            pvc: None,
            s3: None,
        }
    }
}

fn default_storage_type() -> String {
    "pvc".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PvcSpec {
    pub claim_name: String,
    #[serde(default = "default_sub_path")]
    pub sub_path: String,
}

fn default_sub_path() -> String {
    "runs/".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct S3Spec {
    pub bucket: String,
    pub prefix: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ImageSpec {
    #[serde(default)]
    pub operator: Option<String>,
    #[serde(default)]
    pub worker: Option<String>,
    #[serde(default)]
    pub reporter: Option<String>,
}

// --- Status ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IncidentBenchRunStatus {
    /// Operator lifecycle phase.
    #[serde(default)]
    pub phase: LifecyclePhase,

    /// Kubernetes-style conditions.
    #[serde(default)]
    pub conditions: Vec<Condition>,

    /// Current benchmark phase name (e.g., "overlap").
    #[serde(default)]
    pub current_benchmark_phase: Option<String>,

    /// Current benchmark phase index (0-based).
    #[serde(default)]
    pub current_benchmark_phase_index: Option<u32>,

    /// Worker status.
    #[serde(default)]
    pub workers: Option<WorkerStatus>,

    /// Unique run identifier.
    #[serde(default)]
    pub run_id: Option<String>,

    /// Run start time.
    #[serde(default)]
    pub start_time: Option<String>,

    /// Run completion time.
    #[serde(default)]
    pub completion_time: Option<String>,

    /// Progress metrics (updated during Running phase).
    #[serde(default)]
    pub progress: Option<ProgressStatus>,

    /// Final results (populated after completion).
    #[serde(default)]
    pub results: Option<RunResults>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub enum LifecyclePhase {
    #[default]
    Pending,
    Preparing,
    Initializing,
    Running,
    Aggregating,
    Reporting,
    Completed,
    Failed,
}

impl std::fmt::Display for LifecyclePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LifecyclePhase::Pending => write!(f, "Pending"),
            LifecyclePhase::Preparing => write!(f, "Preparing"),
            LifecyclePhase::Initializing => write!(f, "Initializing"),
            LifecyclePhase::Running => write!(f, "Running"),
            LifecyclePhase::Aggregating => write!(f, "Aggregating"),
            LifecyclePhase::Reporting => write!(f, "Reporting"),
            LifecyclePhase::Completed => write!(f, "Completed"),
            LifecyclePhase::Failed => write!(f, "Failed"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Condition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub last_transition_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkerStatus {
    pub ingest_desired: u32,
    pub ingest_ready: u32,
    pub query_desired: u32,
    pub query_ready: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProgressStatus {
    pub elapsed_seconds: u64,
    pub total_seconds: u64,
    pub achieved_ingest_eps: u64,
    pub target_ingest_eps: u64,
    pub achieved_query_qps: f64,
    pub target_query_qps: f64,
    pub kafka_consumer_lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunResults {
    pub valid: bool,
    #[serde(default)]
    pub validity_violations: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub harness_saturated: bool,
    pub scorecard: Option<Scorecard>,
}
