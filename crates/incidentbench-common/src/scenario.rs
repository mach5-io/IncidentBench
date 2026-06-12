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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn json_value_schema(_gen: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
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

/// Top-level scenario definition. This is the authoring format that users write.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Scenario {
    pub scenario: ScenarioMeta,
    /// Data streams — each stream maps to one index, one Kafka topic, and one ingest pipeline.
    /// Absent or empty → no ingest workers, no Kafka setup, no ingest pipeline.
    #[serde(default)]
    pub data_streams: Option<Vec<DataStream>>,
    pub query_mix: QueryMix,
    /// Optional analyst groups with per-group query weight distributions.
    #[serde(default)]
    pub query_groups: Option<Vec<QueryGroup>>,
    pub timeline: Timeline,
    #[serde(default)]
    pub valid_run_criteria: ValidRunCriteria,
    #[serde(default)]
    pub report: ReportConfig,
    /// Global query timeout. Used by SQL queries and as a fallback for query_session categories.
    #[serde(default = "default_timeout_ms")]
    pub default_timeout_ms: u64,
    /// Optional session-based execution mode for dashboard page-load simulation.
    /// When present, workers run session loops instead of the rate-controlled query loop.
    #[serde(default)]
    pub query_session: Option<QuerySession>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ScenarioMeta {
    pub name: String,
    pub version: String,
    pub display_name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub domain: String,
}

// --- Data Stream ---

/// A single data stream: one index, one Kafka topic, one ingest pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DataStream {
    /// Stream identifier (used in deployment names, configmap keys).
    pub name: String,
    /// Index schema for this stream.
    pub schema: Schema,
    /// Data generator configuration for producing events.
    pub data_generator: DataGeneratorConfig,
    /// Number of Kafka partitions for this stream's topic. Defaults to ingest_replicas.
    #[serde(default)]
    pub kafka_partitions: Option<u32>,
    /// Number of ingest worker pods for this stream. Total EPS is divided across replicas.
    pub ingest_replicas: u32,
    /// Per-phase ingest rates for this stream. Keys are phase names from the timeline.
    /// Total EPS is divided across ingest_replicas workers.
    pub ingest: HashMap<String, StreamPhaseIngest>,
}

/// Ingest rate configuration for a single phase within a data stream.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StreamPhaseIngest {
    /// Total events per second for this stream in this phase (divided across ingest_replicas).
    pub target_eps: u64,
    /// Kafka batch size. Defaults to 500.
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

// --- Schema ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Schema {
    pub index_name: String,
    pub timestamp_field: String,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    pub generator: String,
    #[serde(default)]
    #[schemars(schema_with = "json_value_schema")]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Timestamp,
    Keyword,
    Text,
    Int,
    Long,
    Float,
    Ip,
}

impl FieldType {
    /// Map scenario field type to Mach5/OpenSearch mapping type.
    pub fn to_mapping_type(self) -> &'static str {
        match self {
            FieldType::Timestamp => "date",
            FieldType::Keyword => "keyword",
            FieldType::Text => "text",
            FieldType::Int => "integer",
            FieldType::Long => "long",
            FieldType::Float => "float",
            FieldType::Ip => "ip",
        }
    }
}

// --- Data Generator ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DataGeneratorConfig {
    #[serde(rename = "type")]
    pub generator_type: String,
    #[serde(default)]
    #[schemars(schema_with = "json_value_schema")]
    pub config: serde_json::Value,
}

// --- Query Session (optional dashboard page-load simulation) ---

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IterationMode {
    #[default]
    Sequential, // round-robin through queries in order
    WeightedRandom, // existing behaviour
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QuerySession {
    pub categories: Vec<QueryCategory>,
    /// Pause between session ticks in ms; 0 = loop as fast as possible.
    #[serde(default)]
    pub think_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryCategory {
    pub name: String,
    pub queries: Vec<QueryDef>,
    #[serde(default)]
    pub iteration: IterationMode,
    /// Queries fired simultaneously from this category per tick; default 1.
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,
}

fn default_parallelism() -> usize {
    1
}

fn default_timeout_ms() -> u64 {
    10_000
}

// --- Query Mix ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryMix {
    pub queries: Vec<QueryDef>,
}

// --- Query Groups ---

/// An analyst group with its own query weight distribution.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryGroup {
    pub name: String,
    pub weight: f64,
    #[serde(default)]
    pub mix_override: Option<HashMap<String, f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryDef {
    pub name: String,
    #[serde(rename = "type")]
    pub query_type: String,
    pub template: String,
    /// Which index this query targets (must match a data_stream's schema.index_name).
    pub index: String,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub timeout_ms: u64,
    /// Inline SQL text. Takes priority over sql_file when both are set.
    /// Used with query_type = "sql".
    #[serde(default)]
    pub sql: Option<String>,
    /// Path to a SQL file mounted at /queries/ inside the worker pod.
    /// Resolved by the operator from sql_dir; slash-separated path is flattened
    /// to {category}_{filename} in the ConfigMap key.
    #[serde(default)]
    pub sql_file: Option<String>,
    /// Category name for session mode. When all queries in query_mix carry a
    /// non-None category the operator automatically groups them into a
    /// QuerySession (round-robin per category, join_all across categories).
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub variables: HashMap<String, VariableDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VariableDef {
    pub source: String,
}

// --- Timeline ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Timeline {
    pub phases: Vec<PhaseDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PhaseDef {
    pub name: String,
    pub display_name: String,
    pub duration_seconds: u64,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IngestConfig {
    pub target_eps: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

fn default_batch_size() -> u32 {
    500
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryConfig {
    #[serde(default)]
    pub target_qps: f64,
    #[serde(default)]
    pub mix_override: Option<HashMap<String, f64>>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            target_qps: 0.0,
            mix_override: None,
        }
    }
}

// --- Valid Run Criteria ---

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ValidRunCriteria {
    #[serde(default)]
    pub rules: Vec<ValidRunRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ValidRunRule {
    pub name: String,
    pub condition: String,
    #[serde(default)]
    pub message: String,
}

// --- Report Config ---

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ReportConfig {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub emphasis: Vec<String>,
}

// --- Effective rates (after scaling) ---

/// Per-phase rate targets for a single worker, pre-computed by the operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRateTable {
    pub worker_index: u32,
    pub total_workers: u32,
    pub phases: Vec<WorkerPhaseRate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPhaseRate {
    pub phase_name: String,
    pub duration_seconds: u64,
    /// Per-worker EPS target (for ingest workers).
    pub ingest_eps: u64,
    /// Per-worker QPS target as milli-QPS (for query workers).
    /// e.g. 5000 = 5.0 QPS.
    pub query_mqps: u64,
}

// --- Validation ---

impl Scenario {
    /// Returns true when this scenario has ingest work (data_streams present and non-empty).
    /// All conditional branches for Kafka, ingest pipelines, and ingest workers gate on this.
    pub fn has_ingest(&self) -> bool {
        self.data_streams.as_ref().is_some_and(|s| !s.is_empty())
    }

    /// Returns true when workers should run the session loop (join_all per category,
    /// sequential round-robin within each category) rather than the rate-controlled
    /// weighted-random loop.
    ///
    /// Two ways to activate session mode:
    ///   1. Explicit: `query_session` is present in the scenario YAML.
    ///   2. Auto-detect: every query in `query_mix` has a non-None `category` field.
    ///      The operator groups them by category and builds a QuerySession automatically.
    pub fn is_session_mode(&self) -> bool {
        if self.query_session.is_some() {
            return true;
        }
        !self.query_mix.queries.is_empty()
            && self.query_mix.queries.iter().all(|q| q.category.is_some())
    }

    /// Validate the scenario structure. Returns a list of errors.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        let phase_names: Vec<&str> = self
            .timeline
            .phases
            .iter()
            .map(|p| p.name.as_str())
            .collect();

        let mut seen_index_names = std::collections::HashSet::new();

        // Validate data streams only when present.
        if let Some(streams) = &self.data_streams {
            if streams.is_empty() {
                errors.push("data_streams must have at least one stream if specified".to_string());
            }

            let mut seen_stream_names = std::collections::HashSet::new();

            for stream in streams {
                if !seen_stream_names.insert(&stream.name) {
                    errors.push(format!("Duplicate data stream name: '{}'", stream.name));
                }
                if !seen_index_names.insert(&stream.schema.index_name) {
                    errors.push(format!(
                        "Duplicate index_name: '{}'",
                        stream.schema.index_name
                    ));
                }

                if stream.schema.fields.is_empty() {
                    errors.push(format!(
                        "Stream '{}': schema must have at least one field",
                        stream.name
                    ));
                }

                let has_ts = stream
                    .schema
                    .fields
                    .iter()
                    .any(|f| f.name == stream.schema.timestamp_field);
                if !has_ts {
                    errors.push(format!(
                        "Stream '{}': timestamp field '{}' not found in schema fields",
                        stream.name, stream.schema.timestamp_field
                    ));
                }

                if stream.ingest_replicas == 0 {
                    errors.push(format!(
                        "Stream '{}': ingest_replicas must be >= 1",
                        stream.name
                    ));
                }

                for phase_name in &phase_names {
                    if !stream.ingest.contains_key(*phase_name) {
                        errors.push(format!(
                            "Stream '{}': missing ingest config for phase '{}'",
                            stream.name, phase_name
                        ));
                    }
                }
            }
        }

        if self.timeline.phases.is_empty() {
            errors.push("Timeline must have at least one phase".to_string());
        }

        // 6-phase name enforcement only applies when ingest is present.
        if self.has_ingest() {
            let expected_phases = [
                "baseline",
                "incident_trigger",
                "ingestion_surge",
                "overlap",
                "recovery",
                "post_incident",
            ];
            let actual_phases: Vec<&str> = phase_names.clone();
            if actual_phases != expected_phases {
                errors.push(format!(
                    "Timeline must have exactly these phases in order: {:?}, got: {:?}",
                    expected_phases, actual_phases
                ));
            }
        }

        // Validate minimum phase duration.
        for phase in &self.timeline.phases {
            if phase.duration_seconds < 30 {
                errors.push(format!(
                    "Phase '{}' duration must be at least 30 seconds, got {}",
                    phase.name, phase.duration_seconds
                ));
            }
        }

        // Validate query index references only when indexes are known (ingest present).
        if self.has_ingest() {
            for query in &self.query_mix.queries {
                if !seen_index_names.contains(&query.index) {
                    errors.push(format!(
                        "Query '{}' references unknown index '{}'. Valid indexes: {:?}",
                        query.name, query.index, seen_index_names
                    ));
                }
            }
        }

        // Validate query groups if present.
        if let Some(ref groups) = self.query_groups {
            if groups.is_empty() {
                errors.push("query_groups must have at least one group if specified".to_string());
            }

            let group_weight_sum: f64 = groups.iter().map(|g| g.weight).sum();
            if (group_weight_sum - 1.0).abs() > 0.01 {
                errors.push(format!(
                    "Query group weights must sum to 1.0, got {:.3}",
                    group_weight_sum
                ));
            }

            let mut seen_names = std::collections::HashSet::new();
            for group in groups {
                if !seen_names.insert(&group.name) {
                    errors.push(format!("Duplicate query group name: '{}'", group.name));
                }

                if group.weight <= 0.0 {
                    errors.push(format!(
                        "Query group '{}' weight must be positive, got {}",
                        group.name, group.weight
                    ));
                }

                if let Some(ref overrides) = group.mix_override {
                    let query_names: std::collections::HashSet<&str> = self
                        .query_mix
                        .queries
                        .iter()
                        .map(|q| q.name.as_str())
                        .collect();
                    for key in overrides.keys() {
                        if !query_names.contains(key.as_str()) {
                            errors.push(format!(
                                "Query group '{}' mix_override references unknown query '{}'",
                                group.name, key
                            ));
                        }
                    }
                    let override_sum: f64 = overrides.values().sum();
                    if (override_sum - 1.0).abs() > 0.01 {
                        errors.push(format!(
                            "Query group '{}' mix_override weights must sum to 1.0, got {:.3}",
                            group.name, override_sum
                        ));
                    }
                }
            }
        }

        errors
    }

    /// Compute total duration in seconds.
    pub fn total_duration_seconds(&self) -> u64 {
        self.timeline
            .phases
            .iter()
            .map(|p| p.duration_seconds)
            .sum()
    }

    /// Compute total events at target rates across all streams.
    pub fn total_events_at_target(&self) -> u64 {
        self.data_streams
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|stream| {
                self.timeline
                    .phases
                    .iter()
                    .map(|p| {
                        stream
                            .ingest
                            .get(&p.name)
                            .map(|i| i.target_eps * p.duration_seconds)
                            .unwrap_or(0)
                    })
                    .sum::<u64>()
            })
            .sum()
    }

    /// Total ingest replicas across all streams.
    pub fn total_ingest_replicas(&self) -> u32 {
        self.data_streams
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|s| s.ingest_replicas)
            .sum()
    }

    /// Apply duration and rate scaling. Returns a new Scenario with scaled values.
    pub fn with_scaling(&self, duration_scale: f64, rate_scale: f64) -> Self {
        let mut scaled = self.clone();
        for phase in &mut scaled.timeline.phases {
            phase.duration_seconds =
                ((phase.duration_seconds as f64) * duration_scale).round() as u64;
            phase.duration_seconds = phase.duration_seconds.max(1);
            phase.query.target_qps *= rate_scale;
        }
        if let Some(streams) = &mut scaled.data_streams {
            for stream in streams {
                for ingest in stream.ingest.values_mut() {
                    ingest.target_eps = ((ingest.target_eps as f64) * rate_scale).round() as u64;
                }
            }
        }
        scaled
    }

    /// Resolve the effective QueryMix for a given query group.
    pub fn resolve_group_query_mix(&self, group: &QueryGroup) -> QueryMix {
        match &group.mix_override {
            None => self.query_mix.clone(),
            Some(overrides) => {
                let queries = self
                    .query_mix
                    .queries
                    .iter()
                    .filter(|q| overrides.contains_key(&q.name))
                    .cloned()
                    .collect();
                QueryMix { queries }
            }
        }
    }

    /// Compute per-worker rate table for an ingest stream.
    pub fn compute_ingest_rate_table(
        &self,
        stream: &DataStream,
        worker_index: u32,
    ) -> WorkerRateTable {
        let replicas = stream.ingest_replicas.max(1); // Guard against division by zero
        let phases = self
            .timeline
            .phases
            .iter()
            .map(|p| {
                let total_eps = stream
                    .ingest
                    .get(&p.name)
                    .map(|i| i.target_eps)
                    .unwrap_or(0);
                let per_worker_eps = total_eps / (replicas as u64);
                // Give remainder to last worker.
                let extra = if worker_index == replicas - 1 {
                    total_eps % (replicas as u64)
                } else {
                    0
                };
                WorkerPhaseRate {
                    phase_name: p.name.clone(),
                    duration_seconds: p.duration_seconds,
                    ingest_eps: per_worker_eps + extra,
                    query_mqps: 0,
                }
            })
            .collect();
        WorkerRateTable {
            worker_index,
            total_workers: replicas,
            phases,
        }
    }

    /// Compute per-worker rate table for query workers.
    pub fn compute_query_rate_table(
        &self,
        query_workers: u32,
        worker_index: u32,
    ) -> WorkerRateTable {
        let query_workers = query_workers.max(1); // Guard against division by zero
        let phases = self
            .timeline
            .phases
            .iter()
            .map(|p| {
                let total_mqps = (p.query.target_qps * 1000.0) as u64;
                let per_worker_mqps = total_mqps / (query_workers as u64);
                let extra = if worker_index == query_workers - 1 {
                    total_mqps % (query_workers as u64)
                } else {
                    0
                };
                WorkerPhaseRate {
                    phase_name: p.name.clone(),
                    duration_seconds: p.duration_seconds,
                    ingest_eps: 0,
                    query_mqps: per_worker_mqps + extra,
                }
            })
            .collect();
        WorkerRateTable {
            worker_index,
            total_workers: query_workers,
            phases,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_scenario() -> Scenario {
        let stream = DataStream {
            name: "test-logs".to_string(),
            schema: Schema {
                index_name: "test-logs".to_string(),
                timestamp_field: "@timestamp".to_string(),
                fields: vec![
                    FieldDef {
                        name: "@timestamp".to_string(),
                        field_type: FieldType::Timestamp,
                        generator: "now".to_string(),
                        config: serde_json::Value::Null,
                    },
                    FieldDef {
                        name: "level".to_string(),
                        field_type: FieldType::Keyword,
                        generator: "weighted_enum".to_string(),
                        config: serde_json::json!({"values": {"INFO": 0.9, "ERROR": 0.1}}),
                    },
                ],
            },
            data_generator: DataGeneratorConfig {
                generator_type: "template".to_string(),
                config: serde_json::json!({"seed": 42}),
            },
            kafka_partitions: None,
            ingest_replicas: 2,
            ingest: [
                (
                    "baseline".to_string(),
                    StreamPhaseIngest {
                        target_eps: 100,
                        batch_size: 500,
                    },
                ),
                (
                    "incident_trigger".to_string(),
                    StreamPhaseIngest {
                        target_eps: 200,
                        batch_size: 500,
                    },
                ),
                (
                    "ingestion_surge".to_string(),
                    StreamPhaseIngest {
                        target_eps: 500,
                        batch_size: 500,
                    },
                ),
                (
                    "overlap".to_string(),
                    StreamPhaseIngest {
                        target_eps: 500,
                        batch_size: 500,
                    },
                ),
                (
                    "recovery".to_string(),
                    StreamPhaseIngest {
                        target_eps: 200,
                        batch_size: 500,
                    },
                ),
                (
                    "post_incident".to_string(),
                    StreamPhaseIngest {
                        target_eps: 100,
                        batch_size: 500,
                    },
                ),
            ]
            .into_iter()
            .collect(),
        };

        let phases = vec![
            PhaseDef {
                name: "baseline".to_string(),
                display_name: "Baseline".to_string(),
                duration_seconds: 30,
                query: QueryConfig {
                    target_qps: 10.0,
                    mix_override: None,
                },
                description: String::new(),
            },
            PhaseDef {
                name: "incident_trigger".to_string(),
                display_name: "Incident Trigger".to_string(),
                duration_seconds: 30,
                query: QueryConfig {
                    target_qps: 10.0,
                    mix_override: None,
                },
                description: String::new(),
            },
            PhaseDef {
                name: "ingestion_surge".to_string(),
                display_name: "Ingestion Surge".to_string(),
                duration_seconds: 30,
                query: QueryConfig {
                    target_qps: 20.0,
                    mix_override: None,
                },
                description: String::new(),
            },
            PhaseDef {
                name: "overlap".to_string(),
                display_name: "Overlap".to_string(),
                duration_seconds: 30,
                query: QueryConfig {
                    target_qps: 50.0,
                    mix_override: None,
                },
                description: String::new(),
            },
            PhaseDef {
                name: "recovery".to_string(),
                display_name: "Recovery".to_string(),
                duration_seconds: 30,
                query: QueryConfig {
                    target_qps: 20.0,
                    mix_override: None,
                },
                description: String::new(),
            },
            PhaseDef {
                name: "post_incident".to_string(),
                display_name: "Post-Incident".to_string(),
                duration_seconds: 30,
                query: QueryConfig {
                    target_qps: 10.0,
                    mix_override: None,
                },
                description: String::new(),
            },
        ];

        Scenario {
            scenario: ScenarioMeta {
                name: "test".to_string(),
                version: "1.0.0".to_string(),
                display_name: "Test".to_string(),
                description: String::new(),
                domain: "sre".to_string(),
            },
            data_streams: Some(vec![stream]),
            query_mix: QueryMix {
                queries: vec![QueryDef {
                    name: "error_search".to_string(),
                    query_type: "search".to_string(),
                    template: "level:ERROR".to_string(),
                    index: "test-logs".to_string(),
                    sort: None,
                    limit: None,
                    timeout_ms: 10000,
                    sql: None,
                    sql_file: None,
                    category: None,
                    description: String::new(),
                    variables: HashMap::new(),
                }],
            },
            query_groups: None,
            timeline: Timeline { phases },
            valid_run_criteria: ValidRunCriteria::default(),
            report: ReportConfig::default(),
            default_timeout_ms: 10_000,
            query_session: None,
        }
    }

    #[test]
    fn test_scenario_validates_ok() {
        let scenario = make_test_scenario();
        let errors = scenario.validate();
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_scenario_validates_empty_streams() {
        let mut scenario = make_test_scenario();
        scenario.data_streams = Some(vec![]);
        let errors = scenario.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("data_streams must have at least one stream if specified")));
    }

    #[test]
    fn test_scenario_validates_bad_query_index() {
        let mut scenario = make_test_scenario();
        scenario.query_mix.queries[0].index = "nonexistent".to_string();
        let errors = scenario.validate();
        assert!(errors.iter().any(|e| e.contains("nonexistent")));
    }

    #[test]
    fn test_ingest_rate_table_divides_evenly() {
        let scenario = make_test_scenario();
        let stream = scenario.data_streams.as_ref().unwrap()[0].clone();
        let table = scenario.compute_ingest_rate_table(&stream, 0);
        // 100 EPS / 2 replicas = 50 per worker
        let baseline = table
            .phases
            .iter()
            .find(|p| p.phase_name == "baseline")
            .unwrap();
        assert_eq!(baseline.ingest_eps, 50);
    }

    #[test]
    fn test_ingest_rate_table_remainder_to_last_worker() {
        let mut scenario = make_test_scenario();
        // 101 EPS / 2 replicas = 50 + 1 for last worker
        scenario.data_streams.as_mut().unwrap()[0]
            .ingest
            .get_mut("baseline")
            .unwrap()
            .target_eps = 101;
        let stream = scenario.data_streams.as_ref().unwrap()[0].clone();
        let table0 = scenario.compute_ingest_rate_table(&stream, 0);
        let table1 = scenario.compute_ingest_rate_table(&stream, 1);
        let w0_baseline = table0
            .phases
            .iter()
            .find(|p| p.phase_name == "baseline")
            .unwrap();
        let w1_baseline = table1
            .phases
            .iter()
            .find(|p| p.phase_name == "baseline")
            .unwrap();
        assert_eq!(w0_baseline.ingest_eps, 50);
        assert_eq!(w1_baseline.ingest_eps, 51); // Gets remainder
    }

    #[test]
    fn test_ingest_rate_table_zero_replicas_no_panic() {
        let mut scenario = make_test_scenario();
        scenario.data_streams.as_mut().unwrap()[0].ingest_replicas = 0;
        let stream = scenario.data_streams.as_ref().unwrap()[0].clone();
        // Should not panic — the .max(1) guard handles this.
        let table = scenario.compute_ingest_rate_table(&stream, 0);
        let baseline = table
            .phases
            .iter()
            .find(|p| p.phase_name == "baseline")
            .unwrap();
        assert_eq!(baseline.ingest_eps, 100); // All EPS goes to the single (virtual) worker
    }

    #[test]
    fn test_query_rate_table_zero_workers_no_panic() {
        let scenario = make_test_scenario();
        // Should not panic — the .max(1) guard handles this.
        let table = scenario.compute_query_rate_table(0, 0);
        let baseline = table
            .phases
            .iter()
            .find(|p| p.phase_name == "baseline")
            .unwrap();
        assert_eq!(baseline.query_mqps, 10000); // 10.0 QPS * 1000 = 10000 mQPS
    }

    #[test]
    fn test_query_rate_table_distributes_qps() {
        let scenario = make_test_scenario();
        let table = scenario.compute_query_rate_table(4, 0);
        let overlap = table
            .phases
            .iter()
            .find(|p| p.phase_name == "overlap")
            .unwrap();
        // 50 QPS * 1000 = 50000 mQPS / 4 workers = 12500 mQPS each
        assert_eq!(overlap.query_mqps, 12500);
    }

    #[test]
    fn test_query_rate_table_supports_sub_one_qps_per_worker() {
        let mut scenario = make_test_scenario();
        for phase in &mut scenario.timeline.phases {
            phase.query.target_qps = 1.0;
        }

        let worker_rates: Vec<u64> = (0..4)
            .map(|worker_index| {
                let table = scenario.compute_query_rate_table(4, worker_index);
                table
                    .phases
                    .iter()
                    .find(|p| p.phase_name == "baseline")
                    .unwrap()
                    .query_mqps
            })
            .collect();

        assert_eq!(worker_rates, vec![250, 250, 250, 250]);
        assert_eq!(worker_rates.iter().sum::<u64>(), 1000);
    }

    #[test]
    fn test_total_duration() {
        let scenario = make_test_scenario();
        assert_eq!(scenario.total_duration_seconds(), 180); // 6 * 30
    }

    #[test]
    fn test_with_scaling() {
        let scenario = make_test_scenario();
        let scaled = scenario.with_scaling(2.0, 0.5);
        let baseline = scaled
            .timeline
            .phases
            .iter()
            .find(|p| p.name == "baseline")
            .unwrap();
        assert_eq!(baseline.duration_seconds, 60); // 30 * 2.0
        assert_eq!(baseline.query.target_qps, 5.0); // 10.0 * 0.5
        let stream_baseline = scaled.data_streams.as_ref().unwrap()[0]
            .ingest
            .get("baseline")
            .unwrap();
        assert_eq!(stream_baseline.target_eps, 50); // 100 * 0.5
    }
}
