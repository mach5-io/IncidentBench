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

use crate::scenario::{QueryDef, Schema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Warehouse configuration passed to the adapter during prepare().
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseConfig {
    pub name: String,
    pub num_mediators: u32,
    pub num_os: u32,
}

/// Configuration for a single data stream passed to the adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataStreamConfig {
    /// Stream identifier.
    pub name: String,
    /// Index schema for this stream.
    pub schema: Schema,
    /// Kafka topic for this stream (= index_name).
    pub kafka_topic: String,
}

/// Result returned by the adapter's prepare() method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareResult {
    /// Map of stream_name → Kafka consumer group used by the target's ingest pipeline.
    /// The MetricsAggregator polls each consumer group for lag.
    pub consumer_groups: HashMap<String, String>,
    /// Map of warehouse name → OpenSearch-compatible query endpoint.
    /// When a single warehouse is used, this map has one entry.
    pub query_endpoints: HashMap<String, String>,
}

/// Result of executing a single query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub hit_count: u64,
    pub error: Option<String>,
    pub duration_ms: f64,
}

/// Target platform adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    pub adapter: String,
    pub config: HashMap<String, serde_json::Value>,
}

/// The target adapter trait. Implementations handle platform-specific
/// setup, querying, and teardown.
///
/// Ingestion is always via Kafka — there is no `ingest_batch()` method.
/// The adapter's `prepare()` configures the target to consume from Kafka.
#[async_trait]
pub trait TargetAdapter: Send + Sync {
    /// Adapter identity.
    fn name(&self) -> &str;

    /// Prepare the target platform for all data streams:
    /// - Create Kafka connection
    /// - Create indexes matching each stream's schema
    /// - Create ingest pipelines to consume from each stream's Kafka topic
    /// - Create one or more query warehouses (units of query isolation, independent of indexes)
    ///
    /// Returns per-stream consumer group names (for lag monitoring) and a map of warehouse → query endpoint.
    async fn prepare(
        &self,
        streams: &[DataStreamConfig],
        kafka_bootstrap_servers: &str,
        warehouses: &[WarehouseConfig],
    ) -> anyhow::Result<PrepareResult>;

    /// Execute a single query against the target via the given warehouse endpoint.
    async fn execute_query(
        &self,
        query: &QueryDef,
        index_name: &str,
        query_endpoint: &str,
        variables: &HashMap<String, String>,
    ) -> anyhow::Result<QueryResult>;

    /// Tear down resources created during prepare():
    /// - Delete ingest pipelines (one per stream)
    /// - Delete all warehouses (query isolation units created for the benchmark)
    /// - Delete indexes (one per stream)
    /// - Delete Kafka connection
    /// - Delete the Mach5 namespace (created during prepare)
    async fn cleanup(
        &self,
        streams: &[DataStreamConfig],
        warehouses: &[WarehouseConfig],
    ) -> anyhow::Result<()>;
}
