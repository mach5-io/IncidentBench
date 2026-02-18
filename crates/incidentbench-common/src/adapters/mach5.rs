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

use crate::adapter::{
    DataStreamConfig, PrepareResult, QueryResult, TargetAdapter, WarehouseConfig,
};
use crate::scenario::{FieldDef, FieldType, QueryDef};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info, warn};

/// Mach5 target adapter.
///
/// Implements the TargetAdapter trait for the Mach5 search and analytics platform.
/// Uses the Mach5 REST API for infrastructure setup/teardown and the OpenSearch-compatible
/// query endpoint (via warehouse) for query execution.
///
/// Supports multiple data streams (indexes) and warehouses. Each stream maps to one index
/// and one ingest pipeline. Warehouses are units of query isolation — they provide dedicated
/// query nodes that can query any/all indexes in the namespace.
pub struct Mach5Adapter {
    client: Client,
    /// Mach5 REST API base URL (e.g., "https://mach5-cluster:8080").
    endpoint: String,
    /// Mach5 namespace for all resources.
    namespace: String,
    /// Kafka connection name (deterministic, shared across runs).
    connection_name: String,
}

impl Mach5Adapter {
    pub fn new(config: &HashMap<String, serde_json::Value>) -> anyhow::Result<Self> {
        let endpoint = config
            .get("endpoint")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Mach5 adapter requires 'endpoint' in config"))?
            .trim_end_matches('/')
            .to_string();

        let namespace = config
            .get("namespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Mach5 adapter requires 'namespace' in config (the adapter creates and deletes this namespace)"))?
            .to_string();

        Ok(Self {
            client: Client::builder()
                .danger_accept_invalid_certs(true) // TODO: make configurable
                .build()?,
            endpoint,
            namespace,
            connection_name: "incidentbench-kafka-conn".to_string(),
        })
    }

    fn api_base(&self) -> String {
        format!("{}/apis", self.endpoint)
    }

    /// Build OpenSearch index mappings from scenario schema fields.
    fn build_mappings(fields: &[FieldDef]) -> Value {
        let mut properties = Map::new();

        for field in fields {
            let parts: Vec<&str> = field.name.split('.').collect();
            if parts.len() == 1 {
                properties.insert(
                    field.name.clone(),
                    json!({ "type": field.field_type.to_mapping_type() }),
                );
            } else {
                // Nested field (e.g., "kubernetes.namespace").
                // Build nested object structure.
                insert_nested_mapping(&mut properties, &parts, field.field_type);
            }
        }

        json!({ "properties": properties })
    }

    /// Translate a scenario query definition into an OpenSearch DSL body.
    fn build_query_body(query: &QueryDef, variables: &HashMap<String, String>) -> Value {
        let template = substitute_variables(&query.template, variables);

        match query.query_type.as_str() {
            "search" => build_search_query(&template, query),
            "aggregation" => build_aggregation_query(&template, query),
            _ => {
                // Fallback: try to parse as a simple term query.
                json!({
                    "query": { "query_string": { "query": template } }
                })
            }
        }
    }
}

fn insert_nested_mapping(
    properties: &mut Map<String, Value>,
    parts: &[&str],
    field_type: FieldType,
) {
    if parts.len() == 1 {
        properties.insert(
            parts[0].to_string(),
            json!({ "type": field_type.to_mapping_type() }),
        );
        return;
    }

    let entry = properties
        .entry(parts[0].to_string())
        .or_insert_with(|| json!({ "properties": {} }));

    if let Some(inner_props) = entry
        .as_object_mut()
        .and_then(|o| o.get_mut("properties"))
        .and_then(|p| p.as_object_mut())
    {
        insert_nested_mapping(inner_props, &parts[1..], field_type);
    }
}

fn substitute_variables(template: &str, variables: &HashMap<String, String>) -> String {
    let mut result = template.to_string();
    for (key, value) in variables {
        result = result.replace(&format!("{{{{{}}}}}", key), value);
    }
    result
}

/// Build a search query from the template string.
fn build_search_query(template: &str, query: &QueryDef) -> Value {
    let mut body = Map::new();

    // Parse simple query patterns.
    if template.contains(" AND ") {
        // Bool must query with multiple conditions.
        let clauses: Vec<&str> = template.split(" AND ").collect();
        let must: Vec<Value> = clauses
            .iter()
            .map(|c| parse_query_clause(c.trim()))
            .collect();
        body.insert("query".to_string(), json!({ "bool": { "must": must } }));
    } else {
        body.insert("query".to_string(), parse_query_clause(template.trim()));
    }

    if let Some(sort) = &query.sort {
        let parts: Vec<&str> = sort.split(':').collect();
        if parts.len() == 2 {
            body.insert("sort".to_string(), json!([{ parts[0]: parts[1] }]));
        }
    }

    if let Some(limit) = query.limit {
        body.insert("size".to_string(), json!(limit));
    }

    Value::Object(body)
}

/// Parse a single query clause like "level:ERROR" or "response_time_ms:>5000".
fn parse_query_clause(clause: &str) -> Value {
    if let Some(pos) = clause.find(':') {
        let field = &clause[..pos];
        let value = &clause[pos + 1..];

        if value.starts_with('>') {
            // Range query.
            let num_str = value.trim_start_matches('>');
            json!({ "range": { field: { "gt": num_str.parse::<f64>().unwrap_or(0.0) } } })
        } else if value.contains('*') {
            // Wildcard query.
            json!({ "wildcard": { field: value } })
        } else {
            // Term query.
            json!({ "term": { field: value } })
        }
    } else {
        json!({ "query_string": { "query": clause } })
    }
}

/// Build an aggregation query from the template string.
fn build_aggregation_query(template: &str, _query: &QueryDef) -> Value {
    let mut body = Map::new();
    body.insert("size".to_string(), json!(0));

    let lines: Vec<&str> = template
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();

    for line in &lines {
        if line.starts_with("filter:") {
            let filter_str = line.trim_start_matches("filter:").trim();
            if filter_str == "*" {
                body.insert("query".to_string(), json!({ "match_all": {} }));
            } else {
                body.insert("query".to_string(), parse_query_clause(filter_str));
            }
        } else if line.starts_with("aggregate:") {
            let agg_str = line.trim_start_matches("aggregate:").trim();
            let aggs = parse_aggregation(agg_str);
            body.insert("aggs".to_string(), aggs);
        }
    }

    Value::Object(body)
}

/// Parse aggregation expressions like "terms(field)" or "date_histogram(@timestamp, interval=1m) > terms(status)".
fn parse_aggregation(expr: &str) -> Value {
    let parts: Vec<&str> = expr.split(" > ").collect();

    if parts.len() == 1 {
        parse_single_agg(parts[0].trim())
    } else {
        // Nested aggregation: outer > inner
        parse_single_agg_with_sub(parts[0].trim(), &parts[1..])
    }
}

fn parse_single_agg(expr: &str) -> Value {
    if expr.starts_with("terms(") {
        let field = expr
            .trim_start_matches("terms(")
            .trim_end_matches(')')
            .trim();
        json!({
            "agg_result": { "terms": { "field": field } }
        })
    } else if expr.starts_with("date_histogram(") {
        let inner = expr
            .trim_start_matches("date_histogram(")
            .trim_end_matches(')');
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        let field = parts.first().unwrap_or(&"@timestamp");
        let interval = parts
            .iter()
            .find(|p| p.starts_with("interval="))
            .map(|p| p.trim_start_matches("interval="))
            .unwrap_or("1m");
        json!({
            "agg_result": {
                "date_histogram": { "field": field, "fixed_interval": interval }
            }
        })
    } else {
        json!({ "agg_result": { "terms": { "field": expr } } })
    }
}

fn parse_single_agg_with_sub(expr: &str, sub_parts: &[&str]) -> Value {
    let sub_aggs = if sub_parts.len() == 1 {
        parse_single_agg(sub_parts[0].trim())
    } else {
        parse_single_agg_with_sub(sub_parts[0].trim(), &sub_parts[1..])
    };

    if expr.starts_with("terms(") {
        let field = expr
            .trim_start_matches("terms(")
            .trim_end_matches(')')
            .trim();
        json!({
            "agg_result": {
                "terms": { "field": field },
            },
            "agg_result.aggs": sub_aggs,
        })
    } else if expr.starts_with("date_histogram(") {
        let inner = expr
            .trim_start_matches("date_histogram(")
            .trim_end_matches(')');
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        let field = parts.first().unwrap_or(&"@timestamp");
        let interval = parts
            .iter()
            .find(|p| p.starts_with("interval="))
            .map(|p| p.trim_start_matches("interval="))
            .unwrap_or("1m");

        // Build properly nested aggs.
        json!({
            "timeline": {
                "date_histogram": { "field": field, "fixed_interval": interval },
                "aggs": sub_aggs
            }
        })
    } else {
        sub_aggs
    }
}

#[async_trait]
impl TargetAdapter for Mach5Adapter {
    fn name(&self) -> &str {
        "mach5"
    }

    async fn prepare(
        &self,
        streams: &[DataStreamConfig],
        kafka_bootstrap_servers: &str,
        warehouses: &[WarehouseConfig],
    ) -> anyhow::Result<PrepareResult> {
        let base = self.api_base();
        let ns = &self.namespace;
        let conn_name = &self.connection_name;

        let stream_names: Vec<&str> = streams.iter().map(|s| s.name.as_str()).collect();
        let warehouse_names: Vec<&str> = warehouses.iter().map(|w| w.name.as_str()).collect();
        info!(
            "Preparing Mach5 resources in namespace '{}': streams {:?}, warehouses {:?}",
            ns, stream_names, warehouse_names
        );

        // Step 0: Create the Mach5 namespace.
        let ns_url = format!("{}/namespaces/{}", base, ns);
        debug!("Creating Mach5 namespace: {}", ns_url);
        let ns_resp = self.client.put(&ns_url).json(&json!({})).send().await?;
        if !ns_resp.status().is_success() {
            let status = ns_resp.status();
            let body = ns_resp.text().await.unwrap_or_default();
            if status.as_u16() == 409
                || body.contains("AlreadyExists")
                || body.contains("already exists")
            {
                info!("Mach5 namespace already exists: {}", ns);
            } else {
                anyhow::bail!(
                    "Failed to create Mach5 namespace '{}' ({}): {}",
                    ns,
                    status,
                    body
                );
            }
        } else {
            info!("Mach5 namespace created: {}", ns);
        }

        // Step 1: Create Kafka connection.
        let conn_url = format!("{}/namespaces/{}/connections/{}", base, ns, conn_name);
        let conn_body = json!({
            "kafka": {
                "bootstrap_servers": kafka_bootstrap_servers
            }
        });
        debug!("Creating Kafka connection: {}", conn_url);
        let conn_resp = self.client.put(&conn_url).json(&conn_body).send().await?;
        if !conn_resp.status().is_success() {
            let status = conn_resp.status();
            let body = conn_resp.text().await.unwrap_or_default();
            if status.as_u16() == 409
                || body.contains("AlreadyExists")
                || body.contains("already exists")
            {
                info!("Kafka connection already exists: {}", conn_name);
            } else {
                anyhow::bail!("Failed to create Kafka connection ({}): {}", status, body);
            }
        } else {
            info!("Kafka connection created: {}", conn_name);
        }

        // Step 2: Create indexes for all streams in parallel.
        let mut index_futures = Vec::new();
        for stream in streams {
            let index_name = &stream.schema.index_name;
            let index_url = format!("{}/namespaces/{}/indexes/{}", base, ns, index_name);
            let mappings = Self::build_mappings(&stream.schema.fields);
            let index_body = json!({
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                },
                "mappings": mappings,
                "aliases": {}
            });
            debug!("Creating index: {}", index_url);
            let client = &self.client;
            let idx_name = index_name.clone();
            index_futures.push(async move {
                let resp = client.put(&index_url).json(&index_body).send().await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    if status.as_u16() == 409
                        || body.contains("AlreadyExists")
                        || body.contains("already exists")
                    {
                        info!("Index already exists: {}", idx_name);
                    } else {
                        anyhow::bail!(
                            "Failed to create index '{}' ({}): {}",
                            idx_name,
                            status,
                            body
                        );
                    }
                } else {
                    info!("Index created: {}", idx_name);
                }
                Ok::<_, anyhow::Error>(())
            });
        }
        futures::future::try_join_all(index_futures).await?;

        // Step 3: Create ingest pipelines for all streams in parallel.
        let mut consumer_groups: HashMap<String, String> = HashMap::new();
        let mut pipeline_futures = Vec::new();
        for stream in streams {
            let index_name = &stream.schema.index_name;
            let pipeline_name = format!("{}-pipeline", index_name);
            let consumer_group = format!("{}-cg", index_name);
            consumer_groups.insert(stream.name.clone(), consumer_group.clone());

            let pipeline_url = format!(
                "{}/namespaces/{}/ingest_pipelines/{}",
                base, ns, pipeline_name
            );
            let pipeline_body = json!({
                "index": index_name,
                "source_config": {
                    "connection": {
                        "namespace": ns,
                        "name": conn_name
                    },
                    "config": {
                        "type": "kafka",
                        "topic": &stream.kafka_topic,
                        "group_id": &consumer_group,
                        "data_format": "json"
                    }
                },
                "op_mode": "appendorupsert",
                "enabled": true,
                "poll_frequency_secs": 5,
                "max_ingest_workflows_limit": 4
            });
            debug!("Creating ingest pipeline: {}", pipeline_url);
            let client = &self.client;
            let p_name = pipeline_name.clone();
            pipeline_futures.push(async move {
                let resp = client
                    .put(&pipeline_url)
                    .json(&pipeline_body)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    if status.as_u16() == 409
                        || body.contains("AlreadyExists")
                        || body.contains("already exists")
                    {
                        info!("Ingest pipeline already exists: {}", p_name);
                    } else {
                        anyhow::bail!(
                            "Failed to create ingest pipeline '{}' ({}): {}",
                            p_name,
                            status,
                            body
                        );
                    }
                } else {
                    info!("Ingest pipeline created: {}", p_name);
                }
                Ok::<_, anyhow::Error>(())
            });
        }
        futures::future::try_join_all(pipeline_futures).await?;

        // Step 4: Create all warehouses in parallel.
        // Deduplicate — multiple groups may reference the same warehouse name.
        let mut unique_warehouses: HashMap<&str, &WarehouseConfig> = HashMap::new();
        for wh in warehouses {
            unique_warehouses.entry(&wh.name).or_insert(wh);
        }

        let mut warehouse_futures = Vec::new();
        for wh in unique_warehouses.values() {
            let warehouse_url = format!("{}/namespaces/{}/warehouses/{}", base, ns, wh.name);
            let warehouse_body = json!({
                "resource": {
                    "num_mediators": wh.num_mediators,
                    "num_os": wh.num_os,
                    "num_replica": 0,
                    "os": { "memory": "2147483648" },
                    "md": { "memory": "2147483648" },
                    "mdx": { "memory": "2147483648" },
                    "ir": { "memory": "1073741824" },
                    "osd": { "memory": "1073741824" },
                    "osd_enabled": true,
                    "immutable": false,
                    "segment_cache_capacity": 1073741824,
                    "local_parallelism": 4,
                    "cache_warming_enabled": false,
                    "cache_warming_query_history": 100,
                    "os_processors": 2,
                    "index_access_memory_limit": 536870912,
                    "read_cache_size_limit": 268435456,
                    "inactive_mode": {
                        "type": "Manual",
                        "idle_timeout_seconds": 300
                    }
                },
                "enabled": true,
                "memory_policy": "budgeted"
            });
            debug!("Creating warehouse: {}", warehouse_url);
            warehouse_futures.push(async move {
                let resp = self
                    .client
                    .put(&warehouse_url)
                    .json(&warehouse_body)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    if status.as_u16() == 409
                        || body.contains("AlreadyExists")
                        || body.contains("already exists")
                    {
                        info!("Warehouse already exists: {}", wh.name);
                    } else {
                        anyhow::bail!(
                            "Failed to create warehouse '{}' ({}): {}",
                            wh.name,
                            status,
                            body
                        );
                    }
                } else {
                    info!("Warehouse created: {}", wh.name);
                }
                Ok::<_, anyhow::Error>(wh.name.clone())
            });
        }

        let results = futures::future::try_join_all(warehouse_futures).await?;

        // Step 5: Resolve warehouse OS endpoints and poll until ready for queries.
        // Use the first stream's index for the readiness check.
        let first_index = streams
            .first()
            .map(|s| s.schema.index_name.as_str())
            .unwrap_or("_cat");

        let mut query_endpoints: HashMap<String, String> = HashMap::new();
        for wh_name in &results {
            let warehouse_get_url = format!("{}/namespaces/{}/warehouses/{}", base, ns, wh_name);
            info!("Waiting for warehouse '{}' to become ready...", wh_name);

            // Get warehouse ID to derive the OS service name.
            let wh_resp = self.client.get(&warehouse_get_url).send().await?;
            let wh_body: Value = wh_resp.json().await?;
            let wh_id = wh_body
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Warehouse '{}' missing 'id' field", wh_name))?;

            // Mach5 warehouse controller names the OS service: warehouse-os-{id[:34]}
            let id_len = wh_id.len().min(34);
            let id_prefix = &wh_id[..id_len];
            let os_endpoint = format!(
                "http://warehouse-os-{}.mach5.svc.cluster.local:9200",
                id_prefix
            );
            info!(
                "Warehouse '{}' id={}. Polling OS endpoint: {}",
                wh_name, wh_id, os_endpoint
            );

            // Poll: try to search the first index on the warehouse OS until it responds with 200.
            let search_url = format!("{}/{}/_search?size=0", os_endpoint, first_index);
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                match self
                    .client
                    .post(&search_url)
                    .json(&json!({"query": {"match_all": {}}}))
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        info!("Warehouse '{}' ready for queries: {}", wh_name, os_endpoint);
                        break;
                    }
                    Ok(resp) => {
                        debug!(
                            "Warehouse '{}' not ready yet (HTTP {}), retrying in 5s...",
                            wh_name,
                            resp.status()
                        );
                    }
                    Err(e) => {
                        debug!(
                            "Warehouse '{}' OS not reachable ({}), retrying in 5s...",
                            wh_name, e
                        );
                    }
                }
            }

            query_endpoints.insert(wh_name.clone(), os_endpoint);
        }

        Ok(PrepareResult {
            consumer_groups,
            query_endpoints,
        })
    }

    async fn execute_query(
        &self,
        query: &QueryDef,
        index_name: &str,
        query_endpoint: &str,
        variables: &HashMap<String, String>,
    ) -> anyhow::Result<QueryResult> {
        let url = format!("{}/{}/_search", query_endpoint, index_name);
        let body = Self::build_query_body(query, variables);

        let start = Instant::now();
        let timeout = std::time::Duration::from_millis(query.timeout_ms.max(1000));

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .timeout(timeout)
            .send()
            .await;

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        match resp {
            Ok(response) => {
                if response.status().is_success() {
                    let body: Value = response.json().await?;
                    let hit_count = body
                        .get("hits")
                        .and_then(|h| h.get("total"))
                        .and_then(|t| t.get("value").or(Some(t)))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    Ok(QueryResult {
                        hit_count,
                        error: None,
                        duration_ms,
                    })
                } else {
                    let status = response.status();
                    let err_body = response.text().await.unwrap_or_default();
                    Ok(QueryResult {
                        hit_count: 0,
                        error: Some(format!("HTTP {}: {}", status, err_body)),
                        duration_ms,
                    })
                }
            }
            Err(e) => Ok(QueryResult {
                hit_count: 0,
                error: Some(format!("Request failed: {}", e)),
                duration_ms,
            }),
        }
    }

    async fn cleanup(
        &self,
        streams: &[DataStreamConfig],
        warehouses: &[WarehouseConfig],
    ) -> anyhow::Result<()> {
        let base = self.api_base();
        let ns = &self.namespace;

        // Deduplicate warehouse names.
        let mut unique_wh_names: Vec<String> = Vec::new();
        for wh in warehouses {
            if !unique_wh_names.contains(&wh.name) {
                unique_wh_names.push(wh.name.clone());
            }
        }

        let stream_names: Vec<&str> = streams.iter().map(|s| s.name.as_str()).collect();
        info!(
            "Cleaning up Mach5 resources in namespace '{}': streams {:?}, warehouses {:?}",
            ns, stream_names, unique_wh_names
        );

        // Step 1: Delete pipelines and all warehouses in parallel.
        let mut delete_futures: Vec<
            std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>,
        > = Vec::new();

        // Delete pipelines per stream.
        for stream in streams {
            let pipeline_name = format!("{}-pipeline", stream.schema.index_name);
            let pipeline_url = format!(
                "{}/namespaces/{}/ingest_pipelines/{}",
                base, ns, pipeline_name
            );
            let p_name = pipeline_name.clone();
            delete_futures.push(Box::pin(async move {
                match self.client.delete(&pipeline_url).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() || resp.status().as_u16() == 404 {
                            info!("Deleted ingest pipeline: {}", p_name);
                        } else {
                            warn!("Failed to delete pipeline '{}': {}", p_name, resp.status());
                        }
                    }
                    Err(e) => warn!("Failed to delete pipeline '{}': {}", p_name, e),
                }
            }));
        }

        // Delete warehouses.
        for wh_name in &unique_wh_names {
            let url = format!("{}/namespaces/{}/warehouses/{}", base, ns, wh_name);
            let name = wh_name.clone();
            delete_futures.push(Box::pin(async move {
                match self.client.delete(&url).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() || resp.status().as_u16() == 404 {
                            info!("Deleted warehouse: {}", name);
                        } else {
                            warn!("Failed to delete warehouse '{}': {}", name, resp.status());
                        }
                    }
                    Err(e) => warn!("Failed to delete warehouse '{}': {}", name, e),
                }
            }));
        }

        futures::future::join_all(delete_futures).await;

        // Step 2: Delete indexes and connection in parallel.
        let mut delete_futures2: Vec<
            std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>,
        > = Vec::new();

        // Delete indexes per stream.
        for stream in streams {
            let index_name = stream.schema.index_name.clone();
            let index_url = format!("{}/namespaces/{}/indexes/{}", base, ns, index_name);
            delete_futures2.push(Box::pin(async move {
                match self.client.delete(&index_url).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() || resp.status().as_u16() == 404 {
                            info!("Deleted index: {}", index_name);
                        } else {
                            warn!("Failed to delete index '{}': {}", index_name, resp.status());
                        }
                    }
                    Err(e) => warn!("Failed to delete index '{}': {}", index_name, e),
                }
            }));
        }

        // Delete connection.
        let conn_url = format!(
            "{}/namespaces/{}/connections/{}",
            base, ns, self.connection_name
        );
        let conn_name = self.connection_name.clone();
        delete_futures2.push(Box::pin(async move {
            match self.client.delete(&conn_url).send().await {
                Ok(resp) => {
                    if resp.status().is_success() || resp.status().as_u16() == 404 {
                        info!("Deleted connection: {}", conn_name);
                    } else {
                        warn!(
                            "Failed to delete connection '{}': {}",
                            conn_name,
                            resp.status()
                        );
                    }
                }
                Err(e) => warn!("Failed to delete connection '{}': {}", conn_name, e),
            }
        }));

        futures::future::join_all(delete_futures2).await;

        // Step 3: Delete the Mach5 namespace itself.
        let ns_url = format!("{}/namespaces/{}", base, ns);
        let ns_result = self.client.delete(&ns_url).send().await;
        if let Ok(resp) = ns_result {
            if resp.status().is_success() || resp.status().as_u16() == 404 {
                info!("Deleted Mach5 namespace: {}", ns);
            } else {
                warn!(
                    "Failed to delete Mach5 namespace '{}': {}",
                    ns,
                    resp.status()
                );
            }
        }

        info!("Mach5 cleanup complete");
        Ok(())
    }
}
