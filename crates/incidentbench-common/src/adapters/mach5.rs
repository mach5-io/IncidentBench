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
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

struct PgConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
    /// Mach5 warehouse name — passed as PostgreSQL startup option.
    warehouse: String,
}

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
    /// PostgreSQL gateway config — present when sql queries are expected.
    pg_config: Option<PgConfig>,
    /// One connection per session category (or per query name outside session mode)
    /// so each logical execution lane has its own backend PID. This keeps a pgwire
    /// CancelRequest scoped to the active statement for that lane instead of whatever
    /// later statement happens to reuse a shared connection.
    /// Arc allows cloning the pointer out of the lock before executing, so the
    /// lock is held only during init/lookup — not for the duration of the query.
    pg_clients: tokio::sync::Mutex<
        std::collections::HashMap<String, std::sync::Arc<tokio_postgres::Client>>,
    >,
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

        // PostgreSQL gateway config — all fields optional; sql queries require them at runtime.
        let pg_config = {
            let host = config
                .get("pg_host")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let port = config
                .get("pg_port")
                .and_then(|v| v.as_u64())
                .map(|p| p as u16)
                .unwrap_or(5432);
            let user = config
                .get("pg_user")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let password = config
                .get("pg_password")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let dbname = config
                .get("pg_dbname")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let warehouse = config
                .get("warehouse")
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            match (host, user, password, dbname) {
                (Some(host), Some(user), Some(password), Some(dbname)) => Some(PgConfig {
                    host,
                    port,
                    user,
                    password,
                    dbname,
                    warehouse,
                }),
                _ => None,
            }
        };

        Ok(Self {
            client: Client::builder()
                .danger_accept_invalid_certs(true) // TODO: make configurable
                .build()?,
            endpoint,
            namespace,
            connection_name: "incidentbench-kafka-conn".to_string(),
            pg_config,
            pg_clients: tokio::sync::Mutex::new(std::collections::HashMap::new()),
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

    pub(crate) async fn execute_sql_query(&self, query: &QueryDef) -> anyhow::Result<QueryResult> {
        let cfg = self.pg_config.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "SQL query requires pg_host/pg_user/pg_password/pg_dbname in adapter config"
            )
        })?;

        // Session mode fires one query per category per tick, then round-robins within
        // that category on later ticks. Reuse one pg connection per category so the
        // backend PID remains stable for that logical lane across timeouts and retries.
        // Outside session mode, fall back to query name to avoid collapsing unrelated
        // queries onto the same PID.
        let client_key = query.category.as_deref().unwrap_or(&query.name).to_string();

        // A pgwire CancelRequest targets a PID — a shared connection would cause a cancel
        // for one logical lane to hit whichever query is currently executing on that PID.
        // Arc lets us clone the pointer out before dropping the lock so execution is
        // fully concurrent across categories.
        let pg: std::sync::Arc<tokio_postgres::Client> = {
            let mut clients = self.pg_clients.lock().await;
            if !clients.contains_key(&client_key) {
                let mut conn_str = format!(
                    "host={} port={} user={} password={} dbname={}",
                    cfg.host, cfg.port, cfg.user, cfg.password, cfg.dbname
                );
                if !cfg.warehouse.is_empty() {
                    conn_str.push_str(&format!(" options='-c warehouse={}'", cfg.warehouse));
                }
                let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
                    .await
                    .map_err(|e| anyhow::anyhow!("PostgreSQL connection failed: {}", e))?;
                tokio::spawn(connection);
                clients.insert(client_key.clone(), std::sync::Arc::new(client));
            }
            std::sync::Arc::clone(clients.get(&client_key).unwrap())
        }; // lock released — other category queries initialise / execute concurrently

        // Prefer inline sql field; fall back to reading from sql_file mount path; then template.
        let sql_owned;
        let sql: &str = if let Some(inline) = query.sql.as_deref().filter(|s| !s.is_empty()) {
            inline
        } else if let Some(file_path) = &query.sql_file {
            let mount_key = file_path.replace('/', "_");
            let full_path = format!("/queries/{}", mount_key);
            sql_owned = std::fs::read_to_string(&full_path)
                .map_err(|e| anyhow::anyhow!("Failed to read SQL file {}: {}", full_path, e))?;
            &sql_owned
        } else {
            query.template.as_str()
        };

        let timeout_ms = if query.timeout_ms > 0 {
            query.timeout_ms
        } else {
            10_000
        };
        // cancel_token captures this connection's backend PID/key — owned value, no lock needed.
        let cancel_token = pg.cancel_token();
        let start = Instant::now();

        let result =
            tokio::time::timeout(Duration::from_millis(timeout_ms), pg.query(sql, &[])).await;
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(Ok(rows)) => Ok(QueryResult {
                hit_count: rows.len() as u64,
                row_count: Some(rows.len() as u64),
                duration_ms,
                ..Default::default()
            }),
            Ok(Err(e)) => Ok(QueryResult {
                error: Some(e.to_string()),
                duration_ms,
                ..Default::default()
            }),
            Err(_elapsed) => {
                // Send pgwire CancelRequest to the Mach5 gateway. The gateway maps this
                // connection's backend PID to the active MDX query_id and cancels only
                // that statement. The connection background task drains the resulting
                // ErrorResponse + ReadyForQuery and returns to idle — no reconnect needed.
                if let Err(e) = cancel_token.cancel_query(NoTls).await {
                    warn!(
                        query = %query.name,
                        timeout_ms,
                        "Failed to send CancelRequest: {}",
                        e
                    );
                }
                Ok(QueryResult {
                    duration_ms,
                    timed_out: true,
                    ..Default::default()
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

        let mut consumer_groups: HashMap<String, String> = HashMap::new();

        if !streams.is_empty() {
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
        } else {
            info!("No data streams — skipping Kafka connection and ingest pipeline setup");
        }

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

        // Step 5: Poll warehouse readiness via the Mach5 REST API, then record query endpoints.
        // Polling via the API (rather than directly hitting the OS service) works whether the
        // operator runs in-cluster or remotely (cross-cluster / kind dev setup).
        let mut query_endpoints: HashMap<String, String> = HashMap::new();
        for wh_name in &results {
            let warehouse_get_url = format!("{}/namespaces/{}/warehouses/{}", base, ns, wh_name);
            info!("Waiting for warehouse '{}' to become ready...", wh_name);

            // Give the warehouse controller a moment to schedule pods before polling.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            let wh_id = loop {
                match self.client.get(&warehouse_get_url).send().await {
                    Ok(resp) => {
                        let body: Value = match resp.json().await {
                            Ok(b) => b,
                            Err(e) => {
                                debug!("Warehouse '{}' status parse error: {}", wh_name, e);
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        };
                        let id = body
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        // Mach5 API has no explicit status field — enabled:true means the
                        // warehouse resource exists and the controller has accepted it.
                        // Use enabled:true as the readiness signal; explicit status fields
                        // (if added in future API versions) also accepted.
                        let explicit_status =
                            body.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        let enabled = body
                            .get("enabled")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        let is_ready = enabled
                            || explicit_status == "ready"
                            || explicit_status == "Running"
                            || explicit_status == "running";
                        if is_ready && !id.is_empty() {
                            info!(
                                "Warehouse '{}' is ready (enabled={}, status='{}')",
                                wh_name, enabled, explicit_status
                            );
                            break id;
                        }
                        debug!("Warehouse '{}' not ready yet (enabled={}, status='{}'), retrying in 5s...", wh_name, enabled, explicit_status);
                    }
                    Err(e) => {
                        debug!(
                            "Warehouse '{}' API not reachable ({}), retrying in 5s...",
                            wh_name, e
                        );
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            };

            // Build the query endpoint: use the Mach5 nginx proxy URL so it works from any
            // network location. Direct OS service URLs only work in-cluster.
            let id_len = wh_id.len().min(34);
            let id_prefix = &wh_id[..id_len.max(1)];
            let os_endpoint = format!(
                "{}/namespaces/{}/warehouses/{}/query",
                self.endpoint, ns, wh_name
            );
            info!(
                "Warehouse '{}' id={}. Query endpoint: {}",
                wh_name, id_prefix, os_endpoint
            );
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
        if query.query_type == "sql" {
            return self.execute_sql_query(query).await;
        }

        let url = format!("{}/{}/_search", query_endpoint, index_name);
        let body = Self::build_query_body(query, variables);

        let start = Instant::now();
        let timeout = Duration::from_millis(query.timeout_ms.max(1000));

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
                        duration_ms,
                        ..Default::default()
                    })
                } else {
                    let status = response.status();
                    let err_body = response.text().await.unwrap_or_default();
                    Ok(QueryResult {
                        error: Some(format!("HTTP {}: {}", status, err_body)),
                        duration_ms,
                        ..Default::default()
                    })
                }
            }
            Err(e) => {
                let timed_out = e.is_timeout();
                Ok(QueryResult {
                    error: Some(format!("Request failed: {}", e)),
                    duration_ms,
                    timed_out,
                    ..Default::default()
                })
            }
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

        // Delete warehouses (skip for protected namespaces — warehouse is pre-existing).
        if ns != "default" {
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

        // Step 3: Delete the Mach5 namespace itself (skip for protected namespaces).
        if ns == "default" {
            info!(
                "Skipping namespace deletion for protected namespace '{}'",
                ns
            );
        } else {
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
        }

        info!("Mach5 cleanup complete");
        Ok(())
    }
}
