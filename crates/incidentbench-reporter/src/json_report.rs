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

use incidentbench_common::metrics::{DerivedMetrics, PerQueryTimeSeries, Scorecard, TimeSeries};
use incidentbench_common::scenario::Scenario;
use serde_json::json;
use sha2::{Digest, Sha256};

pub fn generate(
    scenario: &Scenario,
    timeseries: &TimeSeries,
    derived: &DerivedMetrics,
    scorecard: &Scorecard,
    timed_out_queries: &[serde_json::Value],
    per_category_latency: &[serde_json::Value],
    per_query_latency: &[serde_json::Value],
    per_query_timeseries: &[PerQueryTimeSeries],
) -> anyhow::Result<String> {
    // Compute per-phase summaries.
    let phases: Vec<serde_json::Value> = scenario
        .timeline
        .phases
        .iter()
        .map(|phase_def| {
            let phase_points: Vec<_> = timeseries
                .points
                .iter()
                .filter(|p| p.phase == phase_def.name)
                .collect();

            let ingest_total: u64 = phase_points.iter().map(|p| p.ingest_events_produced).sum();
            let phase_duration_s = phase_def.duration_seconds as f64;

            let ingest_avg_eps = if phase_duration_s > 0.0 {
                ingest_total as f64 / phase_duration_s
            } else {
                0.0
            };

            let query_total: u64 = phase_points.iter().map(|p| p.query_executed).sum();
            let query_avg_qps = if phase_duration_s > 0.0 {
                query_total as f64 / phase_duration_s
            } else {
                0.0
            };

            let query_errors: u64 = phase_points.iter().map(|p| p.query_errors).sum();
            let query_timeouts: u64 = timed_out_queries
                .iter()
                .filter(|r| {
                    r.get("phase")
                        .and_then(|v| v.as_str())
                        .map(|phase| phase == phase_def.name)
                        .unwrap_or(false)
                })
                .count() as u64;
            let query_non_timeout_errors = query_errors.saturating_sub(query_timeouts);
            let query_error_rate = if query_total > 0 {
                query_errors as f64 / query_total as f64
            } else {
                0.0
            };

            let avg_p50 = mean_of(phase_points.iter().map(|p| p.query_latency.p50));
            let avg_p95 = mean_of(phase_points.iter().map(|p| p.query_latency.p95));
            let avg_p99 = mean_of(phase_points.iter().map(|p| p.query_latency.p99));
            let max_lag = phase_points
                .iter()
                .map(|p| p.kafka_consumer_lag)
                .max()
                .unwrap_or(0);

            // Sum target EPS across all data streams for this phase.
            let phase_target_eps: u64 = scenario
                .data_streams
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|s| {
                    s.ingest
                        .get(&phase_def.name)
                        .map(|i| i.target_eps)
                        .unwrap_or(0)
                })
                .sum();

            json!({
                "name": phase_def.name,
                "display_name": phase_def.display_name,
                "duration_s": phase_def.duration_seconds,
                "ingest": {
                    "target_eps": phase_target_eps,
                    "achieved_avg_eps": ingest_avg_eps,
                    "total_events": ingest_total,
                },
                "query": {
                    "target_qps": phase_def.query.target_qps,
                    "achieved_avg_qps": query_avg_qps,
                    "concurrent_sessions": phase_points
                        .iter()
                        .map(|p| p.concurrent_sessions)
                        .max()
                        .unwrap_or(0),
                    "total_queries": query_total,
                    "total_errors": query_errors,
                    "total_non_timeout_errors": query_non_timeout_errors,
                    "total_timeouts": query_timeouts,
                    "error_rate": query_error_rate,
                    "latency_p50_ms": avg_p50,
                    "latency_p95_ms": avg_p95,
                    "latency_p99_ms": avg_p99,
                },
                "kafka_peak_consumer_lag": max_lag,
            })
        })
        .collect();

    // Check valid-run criteria.
    let (valid, violations, warnings) = evaluate_validity(timeseries, scenario);

    // Per-query-group comparison (populated when query_groups are configured).
    let query_group_comparison = build_query_group_comparison(timeseries, scenario);

    let report = json!({
        "incidentbench_version": incidentbench_common::VERSION,
        "scenario": {
            "name": scenario.scenario.name,
            "version": scenario.scenario.version,
            "display_name": scenario.scenario.display_name,
            "description": scenario.scenario.description,
            "domain": scenario.scenario.domain,
        },
        "target": {
            "adapter": "mach5",
            "config_hash": compute_config_hash(scenario),
        },
        "harness": {
            "workers": {
                "ingest_replicas": 10, // TODO: from run config
                "query_replicas": 4,
            },
            "kafka": {
                "bootstrap_servers": "",
                "topics": scenario.data_streams.as_deref().unwrap_or(&[]).iter().map(|s| s.schema.index_name.as_str()).collect::<Vec<_>>(),
                "partitions": 10,
            },
            "scaling": {
                "rate_scale": 1.0,
                "duration_scale": 1.0,
            },
        },
        "run": {
            "id": "",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "valid": valid,
            "validity_violations": violations,
            "warnings": warnings,
            "harness_saturated": timeseries.points.iter().any(|p| p.harness_saturated),
            "saturated_workers": [],
        },
        "scorecard": {
            "baseline_p99_ms": scorecard.baseline_p99_ms,
            "overlap_p99_ms": scorecard.overlap_p99_ms,
            "p99_degradation_ratio": scorecard.p99_degradation_ratio,
            "query_error_rate_overlap": scorecard.query_error_rate_overlap,
            "peak_backlog": scorecard.peak_backlog,
            "backlog_drain_time_s": scorecard.backlog_drain_time_s,
            "recovery_time_s": scorecard.recovery_time_s,
        },
        "derived_metrics": derived,
        "phases": phases,
        "timeseries": {
            "resolution_s": timeseries.resolution_s,
            "point_count": timeseries.points.len(),
        },
        "query_groups": query_group_comparison,
        "timed_out_queries": timed_out_queries,
        "per_category_latency": per_category_latency,
        "per_query_latency": per_query_latency,
        "per_query_timeseries": per_query_timeseries,
    });

    serde_json::to_string_pretty(&report).map_err(Into::into)
}

pub fn build_per_category_latency(
    scenario: &Scenario,
    timed_out_queries: &[serde_json::Value],
    per_query_latency: &[serde_json::Value],
) -> Vec<serde_json::Value> {
    let pql_lookup: std::collections::HashMap<&str, &serde_json::Value> = per_query_latency
        .iter()
        .filter_map(|v| v.get("query_name").and_then(|n| n.as_str()).map(|n| (n, v)))
        .collect();

    let mut timeout_counts: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    for row in timed_out_queries {
        if let Some(category) = row.get("category").and_then(|v| v.as_str()) {
            *timeout_counts.entry(category.to_string()).or_default() += 1;
        }
    }

    let mut category_rollups: std::collections::BTreeMap<
        String,
        (f64, f64, f64, f64, f64, u64, u64),
    > = std::collections::BTreeMap::new(); // category -> (p50_sum, p95_max, p99_max, min, max, count, errors)

    for query in &scenario.query_mix.queries {
        let Some(category) = &query.category else {
            continue;
        };
        let Some(entry) = pql_lookup.get(query.name.as_str()) else {
            continue;
        };

        let p50 = entry.get("p50").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p95 = entry.get("p95").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p99 = entry.get("p99").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let min = entry.get("min").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let max = entry.get("max").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let count = entry.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
        let errors = entry
            .get("error_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            + entry
                .get("timeout_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

        let row = category_rollups.entry(category.clone()).or_insert((
            0.0,
            0.0,
            0.0,
            f64::INFINITY,
            0.0,
            0,
            0,
        ));
        row.0 += p50 * count as f64;
        row.1 = row.1.max(p95);
        row.2 = row.2.max(p99);
        if count > 0 {
            row.3 = row.3.min(min);
        }
        row.4 = row.4.max(max);
        row.5 += count;
        row.6 += errors;
    }

    category_rollups
        .into_iter()
        .map(|(category, (p50_sum, p95, p99, min, max, count, errors))| {
            let timeout_count = timeout_counts.get(&category).copied().unwrap_or(0);
            json!({
                "query_name": category,
                "min": if count > 0 { min } else { 0.0 },
                "p50": if count > 0 { p50_sum / count as f64 } else { 0.0 },
                "p95": p95,
                "p99": p99,
                "max": max,
                "count": count,
                "error_count": errors.saturating_sub(timeout_count),
                "timeout_count": timeout_count,
            })
        })
        .collect()
}

/// Build per-query-group comparison metrics from the time-series.
fn build_query_group_comparison(timeseries: &TimeSeries, scenario: &Scenario) -> serde_json::Value {
    // Collect all group names that appear in the time-series.
    let mut group_names = std::collections::HashSet::new();
    for point in &timeseries.points {
        for group_name in point.query_group_metrics.keys() {
            group_names.insert(group_name.clone());
        }
    }

    if group_names.is_empty() {
        return json!(null);
    }

    let mut groups = serde_json::Map::new();
    let baseline_duration_s = scenario
        .timeline
        .phases
        .iter()
        .find(|p| p.name == "baseline")
        .map(|p| p.duration_seconds as f64)
        .unwrap_or(0.0);
    let overlap_duration_s = scenario
        .timeline
        .phases
        .iter()
        .find(|p| p.name == "overlap")
        .map(|p| p.duration_seconds as f64)
        .unwrap_or(0.0);
    for group_name in &group_names {
        let baseline_points: Vec<_> = timeseries
            .points
            .iter()
            .filter(|p| p.phase == "baseline")
            .filter_map(|p| p.query_group_metrics.get(group_name))
            .collect();
        let overlap_points: Vec<_> = timeseries
            .points
            .iter()
            .filter(|p| p.phase == "overlap")
            .filter_map(|p| p.query_group_metrics.get(group_name))
            .collect();

        let baseline_p99 = mean_of(baseline_points.iter().map(|m| m.latency.p99));
        let overlap_p99 = mean_of(overlap_points.iter().map(|m| m.latency.p99));
        let baseline_qps = if baseline_duration_s > 0.0 {
            baseline_points.iter().map(|m| m.executed).sum::<u64>() as f64 / baseline_duration_s
        } else {
            0.0
        };
        let overlap_qps = if overlap_duration_s > 0.0 {
            overlap_points.iter().map(|m| m.executed).sum::<u64>() as f64 / overlap_duration_s
        } else {
            0.0
        };
        let overlap_errors: u64 = overlap_points.iter().map(|m| m.errors).sum();
        let overlap_total: u64 = overlap_points.iter().map(|m| m.executed).sum();
        let error_rate = if overlap_total > 0 {
            overlap_errors as f64 / overlap_total as f64
        } else {
            0.0
        };
        let degradation = if baseline_p99 > 0.0 {
            overlap_p99 / baseline_p99
        } else {
            0.0
        };

        let warehouse_name = baseline_points
            .first()
            .or(overlap_points.first())
            .map(|m| m.warehouse_name.as_str())
            .unwrap_or("");

        groups.insert(
            group_name.clone(),
            json!({
                "warehouse": warehouse_name,
                "baseline_p99_ms": baseline_p99,
                "overlap_p99_ms": overlap_p99,
                "p99_degradation_ratio": degradation,
                "baseline_avg_qps": baseline_qps,
                "overlap_avg_qps": overlap_qps,
                "overlap_error_rate": error_rate,
            }),
        );
    }

    serde_json::Value::Object(groups)
}

/// Compute a SHA-256 hash of the scenario configuration for reproducibility tracking.
fn compute_config_hash(scenario: &Scenario) -> String {
    let yaml = serde_yaml::to_string(scenario).unwrap_or_default();
    let hash = Sha256::digest(yaml.as_bytes());
    format!("{:x}", hash)
}

/// Evaluate run validity based on built-in checks and scenario-defined valid_run_criteria rules.
/// Returns (valid, violations, warnings).
pub fn evaluate_validity(
    timeseries: &TimeSeries,
    scenario: &Scenario,
) -> (bool, Vec<String>, Vec<String>) {
    let mut violations = Vec::new();
    let mut warnings = Vec::new();

    let baseline_points: Vec<_> = timeseries
        .points
        .iter()
        .filter(|p| p.phase == "baseline")
        .collect();
    let overlap_points: Vec<_> = timeseries
        .points
        .iter()
        .filter(|p| p.phase == "overlap")
        .collect();

    // Baseline stability: p99 must not vary by more than 2x from median.
    if !baseline_points.is_empty() {
        let mut p99s: Vec<f64> = baseline_points
            .iter()
            .map(|p| p.query_latency.p99)
            .collect();
        p99s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = p99s[p99s.len() / 2];
        if let Some(max_p99) = p99s.last() {
            if median > 0.0 && *max_p99 > median * 2.0 {
                violations.push(format!(
                    "Baseline p99 unstable: max {:.1}ms > 2x median {:.1}ms",
                    max_p99, median
                ));
            }
        }
    }

    // Ingest target met at baseline (sum across all streams).
    if !baseline_points.is_empty() {
        let baseline_target_eps: u64 = scenario
            .data_streams
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|s| s.ingest.get("baseline").map(|i| i.target_eps).unwrap_or(0))
            .sum();
        if baseline_target_eps > 0 {
            let baseline_duration_s = scenario
                .timeline
                .phases
                .iter()
                .find(|p| p.name == "baseline")
                .map(|p| p.duration_seconds as f64)
                .unwrap_or(0.0);
            let achieved_eps = if baseline_duration_s > 0.0 {
                baseline_points
                    .iter()
                    .map(|p| p.ingest_events_produced)
                    .sum::<u64>() as f64
                    / baseline_duration_s
            } else {
                0.0
            };
            let threshold = baseline_target_eps as f64 * 0.9;
            if achieved_eps < threshold {
                violations.push(format!(
                    "Baseline ingest EPS {:.0} < 90% of target {}",
                    achieved_eps, baseline_target_eps
                ));
            }
        }
    }

    // Overlap query underperformance (advisory).
    if !overlap_points.is_empty() {
        let overlap_phase = scenario
            .timeline
            .phases
            .iter()
            .find(|p| p.name == "overlap");
        if let Some(op) = overlap_phase {
            let achieved_qps = if op.duration_seconds > 0 {
                overlap_points.iter().map(|p| p.query_executed).sum::<u64>() as f64
                    / op.duration_seconds as f64
            } else {
                0.0
            };
            let threshold = op.query.target_qps * 0.5;
            if achieved_qps < threshold {
                warnings.push(format!(
                    "Overlap QPS {:.1} < 50% of target {:.1}",
                    achieved_qps, op.query.target_qps
                ));
            }
        }
    }

    // Harness saturation (advisory).
    if timeseries.points.iter().any(|p| p.harness_saturated) {
        warnings.push("Harness saturation detected".to_string());
    }

    // Evaluate scenario-defined valid_run_criteria rules.
    let derived = incidentbench_common::metrics::compute_derived(
        &incidentbench_common::metrics::TimeSeries {
            resolution_s: 1,
            points: timeseries.points.clone(),
        },
    );
    for rule in &scenario.valid_run_criteria.rules {
        let violated = evaluate_criteria_rule(&rule.condition, &derived, timeseries, scenario);
        if violated {
            let msg = if rule.message.is_empty() {
                format!("Criteria '{}' violated: {}", rule.name, rule.condition)
            } else {
                format!("{}: {}", rule.name, rule.message)
            };
            violations.push(msg);
        }
    }

    let valid = violations.is_empty();
    (valid, violations, warnings)
}

/// Evaluate a single valid_run_criteria condition.
/// Returns true if the condition is violated (i.e. the run fails the rule).
/// Supports simple expressions like:
///   "query_error_rate_overlap < 0.1" — violated if error rate >= 0.1
///   "p99_degradation_ratio < 5.0" — violated if degradation >= 5.0
///   "peak_backlog < 10000" — violated if peak backlog >= 10000
fn evaluate_criteria_rule(
    condition: &str,
    derived: &incidentbench_common::metrics::DerivedMetrics,
    _timeseries: &TimeSeries,
    _scenario: &Scenario,
) -> bool {
    let parts: Vec<&str> = condition.split_whitespace().collect();
    if parts.len() != 3 {
        return false; // Unparseable condition — skip.
    }

    let metric_name = parts[0];
    let operator = parts[1];
    let threshold: f64 = match parts[2].parse() {
        Ok(v) => v,
        Err(_) => return false,
    };

    let actual = match metric_name {
        "query_error_rate_overlap" => derived.query_error_rate_overlap,
        "query_p99_ratio" | "p99_degradation_ratio" => derived.query_p99_ratio,
        "ingest_peak_backlog" | "peak_backlog" => derived.ingest_peak_backlog as f64,
        "ingest_backlog_drain_time_s" | "backlog_drain_time_s" => {
            derived.ingest_backlog_drain_time_s
        }
        "recovery_time_to_baseline_s" | "recovery_time_s" => derived.recovery_time_to_baseline_s,
        "query_baseline_p99" | "baseline_p99_ms" => derived.query_baseline_p99,
        "query_overlap_p99" | "overlap_p99_ms" => derived.query_overlap_p99,
        "query_throughput_ratio" => derived.query_throughput_ratio,
        "ingest_throughput_ratio" => derived.ingest_throughput_ratio,
        _ => return false, // Unknown metric — skip.
    };

    // "violated" means the condition is NOT met.
    match operator {
        "<" => !(actual < threshold),
        "<=" => !(actual <= threshold),
        ">" => !(actual > threshold),
        ">=" => !(actual >= threshold),
        "==" => !((actual - threshold).abs() < f64::EPSILON),
        _ => false,
    }
}

fn mean_of(iter: impl Iterator<Item = f64>) -> f64 {
    let mut count = 0u64;
    let mut sum = 0.0;
    for v in iter {
        sum += v;
        count += 1;
    }
    if count > 0 {
        sum / count as f64
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use incidentbench_common::metrics::{AggregatedMetricPoint, PercentileSummary};

    fn make_scenario_for_test() -> Scenario {
        use incidentbench_common::scenario::*;

        let stream = DataStream {
            name: "test".to_string(),
            schema: Schema {
                index_name: "test".to_string(),
                timestamp_field: "@timestamp".to_string(),
                fields: vec![FieldDef {
                    name: "@timestamp".to_string(),
                    field_type: FieldType::Timestamp,
                    generator: "now".to_string(),
                    config: serde_json::Value::Null,
                }],
            },
            data_generator: DataGeneratorConfig {
                generator_type: "template".to_string(),
                config: serde_json::json!({"seed": 1}),
            },
            kafka_partitions: None,
            ingest_replicas: 1,
            ingest: [
                ("baseline", 100),
                ("incident_trigger", 200),
                ("ingestion_surge", 500),
                ("overlap", 500),
                ("recovery", 200),
                ("post_incident", 100),
            ]
            .into_iter()
            .map(|(n, eps)| {
                (
                    n.to_string(),
                    StreamPhaseIngest {
                        target_eps: eps,
                        batch_size: 500,
                    },
                )
            })
            .collect(),
        };

        Scenario {
            scenario: ScenarioMeta {
                name: "test".to_string(),
                version: "1.0".to_string(),
                display_name: "Test".to_string(),
                description: String::new(),
                domain: "sre".to_string(),
            },
            data_streams: Some(vec![stream]),
            default_timeout_ms: 10_000,
            query_session: None,
            query_mix: QueryMix {
                queries: vec![QueryDef {
                    name: "q1".to_string(),
                    query_type: "search".to_string(),
                    template: "*".to_string(),
                    index: "test".to_string(),
                    sort: None,
                    limit: None,
                    timeout_ms: 5000,
                    description: String::new(),
                    variables: std::collections::HashMap::new(),
                    sql: None,
                    sql_file: None,
                    category: None,
                }],
            },
            query_groups: None,
            timeline: Timeline {
                phases: [
                    "baseline",
                    "incident_trigger",
                    "ingestion_surge",
                    "overlap",
                    "recovery",
                    "post_incident",
                ]
                .iter()
                .map(|n| PhaseDef {
                    name: n.to_string(),
                    display_name: n.to_string(),
                    duration_seconds: 30,
                    query: QueryConfig {
                        target_qps: 10.0,
                        mix_override: None,
                    },
                    description: String::new(),
                })
                .collect(),
            },
            valid_run_criteria: ValidRunCriteria {
                rules: vec![ValidRunRule {
                    name: "max_error_rate".to_string(),
                    condition: "query_error_rate_overlap < 0.1".to_string(),
                    message: "error rate too high".to_string(),
                }],
            },
            report: ReportConfig::default(),
        }
    }

    fn make_point(phase: &str, p99: f64, ingest_produced: u64) -> AggregatedMetricPoint {
        AggregatedMetricPoint {
            timestamp_s: 0,
            phase: phase.to_string(),
            ingest_events_produced: ingest_produced,
            ingest_events_acknowledged: ingest_produced,
            ingest_events_failed: 0,
            ingest_target_eps: ingest_produced,
            ingest_kafka_produce_latency: PercentileSummary::default(),
            query_executed: 10,
            query_errors: 0,
            query_target_qps: 10.0,
            query_latency: PercentileSummary {
                p50: p99 * 0.5,
                p95: p99 * 0.8,
                p99,
                max: p99 * 1.2,
                count: 10,
            },
            kafka_consumer_lag: 0,
            ingest_workers_reporting: 1,
            query_workers_reporting: 1,
            concurrent_sessions: 0,
            harness_saturated: false,
            max_worker_cpu: 0.3,
            query_group_metrics: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_evaluate_validity_passes_clean_run() {
        let scenario = make_scenario_for_test();
        let mut points = Vec::new();
        for _ in 0..10 {
            points.push(make_point("baseline", 5.0, 300));
        }
        for _ in 0..10 {
            points.push(make_point("overlap", 8.0, 1500));
        }
        let ts = TimeSeries {
            resolution_s: 1,
            points,
        };
        let (valid, violations, _) = evaluate_validity(&ts, &scenario);
        assert!(valid, "Expected valid, violations: {:?}", violations);
    }

    #[test]
    fn test_evaluate_validity_fails_unstable_baseline() {
        let scenario = make_scenario_for_test();
        let mut points = Vec::new();
        for _ in 0..5 {
            points.push(make_point("baseline", 5.0, 300));
        }
        // Inject a huge p99 spike in baseline
        points.push(make_point("baseline", 50.0, 300));
        for _ in 0..10 {
            points.push(make_point("overlap", 8.0, 1500));
        }
        let ts = TimeSeries {
            resolution_s: 1,
            points,
        };
        let (valid, violations, _) = evaluate_validity(&ts, &scenario);
        assert!(!valid);
        assert!(violations
            .iter()
            .any(|v| v.contains("Baseline p99 unstable")));
    }

    #[test]
    fn test_evaluate_validity_custom_criteria_violation() {
        let scenario = make_scenario_for_test();
        let mut points = Vec::new();
        for _ in 0..10 {
            points.push(make_point("baseline", 5.0, 100));
        }
        // Overlap has errors
        let mut overlap_point = make_point("overlap", 8.0, 500);
        overlap_point.ingest_events_produced = 1500;
        overlap_point.ingest_events_acknowledged = 1500;
        overlap_point.ingest_target_eps = 1500;
        overlap_point.query_errors = 5; // 5/10 = 50% error rate > 10%
        for _ in 0..10 {
            points.push(overlap_point.clone());
        }
        let ts = TimeSeries {
            resolution_s: 1,
            points,
        };
        let (valid, violations, _) = evaluate_validity(&ts, &scenario);
        assert!(!valid);
        assert!(violations.iter().any(|v| v.contains("error rate too high")));
    }

    #[test]
    fn test_config_hash_deterministic() {
        let scenario = make_scenario_for_test();
        let hash1 = compute_config_hash(&scenario);
        let hash2 = compute_config_hash(&scenario);
        assert_eq!(hash1, hash2);
        assert!(!hash1.is_empty());
        assert!(hash1.len() == 64); // SHA-256 hex = 64 chars
    }
}
