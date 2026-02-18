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

use incidentbench_common::metrics::{DerivedMetrics, Scorecard, TimeSeries};
use incidentbench_common::scenario::Scenario;

/// Generate a self-contained HTML report with embedded CSS and JavaScript.
/// Uses inline Chart.js for timeline visualization.
pub fn generate(
    scenario: &Scenario,
    timeseries: &TimeSeries,
    _derived: &DerivedMetrics,
    scorecard: &Scorecard,
) -> anyhow::Result<String> {
    let timestamps: Vec<u64> = timeseries.points.iter().map(|p| p.timestamp_s).collect();
    let ingest_eps: Vec<u64> = timeseries
        .points
        .iter()
        .map(|p| p.ingest_events_produced)
        .collect();
    let ingest_target: Vec<u64> = timeseries
        .points
        .iter()
        .map(|p| p.ingest_target_eps)
        .collect();
    let query_p99: Vec<f64> = timeseries
        .points
        .iter()
        .map(|p| p.query_latency.p99)
        .collect();
    let query_p50: Vec<f64> = timeseries
        .points
        .iter()
        .map(|p| p.query_latency.p50)
        .collect();
    let _query_qps: Vec<u64> = timeseries.points.iter().map(|p| p.query_executed).collect();
    let kafka_lag: Vec<u64> = timeseries
        .points
        .iter()
        .map(|p| p.kafka_consumer_lag)
        .collect();
    let phases: Vec<String> = timeseries.points.iter().map(|p| p.phase.clone()).collect();

    // Find phase boundary timestamps.
    let mut phase_boundaries: Vec<(u64, String)> = Vec::new();
    let mut current_phase = String::new();
    for p in &timeseries.points {
        if p.phase != current_phase {
            phase_boundaries.push((p.timestamp_s, p.phase.clone()));
            current_phase = p.phase.clone();
        }
    }

    // Use the same validity evaluation as the JSON report (including scenario criteria).
    let (is_valid, _violations, _warnings) =
        crate::json_report::evaluate_validity(timeseries, scenario);
    let validity_status = if is_valid { "VALID" } else { "INVALID" };
    let validity_class = if is_valid { "valid" } else { "invalid" };

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IncidentBench Report — {scenario_name}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  :root {{
    --bg: #0a0a0a; --fg: #e0e0e0; --accent: #4fc3f7; --danger: #ef5350;
    --success: #66bb6a; --warning: #ffa726; --card-bg: #1a1a1a;
    --border: #333;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'Inter', system-ui, sans-serif; background: var(--bg); color: var(--fg); padding: 2rem; line-height: 1.6; }}
  h1 {{ color: var(--accent); margin-bottom: 0.5rem; font-size: 1.8rem; }}
  h2 {{ color: var(--accent); margin: 2rem 0 1rem; font-size: 1.3rem; border-bottom: 1px solid var(--border); padding-bottom: 0.5rem; }}
  .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; }}
  .badge {{ padding: 0.25rem 0.75rem; border-radius: 4px; font-weight: 600; font-size: 0.85rem; }}
  .badge.valid {{ background: var(--success); color: #000; }}
  .badge.invalid {{ background: var(--danger); color: #fff; }}
  .scorecard {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin: 1rem 0; }}
  .metric-card {{ background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 1.25rem; }}
  .metric-card .label {{ color: #999; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }}
  .metric-card .value {{ font-size: 2rem; font-weight: 700; margin: 0.25rem 0; }}
  .metric-card .unit {{ color: #666; font-size: 0.85rem; }}
  .chart-container {{ background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; margin: 1rem 0; }}
  table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; }}
  th, td {{ padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid var(--border); }}
  th {{ color: #999; font-weight: 600; font-size: 0.85rem; text-transform: uppercase; }}
  .meta {{ color: #666; font-size: 0.85rem; }}
  canvas {{ max-height: 300px; }}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>IncidentBench Report</h1>
    <div class="meta">{scenario_display} v{scenario_version} &mdash; {timestamp}</div>
  </div>
  <span class="badge {validity_class}">{validity_status}</span>
</div>

<h2>Scorecard</h2>
<div class="scorecard">
  <div class="metric-card">
    <div class="label">Baseline p99</div>
    <div class="value">{baseline_p99:.1}</div>
    <div class="unit">ms</div>
  </div>
  <div class="metric-card">
    <div class="label">Overlap p99</div>
    <div class="value">{overlap_p99:.1}</div>
    <div class="unit">ms</div>
  </div>
  <div class="metric-card">
    <div class="label">Degradation Ratio</div>
    <div class="value">{degradation:.1}x</div>
    <div class="unit">overlap / baseline</div>
  </div>
  <div class="metric-card">
    <div class="label">Query Error Rate</div>
    <div class="value">{error_rate:.1}%</div>
    <div class="unit">during overlap</div>
  </div>
  <div class="metric-card">
    <div class="label">Peak Backlog</div>
    <div class="value">{peak_backlog}</div>
    <div class="unit">events (Kafka lag)</div>
  </div>
  <div class="metric-card">
    <div class="label">Backlog Drain</div>
    <div class="value">{drain_time:.1}</div>
    <div class="unit">seconds</div>
  </div>
  <div class="metric-card">
    <div class="label">Recovery Time</div>
    <div class="value">{recovery_time:.1}</div>
    <div class="unit">seconds to baseline</div>
  </div>
</div>

<h2>Ingestion Timeline</h2>
<div class="chart-container"><canvas id="ingestChart"></canvas></div>

<h2>Query Latency Timeline</h2>
<div class="chart-container"><canvas id="latencyChart"></canvas></div>

<h2>Kafka Consumer Lag</h2>
<div class="chart-container"><canvas id="lagChart"></canvas></div>

{query_group_section}

<h2>Phase Summary</h2>
<table>
  <tr><th>Phase</th><th>Duration</th><th>Ingest EPS</th><th>Query QPS</th><th>p99 Latency</th><th>Kafka Lag</th></tr>
  {phase_rows}
</table>

<h2>Run Metadata</h2>
<table>
  <tr><td>Harness Version</td><td>{version}</td></tr>
  <tr><td>Scenario</td><td>{scenario_name} v{scenario_version}</td></tr>
  <tr><td>Domain</td><td>{domain}</td></tr>
  <tr><td>Total Duration</td><td>{total_duration}s</td></tr>
  <tr><td>Total Events</td><td>{total_events}</td></tr>
</table>

<script>
const labels = {timestamps_json};
const phases = {phases_json};
const phaseBoundaries = {boundaries_json};

function phaseColors(phases) {{
  const colorMap = {{
    'baseline': 'rgba(102,187,106,0.1)',
    'incident_trigger': 'rgba(255,167,38,0.1)',
    'ingestion_surge': 'rgba(239,83,80,0.15)',
    'overlap': 'rgba(239,83,80,0.25)',
    'recovery': 'rgba(255,167,38,0.1)',
    'post_incident': 'rgba(102,187,106,0.1)',
  }};
  return phases.map(p => colorMap[p] || 'rgba(0,0,0,0)');
}}

new Chart(document.getElementById('ingestChart'), {{
  type: 'line',
  data: {{
    labels: labels,
    datasets: [
      {{ label: 'Achieved EPS', data: {ingest_eps_json}, borderColor: '#4fc3f7', borderWidth: 1.5, pointRadius: 0, fill: false }},
      {{ label: 'Target EPS', data: {ingest_target_json}, borderColor: '#666', borderWidth: 1, borderDash: [5,5], pointRadius: 0, fill: false }},
    ]
  }},
  options: {{ responsive: true, scales: {{ y: {{ beginAtZero: true, ticks: {{ color: '#999' }} }}, x: {{ ticks: {{ color: '#999', maxTicksLimit: 10 }} }} }}, plugins: {{ legend: {{ labels: {{ color: '#ccc' }} }} }} }}
}});

new Chart(document.getElementById('latencyChart'), {{
  type: 'line',
  data: {{
    labels: labels,
    datasets: [
      {{ label: 'p99', data: {query_p99_json}, borderColor: '#ef5350', borderWidth: 1.5, pointRadius: 0, fill: false }},
      {{ label: 'p50', data: {query_p50_json}, borderColor: '#66bb6a', borderWidth: 1.5, pointRadius: 0, fill: false }},
    ]
  }},
  options: {{ responsive: true, scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'ms', color: '#999' }}, ticks: {{ color: '#999' }} }}, x: {{ ticks: {{ color: '#999', maxTicksLimit: 10 }} }} }}, plugins: {{ legend: {{ labels: {{ color: '#ccc' }} }} }} }}
}});

new Chart(document.getElementById('lagChart'), {{
  type: 'line',
  data: {{
    labels: labels,
    datasets: [
      {{ label: 'Consumer Lag', data: {kafka_lag_json}, borderColor: '#ffa726', borderWidth: 1.5, pointRadius: 0, fill: true, backgroundColor: 'rgba(255,167,38,0.1)' }},
    ]
  }},
  options: {{ responsive: true, scales: {{ y: {{ beginAtZero: true, ticks: {{ color: '#999' }} }}, x: {{ ticks: {{ color: '#999', maxTicksLimit: 10 }} }} }}, plugins: {{ legend: {{ labels: {{ color: '#ccc' }} }} }} }}
}});
</script>
</body>
</html>"#,
        scenario_name = scenario.scenario.name,
        scenario_display = scenario.scenario.display_name,
        scenario_version = scenario.scenario.version,
        timestamp = chrono::Utc::now().to_rfc3339(),
        validity_class = validity_class,
        validity_status = validity_status,
        baseline_p99 = scorecard.baseline_p99_ms,
        overlap_p99 = scorecard.overlap_p99_ms,
        degradation = scorecard.p99_degradation_ratio,
        error_rate = scorecard.query_error_rate_overlap * 100.0,
        peak_backlog = scorecard.peak_backlog,
        drain_time = scorecard.backlog_drain_time_s,
        recovery_time = scorecard.recovery_time_s,
        phase_rows = generate_phase_rows(scenario, timeseries),
        version = incidentbench_common::VERSION,
        domain = scenario.scenario.domain,
        total_duration = scenario.total_duration_seconds(),
        total_events = scenario.total_events_at_target(),
        timestamps_json = serde_json::to_string(&timestamps).unwrap_or_default(),
        phases_json = serde_json::to_string(&phases).unwrap_or_default(),
        boundaries_json = serde_json::to_string(&phase_boundaries).unwrap_or_default(),
        ingest_eps_json = serde_json::to_string(&ingest_eps).unwrap_or_default(),
        ingest_target_json = serde_json::to_string(&ingest_target).unwrap_or_default(),
        query_p99_json = serde_json::to_string(&query_p99).unwrap_or_default(),
        query_p50_json = serde_json::to_string(&query_p50).unwrap_or_default(),
        kafka_lag_json = serde_json::to_string(&kafka_lag).unwrap_or_default(),
        query_group_section = generate_query_group_section(timeseries),
    );

    Ok(html)
}

fn generate_query_group_section(timeseries: &TimeSeries) -> String {
    // Collect all group names that appear in the time-series.
    let mut group_names = std::collections::BTreeSet::new();
    for point in &timeseries.points {
        for group_name in point.query_group_metrics.keys() {
            group_names.insert(group_name.clone());
        }
    }

    if group_names.is_empty() {
        return String::new();
    }

    let mut rows = String::new();
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

        let baseline_p99 = if !baseline_points.is_empty() {
            baseline_points.iter().map(|m| m.latency.p99).sum::<f64>()
                / baseline_points.len() as f64
        } else {
            0.0
        };
        let overlap_p99 = if !overlap_points.is_empty() {
            overlap_points.iter().map(|m| m.latency.p99).sum::<f64>() / overlap_points.len() as f64
        } else {
            0.0
        };
        let degradation = if baseline_p99 > 0.0 {
            overlap_p99 / baseline_p99
        } else {
            0.0
        };
        let baseline_qps = if !baseline_points.is_empty() {
            baseline_points
                .iter()
                .map(|m| m.executed as f64)
                .sum::<f64>()
                / baseline_points.len() as f64
        } else {
            0.0
        };
        let overlap_qps = if !overlap_points.is_empty() {
            overlap_points
                .iter()
                .map(|m| m.executed as f64)
                .sum::<f64>()
                / overlap_points.len() as f64
        } else {
            0.0
        };
        let overlap_errors: u64 = overlap_points.iter().map(|m| m.errors).sum();
        let overlap_total: u64 = overlap_points.iter().map(|m| m.executed).sum();
        let error_rate = if overlap_total > 0 {
            overlap_errors as f64 / overlap_total as f64 * 100.0
        } else {
            0.0
        };
        let warehouse_name = baseline_points
            .first()
            .or(overlap_points.first())
            .map(|m| m.warehouse_name.as_str())
            .unwrap_or("");

        rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{:.1}</td><td>{:.1}</td><td>{:.1}x</td><td>{:.1}</td><td>{:.1}</td><td>{:.1}%</td></tr>\n  ",
            group_name, warehouse_name, baseline_p99, overlap_p99, degradation, baseline_qps, overlap_qps, error_rate
        ));
    }

    format!(
        r#"<h2>Query Group Comparison</h2>
<table>
  <tr><th>Group</th><th>Warehouse</th><th>Baseline p99</th><th>Overlap p99</th><th>Degradation</th><th>Baseline QPS</th><th>Overlap QPS</th><th>Error Rate</th></tr>
  {}
</table>"#,
        rows.trim_end()
    )
}

fn generate_phase_rows(scenario: &Scenario, timeseries: &TimeSeries) -> String {
    scenario
        .timeline
        .phases
        .iter()
        .map(|phase_def| {
            let phase_points: Vec<_> = timeseries
                .points
                .iter()
                .filter(|p| p.phase == phase_def.name)
                .collect();

            let avg_eps = if !phase_points.is_empty() {
                phase_points
                    .iter()
                    .map(|p| p.ingest_events_produced as f64)
                    .sum::<f64>()
                    / phase_points.len() as f64
            } else {
                0.0
            };

            let avg_qps = if !phase_points.is_empty() {
                phase_points.iter().map(|p| p.query_executed as f64).sum::<f64>()
                    / phase_points.len() as f64
            } else {
                0.0
            };

            let avg_p99 = if !phase_points.is_empty() {
                phase_points
                    .iter()
                    .map(|p| p.query_latency.p99)
                    .sum::<f64>()
                    / phase_points.len() as f64
            } else {
                0.0
            };

            let peak_lag = phase_points
                .iter()
                .map(|p| p.kafka_consumer_lag)
                .max()
                .unwrap_or(0);

            // Sum target EPS across all data streams for this phase.
            let phase_target_eps: u64 = scenario.data_streams.iter()
                .map(|s| s.ingest.get(&phase_def.name).map(|i| i.target_eps).unwrap_or(0))
                .sum();

            format!(
                "<tr><td>{}</td><td>{}s</td><td>{:.0} / {}</td><td>{:.1} / {:.1}</td><td>{:.1}ms</td><td>{}</td></tr>",
                phase_def.display_name,
                phase_def.duration_seconds,
                avg_eps,
                phase_target_eps,
                avg_qps,
                phase_def.query.target_qps,
                avg_p99,
                peak_lag
            )
        })
        .collect::<Vec<_>>()
        .join("\n  ")
}
