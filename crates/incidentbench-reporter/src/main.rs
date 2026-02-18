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

mod html_report;
mod json_report;

use clap::Parser;
use incidentbench_common::metrics::{compute_derived, TimeSeries};
use incidentbench_common::scenario::Scenario;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "incidentbench-reporter", version)]
struct Cli {
    /// Input directory containing metrics.json and scenario.yaml.
    #[arg(long)]
    input: String,

    /// Output directory for reports.
    #[arg(long)]
    output: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();

    info!(input = %cli.input, output = %cli.output, "Report generator starting");

    // Read inputs.
    let metrics_path = format!("{}/metrics.json", cli.input);
    let scenario_path = format!("{}/scenario.yaml", cli.input);

    let metrics_str = tokio::fs::read_to_string(&metrics_path).await?;
    let timeseries: TimeSeries = serde_json::from_str(&metrics_str)?;

    let scenario_str = tokio::fs::read_to_string(&scenario_path).await?;
    let scenario: Scenario = serde_yaml::from_str(&scenario_str)?;

    // Compute derived metrics.
    let derived = compute_derived(&timeseries);
    let scorecard = incidentbench_common::metrics::Scorecard::from_derived(&derived);

    info!(?scorecard, "Scorecard computed");

    // Generate JSON report.
    let json_report = json_report::generate(&scenario, &timeseries, &derived, &scorecard)?;
    let json_path = format!("{}/run.json", cli.output);
    tokio::fs::create_dir_all(&cli.output).await?;
    tokio::fs::write(&json_path, &json_report).await?;
    info!(path = %json_path, "JSON report written");

    // Generate HTML report.
    let html_report = html_report::generate(&scenario, &timeseries, &derived, &scorecard)?;
    let html_path = format!("{}/report.html", cli.output);
    tokio::fs::write(&html_path, &html_report).await?;
    info!(path = %html_path, "HTML report written");

    // Write timeseries CSV.
    let csv_path = format!("{}/timeseries.csv", cli.output);
    let csv_content = generate_csv(&timeseries);
    tokio::fs::write(&csv_path, &csv_content).await?;
    info!(path = %csv_path, "Timeseries CSV written");

    info!("Report generation complete");
    Ok(())
}

fn generate_csv(ts: &TimeSeries) -> String {
    let mut csv = String::new();
    csv.push_str(
        "timestamp_s,phase,ingest_events_produced,ingest_events_acked,ingest_events_failed,\
        ingest_target_eps,query_executed,query_errors,query_target_qps,\
        query_latency_p50,query_latency_p95,query_latency_p99,kafka_consumer_lag\n",
    );

    for p in &ts.points {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{:.1},{:.2},{:.2},{:.2},{}\n",
            p.timestamp_s,
            p.phase,
            p.ingest_events_produced,
            p.ingest_events_acknowledged,
            p.ingest_events_failed,
            p.ingest_target_eps,
            p.query_executed,
            p.query_errors,
            p.query_target_qps,
            p.query_latency.p50,
            p.query_latency.p95,
            p.query_latency.p99,
            p.kafka_consumer_lag,
        ));
    }

    csv
}
