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

use incidentbench_common::crd::IncidentBenchRun;
use kube::api::DeleteParams;
use kube::{Api, Client, ResourceExt};

/// Show the status of a single IncidentBenchRun.
pub async fn show(run_name: &str, namespace: &str) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let api: Api<IncidentBenchRun> = Api::namespaced(client, namespace);

    let run = api.get(run_name).await?;
    let status = run.status.as_ref();

    println!("Name:      {}", run.name_any());
    println!("Namespace: {}", namespace);

    if let Some(s) = status {
        println!("Phase:     {}", s.phase);

        if let Some(ref run_id) = s.run_id {
            println!("Run ID:    {}", run_id);
        }

        if let Some(ref start) = s.start_time {
            println!("Started:   {}", start);
        }

        if let Some(ref phase) = s.current_benchmark_phase {
            println!("Benchmark Phase: {}", phase);
        }

        if let Some(ref workers) = s.workers {
            println!(
                "Workers:   ingest {}/{}, query {}/{}",
                workers.ingest_ready,
                workers.ingest_desired,
                workers.query_ready,
                workers.query_desired
            );
        }

        if let Some(ref progress) = s.progress {
            println!(
                "Progress:  {}/{}s ({:.0}%)",
                progress.elapsed_seconds,
                progress.total_seconds,
                if progress.total_seconds > 0 {
                    progress.elapsed_seconds as f64 / progress.total_seconds as f64 * 100.0
                } else {
                    0.0
                }
            );
            println!(
                "Ingest:    {} / {} EPS",
                progress.achieved_ingest_eps, progress.target_ingest_eps
            );
            println!(
                "Query:     {:.1} / {:.1} QPS",
                progress.achieved_query_qps, progress.target_query_qps
            );
            println!("Kafka Lag: {}", progress.kafka_consumer_lag);
        }

        if let Some(ref results) = s.results {
            println!();
            println!("Valid:     {}", if results.valid { "YES" } else { "NO" });
            if !results.validity_violations.is_empty() {
                println!("Violations:");
                for v in &results.validity_violations {
                    println!("  - {}", v);
                }
            }
            if let Some(ref sc) = results.scorecard {
                println!();
                println!("Scorecard:");
                println!("  Baseline p99:     {:.1} ms", sc.baseline_p99_ms);
                println!("  Overlap p99:      {:.1} ms", sc.overlap_p99_ms);
                println!("  Degradation:      {:.1}x", sc.p99_degradation_ratio);
                println!(
                    "  Error Rate:       {:.1}%",
                    sc.query_error_rate_overlap * 100.0
                );
                println!("  Peak Backlog:     {}", sc.peak_backlog);
                println!("  Drain Time:       {:.1}s", sc.backlog_drain_time_s);
                println!("  Recovery Time:    {:.1}s", sc.recovery_time_s);
            }
        }

        if !s.conditions.is_empty() {
            println!();
            println!("Conditions:");
            for c in &s.conditions {
                println!(
                    "  {} = {}{}",
                    c.condition_type,
                    c.status,
                    c.message
                        .as_ref()
                        .map(|m| format!(" ({})", m))
                        .unwrap_or_default()
                );
            }
        }
    } else {
        println!("Phase:     Pending (no status yet)");
    }

    Ok(())
}

/// List all IncidentBenchRun resources in a namespace.
pub async fn list(namespace: &str) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let api: Api<IncidentBenchRun> = Api::namespaced(client, namespace);

    let runs = api.list(&Default::default()).await?;

    if runs.items.is_empty() {
        println!(
            "No IncidentBenchRun resources found in namespace '{}'",
            namespace
        );
        return Ok(());
    }

    println!(
        "{:<30} {:<15} {:<15} {:<10}",
        "NAME", "PHASE", "BENCHMARK", "AGE"
    );

    for run in &runs.items {
        let name = run.name_any();
        let phase = run
            .status
            .as_ref()
            .map(|s| s.phase.to_string())
            .unwrap_or_else(|| "Pending".to_string());
        let bench_phase = run
            .status
            .as_ref()
            .and_then(|s| s.current_benchmark_phase.clone())
            .unwrap_or_else(|| "-".to_string());
        let age = run
            .metadata
            .creation_timestamp
            .as_ref()
            .map(|ts| {
                let created = ts.0;
                let duration = chrono::Utc::now() - created;
                if duration.num_hours() > 0 {
                    format!("{}h", duration.num_hours())
                } else if duration.num_minutes() > 0 {
                    format!("{}m", duration.num_minutes())
                } else {
                    format!("{}s", duration.num_seconds())
                }
            })
            .unwrap_or_else(|| "-".to_string());

        println!("{:<30} {:<15} {:<15} {:<10}", name, phase, bench_phase, age);
    }

    Ok(())
}

/// Delete an IncidentBenchRun resource.
pub async fn delete(run_name: &str, namespace: &str) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let api: Api<IncidentBenchRun> = Api::namespaced(client, namespace);

    println!(
        "Deleting IncidentBenchRun '{}' in namespace '{}'...",
        run_name, namespace
    );
    println!("The operator's finalizer will clean up Kafka topics and Mach5 resources.");

    api.delete(run_name, &DeleteParams::default()).await?;

    println!(
        "Deletion initiated. Use 'incidentbench status {}' to check progress.",
        run_name
    );
    Ok(())
}
