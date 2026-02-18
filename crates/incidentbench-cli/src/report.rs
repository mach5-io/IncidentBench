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
use kube::{Api, Client};

/// Download report files from a completed run.
pub async fn download(run_name: &str, namespace: &str, output_dir: &str) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let api: Api<IncidentBenchRun> = Api::namespaced(client, namespace);

    let run = api.get(run_name).await?;
    let status = run
        .status
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Run has no status"))?;

    let results = status
        .results
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Run has no results — is it completed?"))?;

    println!("Run: {}/{}", namespace, run_name);
    println!("Valid: {}", results.valid);

    if !results.validity_violations.is_empty() {
        println!("Violations:");
        for v in &results.validity_violations {
            println!("  - {}", v);
        }
    }
    if !results.warnings.is_empty() {
        println!("Warnings:");
        for w in &results.warnings {
            println!("  - {}", w);
        }
    }
    println!("Harness saturated: {}", results.harness_saturated);

    if let Some(ref sc) = results.scorecard {
        println!();
        println!("Scorecard:");
        println!("  Baseline P99:         {:.2} ms", sc.baseline_p99_ms);
        println!("  Overlap P99:          {:.2} ms", sc.overlap_p99_ms);
        println!("  P99 Degradation:      {:.2}x", sc.p99_degradation_ratio);
        println!("  Query Error Rate:     {:.4}", sc.query_error_rate_overlap);
        println!("  Peak Backlog:         {}", sc.peak_backlog);
        println!("  Backlog Drain Time:   {:.1}s", sc.backlog_drain_time_s);
        println!("  Recovery Time:        {:.1}s", sc.recovery_time_s);
    }

    // Write JSON results to output directory.
    let json = serde_json::to_string_pretty(results)?;
    let output_path = format!("{}/results.json", output_dir);
    tokio::fs::create_dir_all(output_dir).await?;
    tokio::fs::write(&output_path, &json).await?;
    println!();
    println!("Results written to: {}", output_path);

    Ok(())
}

/// Regenerate reports from raw metrics (runs locally, no cluster needed).
pub async fn regenerate(metrics_path: &str, output_dir: &str) -> anyhow::Result<()> {
    println!("Regenerating reports from: {}", metrics_path);
    println!("Output: {}", output_dir);

    // This delegates to the reporter binary logic.
    // In a real implementation, we'd call the report generation functions directly
    // from incidentbench-common.
    let status = tokio::process::Command::new("incidentbench-reporter")
        .arg("--input")
        .arg(metrics_path)
        .arg("--output")
        .arg(output_dir)
        .status()
        .await?;

    if status.success() {
        println!("Reports regenerated successfully.");
        println!("  {}/report.html", output_dir);
        println!("  {}/run.json", output_dir);
    } else {
        anyhow::bail!("Report regeneration failed");
    }

    Ok(())
}
