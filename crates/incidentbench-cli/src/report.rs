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

use incidentbench_common::crd::{IncidentBenchRun, LifecyclePhase};
use kube::{Api, Client};
use tokio::io::AsyncWriteExt;

/// Download report files from a completed run.
///
/// Spins up a short-lived pod that mounts the results PVC, copies report.html,
/// run.json, timeseries.csv, and the raw latency/timeout JSON files to the
/// local output directory via kubectl cp, then removes the pod.
pub async fn download(run_name: &str, namespace: &str, output_dir: &str) -> anyhow::Result<()> {
    use tokio::process::Command;

    let client = Client::try_default().await?;
    let api: Api<IncidentBenchRun> = Api::namespaced(client, namespace);

    let run = api.get(run_name).await?;
    let phase = run
        .status
        .as_ref()
        .map(|s| s.phase.clone())
        .unwrap_or_default();

    if phase != LifecyclePhase::Completed {
        anyhow::bail!("Run is not Completed yet (current phase: {})", phase);
    }

    let pvc_name = format!("{}-results", run_name);
    let pod_name = format!("{}-report-dl", run_name);

    println!("Fetching report from PVC {} ...", pvc_name);

    // Create a temporary pod that mounts the results PVC.
    let pod_manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": &pod_name, "namespace": namespace },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{
                "name": "report-reader",
                "image": "busybox:1.36",
                "command": ["sleep", "120"],
                "volumeMounts": [{ "name": "results", "mountPath": "/results" }]
            }],
            "volumes": [{
                "name": "results",
                "persistentVolumeClaim": { "claimName": &pvc_name }
            }]
        }
    });

    // Apply the pod.
    let mut child = Command::new("kubectl")
        .args(["apply", "-f", "-", "-n", namespace])
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(pod_manifest.to_string().as_bytes()).await?;
    }
    child.wait().await?;

    // Wait for the pod to be Running.
    let wait = Command::new("kubectl")
        .args([
            "wait",
            "--for=condition=Ready",
            &format!("pod/{}", pod_name),
            "-n",
            namespace,
            "--timeout=60s",
        ])
        .status()
        .await?;
    if !wait.success() {
        let _ = Command::new("kubectl")
            .args([
                "delete",
                "pod",
                &pod_name,
                "-n",
                namespace,
                "--ignore-not-found",
            ])
            .status()
            .await;
        anyhow::bail!("Timed out waiting for report-reader pod");
    }

    tokio::fs::create_dir_all(output_dir).await?;

    // Copy each report file locally.
    let files = [
        "report.html",
        "run.json",
        "timeseries.csv",
        "timed_out_queries.json",
        "per_query_latency.json",
        "per_query_timeseries.json",
        "per_category_latency.json",
    ];
    for file in &files {
        let src = format!("{}/{}:/results/{}", namespace, pod_name, file);
        let dst = format!("{}/{}", output_dir, file);
        let status = Command::new("kubectl")
            .args(["cp", &src, &dst])
            .status()
            .await?;
        if status.success() {
            println!("  {}", dst);
        } else {
            eprintln!(
                "  Warning: {} not found in PVC (reporter may have failed)",
                file
            );
        }
    }

    // Clean up.
    Command::new("kubectl")
        .args([
            "delete",
            "pod",
            &pod_name,
            "-n",
            namespace,
            "--ignore-not-found",
        ])
        .status()
        .await?;

    println!();
    println!("Reports saved to: {}/", output_dir);
    println!("  Open {}/report.html in your browser.", output_dir);

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
        println!("  {}/timed_out_queries.json", output_dir);
        println!("  {}/per_query_latency.json", output_dir);
        println!("  {}/per_query_timeseries.json", output_dir);
        println!("  {}/per_category_latency.json", output_dir);
    } else {
        anyhow::bail!("Report regeneration failed");
    }

    Ok(())
}
