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
use incidentbench_common::scenario::Scenario;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use std::collections::HashMap;

/// Execute the `run` command — create an IncidentBenchRun CR.
pub async fn execute(
    scenario_path: &str,
    target: &str,
    target_config_path: &str,
    kafka_bootstrap: &str,
    duration_scale: f64,
    rate_scale: f64,
    replicas_ingest: u32,
    replicas_query: u32,
    dry_run: bool,
) -> anyhow::Result<()> {
    // Load and validate scenario.
    let scenario_str = tokio::fs::read_to_string(scenario_path).await?;
    let scenario: Scenario = serde_yaml::from_str(&scenario_str)?;

    let errors = scenario.validate();
    if !errors.is_empty() {
        for err in &errors {
            eprintln!("Validation error: {}", err);
        }
        anyhow::bail!("Scenario validation failed ({} errors)", errors.len());
    }

    // Load target config.
    let target_config_str = tokio::fs::read_to_string(target_config_path).await?;
    let target_config: HashMap<String, serde_json::Value> =
        serde_yaml::from_str(&target_config_str)?;

    // Apply scaling and show plan.
    let scaled = scenario.with_scaling(duration_scale, rate_scale);
    let total_events = scaled.total_events_at_target();
    let total_duration = scaled.total_duration_seconds();

    println!("IncidentBench Run Plan");
    println!("======================");
    println!(
        "Scenario:        {} v{}",
        scenario.scenario.name, scenario.scenario.version
    );
    println!("Target:          {}", target);
    println!("Kafka:           {}", kafka_bootstrap);
    println!("Duration Scale:  {}x", duration_scale);
    println!("Rate Scale:      {}x", rate_scale);
    println!("Ingest Workers:  {}", replicas_ingest);
    println!("Query Workers:   {}", replicas_query);
    println!(
        "Total Duration:  {}s ({:.1} min)",
        total_duration,
        total_duration as f64 / 60.0
    );
    println!(
        "Total Events:    ~{:.1}M",
        total_events as f64 / 1_000_000.0
    );
    println!();

    println!("Data Streams:");
    for stream in scaled.data_streams.as_deref().unwrap_or(&[]) {
        println!(
            "  {} (index: {}, replicas: {})",
            stream.name, stream.schema.index_name, stream.ingest_replicas
        );
    }
    println!();

    println!("Phase Timeline:");
    for phase in &scaled.timeline.phases {
        // Sum target EPS across all data streams for this phase.
        let phase_total_eps: u64 = scaled
            .data_streams
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|s| s.ingest.get(&phase.name).map(|i| i.target_eps).unwrap_or(0))
            .sum();
        println!(
            "  {:20} {:4}s  {:>7} EPS  {:>5.1} QPS",
            phase.display_name, phase.duration_seconds, phase_total_eps, phase.query.target_qps,
        );
    }

    if dry_run {
        println!();
        println!("(dry-run mode — no resources created)");
        return Ok(());
    }

    // Build CR name from scenario name — must be RFC 1123 lowercase DNS subdomain.
    let sanitized_name: String = scenario
        .scenario
        .name
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let sanitized_name = sanitized_name
        .trim_matches(|c| c == '-' || c == '.')
        .to_string();
    let run_name = format!(
        "{}-{}",
        sanitized_name,
        chrono::Utc::now().format("%Y%m%d-%H%M%S")
    );

    println!();
    println!("Creating IncidentBenchRun: {}", run_name);

    // Build the CR.
    let cr = serde_json::json!({
        "apiVersion": "incidentbench.io/v1alpha1",
        "kind": "IncidentBenchRun",
        "metadata": {
            "name": run_name,
            "namespace": "incidentbench"
        },
        "spec": {
            "scenario": scenario,
            "target": {
                "adapter": target,
                "config": target_config,
            },
            "kafka": {
                "bootstrapServers": kafka_bootstrap,
            },
            "scaling": {
                "durationScale": duration_scale,
                "rateScale": rate_scale,
            },
            "workers": {
                "ingest": { "replicas": replicas_ingest },
                "query": { "replicas": replicas_query },
            },
            "dryRun": false,
        }
    });

    let client = Client::try_default().await?;
    let api: Api<IncidentBenchRun> = Api::namespaced(client, "incidentbench");

    let pp = PatchParams::apply("incidentbench-cli");
    let _run_resource: IncidentBenchRun = api.patch(&run_name, &pp, &Patch::Apply(cr)).await?;

    println!("Created: {}", run_name);
    println!();
    println!("Watch progress:");
    println!("  incidentbench metrics {} --live", run_name);
    println!();
    println!("Get report when done:");
    println!("  incidentbench report get {}", run_name);

    Ok(())
}

/// Execute the `validate` command — local validation only.
pub async fn validate(scenario_path: &str) -> anyhow::Result<()> {
    let scenario_str = tokio::fs::read_to_string(scenario_path).await?;
    let scenario: Scenario = serde_yaml::from_str(&scenario_str)?;

    let errors = scenario.validate();

    if errors.is_empty() {
        println!("Scenario '{}' is valid.", scenario.scenario.name);
        println!();
        println!("  Version:    {}", scenario.scenario.version);
        println!("  Domain:     {}", scenario.scenario.domain);
        println!(
            "  Streams:    {}",
            scenario.data_streams.as_deref().map_or(0, |s| s.len())
        );
        for stream in scenario.data_streams.as_deref().unwrap_or(&[]) {
            println!(
                "    {} -> index '{}' ({} fields)",
                stream.name,
                stream.schema.index_name,
                stream.schema.fields.len()
            );
        }
        println!("  Queries:    {}", scenario.query_mix.queries.len());
        println!("  Phases:     {}", scenario.timeline.phases.len());
        println!("  Duration:   {}s", scenario.total_duration_seconds());
        println!(
            "  Events:     ~{:.1}M",
            scenario.total_events_at_target() as f64 / 1_000_000.0
        );
    } else {
        eprintln!(
            "Scenario '{}' has {} validation error(s):",
            scenario.scenario.name,
            errors.len()
        );
        for err in &errors {
            eprintln!("  - {}", err);
        }
        std::process::exit(1);
    }

    Ok(())
}
