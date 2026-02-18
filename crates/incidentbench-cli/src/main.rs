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

mod metrics;
mod report;
mod run;
mod status;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "incidentbench",
    version,
    about = "IncidentBench — resilience benchmark for search and analytics platforms"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create an IncidentBenchRun from a scenario file.
    Run {
        /// Path to the scenario YAML file.
        scenario: String,

        /// Target adapter name.
        #[arg(long)]
        target: String,

        /// Path to target adapter config file.
        #[arg(long)]
        target_config: String,

        /// Kafka bootstrap servers.
        #[arg(long)]
        kafka_bootstrap: String,

        /// Multiply all phase durations.
        #[arg(long, default_value = "1.0")]
        duration_scale: f64,

        /// Multiply all rate targets.
        #[arg(long, default_value = "1.0")]
        rate_scale: f64,

        /// Number of ingest worker pods.
        #[arg(long, default_value = "10")]
        replicas_ingest: u32,

        /// Number of query worker pods.
        #[arg(long, default_value = "4")]
        replicas_query: u32,

        /// Validate and print execution plan without running.
        #[arg(long)]
        dry_run: bool,

        /// Verbose logging.
        #[arg(long, short)]
        verbose: bool,
    },
    /// Validate a scenario YAML (local, no cluster required).
    Validate {
        /// Path to the scenario YAML file.
        scenario: String,
    },
    /// Show run status from CR.
    Status {
        /// Name of the IncidentBenchRun.
        run_name: String,

        /// Kubernetes namespace.
        #[arg(long, short, default_value = "incidentbench")]
        namespace: String,
    },
    /// Stream live metrics from a running benchmark.
    Metrics {
        /// Name of the IncidentBenchRun.
        run_name: String,

        /// Kubernetes namespace.
        #[arg(long, short, default_value = "incidentbench")]
        namespace: String,

        /// Show interactive terminal UI.
        #[arg(long)]
        live: bool,

        /// Output format for streaming mode.
        #[arg(long, default_value = "table")]
        format: String,
    },
    /// Stream logs from worker pods.
    Logs {
        /// Name of the IncidentBenchRun.
        run_name: String,

        /// Specific worker index to follow.
        #[arg(long)]
        worker: Option<u32>,

        /// Kubernetes namespace.
        #[arg(long, short, default_value = "incidentbench")]
        namespace: String,
    },
    /// Download report from a completed run.
    Report {
        #[command(subcommand)]
        command: ReportCommands,
    },
    /// List IncidentBenchRun resources.
    List {
        /// Kubernetes namespace.
        #[arg(long, short, default_value = "incidentbench")]
        namespace: String,
    },
    /// Delete an IncidentBenchRun and clean up resources.
    Delete {
        /// Name of the IncidentBenchRun to delete.
        run_name: String,

        /// Kubernetes namespace.
        #[arg(long, short, default_value = "incidentbench")]
        namespace: String,
    },
    /// Print version information.
    Version,
}

#[derive(Subcommand)]
enum ReportCommands {
    /// Download report from a completed run.
    Get {
        /// Name of the IncidentBenchRun.
        run_name: String,

        /// Output directory.
        #[arg(long, short, default_value = ".")]
        output: String,

        /// Kubernetes namespace.
        #[arg(long, short, default_value = "incidentbench")]
        namespace: String,
    },
    /// Regenerate report from raw metrics (local).
    Regenerate {
        /// Path to the metrics directory.
        metrics_path: String,

        /// Output directory.
        #[arg(long, short, default_value = ".")]
        output: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            scenario,
            target,
            target_config,
            kafka_bootstrap,
            duration_scale,
            rate_scale,
            replicas_ingest,
            replicas_query,
            dry_run,
            verbose,
        } => {
            init_logging(verbose);
            run::execute(
                &scenario,
                &target,
                &target_config,
                &kafka_bootstrap,
                duration_scale,
                rate_scale,
                replicas_ingest,
                replicas_query,
                dry_run,
            )
            .await
        }
        Commands::Validate { scenario } => {
            init_logging(false);
            run::validate(&scenario).await
        }
        Commands::Status {
            run_name,
            namespace,
        } => {
            init_logging(false);
            status::show(&run_name, &namespace).await
        }
        Commands::Metrics {
            run_name,
            namespace,
            live,
            format,
        } => {
            init_logging(false);
            if live {
                metrics::live_tui(&run_name, &namespace).await
            } else {
                metrics::stream(&run_name, &namespace, &format).await
            }
        }
        Commands::Logs {
            run_name: _,
            worker: _,
            namespace: _,
        } => {
            init_logging(false);
            eprintln!("Logs streaming not yet implemented");
            Ok(())
        }
        Commands::Report { command } => match command {
            ReportCommands::Get {
                run_name,
                output,
                namespace,
            } => {
                init_logging(false);
                report::download(&run_name, &namespace, &output).await
            }
            ReportCommands::Regenerate {
                metrics_path,
                output,
            } => {
                init_logging(false);
                report::regenerate(&metrics_path, &output).await
            }
        },
        Commands::List { namespace } => {
            init_logging(false);
            status::list(&namespace).await
        }
        Commands::Delete {
            run_name,
            namespace,
        } => {
            init_logging(false);
            status::delete(&run_name, &namespace).await
        }
        Commands::Version => {
            println!("incidentbench {}", incidentbench_common::VERSION);
            Ok(())
        }
    }
}

fn init_logging(verbose: bool) {
    let filter = if verbose { "debug" } else { "warn" };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(filter.parse().unwrap()))
        .with_target(false)
        .init();
}
