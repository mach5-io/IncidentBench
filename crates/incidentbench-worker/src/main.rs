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

mod aggregator;
mod barrier;
mod health;
mod ingest;
mod phase_controller;
pub(crate) mod proc_metrics;
mod query;

use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "incidentbench-worker", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run as an ingest worker.
    Ingest {
        /// Path to the worker configuration (mounted ConfigMap).
        #[arg(long)]
        config: String,
    },
    /// Run as a query worker.
    Query {
        /// Path to the worker configuration (mounted ConfigMap).
        #[arg(long)]
        config: String,
    },
    /// Run as the PhaseController.
    PhaseController {
        /// Path to the phase controller configuration.
        #[arg(long)]
        config: String,

        /// gRPC listen address.
        #[arg(long, default_value = "0.0.0.0:50051")]
        listen: String,
    },
    /// Run as the MetricsAggregator.
    Aggregator {
        /// Path to the aggregator configuration.
        #[arg(long)]
        config: String,

        /// gRPC listen address.
        #[arg(long, default_value = "0.0.0.0:50052")]
        listen: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .json()
        .init();

    let cli = Cli::parse();

    // Spawn health server for Kubernetes probes.
    tokio::spawn(health::serve(8080));

    match cli.command {
        Command::Ingest { config } => {
            info!("Starting ingest worker");
            ingest::run(&config).await
        }
        Command::Query { config } => {
            info!("Starting query worker");
            query::run(&config).await
        }
        Command::PhaseController { config, listen } => {
            info!("Starting PhaseController on {}", listen);
            phase_controller::run(&config, &listen).await
        }
        Command::Aggregator { config, listen } => {
            info!("Starting MetricsAggregator on {}", listen);
            aggregator::run(&config, &listen).await
        }
    }
}
