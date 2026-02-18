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

mod controller;
mod resources;

use incidentbench_common::crd::IncidentBenchRun;
use kube::{Client, CustomResourceExt};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Check for CRD generation mode before initializing tracing.
    if std::env::args().any(|a| a == "--print-crd") {
        let crd = IncidentBenchRun::crd();
        println!("{}", serde_yaml::to_string(&crd)?);
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .json()
        .init();

    info!(
        version = incidentbench_common::VERSION,
        "IncidentBench Operator starting"
    );

    let client = Client::try_default().await?;

    info!("Connected to Kubernetes cluster");

    controller::run(client).await
}
