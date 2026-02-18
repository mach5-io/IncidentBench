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

pub mod adapter;
pub mod adapters;
pub mod crd;
pub mod generator;
pub mod metrics;
pub mod ratelimit;
pub mod scenario;

/// Proto-generated types.
pub mod proto {
    pub mod worker {
        tonic::include_proto!("incidentbench.worker");
    }
    pub mod phasecontroller {
        tonic::include_proto!("incidentbench.phasecontroller");
    }
    pub mod aggregator {
        tonic::include_proto!("incidentbench.aggregator");
    }
}

/// Harness version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
