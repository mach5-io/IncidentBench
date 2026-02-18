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

pub mod mach5;

use crate::adapter::TargetAdapter;
use std::collections::HashMap;

/// Create a target adapter by name.
pub fn create_adapter(
    adapter_name: &str,
    config: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<Box<dyn TargetAdapter>> {
    match adapter_name {
        "mach5" => Ok(Box::new(mach5::Mach5Adapter::new(config)?)),
        _ => anyhow::bail!("Unknown adapter: {}", adapter_name),
    }
}
