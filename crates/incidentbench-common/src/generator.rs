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

use crate::scenario::{FieldDef, Schema};
use chrono::Utc;
use rand::distributions::{Distribution, WeightedIndex};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Map, Value};
use siphasher::sip::SipHasher13;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Deterministic data generator for a single worker.
///
/// workerSeed = hash(runSeed, workerIndex)
/// This ensures reproducibility for a given (seed, workerCount) pair.
pub struct DataGenerator {
    rng: StdRng,
    schema: Schema,
    /// Whether we're in an "incident" phase (affects weighted distributions).
    incident_mode: bool,
    /// Cached field generators.
    field_generators: Vec<FieldGenerator>,
}

enum FieldGenerator {
    Timestamp,
    WeightedEnum {
        baseline_values: Vec<String>,
        baseline_weights: WeightedIndex<f64>,
        incident_values: Vec<String>,
        incident_weights: WeightedIndex<f64>,
    },
    StaticWeightedEnum {
        values: Vec<String>,
        weights: WeightedIndex<f64>,
    },
    Pattern {
        pattern: String,
        range_min: u32,
        range_max: u32,
    },
    Hex {
        length: usize,
    },
    Distribution {
        baseline_mean: f64,
        baseline_stddev: f64,
        incident_mean: f64,
        incident_stddev: f64,
        min: f64,
        max: f64,
    },
    Template {
        templates: HashMap<String, Vec<String>>,
    },
    Conditional {
        condition_field: String,
        condition_values: Vec<String>,
        inner: Box<FieldGenerator>,
    },
    Derived {
        expression: String,
    },
    StaticEnum {
        values: Vec<String>,
    },
    Fallback,
}

impl DataGenerator {
    /// Create a new generator for a specific worker.
    pub fn new(schema: &Schema, run_seed: u64, worker_index: u32) -> Self {
        let worker_seed = derive_worker_seed(run_seed, worker_index);
        let rng = StdRng::seed_from_u64(worker_seed);

        let field_generators = schema
            .fields
            .iter()
            .map(|f| build_field_generator(f))
            .collect();

        Self {
            rng,
            schema: schema.clone(),
            incident_mode: false,
            field_generators,
        }
    }

    /// Set whether we're in an incident phase (affects weighted distributions).
    pub fn set_incident_mode(&mut self, incident: bool) {
        self.incident_mode = incident;
    }

    /// Generate a single event as a JSON object.
    pub fn generate_event(&mut self) -> Value {
        let mut event = Map::new();
        let incident = self.incident_mode;

        // First pass: generate all non-derived fields.
        for (i, field) in self.schema.fields.iter().enumerate() {
            let value =
                generate_field_value(&self.field_generators[i], &mut self.rng, incident, &event);
            if let Some(v) = value {
                set_nested_field(&mut event, &field.name, v);
            }
        }

        Value::Object(event)
    }

    /// Generate a batch of events.
    pub fn generate_batch(&mut self, count: usize) -> Vec<Value> {
        (0..count).map(|_| self.generate_event()).collect()
    }
}

/// Derive a per-worker seed from the run seed and worker index.
fn derive_worker_seed(run_seed: u64, worker_index: u32) -> u64 {
    let mut hasher = SipHasher13::new();
    run_seed.hash(&mut hasher);
    worker_index.hash(&mut hasher);
    hasher.finish()
}

fn build_field_generator(field: &FieldDef) -> FieldGenerator {
    match field.generator.as_str() {
        "now" => FieldGenerator::Timestamp,
        "weighted_enum" => build_weighted_enum_generator(&field.config),
        "enum" => build_static_enum_generator(&field.config),
        "pattern" => build_pattern_generator(&field.config),
        "hex" => {
            let length = field
                .config
                .get("length")
                .and_then(|v| v.as_u64())
                .unwrap_or(16) as usize;
            FieldGenerator::Hex { length }
        }
        "distribution" => build_distribution_generator(&field.config),
        "template" => build_template_generator(&field.config),
        "conditional" => build_conditional_generator(&field.config),
        "derived" => {
            let expression = field
                .config
                .get("expression")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            FieldGenerator::Derived { expression }
        }
        _ => FieldGenerator::Fallback,
    }
}

fn build_weighted_enum_generator(config: &Value) -> FieldGenerator {
    // Check if it has baseline/incident variants
    if config.get("baseline").is_some() || config.get("incident").is_some() {
        let (baseline_values, baseline_weights) =
            extract_weighted_values(config.get("baseline").unwrap_or(config));
        let (incident_values, incident_weights) =
            extract_weighted_values(config.get("incident").unwrap_or(config));
        FieldGenerator::WeightedEnum {
            baseline_values,
            baseline_weights,
            incident_values,
            incident_weights,
        }
    } else if let Some(values) = config.get("values") {
        let (vals, weights) = extract_weighted_values(values);
        FieldGenerator::StaticWeightedEnum {
            values: vals,
            weights,
        }
    } else {
        FieldGenerator::Fallback
    }
}

fn extract_weighted_values(value: &Value) -> (Vec<String>, WeightedIndex<f64>) {
    let mut values = Vec::new();
    let mut weights = Vec::new();

    if let Some(obj) = value.as_object() {
        for (k, v) in obj {
            values.push(k.clone());
            weights.push(v.as_f64().unwrap_or(1.0));
        }
    }

    if weights.is_empty() {
        values.push("unknown".to_string());
        weights.push(1.0);
    }

    let dist = WeightedIndex::new(&weights).expect("valid weights");
    (values, dist)
}

fn build_static_enum_generator(config: &Value) -> FieldGenerator {
    let values = config
        .get("values")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    FieldGenerator::StaticEnum { values }
}

fn build_pattern_generator(config: &Value) -> FieldGenerator {
    let pattern = config
        .get("pattern")
        .and_then(|v| v.as_str())
        .unwrap_or("{service}-{id}")
        .to_string();
    let range_min = config
        .get("pod_id")
        .and_then(|v| v.get("min"))
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;
    let range_max = config
        .get("pod_id")
        .and_then(|v| v.get("max"))
        .and_then(|v| v.as_u64())
        .unwrap_or(20) as u32;

    FieldGenerator::Pattern {
        pattern,
        range_min,
        range_max,
    }
}

fn build_distribution_generator(config: &Value) -> FieldGenerator {
    let baseline = config.get("baseline").unwrap_or(config);
    let incident = config.get("incident").unwrap_or(baseline);

    FieldGenerator::Distribution {
        baseline_mean: baseline
            .get("mean")
            .and_then(|v| v.as_f64())
            .unwrap_or(50.0),
        baseline_stddev: baseline
            .get("stddev")
            .and_then(|v| v.as_f64())
            .unwrap_or(30.0),
        incident_mean: incident
            .get("mean")
            .and_then(|v| v.as_f64())
            .unwrap_or(500.0),
        incident_stddev: incident
            .get("stddev")
            .and_then(|v| v.as_f64())
            .unwrap_or(400.0),
        min: baseline.get("min").and_then(|v| v.as_f64()).unwrap_or(1.0),
        max: baseline
            .get("max")
            .and_then(|v| v.as_f64())
            .unwrap_or(30000.0),
    }
}

fn build_template_generator(config: &Value) -> FieldGenerator {
    let mut templates = HashMap::new();
    if let Some(obj) = config.get("templates").and_then(|v| v.as_object()) {
        for (level, arr) in obj {
            if let Some(arr) = arr.as_array() {
                templates.insert(
                    level.clone(),
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect(),
                );
            }
        }
    }
    FieldGenerator::Template { templates }
}

fn build_conditional_generator(config: &Value) -> FieldGenerator {
    let condition = config
        .get("condition")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Parse simple "field in [val1, val2]" conditions.
    let (field, values) = parse_condition(condition);

    let inner = if let Some(vals) = config.get("values") {
        let (v, w) = extract_weighted_values(vals);
        Box::new(FieldGenerator::StaticWeightedEnum {
            values: v,
            weights: w,
        })
    } else {
        Box::new(FieldGenerator::Fallback)
    };

    FieldGenerator::Conditional {
        condition_field: field,
        condition_values: values,
        inner,
    }
}

fn parse_condition(condition: &str) -> (String, Vec<String>) {
    // Parse "field in [val1, val2]"
    if let Some(in_pos) = condition.find(" in ") {
        let field = condition[..in_pos].trim().to_string();
        let values_str = &condition[in_pos + 4..];
        let values_str = values_str
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']');
        let values: Vec<String> = values_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        (field, values)
    } else {
        (String::new(), Vec::new())
    }
}

fn generate_field_value(
    gen: &FieldGenerator,
    rng: &mut StdRng,
    incident: bool,
    current_event: &Map<String, Value>,
) -> Option<Value> {
    match gen {
        FieldGenerator::Timestamp => Some(Value::String(
            Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        )),
        FieldGenerator::WeightedEnum {
            baseline_values,
            baseline_weights,
            incident_values,
            incident_weights,
        } => {
            let (vals, weights) = if incident {
                (incident_values, incident_weights)
            } else {
                (baseline_values, baseline_weights)
            };
            let idx = weights.sample(rng);
            let val = &vals[idx];
            // Try to parse as number for numeric fields.
            if let Ok(n) = val.parse::<i64>() {
                Some(Value::Number(n.into()))
            } else {
                Some(Value::String(val.clone()))
            }
        }
        FieldGenerator::StaticWeightedEnum { values, weights } => {
            let idx = weights.sample(rng);
            let val = &values[idx];
            if let Ok(n) = val.parse::<i64>() {
                Some(Value::Number(n.into()))
            } else {
                Some(Value::String(val.clone()))
            }
        }
        FieldGenerator::StaticEnum { values } => {
            if values.is_empty() {
                return Some(Value::String("unknown".to_string()));
            }
            let idx = rng.gen_range(0..values.len());
            Some(Value::String(values[idx].clone()))
        }
        FieldGenerator::Pattern {
            pattern,
            range_min,
            range_max,
        } => {
            let pod_id = rng.gen_range(*range_min..=*range_max);
            let service = get_field_str(current_event, "service").unwrap_or("svc");
            let result = pattern
                .replace("{service}", service)
                .replace("{pod_id}", &pod_id.to_string());
            Some(Value::String(result))
        }
        FieldGenerator::Hex { length } => {
            let hex: String = (0..*length)
                .map(|_| {
                    let idx = rng.gen_range(0..16);
                    "0123456789abcdef".as_bytes()[idx] as char
                })
                .collect();
            Some(Value::String(hex))
        }
        FieldGenerator::Distribution {
            baseline_mean,
            baseline_stddev,
            incident_mean,
            incident_stddev,
            min,
            max,
        } => {
            let (mean, stddev) = if incident {
                (*incident_mean, *incident_stddev)
            } else {
                (*baseline_mean, *baseline_stddev)
            };
            // Simple approximation of log-normal using normal distribution.
            let normal: f64 = rng.gen::<f64>() * stddev + mean;
            let clamped = normal.clamp(*min, *max);
            Some(Value::Number((clamped as i64).into()))
        }
        FieldGenerator::Template { templates } => {
            let level = get_field_str(current_event, "level").unwrap_or("INFO");
            if let Some(tmpls) = templates.get(level) {
                if !tmpls.is_empty() {
                    let idx = rng.gen_range(0..tmpls.len());
                    let tmpl = &tmpls[idx];
                    // Simple variable substitution from current event.
                    let result = substitute_template(tmpl, current_event, rng);
                    return Some(Value::String(result));
                }
            }
            Some(Value::String("Log message".to_string()))
        }
        FieldGenerator::Conditional {
            condition_field,
            condition_values,
            inner,
        } => {
            let field_val = get_field_str(current_event, condition_field).unwrap_or("");
            if condition_values.iter().any(|v| v == field_val) {
                generate_field_value(inner, rng, incident, current_event)
            } else {
                None
            }
        }
        FieldGenerator::Derived { expression } => {
            // Simple expression evaluation for "field * constant" patterns.
            if let Some(result) = evaluate_expression(expression, current_event) {
                Some(result)
            } else {
                // For simple field references like "host".
                let val = get_field_value(current_event, expression);
                val.cloned()
            }
        }
        FieldGenerator::Fallback => Some(Value::String("unknown".to_string())),
    }
}

fn get_field_str<'a>(event: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    get_field_value(event, field).and_then(|v| v.as_str())
}

fn get_field_value<'a>(event: &'a Map<String, Value>, field: &str) -> Option<&'a Value> {
    let parts: Vec<&str> = field.split('.').collect();
    if parts.is_empty() {
        return None;
    }
    let mut current: &Value = event.get(parts[0])?;
    for part in &parts[1..] {
        match current {
            Value::Object(obj) => {
                current = obj.get(*part)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn set_nested_field(event: &mut Map<String, Value>, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1 {
        event.insert(path.to_string(), value);
        return;
    }

    // Navigate/create nested objects.
    let mut current = event;
    for part in &parts[..parts.len() - 1] {
        let entry = current
            .entry(part.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        current = entry.as_object_mut().expect("nested field must be object");
    }
    current.insert(parts.last().unwrap().to_string(), value);
}

fn substitute_template(template: &str, event: &Map<String, Value>, rng: &mut StdRng) -> String {
    let mut result = template.to_string();

    // Replace {field_name} patterns with values from the event.
    while let Some(start) = result.find('{') {
        if let Some(end) = result[start..].find('}') {
            let var_name = &result[start + 1..start + end];
            let replacement = match var_name {
                "retry_count" => rng.gen_range(1..5).to_string(),
                "pool_pct" => rng.gen_range(80..99).to_string(),
                "client_id" => format!("client-{}", rng.gen_range(1..100)),
                "method" => "processRequest".to_string(),
                "args_summary" => "...".to_string(),
                "query_time" => rng.gen_range(1..500).to_string(),
                "line" => rng.gen_range(100..999).to_string(),
                _ => get_field_str(event, var_name)
                    .map(|s| s.to_string())
                    .or_else(|| {
                        get_field_value(event, var_name).map(|v| match v {
                            Value::Number(n) => n.to_string(),
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                    })
                    .unwrap_or_else(|| var_name.to_string()),
            };
            result = format!(
                "{}{}{}",
                &result[..start],
                replacement,
                &result[start + end + 1..]
            );
        } else {
            break;
        }
    }

    result
}

fn evaluate_expression(expr: &str, event: &Map<String, Value>) -> Option<Value> {
    // Handle "field * constant" patterns.
    if let Some(pos) = expr.find(" * ") {
        let field = expr[..pos].trim();
        let constant: f64 = expr[pos + 3..].trim().parse().ok()?;
        let field_val = get_field_value(event, field)?;
        let num = field_val.as_f64()?;
        Some(Value::Number(((num * constant) as i64).into()))
    } else {
        None
    }
}
