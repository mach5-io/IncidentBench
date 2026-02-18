# IncidentBench

A Kubernetes-native benchmark harness that simulates real-world incident scenarios against log analytics platforms. It replays realistic ingestion surges and concurrent analyst queries to measure how a platform performs under pressure.

## Architecture

IncidentBench runs as a Kubernetes operator that orchestrates benchmark runs through a defined lifecycle:

```
Pending → Preparing → Initializing → Running → Aggregating → Reporting → Completed
```

| Component | Description |
|---|---|
| **Operator** | Watches `IncidentBenchRun` CRDs and drives the lifecycle |
| **PhaseController** | gRPC service that coordinates timeline phases across workers |
| **IngestWorker** | Produces synthetic log events to Kafka at target EPS rates |
| **QueryWorker** | Executes queries against the target platform at target QPS rates |
| **Aggregator** | Collects per-second metrics from workers, tracks Kafka consumer lag, and computes the final scorecard |

### Workspace Crates

```
crates/
├── incidentbench-common     # Shared types: CRD, scenario, adapter trait, metrics
├── incidentbench-operator   # Kubernetes operator (reconciler + resource builders)
├── incidentbench-worker     # Worker binary (ingest, query, phase-controller, aggregator)
├── incidentbench-reporter   # Report generator
└── incidentbench-cli        # CLI tool
```

## Prerequisites

- Rust 1.75+ (for building)
- Docker (for container images)
- A Kubernetes cluster (v1.28+)
- `kubectl` configured for your cluster
- An external Kafka cluster (or deploy one yourself)
- A Mach5 cluster (target platform)

## Building

### Native Build

```bash
make build
```

### Docker Images

Build and push to your container registry:

```bash
# Default registry (ghcr.io/mach5-io):
make docker-build
make docker-push

# Custom registry (e.g., local registry):
REGISTRY=myregistry.example.com/incidentbench make docker-build docker-push
```

This produces three images:
- `<REGISTRY>/operator:v0.1.0`
- `<REGISTRY>/worker:v0.1.0`
- `<REGISTRY>/reporter:v0.1.0`

### Generate the CRD

The CRD YAML is derived from the Rust structs. Regenerate it after changing the spec:

```bash
make generate-crd
```

Output: `config/crd/incidentbenchrun-crd.yaml`

## Deploying

### 1. Install the CRD

```bash
make install-crd
```

### 2. Deploy the Operator

```bash
REGISTRY=myregistry.example.com/incidentbench make deploy
```

This creates:
- The `incidentbench-system` namespace
- A ServiceAccount, ClusterRole, and ClusterRoleBinding
- The operator Deployment

### 3. Deploy Kafka

IncidentBench requires a Kafka cluster for the ingestion pipeline. Point workers at it via `spec.kafka.bootstrap_servers` in the CR.

Example using a single-node KRaft deployment:

```yaml
# kafka-kraft.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: kafka
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  namespace: kafka
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
        - name: kafka
          image: apache/kafka:3.8.0
          ports:
            - containerPort: 9092
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-bootstrap
  namespace: kafka
spec:
  selector:
    app: kafka
  ports:
    - port: 9092
      targetPort: 9092
```

### Full Local Setup (kind)

For development, a single command sets up everything:

```bash
make deploy-local
```

This creates a kind cluster, builds and loads images, installs the CRD, deploys the operator, and sets up Kafka via Strimzi.

## Running a Benchmark

### 1. Start a Run

Apply an `IncidentBenchRun` CR:

```bash
kubectl apply -f config/samples/smoke-test.yaml
```

### 2. Monitor Progress

```bash
# Watch phase transitions
kubectl get incidentbenchruns -n incidentbench-system -w

# Live status with progress counters
kubectl get ibrun smoke-test-001 -n incidentbench-system -o jsonpath='{.status}' | python3 -m json.tool

# Operator logs
kubectl logs -n incidentbench-system -l app.kubernetes.io/name=incidentbench-operator -f

# Worker pods
kubectl get pods -n incidentbench-system -l app.kubernetes.io/managed-by=incidentbench-operator
```

During the run, `status.progress` updates every reconciliation cycle:

```json
{
  "phase": "Running",
  "current_benchmark_phase": "ingestion_surge",
  "progress": {
    "elapsed_seconds": 90,
    "total_seconds": 180,
    "achieved_ingest_eps": 498,
    "achieved_query_qps": 2,
    "target_ingest_eps": 500,
    "target_query_qps": 2,
    "kafka_consumer_lag": 1250
  }
}
```

### 3. Retrieve Results

When the run reaches `Completed`, the scorecard is written directly into the CR's `status.results` field. No PVC or external storage is needed.

```bash
# Get the scorecard
kubectl get ibrun smoke-test-001 -n incidentbench-system \
  -o jsonpath='{.status.results}' | python3 -m json.tool

# Or get the full status
kubectl get ibrun smoke-test-001 -n incidentbench-system \
  -o jsonpath='{.status}' | python3 -m json.tool

# Save results to a file
kubectl get ibrun smoke-test-001 -n incidentbench-system \
  -o jsonpath='{.status.results}' | python3 -m json.tool > results.json
```

Using the CLI:

```bash
incidentbench report download smoke-test-001 --namespace incidentbench-system --output ./results
```

### 4. Delete a Run

Deleting the CR triggers automatic cleanup of all resources (workers, aggregator, Mach5 namespace, Kafka topic, etc.):

```bash
kubectl delete ibrun smoke-test-001 -n incidentbench-system
```

## Understanding Results

After a run completes, `status.results` contains the benchmark scorecard. Here is an example from the [smoke test](config/samples/smoke-test.yaml):

```json
{
  "valid": true,
  "validity_violations": [],
  "warnings": [],
  "harness_saturated": false,
  "scorecard": {
    "baseline_p99_ms": 16.36,
    "overlap_p99_ms": 3.89,
    "p99_degradation_ratio": 0.24,
    "query_error_rate_overlap": 0.0,
    "peak_backlog": 2778,
    "backlog_drain_time_s": 1.0,
    "recovery_time_s": 0.0
  }
}
```

### Scorecard Fields

| Field | Description |
|---|---|
| `baseline_p99_ms` | Query P99 latency during the **baseline** phase (normal operations). This is the reference point for degradation. |
| `overlap_p99_ms` | Query P99 latency during the **overlap** phase (peak ingestion + peak queries simultaneously). This is the stress measurement. |
| `p99_degradation_ratio` | `overlap_p99_ms / baseline_p99_ms`. Values > 1.0 mean queries got slower under load. Values < 1.0 mean the platform warmed up. |
| `query_error_rate_overlap` | Fraction of queries that failed during the overlap phase. 0.0 = no errors. |
| `peak_backlog` | Maximum Kafka consumer lag (in messages) observed across the entire run. Measures how well the ingestion pipeline keeps up with the event stream. |
| `backlog_drain_time_s` | Seconds it took for the Kafka backlog to drain to zero after ingestion stopped. Lower = faster catch-up. |
| `recovery_time_s` | Seconds after the overlap phase until query P99 returned to within 1.2x of baseline. 0 = immediate recovery. |

### Validity and Warnings

| Field | Description |
|---|---|
| `valid` | `true` if the run produced usable results (no violations). |
| `validity_violations` | List of reasons the run is invalid (e.g., timeline too short, missing baseline/overlap phases). |
| `warnings` | Non-fatal issues (e.g., harness CPU saturation detected). |
| `harness_saturated` | `true` if any worker exceeded 90% CPU — results may undercount achieved throughput. |

### How the Smoke Test CR Maps to Results

The [smoke test](config/samples/smoke-test.yaml) defines a 6-phase timeline (180 seconds total). Here's how each phase contributes to the scorecard:

```
Phase              Duration   Ingest EPS   Query QPS   What it measures
─────────────────  ─────────  ──────────   ─────────   ────────────────────────────
baseline           30s        100          1.0         → baseline_p99_ms
incident_trigger   30s        200          1.0           (ramp-up)
ingestion_surge    30s        500          2.0           (stress builds)
overlap            30s        500          5.0         → overlap_p99_ms, query_error_rate_overlap
recovery           30s        200          2.0         → recovery_time_s
post_incident      30s        100          1.0           (return to normal)
```

- **`baseline_p99_ms = 16.36 ms`** — Measured during the `baseline` phase at 100 EPS / 1 QPS. This is the platform's steady-state query latency.
- **`overlap_p99_ms = 3.89 ms`** — Measured during the `overlap` phase at 500 EPS / 5 QPS. In this case, queries were faster because caches were warm by this point.
- **`p99_degradation_ratio = 0.24`** — 3.89 / 16.36 = 0.24. The platform actually performed better under sustained load (ratio < 1.0 means no degradation).
- **`peak_backlog = 2778`** — The maximum Kafka consumer lag observed. At 500 EPS with batch size 100, a backlog of ~2778 messages means the ingestion pipeline was about 5-6 seconds behind at its peak.
- **`backlog_drain_time_s = 1.0`** — After ingest workers stopped, the target platform consumed the remaining Kafka messages within 1 second.
- **`recovery_time_s = 0.0`** — Query latency never exceeded 2x baseline, so no recovery period was needed.
- **`query_error_rate_overlap = 0.0`** — All queries succeeded during peak stress.

### Interpreting Results for Production Scenarios

For larger production benchmarks (e.g., 50,000 EPS baseline, 500,000 EPS surge), watch for:

- **`p99_degradation_ratio > 2.0`** — Queries are significantly slower under ingestion load
- **`query_error_rate_overlap > 0.01`** — More than 1% of queries failing during incidents
- **`peak_backlog > target_eps * 60`** — Backlog exceeding 1 minute of events; the platform can't keep up
- **`backlog_drain_time_s > 300`** — Taking more than 5 minutes to catch up after an incident
- **`recovery_time_s > 120`** — Query performance takes more than 2 minutes to stabilize
- **`harness_saturated = true`** — The benchmark harness itself was the bottleneck; add more worker replicas

## Configuring a Benchmark

An `IncidentBenchRun` CR has two main parts: the **scenario** (what to simulate) and the **infrastructure** (how to run it).

### Minimal Example

```yaml
apiVersion: incidentbench.io/v1alpha1
kind: IncidentBenchRun
metadata:
  name: my-benchmark
  namespace: incidentbench-system
spec:
  scenario:
    scenario:
      name: "my-test"
      version: "1.0.0"
      display_name: "My Test"
      description: "A simple benchmark"
      domain: "sre"

    schema:
      index_name: "my-test-index"
      timestamp_field: "@timestamp"
      fields:
        - name: "@timestamp"
          type: timestamp
          generator: now
        - name: "level"
          type: keyword
          generator: weighted_enum
          config:
            values:
              INFO: 0.8
              ERROR: 0.2

    data_generator:
      type: "template"
      config:
        seed: 42

    query_mix:
      queries:
        - name: "find_errors"
          type: search
          weight: 1.0
          template: "level:ERROR"
          timeout_ms: 10000

    timeline:
      phases:
        - name: "steady"
          display_name: "Steady State"
          duration_seconds: 60
          ingest:
            target_eps: 100
            batch_size: 50
          query:
            target_qps: 1.0

  target:
    adapter: "mach5"
    config:
      endpoint: "http://mach5-nginx.mach5.svc"
      namespace: "my-test-ns"
      warehouse:
        name: "my-test-wh"
        numMediators: 1
        numOs: 1

  kafka:
    bootstrap_servers: "kafka-bootstrap.kafka.svc.cluster.local:9092"

  workers:
    ingest:
      replicas: 1
    query:
      replicas: 1
```

### Spec Reference

#### `spec.scenario`

Inline scenario definition. Alternatively, use `spec.scenario_ref` to load from a ConfigMap.

##### `scenario.scenario` — Metadata

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Machine-readable identifier |
| `version` | string | yes | Semantic version |
| `display_name` | string | yes | Human-readable name |
| `description` | string | no | What this scenario simulates |
| `domain` | string | no | Category (e.g., `sre`, `security`) |

##### `scenario.schema` — Data Schema

Defines the index and the fields that synthetic log events contain.

| Field | Type | Required | Description |
|---|---|---|---|
| `index_name` | string | yes | Name of the index created on the target platform |
| `timestamp_field` | string | yes | Field name for the event timestamp |
| `fields` | array | yes | Field definitions (see below) |

Each field has:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Field name in the generated events |
| `type` | enum | yes | `timestamp`, `keyword`, `text`, `int`, `long`, `float`, `ip` |
| `generator` | string | yes | Data generator: `now`, `weighted_enum`, `template`, `pattern`, `hex`, `distribution`, `conditional`, `derived`, `enum` |
| `config` | object | no | Generator-specific configuration |

##### `scenario.query_mix` — Query Workload

Defines the queries that workers execute during the benchmark.

```yaml
query_mix:
  queries:
    - name: "error_search"          # Unique query identifier
      type: search                  # search or aggregation
      weight: 0.5                   # Fraction of QPS allocated to this query
      template: "level:ERROR"       # Query template
      timeout_ms: 10000             # Per-query timeout
      limit: 100                    # Max results to return (optional)
      sort: "@timestamp:desc"       # Sort order (optional)
      description: "Find errors"    # Human-readable description
      variables:                    # Template variable sources (optional)
        random_trace_id:
          source: "recently_ingested"
```

Query weights across all queries must sum to 1.0.

##### `scenario.timeline` — Phase Definitions

The timeline defines the benchmark phases. Each phase specifies ingestion and query rates.

```yaml
timeline:
  phases:
    - name: "baseline"
      display_name: "Baseline"
      duration_seconds: 120
      ingest:
        target_eps: 5000        # Events per second
        batch_size: 500         # Events per Kafka batch (default: 500)
      query:
        target_qps: 5.0        # Queries per second
      description: "Normal operating conditions"
```

A typical incident scenario uses 6 phases:

| Phase | Purpose |
|---|---|
| **baseline** | Normal operations, establishes performance baseline |
| **incident_trigger** | Error rates spike, log volume increases |
| **ingestion_surge** | Peak log volume (10-50x normal) |
| **overlap** | Peak ingestion + peak queries simultaneously (core measurement window) |
| **recovery** | Fix deployed, rates declining |
| **post_incident** | Return to baseline, verify platform recovers |

##### `scenario.valid_run_criteria` — Validation Rules

Optional rules that determine whether a run produced valid, usable results:

```yaml
valid_run_criteria:
  rules:
    - name: "baseline_query_stability"
      condition: "query.baseline_p99 < 5000"
      message: "Baseline p99 must be under 5s"
    - name: "minimum_query_volume"
      condition: "query.total_executed >= 500"
      message: "Need at least 500 queries for significance"
```

##### `scenario.query_groups` — Multi-Analyst Simulation

Simulate different analyst personas with different query patterns:

```yaml
query_groups:
  - name: "heavy-analysts"
    weight: 0.4                    # 40% of total QPS
    mix_override:                  # Override query weights for this group
      error_code_agg: 0.3
      service_error_rate: 0.4
      status_code_timeline: 0.3
  - name: "light-analysts"
    weight: 0.6                    # 60% of total QPS
    mix_override:
      error_search: 0.4
      recent_errors: 0.3
      trace_lookup: 0.2
      slow_requests: 0.1
```

When `query_groups` is present, use `workers.queryGroups` (below) to map each group to a warehouse.

#### `spec.target` — Target Platform

| Field | Type | Required | Description |
|---|---|---|---|
| `adapter` | string | yes | Adapter name (currently `mach5`) |
| `config` | object | no | Adapter-specific configuration |

**Mach5 adapter config:**

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | required | Mach5 API endpoint (e.g., `http://mach5-nginx.mach5.svc`) |
| `namespace` | string | `"default"` | Mach5 namespace to create and use for all resources |
| `warehouse.name` | string | `"incidentbench-wh"` | Warehouse name |
| `warehouse.numMediators` | int | `1` | Number of mediator nodes |
| `warehouse.numOs` | int | `2` | Number of OpenSearch query nodes |

The adapter automatically creates and tears down:
- Mach5 namespace
- Kafka connection
- Index with mappings from the scenario schema
- Ingest pipeline consuming from the Kafka topic
- Warehouse(s) for query execution

#### `spec.kafka`

| Field | Type | Default | Description |
|---|---|---|---|
| `bootstrap_servers` | string | `"kafka-bootstrap:9092"` | Kafka bootstrap servers |
| `managed` | bool | `false` | Deploy a managed Kafka cluster (not yet implemented) |

#### `spec.workers`

| Field | Type | Default | Description |
|---|---|---|---|
| `ingest.replicas` | int | `10` | Number of ingest worker pods |
| `ingest.resources` | object | none | Kubernetes resource requests/limits |
| `query.replicas` | int | `4` | Number of query worker pods (single-warehouse mode) |
| `query.resources` | object | none | Kubernetes resource requests/limits |

**Multi-warehouse mode** — set `queryGroups` to map scenario query groups to warehouses:

```yaml
workers:
  ingest:
    replicas: 10
  queryGroups:
    - name: "heavy-analysts"      # Must match a scenario query_group name
      warehouse:
        name: "heavy-wh"
        numMediators: 1
        numOs: 4
      replicas: 2
    - name: "light-analysts"
      warehouse:
        name: "light-wh"
        numMediators: 1
        numOs: 2
      replicas: 4
```

When `queryGroups` is set, the `query` field is ignored.

#### `spec.scaling`

Scale all rates and durations without editing the scenario:

| Field | Type | Default | Description |
|---|---|---|---|
| `duration_scale` | float | `1.0` | Multiplier for all phase durations |
| `rate_scale` | float | `1.0` | Multiplier for all EPS/QPS targets |

Example: run at 2x speed with half the data volume:

```yaml
scaling:
  duration_scale: 0.5
  rate_scale: 0.5
```

#### `spec.images`

Override container images (useful for private registries or development):

```yaml
images:
  worker: "myregistry.example.com/incidentbench/worker:v0.1.0"
```

#### `spec.dry_run`

Set to `true` to validate the scenario and print the execution plan without running the benchmark.

### Example: Full SRE Outage Scenario

See [config/samples/sre-outage-run.yaml](config/samples/sre-outage-run.yaml) for a production-grade scenario that simulates:
- 8 microservices producing structured logs
- Baseline at 5,000 EPS ramping to 50,000 EPS during incident
- 8 different query types (search, aggregation, trace lookup)
- Concurrent query load ramping from 5 to 40 QPS during overlap
- Validity criteria for run quality

### Loading Scenarios from ConfigMaps

Instead of inlining the scenario, reference a ConfigMap:

```yaml
spec:
  scenario_ref:
    config_map:
      name: my-scenario-cm
      key: scenario.yaml    # default key
```

## Make Targets

```
make help          # Show all targets
make build         # Build all crates (native, release)
make docker-build  # Build Docker images
make docker-push   # Push images to registry
make generate-crd  # Generate CRD YAML from Rust types
make install-crd   # Apply CRD to cluster
make deploy        # Deploy operator (CRD + RBAC + Deployment)
make deploy-local  # Full local dev setup with kind
make smoke-test    # Quick validation of deployment
make clean         # Remove build artifacts
```

## Roadmap

- **Elasticsearch adapter** — native adapter for Elasticsearch targets
- **OpenSearch adapter** — native adapter for OpenSearch targets

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to get started.

All commits must include a `Signed-off-by` line (DCO). Use `git commit -s` to add it automatically.

## Security

To report a security vulnerability, please see [SECURITY.md](SECURITY.md).

## License

Copyright 2025 Mach5 Software, Inc.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
