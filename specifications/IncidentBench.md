# IncidentBench v0.1 Specification

## 1. Overview

IncidentBench is a benchmark harness that simulates real production incidents against search and analytics platforms. It measures how systems behave when ingestion spikes and query spikes overlap — the defining operational condition of every real incident.

IncidentBench runs as a Kubernetes operator. Benchmark scenarios are defined as Custom Resources. The harness horizontally scales ingest and query workers across pods, routes ingestion through Kafka (modeling real production data pipelines), and provides real-time metrics via the CLI during the run.

IncidentBench is **not** a throughput benchmark. It is a **resilience benchmark**. It measures stability, isolation, predictability, and recovery — the properties that determine whether a platform survives production chaos or collapses under it.

### 1.1 What IncidentBench Measures

| Property | Question it answers |
|---|---|
| **Stability** | Do query latencies stay predictable when ingestion surges? |
| **Isolation** | Does a query storm degrade ingestion throughput? |
| **Predictability** | Are tail latencies bounded, or do they blow up under stress? |
| **Recovery** | How quickly does the system return to baseline after the incident ends? |

### 1.2 What IncidentBench Does Not Measure

- Peak QPS under ideal conditions
- Maximum ingestion throughput in isolation
- Compression ratios or storage efficiency
- Cluster sizing optimization

These are important properties, but they are already well-served by existing tools. IncidentBench fills the gap: **behavior under overlapping operational stress**.

---

## 2. Architecture

IncidentBench runs as a Kubernetes operator. The benchmark scenario is expressed as a Custom Resource (CR). Applying the CR triggers the operator to deploy horizontally-scaled worker pods that generate load against the target platform. Ingestion flows through Kafka, modeling real production data pipelines.

### 2.1 Component Topology

```
                     ┌──────────────────────────────────────────────────────┐
                     │                 Kubernetes Cluster                   │
                     │                                                      │
  kubectl apply      │  ┌────────────────────────┐                         │
  ───────────────────┼─>│   IncidentBenchRun CR   │                        │
                     │  └───────────┬────────────┘                         │
                     │              │ watches                              │
                     │  ┌───────────▼────────────┐                         │
                     │  │   Operator Controller   │                        │
                     │  │   (Deployment, 1 pod)   │                        │
                     │  └───────────┬─────────────┘                        │
                     │              │ creates + manages                    │
                     │    ┌─────────┼───────────────────────┐              │
                     │    │         │                       │              │
                     │    ▼         ▼                       ▼              │
                     │  ┌─────┐  ┌────────────────┐  ┌──────────────────┐ │
                     │  │Init │  │PhaseController  │  │MetricsAggregator│ │
                     │  │Job  │  │  (Pod, 1)       │  │(StatefulSet, 1) │ │
                     │  └──┬──┘  └───────┬────────┘  └───────┬──────────┘ │
                     │     │       gRPC  │ barrier       gRPC │ streaming  │
                     │     │      ┌──────┼──────┐      ┌──────┼──────┐    │
                     │     │      │      │      │      │      │      │    │
                     │     ▼      ▼      ▼      ▼      ▼      ▼      ▼    │
                     │  ┌────────────────────────────────────────────────┐ │
                     │  │           IngestWorker Deployment              │ │
                     │  │  ┌─────┐ ┌─────┐ ┌─────┐  ...  ┌─────┐      │ │
                     │  │  │ IW0 │ │ IW1 │ │ IW2 │       │ IW9 │      │ │
                     │  │  └──┬──┘ └──┬──┘ └──┬──┘       └──┬──┘      │ │
                     │  └─────┼───────┼───────┼──────────────┼─────────┘ │
                     │        │       │       │              │           │
                     │        ▼       ▼       ▼              ▼           │
                     │  ┌────────────────────────────────────────────┐   │
                     │  │              Kafka Cluster                  │   │
                     │  │  (Strimzi-managed or external)             │   │
                     │  │  Topic: incidentbench-sre-outage           │   │
                     │  └──────────────────┬─────────────────────────┘   │
                     │                     │ consumes                    │
                     │  ┌──────────────────┼─────────────────────────┐   │
                     │  │           QueryWorker Deployment           │   │
                     │  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐         │   │
                     │  │  │ QW0 │ │ QW1 │ │ QW2 │ │ QW3 │         │   │
                     │  │  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘         │   │
                     │  └─────┼───────┼───────┼───────┼─────────────┘   │
                     │        │       │       │       │                  │
                     │        └───────┴───────┴───────┘                  │
                     │                     │ queries                     │
                     │            ┌────────▼─────────┐                   │
                     │            │  Report Gen Job   │ (after run)      │
                     │            └────────┬─────────┘                   │
                     │                     │                              │
                     │            ┌────────▼─────────┐                   │
                     │            │  Results PVC/S3   │                   │
                     │            └──────────────────┘                   │
                     └──────────────────────────────────────────────────────┘
                                           │
                                           ▼
                                ┌─────────────────────┐
                                │   Target Platform    │
                                │   (Mach5 / other)    │
                                │   Consumes from      │
                                │   Kafka via ingest   │
                                │   pipeline           │
                                └─────────────────────┘
```

### 2.2 Ingestion Data Path

Ingest workers do **not** call the target platform's API directly. Instead, they produce events to Kafka topics. The target platform consumes from those topics via its own ingest pipeline.

```
IngestWorker ──produce──> Kafka Topic ──consume──> Mach5 Ingest Pipeline ──> Mach5 Storage
```

This models real production architectures where log data flows through a message bus. It also provides a natural, always-available backlog metric: **Kafka consumer lag**. The harness reads consumer group lag directly from Kafka — no adapter-specific backlog API is needed.

**Topic naming:** One topic per scenario index. The topic name is derived from the scenario's `index_name` field (e.g., scenario index `incidentbench-sre-outage` produces topic `incidentbench-sre-outage`). Topics are created by the operator during its `Preparing` phase.

**Partitioning:** Topics are partitioned to match the ingest worker count by default. Partitioning strategy (round-robin or key-based) is configurable in the scenario.

**Serialization:** Events are serialized as JSON. Each Kafka message is one event.

### 2.3 CRD Specification

```
Group:   incidentbench.io
Version: v1alpha1
Kind:    IncidentBenchRun
Plural:  incidentbenchruns
Short:   ibrun
```

#### 2.3.1 CR Spec

```yaml
apiVersion: incidentbench.io/v1alpha1
kind: IncidentBenchRun
metadata:
  name: sre-outage-run-001
  namespace: incidentbench
spec:
  # Scenario: embedded inline or referenced from a ConfigMap
  scenario:
    name: "sre-outage"
    version: "1.0.0"
    display_name: "SRE Outage"
    # ... full scenario YAML (schema, query_mix, timeline, etc.)

  # Or reference a ConfigMap:
  # scenarioRef:
  #   configMap:
  #     name: sre-outage-scenario
  #     key: scenario.yaml

  # Target platform configuration
  target:
    adapter: "mach5"
    config:
      endpoint: "https://mach5-cluster:8080"
      namespace: "incidentbench-sre-outage"  # Mach5 namespace (created/deleted by the adapter)
      warehouse:                      # Optional warehouse config (defaults shown)
        name: "incidentbench-wh"    # Warehouse name (query isolation unit, independent of indexes)
        numMediators: 1
        numOs: 2
      credentials:
        secretRef:
          name: mach5-credentials

  # Kafka configuration
  kafka:
    bootstrapServers: "kafka-bootstrap.incidentbench.svc:9092"
    # Or let the operator deploy Kafka:
    # managed: true
    # managedConfig:
    #   replicas: 3
    #   storage: 10Gi

  # Scaling
  scaling:
    durationScale: 1.0
    rateScale: 1.0

  # Worker configuration
  workers:
    ingest:
      replicas: 10
      resources:
        requests:
          cpu: "1"
          memory: "512Mi"
        limits:
          cpu: "2"
          memory: "1Gi"
    query:
      replicas: 4
      resources:
        requests:
          cpu: "500m"
          memory: "256Mi"
        limits:
          cpu: "1"
          memory: "512Mi"
    # Multi-warehouse mode (optional). Maps scenario query_groups to warehouses.
    # When present, `query` above is ignored. Each group gets its own Deployment.
    # queryGroups:
    #   - name: "heavy-analysts"       # Must match scenario query_groups[].name
    #     warehouse:
    #       name: "heavy-wh"
    #       numMediators: 1
    #       numOs: 4
    #     replicas: 2
    #   - name: "light-analysts"
    #     warehouse:
    #       name: "light-wh"           # Different warehouse = isolation
    #       numMediators: 1
    #       numOs: 2
    #     replicas: 4

  # Results storage
  results:
    storage:
      type: pvc          # pvc | s3
      pvc:
        claimName: incidentbench-results
        subPath: "runs/"

  # Container images (optional override)
  images:
    operator: "ghcr.io/mach5-io/incidentbench-operator:v0.1.0"
    worker: "ghcr.io/mach5-io/incidentbench-worker:v0.1.0"
    reporter: "ghcr.io/mach5-io/incidentbench-reporter:v0.1.0"

  dryRun: false
```

The core design principle: **the scenario YAML remains the authoring format**. The CRD wraps it with operational parameters (target config, Kafka config, scaling, worker counts) but does not restructure the scenario model.

#### 2.3.2 CR Status

```yaml
status:
  phase: "Running"    # Pending | Preparing | Initializing | Running
                      # | Aggregating | Reporting | Completed | Failed

  conditions:
    - type: "Validated"
      status: "True"
      lastTransitionTime: "2026-02-18T10:00:00Z"
    - type: "Prepared"
      status: "True"
      message: "Kafka topics and Mach5 indexes created"
    - type: "WorkersReady"
      status: "True"
      message: "10 ingest workers, 4 query workers ready"
    - type: "RunComplete"
      status: "False"

  currentBenchmarkPhase: "overlap"
  currentBenchmarkPhaseIndex: 3

  workers:
    ingest:
      desired: 10
      ready: 10
    query:
      desired: 4
      ready: 4

  runId: "uuid"
  startTime: "2026-02-18T10:00:00Z"
  completionTime: null

  progress:
    elapsedSeconds: 270
    totalSeconds: 600
    achievedIngestEPS: 48500
    targetIngestEPS: 50000
    achievedQueryQPS: 38.2
    targetQueryQPS: 40.0
    kafkaConsumerLag: 125000

  # Populated after completion
  results:
    valid: true
    validityViolations: []
    warnings: []
    harnessSaturated: false
    scorecard:
      baselineP99Ms: 45.2
      overlapP99Ms: 312.7
      p99DegradationRatio: 6.92
      queryErrorRateOverlap: 0.02
      peakBacklog: 125000
      backlogDrainTimeS: 45.3
      recoveryTimeS: 67.1
    reportPath: "/results/runs/sre-outage-run-001/report.html"
    jsonReportPath: "/results/runs/sre-outage-run-001/run.json"
```

### 2.4 Operator Lifecycle

The operator manages a reconciliation loop for `IncidentBenchRun` resources. The lifecycle is a linear state machine:

```
Pending ──> Preparing ──> Initializing ──> Running ──> Aggregating ──> Reporting ──> Completed
  │              │              │              │            │              │
  └──────────────┴──────────────┴──────────────┴────────────┴──────────────┴──> Failed
```

**Pending** — CR created. The operator validates the scenario YAML, resolves `scenarioRef` if used, validates target and Kafka configuration, computes effective rates (applying `rateScale` and `durationScale`), computes per-worker rate tables, and generates a unique `runId`.

**Preparing** — The operator automatically handles all infrastructure setup. It creates Kafka topics (named from the scenario's `index_name`, partitioned to match ingest worker count), calls the target adapter's `prepare()` method to create indexes, ingest pipelines, and one or more query warehouses via the Mach5 REST API. When `workers.queryGroups` is configured, one warehouse is created per unique warehouse name across all groups (multiple groups can share the same warehouse for contention testing). This is executed as a short-lived Job. If Kafka is `managed: true`, the operator deploys a Kafka cluster (via Strimzi or KRaft StatefulSet) first. The phase does not complete until all warehouses are ready to serve queries. No manual preparation step is required — applying the CR triggers the full lifecycle.

**Initializing** — Deploys the MetricsAggregator (StatefulSet, 1 replica with PVC for metric buffering). Deploys IngestWorker Deployment (N replicas). For query workers: if `workers.queryGroups` is configured, deploys one QueryWorker Deployment per group (each with its own replica count, warehouse endpoint, and resolved query mix); otherwise deploys a single QueryWorker Deployment (M replicas). Each worker connects to the MetricsAggregator and PhaseController on startup and reports ready. Deploys the PhaseController pod (1 replica) — the timeline conductor. Waits for all workers to report ready.

**Running** — The PhaseController drives the timeline, issuing phase transitions to all workers via the PhaseGate protocol (Section 2.6). Workers generate load: ingest workers produce to Kafka, query workers query the target. Workers stream 1-second metric snapshots to the MetricsAggregator. The operator periodically polls the PhaseController and updates `status.progress` on the CR.

**Aggregating** — All phases complete. Workers flush final metrics. The MetricsAggregator computes the unified 1-second time-series, derived metrics, and valid-run criteria. Writes aggregated data to the results PVC/S3.

**Reporting** — The operator creates a report-generator Job that reads the aggregated metrics and produces `run.json` (JSON report) and `report.html` (self-contained HTML report). Populates `status.results` with the scorecard and report paths.

**Completed** — Worker Deployments, PhaseController, and MetricsAggregator are torn down. Results on PVC/S3 are permanent. The CR persists with the final scorecard in its status.

**Failed** — Set at any point if a fatal error occurs (worker CrashLoopBackOff, PhaseController failure, Kafka unreachable, etc.). Worker pods are NOT torn down (preserved for debugging). Descriptive error in `status.conditions`.

Child resources (Deployments, StatefulSets, Jobs) use `ownerReferences` pointing to the CR. Deleting the CR cascades to delete all children.

**Cleanup via finalizer:** The CR registers a finalizer (`incidentbench.io/cleanup`). When the CR is deleted, the operator's finalizer runs the adapter's `cleanup()` method (removing all Mach5 warehouses — including per-group warehouses — ingest pipelines, indexes, and connections) and deletes Kafka topics before allowing the CR to be removed. This ensures infrastructure created during `Preparing` is always torn down, with no manual cleanup step.

### 2.5 Worker Architecture

IngestWorkers and QueryWorkers share a single container image (`incidentbench-worker`). The mode is set by a command flag (`--mode=ingest` or `--mode=query`).

#### 2.5.1 IngestWorker

Generates synthetic events from the scenario schema and produces them to the Kafka topic at the rate specified by the current phase. Each worker is responsible for `1/N` of the total ingest EPS.

**Configuration** (injected via ConfigMap + Secret mounts):
- Scenario schema and data generator config
- Kafka bootstrap servers and topic name
- Per-worker rate table (EPS per phase, pre-computed by the operator)
- MetricsAggregator gRPC address
- PhaseController gRPC address
- Worker index and total worker count

**Rate distribution example** (SRE-Outage, 10 ingest workers):

| Phase | Total EPS | Per-Worker EPS |
|---|---|---|
| Baseline | 5,000 | 500 |
| Incident Trigger | 15,000 | 1,500 |
| Ingestion Surge | 50,000 | 5,000 |
| Overlap | 50,000 | 5,000 |
| Recovery | 10,000 | 1,000 |
| Post-Incident | 5,000 | 500 |

**Data generation determinism:** Each worker uses a seed derived from the run-level seed and its worker index: `workerSeed = hash(runSeed, workerIndex)`. The combined event stream is deterministic for a given `(seed, workerCount)` pair.

**Kafka production:** Events are serialized as JSON and produced via an async Kafka producer. The worker tracks Kafka produce acknowledgment latency and delivery failures as part of its metrics.

#### 2.5.2 QueryWorker

Executes queries against the target platform via the adapter's `execute_query()` method at the rate specified by the current phase.

**Single-warehouse mode** (default): Each worker is responsible for `1/M` of the total query QPS. Query selection follows the weighted query mix defined in the scenario.

**Multi-warehouse mode** (when `query_groups` is configured): Each worker belongs to a specific query group and targets that group's warehouse endpoint. Its rate is `group_weight * phase_target_qps / group_replica_count`. Its query mix is the group's `mix_override` (or the scenario-level `query_mix` if no override). The worker labels all metric snapshots with its `query_group` name for per-group aggregation.

For queries that reference recently-ingested data (e.g., trace ID lookups), the query worker generates IDs using the same deterministic seed logic as the ingest workers.

#### 2.5.3 Worker Lifecycle

1. Pod starts. Loads config from ConfigMap/Secret mounts.
2. Connects to MetricsAggregator gRPC stream.
3. Connects to PhaseController gRPC stream. Reports `READY`.
4. Blocks until PhaseController signals the first phase (`baseline`).
5. Enters rate-controlled work loop. Snapshots metrics every 1 second and streams to the aggregator.
6. On each phase transition, updates local rate limiter to the new per-worker target.
7. After the final phase (`post_incident`) completes, flushes remaining metrics, reports `DONE`.
8. Pod exits cleanly.

### 2.6 Phase Coordination (PhaseGate)

Phase transitions must be tight across all workers for measurement integrity. The PhaseController is a single pod that holds the authoritative timeline and coordinates transitions via a two-phase barrier protocol over gRPC bidirectional streaming.

#### 2.6.1 Two-Phase Barrier Protocol

**PREPARE** — When the current phase's wall-clock duration is about to elapse (~100ms before), the PhaseController sends `PrepareTransition` to all workers. Each worker finishes its current in-flight operation and replies with `PrepareAck`. Timeout: 2 seconds. If any worker does not ACK, the controller logs a warning and proceeds.

**TRANSITION** — Once all ACKs are received (or timeout), the PhaseController computes a precise future wall-clock instant (e.g., 50ms from now) and sends a `PhaseTransition` message to all workers containing:

```protobuf
message PhaseTransition {
  string from_phase = 1;
  string to_phase = 2;
  int64 transition_time_unix_ns = 3;
  int64 new_target_rate = 4;
}
```

Each worker sets a timer to activate the new phase at exactly `transition_time_unix_ns`. This relies on NTP-synchronized clocks across the cluster (standard in Kubernetes). By sending a future timestamp, network delivery jitter is decoupled from transition accuracy — workers transition within the same 1-second metric bucket.

#### 2.6.2 PhaseController Status

The PhaseController exposes a gRPC status method that the operator polls every 5 seconds:

```json
{
  "state": "running",
  "currentPhase": "overlap",
  "phaseElapsedSeconds": 45,
  "totalElapsedSeconds": 270,
  "totalDurationSeconds": 600,
  "connectedWorkers": { "ingest": 10, "query": 4 },
  "timelineComplete": false
}
```

When `timelineComplete` becomes `true`, the operator transitions the CR to `Aggregating`.

### 2.7 Metrics Aggregation

The MetricsAggregator runs as a StatefulSet (1 replica) with a PersistentVolumeClaim for metric buffering. It receives per-second snapshots from all workers via gRPC streaming and produces the unified time-series.

#### 2.7.1 Per-Second Aggregation

For each 1-second wall-clock bucket, the aggregator collects snapshots from all workers and merges them:

**Additive metrics** (sum across workers):
- `ingest.events_sent`, `ingest.events_accepted`, `ingest.events_rejected`
- `query.executed`, `query.errors`

**Distribution metrics** (t-digest merge):

Each worker computes a t-digest of its latencies for each 1-second bucket. The aggregator merges t-digests from all workers to produce accurate global percentiles (p50, p95, p99, max). T-digest merging has bounded error (typically <1% for p99) and is the same technique used by Elasticsearch's percentile aggregation.

**Per-group aggregation** (when `query_groups` is configured):

Each query worker's metric snapshot includes a `query_group` label. The aggregator partitions incoming query snapshots by group and maintains separate per-group t-digests alongside the global merged digest. This produces per-group latency distributions, QPS counts, and error counts in each 1-second aggregated point.

```protobuf
message LatencyDistribution {
  repeated TDigestCentroid centroids = 1;
  int64 count = 2;
  double min = 3;
  double max = 4;
}
```

**Kafka consumer lag:** The MetricsAggregator reads consumer group lag directly from Kafka via the admin API every second. The consumer group name is provided by the adapter's `prepare()` return value (e.g., `incidentbench-sre-outage-cg` for the Mach5 adapter — see Section 10.5.1). This is the primary backlog metric — it represents how many events have been produced but not yet consumed by the target's ingest pipeline.

#### 2.7.2 Late Arrival Handling

A 3-second watermark grace period is used. Snapshots arriving after the grace period are dropped and logged as warnings. In practice, gRPC streaming within a cluster delivers in <10ms.

#### 2.7.3 Harness Saturation Detection

Each worker reports its CPU utilization in the snapshot. If any worker exceeds 90% CPU sustained for 10+ seconds, the aggregator flags `harness_saturated: true`. This prevents attributing harness limitations to the target platform.

#### 2.7.4 Live Metrics Streaming

The MetricsAggregator exposes a gRPC `StreamMetrics` endpoint that pushes aggregated 1-second snapshots to subscribers in real-time. This is the same data that gets written to disk for the final report. The CLI `metrics` command subscribes to this stream (see Section 8.2).

### 2.8 Target Adapter API

Ingestion is always via Kafka. The adapter handles platform setup and querying only:

```
trait TargetAdapter {
    // Identity
    fn name() -> String;

    // Prepare: create connections, indexes, ingest pipelines, warehouses
    // Accepts a list of warehouse configs (one per unique warehouse).
    // Returns PrepareResult with consumer group (for lag monitoring)
    // and query endpoints (one per warehouse, for query workers)
    fn prepare(config, schema, kafka_topic, warehouses) -> Result<PrepareResult>;

    // Querying — query_endpoint identifies which warehouse to query
    fn execute_query(query, query_endpoint) -> Result<QueryResult>;

    // Teardown: remove all warehouses, ingest pipelines, indexes, connections
    fn cleanup() -> Result<()>;
}
```

```
WarehouseConfig {
    name: String              // Warehouse name (query isolation unit)
    num_mediators: u32        // Coordinator nodes
    num_os: u32               // Query execution nodes
}

PrepareResult {
    consumer_group: String                    // Kafka consumer group used by the target's ingest pipeline
    query_endpoints: HashMap<String, String>  // warehouse_name -> OpenSearch-compatible endpoint
}

QueryResult {
    hit_count: int
    error: Option<String>
    duration_ms: f64
}
```

There is no `ingest_batch()` method. The adapter's `prepare()` method is responsible for configuring the target to consume from the Kafka topic and serve queries — for Mach5, this means creating a Kafka connection, an index, an ingest pipeline, and one or more warehouses via the Mach5 REST API (see Section 10.5 for concrete API calls). When multiple warehouses are requested, the adapter deduplicates by name (multiple groups can share a warehouse) and creates them in parallel.

The `consumer_group` returned by `prepare()` is passed to the MetricsAggregator, which reads Kafka consumer group lag directly from the Kafka admin API. The `query_endpoints` map is used by the operator to configure each query worker group with the correct warehouse endpoint.

### 2.9 Report Generation

After aggregation completes, the operator creates a report-generator Job that reads the aggregated metrics from the results PVC and produces:
- `run.json` — Full JSON report (Section 7.1)
- `report.html` — Self-contained HTML report (Section 7.2)

The Job uses the same results PVC. Output structure matches Section 8.5.

### 2.10 Storage Model

**Results storage** — Two options:

- **PVC (default):** A `ReadWriteMany` PersistentVolumeClaim shared across the MetricsAggregator, report generator, and results retrieval. If RWX is unavailable, the operator falls back to sequential pod scheduling (aggregator writes and terminates, then reporter mounts the PVC).
- **S3 (optional):** S3-compatible object storage. The MetricsAggregator buffers locally then uploads. Configured via `spec.results.storage.s3`.

**Metrics buffer** — The MetricsAggregator uses its own PVC (from StatefulSet `volumeClaimTemplates`) for buffering during the run. Size requirement is minimal (~10MB for a 10-minute run with 14 workers).

**ConfigMaps** — The resolved scenario YAML and per-worker rate tables are stored in ConfigMaps owned by the CR and mounted into worker pods.

### 2.11 Kafka Management

Two modes:

**External (default):** User provides `kafka.bootstrapServers`. The harness produces to and reads lag from an existing Kafka cluster. Topics are created by the operator during `Preparing` and removed by the operator's finalizer when the CR is deleted.

**Managed:** Set `kafka.managed: true`. The operator deploys a Kafka cluster in the benchmark namespace using Strimzi or a KRaft StatefulSet. Suitable for isolated benchmarks where a dedicated Kafka is preferred. The managed Kafka is torn down when the CR is deleted.

### 2.12 Real-Time Observability

Live metrics are delivered via the CLI. No external monitoring dependencies are required.

The MetricsAggregator's gRPC `StreamMetrics` endpoint pushes aggregated 1-second snapshots to any connected subscriber. The CLI provides two modes for consuming this stream:

**Streaming mode** (`incidentbench metrics <run-name>`) — Each 1-second snapshot is printed as a structured line (JSON or human-readable table). Output includes: current phase, ingest EPS (achieved/target), query latency p50/p95/p99, query QPS (achieved/target), Kafka consumer lag, error counts. This is pipe-friendly — works with `jq`, `grep`, or any log processor.

**Live TUI mode** (`incidentbench metrics <run-name> --live`) — A terminal UI showing: current phase with progress bar, ingest EPS gauge, query latency percentiles, Kafka consumer lag, per-worker status, and sparkline charts of the last 60 seconds. This is the "watch it run" experience for demos and debugging.

The CLI connects to the MetricsAggregator pod via automatic `kubectl port-forward` or a direct gRPC address if the aggregator is exposed via a Service.

---

## 3. Scenario Model

A scenario is a YAML file that defines a complete incident simulation.

### 3.1 Scenario Structure

```yaml
scenario:
  name: string                    # Machine-readable identifier
  version: string                 # Scenario version (semver)
  display_name: string            # Human-readable name
  description: string             # Narrative description of the incident
  domain: string                  # Domain tag (sre, soc, ecommerce, saas, etc.)

schema:
  index_name: string              # Target index/table name
  fields: []                      # Field definitions
  timestamp_field: string         # Name of the timestamp field

data_generator:
  type: string                    # Generator type (template, distribution, replay)
  config: {}                      # Generator-specific configuration

query_mix:
  queries: []                     # Query definitions with weights

query_groups: []                  # Optional: analyst group definitions for multi-warehouse testing

timeline:
  phases: []                      # Ordered list of phases

valid_run_criteria:
  rules: []                       # Conditions that must hold for the run to be valid

report:
  title: string
  emphasis: []                    # Which metrics to highlight
```

### 3.2 Phase Definition

Each phase in the timeline has:

```yaml
phase:
  name: string                    # Phase identifier
  display_name: string            # Human-readable label
  duration_seconds: int           # How long this phase runs
  ingest:
    target_eps: int               # Target events per second
    batch_size: int               # Events per Kafka produce batch
  query:
    target_qps: float             # Target queries per second
    mix_override: {} | null       # Optional: override query weights for this phase
  description: string             # What this phase represents narratively
```

### 3.3 Query Definition

```yaml
query:
  name: string                    # Query identifier
  type: string                    # Query type (search, aggregation, etc.)
  weight: float                   # Probability of selection (0.0-1.0, must sum to 1.0)
  template: string                # Query template with variable substitution
  variables: {}                   # Variable generation rules
  timeout_ms: int                 # Per-query timeout
  description: string             # What this query represents
```

### 3.4 Query Group Definition

When `query_groups` is present, the scenario defines multiple analyst groups that can target different warehouses with different query mixes. If absent, all query workers share a single group using the global `query_mix`.

```yaml
query_group:
  name: string                    # Group identifier (e.g., "heavy-analysts", "sre-team")
  weight: float                   # Fraction of total QPS this group receives (0.0-1.0)
  mix_override: {}                # Optional: override query_mix weights for this group
```

**Validation rules:**
- All group `weight` values must sum to 1.0.
- Group names must be unique within the scenario.
- `mix_override` keys must reference query names from `query_mix.queries[]`. Values must sum to 1.0. Unlisted queries get weight 0.
- When `mix_override` is null/absent, the group uses the scenario-level `query_mix` unchanged.

**Rate distribution:** Each group receives `weight * phase_target_qps` QPS. Within a group, QPS is divided evenly across the group's worker replicas (configured in the CRD's `workers.queryGroups`).

### 3.5 Field Definition

```yaml
field:
  name: string
  type: string                    # string, int, float, timestamp, ip, keyword, etc.
  generator: string               # How to generate values (enum, pattern, range, etc.)
  config: {}                      # Generator-specific parameters
```

---

## 4. Phase Structure (Standard)

Every scenario MUST include these six phases in order. Durations and rates vary by scenario, but the phase structure is fixed.

| # | Phase | Purpose |
|---|-------|---------|
| 1 | **Baseline** | Establish steady-state performance. Moderate, constant ingestion. Light, constant query load. All metrics from this phase define the "normal" reference. |
| 2 | **Incident Trigger** | Ingestion begins to ramp. Query load remains at baseline. Represents the onset of the incident before human response. |
| 3 | **Ingestion Surge** | Ingestion hits peak rate. Query load begins to climb as operators start investigating. This is the first stress test. |
| 4 | **Overlap** | Both ingestion and query load are at peak simultaneously. This is the core measurement window. Behavior here is the primary differentiator. |
| 5 | **Recovery** | Ingestion returns to baseline. Query load tapers. The system drains backlog and stabilizes. |
| 6 | **Post-Incident** | Both ingestion and query return to baseline rates. Measures whether the system has fully recovered or has lingering degradation. |

### 4.1 Phase Transition

Phases transition by time, not by completion. If the system cannot keep up with the target rate during a phase, that is measured — it is not a reason to extend the phase.

The harness SHOULD log when achieved throughput falls below the target rate.

### 4.2 Phase Visualization (Rate Profile)

```
Events/sec
    ▲
    │
peak│         ┌──────────┐
    │      ╱  │          │  ╲
    │    ╱    │          │    ╲
base│───╱─────│──────────│─────╲──────
    │         │          │
    └─────────┴──────────┴────────────▶ Time
    Baseline  Trigger/   Recovery  Post
              Surge/
              Overlap

Queries/sec
    ▲
    │
peak│              ┌─────┐
    │           ╱  │     │  ╲
    │         ╱    │     │    ╲
base│────────╱─────│─────│─────╲──────
    │              │     │
    └──────────────┴─────┴────────────▶ Time
    Baseline       Overlap  Recovery
```

The key property: **the ingestion surge begins before the query surge, and they overlap in the middle**. This models reality — data arrives before humans react.

---

## 5. Metrics Specification

### 5.1 Collection

Metrics are collected at **1-second resolution** throughout the run. Each worker streams per-second snapshots to the MetricsAggregator, which merges them into a unified time-series (see Section 2.7). Every second, the aggregated record contains:

**Ingestion metrics (per second):**
- `ingest.events_produced` — events produced to Kafka (sum across all ingest workers)
- `ingest.events_acknowledged` — events acknowledged by Kafka
- `ingest.events_failed` — events that failed to produce (Kafka delivery errors)
- `ingest.kafka_produce_latency_ms` — distribution (p50, p95, p99, max) of Kafka produce acknowledgment durations (merged via t-digest across workers)
- `ingest.kafka_consumer_lag` — Kafka consumer group lag (read from Kafka admin API). This is the primary backlog metric — events produced but not yet consumed by the target's ingest pipeline.

**Query metrics (per second):**
- `query.executed` — queries executed (sum across all query workers)
- `query.errors` — queries that errored or timed out
- `query.latency_ms` — distribution (p50, p95, p99, max) of query latencies (merged via t-digest across workers)
- `query.latency_by_type` — per-query-type latency distributions

**Per-group query metrics** (when `query_groups` is configured):
- `query.group.<name>.executed` — queries executed by this group
- `query.group.<name>.errors` — errors for this group
- `query.group.<name>.latency_ms` — latency distribution for this group
- `query.group.<name>.warehouse` — warehouse name this group targets

The global `query.*` metrics remain as the aggregate across all groups.

**System metrics (per second):**
- `system.ingest_target_eps` — the target ingest rate for the current phase
- `system.query_target_qps` — the target query rate for the current phase
- `system.phase` — current phase name

### 5.2 Derived Metrics

Computed after the run completes:

**Query Derived:**

| Metric | Definition |
|---|---|
| `query.baseline_p50` | p50 latency during Baseline phase |
| `query.baseline_p99` | p99 latency during Baseline phase |
| `query.overlap_p50` | p50 latency during Overlap phase |
| `query.overlap_p99` | p99 latency during Overlap phase |
| `query.p99_ratio` | `overlap_p99 / baseline_p99` — measures latency degradation |
| `query.error_rate_overlap` | Error rate during Overlap phase |
| `query.throughput_ratio` | Achieved QPS during Overlap / target QPS during Overlap |

**Per-Group Query Derived** (when `query_groups` is configured):

| Metric | Definition |
|---|---|
| `query.group.<name>.baseline_p99` | p99 latency during Baseline for this group |
| `query.group.<name>.overlap_p99` | p99 latency during Overlap for this group |
| `query.group.<name>.p99_ratio` | Overlap/Baseline p99 ratio for this group |
| `query.group.<name>.error_rate_overlap` | Error rate during Overlap for this group |

**Ingestion Derived:**

| Metric | Definition |
|---|---|
| `ingest.baseline_eps` | Achieved EPS during Baseline phase |
| `ingest.peak_eps` | Achieved EPS during Surge/Overlap phases |
| `ingest.throughput_ratio` | Achieved EPS during Overlap / target EPS during Overlap |
| `ingest.peak_backlog` | Maximum Kafka consumer lag observed |
| `ingest.backlog_drain_time_s` | Time from peak consumer lag to lag returning to baseline level |
| `ingest.time_to_searchable_p99` | p99 delay between Kafka produce and event becoming searchable in the target (measured via sentinel events) |

**Isolation Derived:**

| Metric | Definition |
|---|---|
| `isolation.query_impact_on_ingest` | Correlation between query load increase and ingest throughput decrease |
| `isolation.ingest_impact_on_query` | Correlation between ingest load increase and query latency increase |
| `recovery.time_to_baseline_s` | Time from Recovery phase start until p99 latency returns to within 1.2x of baseline p99 |

### 5.3 Primary Scorecard

The report MUST include a top-level scorecard with these values:

| Scorecard Metric | Source |
|---|---|
| Baseline p99 query latency | `query.baseline_p99` |
| Overlap p99 query latency | `query.overlap_p99` |
| p99 latency degradation ratio | `query.p99_ratio` |
| Query error rate during overlap | `query.error_rate_overlap` |
| Peak ingestion backlog | `ingest.peak_backlog` |
| Backlog drain time | `ingest.backlog_drain_time_s` |
| Recovery time to baseline | `recovery.time_to_baseline_s` |

This scorecard is the GTM artifact. It must be immediately legible to a non-technical buyer.

### 5.4 Distributed Aggregation

Metrics are collected per-worker and aggregated centrally by the MetricsAggregator (Section 2.7). The aggregation method depends on the metric type:

| Metric type | Aggregation | Example |
|---|---|---|
| Counts | Sum across workers | `ingest.events_produced`, `query.executed` |
| Latency distributions | T-digest merge | `query.latency_ms`, `ingest.kafka_produce_latency_ms` |
| Kafka consumer lag | Read from Kafka admin API (global) | `ingest.kafka_consumer_lag` |
| Max values | Max across workers | Per-worker CPU saturation |

T-digest merging produces accurate global percentiles (p50, p95, p99) with bounded error (<1%) from per-worker local digests. This is the same technique used by Elasticsearch's percentile aggregation.

---

## 6. Valid Run Criteria

A run is only valid if it meets the following conditions. Invalid runs MUST be flagged in the report and MUST NOT be used for comparison.

### 6.1 Mandatory Criteria

| Rule | Condition |
|---|---|
| **Baseline stability** | p99 query latency during Baseline must not vary by more than 2x from the phase median. |
| **Ingest target met at baseline** | Achieved ingest EPS during Baseline must be >= 90% of target EPS. |
| **Query target met at baseline** | Achieved QPS during Baseline must be >= 90% of target QPS. |
| **No crash** | The target platform must remain responsive throughout the run. If the adapter reports consecutive connection failures exceeding 30 seconds, the run is invalid. |
| **Minimum duration** | Each phase must run for at least 30 seconds. |
| **Clock accuracy** | The harness must measure phase durations within 5% of the specified duration. |
| **Worker completeness** | All ingest and query worker pods must remain running for the full duration of the run. If any worker exits prematurely, the run is invalid. |
| **Kafka healthy** | Kafka producers must be able to reach brokers throughout the run. If Kafka produce failures exceed 30 consecutive seconds, the run is invalid. |

### 6.2 Advisory Criteria

These do not invalidate a run but are flagged as warnings:

| Warning | Condition |
|---|---|
| **Ingest underperformance** | Achieved ingest EPS during Surge/Overlap below 50% of target. |
| **Query underperformance** | Achieved QPS during Overlap below 50% of target. |
| **Excessive errors** | Error rate above 10% in any phase. |
| **Harness saturation** | If any worker pod cannot generate its share of the target load (measured by per-worker CPU/memory). |

### 6.3 Harness Saturation Check

Each worker pod reports its own CPU utilization in every metric snapshot. If **any** worker exceeds 90% CPU sustained for 10+ seconds, the report must include a `harness_saturated: true` flag with the affected worker identified. This prevents attributing harness limitations to the target platform.

In a distributed architecture, saturation of a single worker means that worker's share of the load is compromised. The report should show achieved vs target rates per worker to identify whether saturation caused underperformance.

---

## 7. Report Specification

### 7.1 JSON Report

The JSON report is the machine-readable output. It includes:

```json
{
  "incidentbench_version": "0.1.0",
  "scenario": {
    "name": "sre-outage",
    "version": "1.0.0"
  },
  "target": {
    "adapter": "string",
    "config_hash": "string"
  },
  "harness": {
    "workers": {
      "ingest_replicas": 10,
      "query_replicas": 4
    },
    "kafka": {
      "bootstrap_servers": "string",
      "topic": "string",
      "partitions": 10
    },
    "scaling": {
      "rate_scale": 1.0,
      "duration_scale": 1.0
    }
  },
  "run": {
    "id": "uuid",
    "timestamp": "iso8601",
    "valid": true,
    "validity_violations": [],
    "warnings": [],
    "harness_saturated": false,
    "saturated_workers": []
  },
  "scorecard": {
    "baseline_p99_ms": 0.0,
    "overlap_p99_ms": 0.0,
    "p99_degradation_ratio": 0.0,
    "query_error_rate_overlap": 0.0,
    "peak_backlog": 0,
    "backlog_drain_time_s": 0.0,
    "recovery_time_s": 0.0
  },
  "phases": [
    {
      "name": "string",
      "duration_s": 0,
      "ingest": { ... },
      "query": { ... }
    }
  ],
  "timeseries": {
    "resolution_s": 1,
    "points": [ ... ]
  },
  "query_groups": {
    "heavy-analysts": {
      "warehouse": "heavy-wh",
      "baseline_p99_ms": 42.1,
      "overlap_p99_ms": 280.5,
      "p99_degradation_ratio": 6.66,
      "baseline_avg_qps": 6.0,
      "overlap_avg_qps": 15.8,
      "overlap_error_rate": 0.01
    }
  }
}
```

### 7.2 HTML Report

The HTML report is a single self-contained file (no external dependencies). It includes:

1. **Header** — Scenario name, target adapter, run timestamp, validity status.
2. **Scorecard** — The primary scorecard table from Section 5.3.
3. **Timeline Chart** — Overlaid time-series showing:
   - Ingest EPS (achieved vs target)
   - Query latency p99
   - Query throughput (achieved vs target)
   - Kafka consumer lag
   - Phase boundaries as vertical markers
4. **Query Group Comparison** — When `query_groups` is configured: per-group table showing warehouse, baseline p99, overlap p99, degradation ratio, baseline QPS, overlap QPS, and error rate. Enables side-by-side comparison of isolation vs contention.
5. **Phase Summary Table** — Per-phase breakdown of all metrics.
6. **Latency Distribution** — Histogram of query latencies per phase.
7. **Validity Section** — Pass/fail for each valid-run criterion, plus warnings.
8. **Harness Topology** — Worker counts, Kafka config, scaling factors, per-worker achieved rates.
9. **Run Metadata** — Adapter configuration, harness version, run ID.

### 7.3 Report Comparison (Future)

v0.1 does not include comparison reporting. A future version may support diffing two JSON reports to produce a side-by-side comparison. The JSON report format is designed to make this straightforward.

### 7.4 Live Metrics (During Run)

While the HTML and JSON reports are produced after the run completes, real-time visibility is available during the run via the CLI `metrics` command (Section 8.2). The same 1-second metric snapshots that feed the final report are streamed live from the MetricsAggregator.

The live metrics stream and the post-run report use the same underlying data. The report is a static snapshot of what the live stream showed during the run.

---

## 8. Execution Model and CLI

IncidentBench has two execution interfaces: direct `kubectl apply` of an IncidentBenchRun CR, and a convenience CLI that wraps common operations.

### 8.1 Primary Execution (kubectl)

The primary execution model is Kubernetes-native:

```bash
# 1. Apply the benchmark run (operator auto-prepares infrastructure)
kubectl apply -f sre-outage-run.yaml

# 2. Watch it run
incidentbench metrics sre-outage-run-001 --live

# 3. Get the report
incidentbench report sre-outage-run-001

# 4. Delete when done (operator auto-cleans via finalizer)
kubectl delete incidentbenchrun sre-outage-run-001
```

The operator handles all infrastructure setup (Kafka topics, Mach5 indexes, ingest pipelines, warehouse) during its `Preparing` phase — no manual prepare step is needed. When the CR is deleted, the operator's finalizer tears down the infrastructure it created.

Users can also use the convenience CLI instead of raw `kubectl`.

### 8.2 CLI Commands

```
incidentbench run <scenario> [flags]               Create an IncidentBenchRun CR
incidentbench validate <scenario>                  Validate a scenario YAML (local, no cluster)
incidentbench status <run-name>                    Show run status from CR
incidentbench metrics <run-name> [--live]          Stream live metrics from a running benchmark
incidentbench logs <run-name> [--worker=N]         Stream logs from worker pods
incidentbench report <run-name>                    Download report from completed run
incidentbench report regenerate <metrics-path>     Regenerate report from raw metrics (local)
incidentbench list                                 List IncidentBenchRun resources
incidentbench delete <run-name>                    Delete a run and clean up resources
incidentbench version                              Print version
```

**`run`** — Reads the scenario YAML, wraps it in an IncidentBenchRun CR with the specified flags, and applies it to the cluster. The operator automatically handles infrastructure setup (Kafka topics, Mach5 indexes, ingest pipelines, warehouse) during its `Preparing` phase. The user can also `kubectl apply -f run.yaml` directly with a hand-authored CR.

**`validate`** — Runs locally. No cluster needed. Validates scenario YAML against the schema.

**`status`** — One-shot status from the CR: lifecycle phase, current benchmark phase, worker counts, progress.

**`metrics`** — Streams live 1-second metric snapshots from the MetricsAggregator. Default mode prints structured lines (JSON or table) — pipe-friendly for `jq`, `grep`, etc. With `--live`, renders a terminal UI with gauges, sparklines, and phase progress (see Section 2.12).

**`logs`** — Streams logs from worker pods. Optionally filter by worker index.

**`report`** — Downloads `report.html` and `run.json` from the results PVC/S3 to the local machine.

**`report regenerate`** — Runs locally. Regenerates HTML/JSON reports from raw metrics files. Useful for tweaking report formatting without re-running the benchmark.

**`delete`** — Deletes the IncidentBenchRun CR. The operator's finalizer automatically tears down infrastructure (Kafka topics, Mach5 warehouse, ingest pipelines, indexes, connections) before the CR is removed.

### 8.3 Run Flags

```
--target                 Target adapter name (required)
--target-config          Path to target adapter config file (required)
--kafka-bootstrap        Kafka bootstrap servers (required)
--duration-scale         Multiply all phase durations (default: 1.0)
--rate-scale             Multiply all rate targets (default: 1.0)
--replicas-ingest        Number of ingest worker pods (default: 10)
--replicas-query         Number of query worker pods (default: 4)
--dry-run                Validate and print execution plan without running
--verbose                Verbose logging
```

### 8.4 Duration and Rate Scaling

The `--duration-scale` and `--rate-scale` flags adjust scenario intensity without modifying the YAML:

1. **Quick smoke tests** — `--duration-scale 0.1 --rate-scale 0.01` to verify connectivity on kind/minikube.
2. **Hardware-appropriate sizing** — Scale rates to match the test environment's capacity.

Scaling does not change the shape of the workload — only the magnitude. Phase structure, rate ratios, and query mix remain identical.

### 8.5 Output Structure

Results are stored on the PVC or S3 and can be downloaded via `incidentbench report`:

```
results/<run-id>/
  ├── run.json              # Full JSON report
  ├── report.html           # Self-contained HTML report
  ├── timeseries.csv        # Raw 1-second resolution time-series
  ├── metrics.json          # Aggregated metrics with derived values
  ├── scenario.yaml         # Copy of the scenario used
  └── harness.log           # Aggregated harness execution log
```

---

## 9. SRE-Outage Scenario (Flagship)

This is the first and flagship scenario for IncidentBench v0.1.

### 9.1 Narrative

> A deployment introduces a bug in a core microservice. Error rates spike. The service begins emitting 10-50x its normal log volume as retries cascade and stack traces fill the logs. Within minutes, on-call engineers are alerted and begin querying the logging platform aggressively — searching for error patterns, filtering by service, aggregating error codes, and tailing recent logs.
>
> The logging platform must simultaneously absorb the ingestion surge and serve investigative queries without degradation in either.

### 9.2 Domain

`sre`

### 9.3 Schema

The schema represents structured application logs from a microservices environment.

```yaml
schema:
  index_name: "incidentbench-sre-outage"
  timestamp_field: "@timestamp"
  fields:
    - name: "@timestamp"
      type: timestamp
      generator: now

    - name: "level"
      type: keyword
      generator: weighted_enum
      config:
        baseline:
          INFO: 0.70
          WARN: 0.15
          ERROR: 0.10
          DEBUG: 0.05
        incident:
          ERROR: 0.60
          WARN: 0.20
          INFO: 0.10
          DEBUG: 0.10

    - name: "service"
      type: keyword
      generator: weighted_enum
      config:
        values:
          api-gateway: 0.15
          user-service: 0.10
          payment-service: 0.25    # The failing service
          order-service: 0.20
          inventory-service: 0.10
          notification-service: 0.05
          auth-service: 0.10
          search-service: 0.05

    - name: "host"
      type: keyword
      generator: pattern
      config:
        pattern: "{service}-{pod_id}.internal"
        pod_id:
          type: range
          min: 1
          max: 20

    - name: "trace_id"
      type: keyword
      generator: hex
      config:
        length: 32

    - name: "span_id"
      type: keyword
      generator: hex
      config:
        length: 16

    - name: "http_status"
      type: int
      generator: weighted_enum
      config:
        baseline:
          200: 0.85
          201: 0.05
          400: 0.03
          404: 0.02
          500: 0.03
          502: 0.01
          503: 0.01
        incident:
          200: 0.30
          201: 0.02
          400: 0.05
          404: 0.03
          500: 0.35
          502: 0.15
          503: 0.10

    - name: "response_time_ms"
      type: int
      generator: distribution
      config:
        baseline:
          type: lognormal
          mean: 50
          stddev: 30
          min: 1
          max: 2000
        incident:
          type: lognormal
          mean: 500
          stddev: 400
          min: 1
          max: 30000

    - name: "message"
      type: text
      generator: template
      config:
        templates:
          INFO:
            - "Request processed successfully"
            - "Connection established to {service}"
            - "Cache hit for key {trace_id}"
            - "Health check passed"
          WARN:
            - "Slow response from {service}: {response_time_ms}ms"
            - "Retry attempt {retry_count} for {service}"
            - "Connection pool nearing capacity: {pool_pct}%"
            - "Rate limit approaching for client {client_id}"
          ERROR:
            - "Connection refused by {service} on {host}"
            - "Timeout after {response_time_ms}ms calling {service}"
            - "NullPointerException in PaymentProcessor.processOrder(PaymentProcessor.java:{line})"
            - "Circuit breaker OPEN for {service}"
            - "Database connection failed: max retries exceeded"
            - "Failed to deserialize response from {service}: unexpected EOF"
          DEBUG:
            - "Entering {method} with args: {args_summary}"
            - "SQL query executed in {query_time}ms"

    - name: "error_code"
      type: keyword
      generator: conditional
      config:
        condition: "level in [ERROR, WARN]"
        generator: weighted_enum
        values:
          CONN_REFUSED: 0.25
          TIMEOUT: 0.30
          NULL_PTR: 0.15
          CIRCUIT_OPEN: 0.10
          DB_CONN_FAIL: 0.10
          DESER_FAIL: 0.10
        else: null

    - name: "duration_ns"
      type: long
      generator: derived
      config:
        expression: "response_time_ms * 1000000"

    - name: "kubernetes.namespace"
      type: keyword
      generator: enum
      config:
        values: ["production", "production"]  # All production

    - name: "kubernetes.pod_name"
      type: keyword
      generator: derived
      config:
        expression: "host"

    - name: "kubernetes.container_name"
      type: keyword
      generator: derived
      config:
        expression: "service"
```

### 9.4 Query Mix

These represent the queries an SRE team runs during an incident investigation.

```yaml
query_mix:
  queries:
    - name: "error_search"
      type: search
      weight: 0.25
      template: 'level:ERROR AND service:payment-service'
      timeout_ms: 10000
      description: "Search for errors in the failing service"

    - name: "recent_errors"
      type: search
      weight: 0.15
      template: 'level:ERROR'
      sort: "@timestamp:desc"
      limit: 100
      timeout_ms: 10000
      description: "Most recent errors across all services"

    - name: "error_code_agg"
      type: aggregation
      weight: 0.15
      template: |
        filter: level:ERROR
        aggregate: terms(error_code)
      timeout_ms: 15000
      description: "Top error codes"

    - name: "service_error_rate"
      type: aggregation
      weight: 0.15
      template: |
        filter: *
        aggregate: terms(service) > terms(level)
      timeout_ms: 15000
      description: "Error rate breakdown by service"

    - name: "trace_lookup"
      type: search
      weight: 0.10
      template: 'trace_id:{{random_trace_id}}'
      timeout_ms: 10000
      description: "Look up a specific trace"
      variables:
        random_trace_id:
          source: "recently_ingested"

    - name: "status_code_timeline"
      type: aggregation
      weight: 0.10
      template: |
        filter: service:payment-service
        aggregate: date_histogram(@timestamp, interval=1m) > terms(http_status)
      timeout_ms: 20000
      description: "HTTP status code trend for failing service"

    - name: "slow_requests"
      type: search
      weight: 0.05
      template: 'response_time_ms:>5000 AND service:payment-service'
      timeout_ms: 10000
      description: "Find slow requests in the failing service"

    - name: "wildcard_message"
      type: search
      weight: 0.05
      template: 'message:*NullPointerException*'
      timeout_ms: 15000
      description: "Wildcard search for specific exception"
```

### 9.5 Timeline

The default timeline runs for **10 minutes total**. All durations scale with `--duration-scale`.

```yaml
timeline:
  phases:
    - name: "baseline"
      display_name: "Baseline"
      duration_seconds: 120
      ingest:
        target_eps: 5000
        batch_size: 500
      query:
        target_qps: 5.0
      description: >
        Normal operating conditions. Moderate log volume.
        Engineers are not actively investigating.

    - name: "incident_trigger"
      display_name: "Incident Trigger"
      duration_seconds: 60
      ingest:
        target_eps: 15000
        batch_size: 500
      query:
        target_qps: 5.0
      description: >
        Bug deployed. Error rates spike. Log volume triples.
        No human response yet — alerting hasn't fired.

    - name: "ingestion_surge"
      display_name: "Ingestion Surge"
      duration_seconds: 90
      ingest:
        target_eps: 50000
        batch_size: 1000
      query:
        target_qps: 15.0
      description: >
        Cascade in effect. Retries multiply log volume to 10x.
        Alert fires. On-call begins querying.

    - name: "overlap"
      display_name: "Overlap"
      duration_seconds: 120
      ingest:
        target_eps: 50000
        batch_size: 1000
      query:
        target_qps: 40.0
      description: >
        Full incident mode. Ingestion at peak.
        Multiple engineers querying simultaneously.
        This is the core measurement window.

    - name: "recovery"
      display_name: "Recovery"
      duration_seconds: 90
      ingest:
        target_eps: 10000
        batch_size: 500
      query:
        target_qps: 20.0
      description: >
        Fix deployed. Error rates declining.
        Engineers still querying to verify fix.

    - name: "post_incident"
      display_name: "Post-Incident"
      duration_seconds: 120
      ingest:
        target_eps: 5000
        batch_size: 500
      query:
        target_qps: 5.0
      description: >
        Incident resolved. Rates return to baseline.
        Measuring whether the system fully recovers.
```

### 9.6 Rate Profile Summary

| Phase | Duration | Ingest EPS | Ingest Multiplier | Query QPS | Query Multiplier |
|---|---|---|---|---|---|
| Baseline | 120s | 5,000 | 1x | 5 | 1x |
| Incident Trigger | 60s | 15,000 | 3x | 5 | 1x |
| Ingestion Surge | 90s | 50,000 | 10x | 15 | 3x |
| Overlap | 120s | 50,000 | 10x | 40 | 8x |
| Recovery | 90s | 10,000 | 2x | 20 | 4x |
| Post-Incident | 120s | 5,000 | 1x | 5 | 1x |

**Total duration:** 600 seconds (10 minutes)
**Total events:** ~17.7M events at target rates
**Peak concurrent load:** 50,000 EPS + 40 QPS

### 9.7 Valid Run Criteria (Scenario-Specific)

In addition to the global valid-run criteria (Section 6), the SRE-Outage scenario adds:

```yaml
valid_run_criteria:
  rules:
    - name: "baseline_query_stability"
      condition: "query.baseline_p99 < 5000"
      message: "Baseline p99 must be under 5s — if it's higher, the system is too slow or misconfigured for this scenario."

    - name: "baseline_ingest_throughput"
      condition: "ingest.baseline_eps >= 4500"
      message: "Must achieve at least 90% of baseline ingest target."

    - name: "minimum_query_volume"
      condition: "query.total_executed >= 500"
      message: "Must execute at least 500 queries for statistical significance."
```

---

## 10. Implementation Notes

### 10.1 Language

Rust. Rationale:
- Zero-cost abstractions for high-throughput load generation
- No garbage collector — the harness must not introduce latency jitter that gets misattributed to the target platform
- Excellent async ecosystem (`tokio`) for concurrent I/O across workers
- Strong type system for correctness across distributed components
- Single static binary per container image (musl target)
- Familiar to infrastructure/systems engineers

### 10.2 Key Dependencies

| Crate | Purpose |
|---|---|
| `kube-rs` | Kubernetes operator framework (CRD, reconciler) |
| `tonic` | gRPC client and server (PhaseGate, MetricsAggregator) |
| `tokio` | Async runtime |
| `rdkafka` | Kafka producer (ingest workers) and admin API (consumer lag) |
| `serde` + `serde_yaml` | Scenario YAML and config serialization |
| `reqwest` | HTTP client for target adapter REST APIs |
| `tdigest` | Mergeable quantile estimation for distributed latency aggregation |
| `ratatui` | Terminal UI for `incidentbench metrics --live` |
| `clap` | CLI argument parsing |

### 10.3 Concurrency Model

Load generation is distributed across Kubernetes pods, not threads within a single process. Each worker pod runs a single-threaded tokio runtime focused on its share of the workload.

**Per-worker rate control:** Each worker uses a token bucket rate limiter. The rate target is updated at each phase transition via the PhaseGate gRPC stream. If the worker cannot consume tokens fast enough (target exceeds capacity), the shortfall is recorded as missed operations.

**Phase transitions:** Coordinated via the PhaseGate protocol (Section 2.6). The PhaseController pod holds the authoritative timeline and issues transitions via gRPC bidirectional streaming.

**Metrics flow:** Each worker streams 1-second metric snapshots to the MetricsAggregator via gRPC. The aggregator merges across workers and buffers to disk.

### 10.4 Container Images

Three container images, all built from the same Cargo workspace:

| Image | Binary | Purpose |
|---|---|---|
| `incidentbench-operator` | `incidentbench-operator` | K8s operator (reconciler) |
| `incidentbench-worker` | `incidentbench-worker` | Ingest and query workers (`--mode=ingest\|query`), PhaseController, MetricsAggregator |
| `incidentbench-reporter` | `incidentbench-reporter` | Report generation Job |

All images use static musl builds for minimal container size (no libc dependency). Multi-arch builds for `amd64` and `arm64`.

The CLI binary (`incidentbench`) is distributed separately as a standalone tool — not containerized.

### 10.5 Adapter Implementation

v0.1 ships with one adapter: **Mach5**. The adapter trait (Section 2.8) is designed for additional adapters to be contributed. Future adapters (Elasticsearch, OpenSearch) would implement `prepare()` to configure their own Kafka consumer mechanisms.

The Mach5 REST API base URL is `{endpoint}/apis` (e.g., `https://mach5-cluster:8080/apis`). All resources are scoped under a Mach5 namespace (configured in `target.config.namespace`).

#### 10.5.1 Mach5 Prepare Sequence

The adapter's `prepare()` creates four resources. Resource names are derived from the scenario's `index_name` (e.g., `incidentbench-sre-outage`).

**Step 1: Create Kafka Connection**

```
PUT /apis/namespaces/{namespace}/connections/incidentbench-kafka-conn
```

```json
{
  "kafka": {
    "bootstrap_servers": "kafka-bootstrap.incidentbench.svc:9092"
  }
}
```

The bootstrap servers come from the CR's `spec.kafka.bootstrapServers`. If Mach5 requires SSL or SASL for Kafka access, those fields are populated from `target.config.credentials`.

**Step 2: Create Index**

```
PUT /apis/namespaces/{namespace}/indexes/{index_name}
```

```json
{
  "settings": {},
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "level": { "type": "keyword" },
      "service": { "type": "keyword" },
      "host": { "type": "keyword" },
      "trace_id": { "type": "keyword" },
      "span_id": { "type": "keyword" },
      "http_status": { "type": "integer" },
      "response_time_ms": { "type": "integer" },
      "message": { "type": "text" },
      "error_code": { "type": "keyword" },
      "duration_ns": { "type": "long" },
      "kubernetes": {
        "properties": {
          "namespace": { "type": "keyword" },
          "pod_name": { "type": "keyword" },
          "container_name": { "type": "keyword" }
        }
      }
    }
  },
  "aliases": {}
}
```

The mappings are generated from the scenario's `schema.fields` using the type mapping in Section 10.5.4. Steps 1 and 2 are independent and can execute in parallel.

**Step 3: Create Ingest Pipeline**

```
PUT /apis/namespaces/{namespace}/ingest_pipelines/{index_name}-pipeline
```

```json
{
  "index": "incidentbench-sre-outage",
  "source_config": {
    "type": "kafka",
    "consumer_group": "incidentbench-sre-outage-cg",
    "topic": "incidentbench-sre-outage",
    "connection": {
      "namespace": "default",
      "name": "incidentbench-kafka-conn"
    },
    "parser_config": {
      "type": "json_lines",
      "json_lines": {
        "timestamp_field": "@timestamp"
      }
    }
  },
  "op_mode": {
    "op_mode": "streaming",
    "start_time": { "position": "earliest" }
  },
  "enabled": true,
  "poll_frequency_secs": 5,
  "max_ingest_workflows_limit": 4
}
```

Step 3 depends on both Step 1 (connection must exist for the `ConnectionRef`) and Step 2 (index must exist as the pipeline target).

The `consumer_group` is set to `{index_name}-cg`. This name is returned in `PrepareResult.consumer_group` so the MetricsAggregator can poll Kafka for this consumer group's lag.

The `poll_frequency_secs` is set to 5 (not the default 60) to ensure data becomes searchable promptly — this directly affects the `ingest.time_to_searchable_p99` metric.

The `parser_config.json_lines.timestamp_field` is set from the scenario's `schema.timestamp_field`.

**Step 4: Create Warehouse(s)**

**Single-warehouse mode** (default, or when `workers.queryGroups` is absent):

```
PUT /apis/namespaces/{namespace}/warehouses/{warehouse_name}
```

```json
{
  "num_mediators": 1,
  "num_os": 2
}
```

The warehouse name defaults to `incidentbench-wh` and can be overridden via `target.config.warehouse.name` in the CR.

**Multi-warehouse mode** (when `workers.queryGroups` is configured):

The adapter receives a list of `WarehouseConfig` entries (deduplicated by name — multiple groups sharing the same warehouse name result in a single warehouse creation). All warehouses are created in parallel:

```
For each unique warehouse in queryGroups:
  PUT /apis/namespaces/{namespace}/warehouses/{warehouse.name}
  Body: { "num_mediators": warehouse.numMediators, "num_os": warehouse.numOs }
```

A Mach5 warehouse is a unit of query isolation — it provides dedicated query nodes that can query any/all indexes in the namespace. It is **not** tied to a specific index. The adapter creates dedicated warehouses for the benchmark to isolate query resources from other workloads.

`num_mediators` controls coordinator nodes. `num_os` controls query execution nodes.

Step 4 is independent of Step 3 (the pipeline) — warehouses are independent of any particular index. The adapter can create the pipeline and warehouses in parallel after the connection and index are ready.

Each warehouse takes time to start (it launches pods). The adapter polls `GET /apis/namespaces/{namespace}/warehouses/{warehouse_name}` until each warehouse reports ready. The `Preparing` phase does not complete until all warehouses are serving queries.

Once ready, each warehouse exposes an OpenSearch-compatible query endpoint. The adapter discovers these endpoints from the warehouse service addresses and returns them in `PrepareResult.query_endpoints` (a `warehouse_name -> endpoint` map). The operator distributes the correct endpoint to each query worker group.

#### 10.5.2 Mach5 Cleanup Sequence

Reverse order of prepare. The pipeline and warehouses are deleted first, then the index and connection.

1. `DELETE /apis/namespaces/{namespace}/ingest_pipelines/{index_name}-pipeline` (parallel with step 2)
2. For each warehouse created during prepare: `DELETE /apis/namespaces/{namespace}/warehouses/{warehouse_name}` (all warehouse deletions run in parallel with step 1)
3. `DELETE /apis/namespaces/{namespace}/indexes/{index_name}` (parallel with step 4, after steps 1-2)
4. `DELETE /apis/namespaces/{namespace}/connections/incidentbench-kafka-conn` (parallel with step 3, after steps 1-2)

All DELETE operations are idempotent — 404 responses are treated as success. The adapter tracks all warehouses created during `prepare()` to ensure complete cleanup.

#### 10.5.3 Mach5 Query Execution

The warehouse exposes an OpenSearch-compatible endpoint. Query workers send queries using the standard OpenSearch `_search` API:

```
POST {query_endpoint}/{index_name}/_search
```

The adapter translates each scenario query template into OpenSearch DSL. Examples for the SRE-Outage query mix:

**Full-text search** (`error_search`):
```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "level": "ERROR" } },
        { "term": { "service": "payment-service" } }
      ]
    }
  }
}
```

**Sorted search with limit** (`recent_errors`):
```json
{
  "query": { "term": { "level": "ERROR" } },
  "sort": [{ "@timestamp": "desc" }],
  "size": 100
}
```

**Terms aggregation** (`error_code_agg`):
```json
{
  "query": { "term": { "level": "ERROR" } },
  "size": 0,
  "aggs": {
    "error_codes": { "terms": { "field": "error_code" } }
  }
}
```

**Date histogram aggregation** (`status_code_timeline`):
```json
{
  "query": { "term": { "service": "payment-service" } },
  "size": 0,
  "aggs": {
    "timeline": {
      "date_histogram": { "field": "@timestamp", "fixed_interval": "1m" },
      "aggs": {
        "status_codes": { "terms": { "field": "http_status" } }
      }
    }
  }
}
```

**Wildcard search** (`wildcard_message`):
```json
{
  "query": { "wildcard": { "message": "*NullPointerException*" } }
}
```

**Trace lookup** (`trace_lookup`):
```json
{
  "query": { "term": { "trace_id": "{{random_trace_id}}" } }
}
```

The adapter measures query duration from the HTTP request/response round-trip. The `QueryResult.hit_count` is extracted from the response's `hits.total.value`. Timeouts are enforced per-query via HTTP client timeout (from `timeout_ms` in the query definition).

#### 10.5.4 Schema Type Mapping

The adapter translates scenario field types to Mach5 index mapping types:

| Scenario Type | Mach5 Mapping Type |
|---|---|
| `timestamp` | `date` |
| `keyword` | `keyword` |
| `text` | `text` |
| `int` | `integer` |
| `long` | `long` |
| `float` | `float` |
| `ip` | `ip` |

Dotted field names (e.g., `kubernetes.namespace`) are converted to nested object mappings in the index request.

### 10.6 Kafka Integration

**Ingest workers** use `rdkafka`'s `FutureProducer` for async event production. Events are serialized as JSON. Delivery acknowledgments are tracked for produce latency metrics and failure counting.

**Topic partitioning:** Round-robin by default. Key-based partitioning (e.g., by `service` field) is configurable in the scenario for deterministic partition distribution.

**Consumer lag:** The MetricsAggregator reads consumer group lag via the Kafka admin API (`rdkafka::admin`). This provides the backlog metric every second without polling the target platform.

### 10.7 Data Generation

Events are generated in each worker pod using the scenario's schema definition. The generator is deterministic given a seed — `workerSeed = hash(runSeed, workerIndex)` — ensuring reproducibility for a given `(seed, workerCount)` pair.

Event generation must not be the bottleneck. If the generator is slower than the per-worker target rate, the harness saturation check flags it.

### 10.8 Proto Definitions

gRPC services are defined in `.proto` files and compiled via `tonic-build` in `build.rs`:

| Proto | Service | Description |
|---|---|---|
| `phasecontroller.proto` | `PhaseGateService` | Bidirectional streaming for phase barrier protocol |
| `aggregator.proto` | `MetricsService` | Worker → aggregator snapshot streaming; CLI → aggregator live metrics |
| `worker.proto` | (messages only) | `WorkerMetricSnapshot`, `LatencyDistribution`, `TDigestCentroid` |

### 10.9 Repo Structure

```
incidentbench/
  ├── Cargo.toml                          # Workspace root
  ├── crates/
  │   ├── incidentbench-operator/         # K8s operator binary
  │   │   ├── Cargo.toml
  │   │   └── src/
  │   │       ├── main.rs
  │   │       ├── controller.rs           # Reconciler logic
  │   │       └── resources.rs            # Child resource creation
  │   ├── incidentbench-worker/           # Worker + PhaseController + Aggregator binary
  │   │   ├── Cargo.toml
  │   │   └── src/
  │   │       ├── main.rs
  │   │       ├── ingest.rs               # Ingest worker loop
  │   │       ├── query.rs                # Query worker loop
  │   │       ├── phase_controller.rs     # PhaseController server
  │   │       ├── aggregator.rs           # MetricsAggregator server
  │   │       └── barrier.rs              # Two-phase barrier protocol
  │   ├── incidentbench-reporter/         # Report generator binary
  │   │   ├── Cargo.toml
  │   │   └── src/
  │   │       ├── main.rs
  │   │       ├── json_report.rs
  │   │       └── html_report.rs
  │   ├── incidentbench-cli/              # CLI binary
  │   │   ├── Cargo.toml
  │   │   └── src/
  │   │       ├── main.rs
  │   │       ├── run.rs
  │   │       ├── metrics.rs              # Streaming + TUI
  │   │       ├── status.rs
  │   │       └── report.rs
  │   └── incidentbench-common/           # Shared library
  │       ├── Cargo.toml
  │       └── src/
  │           ├── lib.rs
  │           ├── scenario.rs             # Scenario model + validation
  │           ├── generator.rs            # Data generation
  │           ├── adapter.rs              # Adapter trait
  │           ├── adapters/
  │           │   └── mach5.rs
  │           ├── metrics.rs              # Metric types, t-digest
  │           └── ratelimit.rs            # Token bucket rate limiter
  ├── proto/
  │   ├── phasecontroller.proto
  │   ├── aggregator.proto
  │   └── worker.proto
  ├── api/
  │   └── v1alpha1/
  │       └── types.rs                    # CRD type definitions (serde + kube derive)
  ├── config/
  │   ├── crd/
  │   │   └── incidentbench.io_incidentbenchruns.yaml
  │   ├── manager/
  │   │   └── manager.yaml                # Operator Deployment manifest
  │   ├── rbac/
  │   │   └── role.yaml
  │   └── samples/
  │       └── sre-outage-run.yaml         # Example IncidentBenchRun CR
  ├── scenarios/
  │   └── sre-outage/
  │       └── scenario.yaml
  ├── specifications/
  │   └── IncidentBench.md                # This document
  ├── Dockerfile.operator
  ├── Dockerfile.worker
  ├── Dockerfile.reporter
  ├── Makefile
  ├── LICENSE                             # Apache 2.0
  └── README.md
```

### 10.10 Development Setup

Local development uses `kind` or `minikube`:

1. `make docker-build` — Build all container images locally.
2. `make deploy-local` — Create a kind cluster, load images, install CRD and operator, deploy Strimzi for local Kafka.
3. `make smoke-test` — Apply a sample CR with `--rate-scale 0.01 --duration-scale 0.1` to verify end-to-end operation.

The operator's reconciliation logic is tested with `kube-rs` mock client. Worker logic is tested as unit tests with a mock adapter.

---

## 11. Future Scenarios (Roadmap)

These are not in scope for v0.1 but represent the planned expansion path.

| Scenario | Domain | Incident |
|---|---|---|
| **SOC Attack** | Security | Brute-force attack triggers alert storm. Analysts query IOCs while SIEM ingestion spikes. |
| **E-Commerce Flash Sale** | Commerce | Flash sale starts. Clickstream and order logging surge. Dashboards and real-time analytics under load. |
| **SaaS Tenant Storm** | Multi-tenant SaaS | A single large tenant generates disproportionate load. Measures noisy-neighbor isolation. |
| **Data Backfill** | Operations | Historical data backfill runs alongside live ingestion and queries. Measures impact of bulk ingest on live operations. |

---

## 12. Versioning and Compatibility

### 12.1 Harness Version

The harness follows semver. The harness version is `0.1.0`.

### 12.2 Scenario Version

Each scenario has its own version. Scenario versions are independent of the harness version. A scenario version bump means the workload has changed — results from different scenario versions are not directly comparable.

### 12.3 CRD Schema Version

The CRD uses Kubernetes API versioning (`v1alpha1`). The CRD schema version is independent of the harness version. A CRD schema version bump means the CR spec or status structure has changed.

### 12.4 Report Compatibility

The JSON report includes the harness version, scenario version, and CRD schema version. Report comparison tooling (future) will refuse to compare reports with different scenario versions.

---

## 13. Non-Goals for v0.1

To keep scope tight, the following are explicitly out of scope:

- **Automated comparison** — v0.1 produces individual run reports. Side-by-side comparison is manual.
- **Custom phase structures** — v0.1 enforces the standard 6-phase structure. Custom phase definitions are a future extension.
- **Leaderboards or public results** — IncidentBench is a tool, not a competition. No hosted results infrastructure.
- **Target platform provisioning** — IncidentBench assumes the target platform (Mach5, Elasticsearch, etc.) is already running. Cluster setup is out of scope. Kafka provisioning IS in scope (managed mode).
- **Multi-adapter support** — v0.1 ships with the Mach5 adapter only. Elasticsearch and OpenSearch adapters are future contributions.

---

## 14. Success Criteria for v0.1

v0.1 is complete when:

1. Applying an SRE-Outage CR triggers the operator's `Preparing` phase, which automatically creates Kafka topics, Mach5 indexes, ingest pipelines, and a query warehouse via the Mach5 REST API.
2. The run proceeds through the full operator lifecycle and produces a valid run with events flowing through Kafka into Mach5.
3. Ingest workers scale horizontally to produce 50,000 EPS to Kafka without harness saturation.
4. Query workers scale horizontally to sustain 40 QPS against Mach5 without harness saturation.
5. Kafka consumer lag is tracked as the backlog metric throughout the run.
6. Phase transitions have sub-second skew across all workers.
7. The run produces a valid JSON report with all metrics from Section 5.
8. The run produces a self-contained HTML report with all elements from Section 7.2.
9. Valid-run criteria are enforced and reported, including worker completeness and Kafka health.
10. `--duration-scale` and `--rate-scale` flags work correctly.
11. `--dry-run` prints the execution plan without connecting to a target.
12. `incidentbench metrics <run-name>` streams live 1-second metric snapshots from a running benchmark.
13. `incidentbench metrics <run-name> --live` renders a real-time terminal UI with phase progress, EPS, latency, and Kafka lag.
14. Deleting the CR triggers the operator's finalizer, which tears down all infrastructure (Kafka topics, Mach5 warehouse, ingest pipelines, indexes, connections) created during `Preparing`.
15. Runs are reproducible (same seed, same worker count, same scenario → same generated events).
16. `--rate-scale 0.01 --duration-scale 0.1` smoke test runs successfully on kind/minikube.
