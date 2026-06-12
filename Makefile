# IncidentBench Makefile
#
# Targets:
#   build          - Build all workspace crates (native, release)
#   docker-build   - Build Docker images for operator, worker, and reporter
#   docker-push    - Push all Docker images to the registry
#   install-crd    - Apply the CRD to the current Kubernetes cluster
#   deploy-local   - Full local dev setup: kind cluster, images, CRD, operator, Strimzi
#   smoke-test     - Run a quick validation of the local deployment
#   generate-crd   - Generate CRD YAML from the operator binary
#   clean          - Remove build artifacts

# --- Configuration ---
VERSION       ?= v0.1.0
REGISTRY      ?= ghcr.io/mach5-io
OPERATOR_IMG  := $(REGISTRY)/operator:$(VERSION)
WORKER_IMG    := $(REGISTRY)/worker:$(VERSION)
REPORTER_IMG  := $(REGISTRY)/reporter:$(VERSION)

KIND_CLUSTER  ?= incidentbench
KIND_VERSION  ?= v1.30.0
STRIMZI_VERSION ?= 0.45.0

CRD_OUTPUT    := config/crd/incidentbenchrun-crd.yaml
NAMESPACE     := incidentbench-system

.PHONY: build docker-build docker-push install-crd deploy deploy-local smoke-test generate-crd clean help

# --- Default target ---
help: ## Show this help message
	@echo "IncidentBench Makefile"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --- Build ---
build: ## Build all workspace crates (native, release)
	cargo build --release

# --- Docker ---
docker-build: ## Build Docker images for operator, worker, and reporter
	docker build -f Dockerfile.operator -t $(OPERATOR_IMG) .
	docker build -f Dockerfile.worker   -t $(WORKER_IMG)   .
	docker build -f Dockerfile.reporter -t $(REPORTER_IMG) .

docker-push: ## Push all Docker images to the registry
	docker push $(OPERATOR_IMG)
	docker push $(WORKER_IMG)
	docker push $(REPORTER_IMG)

# --- CRD ---
generate-crd: build ## Generate CRD YAML from the operator binary (runs operator --print-crd)
	./target/release/incidentbench-operator --print-crd > $(CRD_OUTPUT)
	@echo "CRD written to $(CRD_OUTPUT)"

install-crd: ## Apply the CRD to the current Kubernetes cluster
	@if [ ! -f $(CRD_OUTPUT) ]; then \
		echo "CRD file not found at $(CRD_OUTPUT). Run 'make generate-crd' first."; \
		exit 1; \
	fi
	kubectl apply -f $(CRD_OUTPUT)

# --- Deploy ---
deploy: ## Deploy operator to the current Kubernetes cluster (set REGISTRY to your registry)
	@echo "=== Installing CRD ==="
	$(MAKE) install-crd
	@echo ""
	@echo "=== Deploying operator (image: $(OPERATOR_IMG)) ==="
	sed 's|OPERATOR_IMAGE_PLACEHOLDER|$(OPERATOR_IMG)|' config/manager/manager.yaml | kubectl apply -f -
	kubectl apply -f config/rbac/role.yaml
	@echo ""
	@echo "=== Waiting for operator to be ready ==="
	kubectl wait --for=condition=Available deployment/incidentbench-operator -n $(NAMESPACE) --timeout=120s
	@echo ""
	@echo "=== Deployment complete ==="
	@echo "To run a benchmark: kubectl apply -f config/samples/sre-outage-run.yaml"

# --- Local Development (kind) ---
deploy-local: ## Create kind cluster, load images, install CRD and operator, deploy Strimzi
	@echo "=== Creating kind cluster '$(KIND_CLUSTER)' ==="
	kind create cluster --name $(KIND_CLUSTER) --image kindest/node:$(KIND_VERSION) --wait 60s 2>/dev/null || \
		echo "Cluster '$(KIND_CLUSTER)' already exists, continuing..."
	@echo ""
	@echo "=== Building Docker images ==="
	$(MAKE) docker-build
	@echo ""
	@echo "=== Loading images into kind cluster ==="
	kind load docker-image $(OPERATOR_IMG)  --name $(KIND_CLUSTER)
	kind load docker-image $(WORKER_IMG)    --name $(KIND_CLUSTER)
	kind load docker-image $(REPORTER_IMG)  --name $(KIND_CLUSTER)
	@echo ""
	@echo "=== Generating and installing CRD ==="
	$(MAKE) generate-crd
	$(MAKE) install-crd
	@echo ""
	@echo "=== Deploying Strimzi Kafka operator (skip with SKIP_STRIMZI=1 for query-only runs) ==="
	@if [ "$(SKIP_STRIMZI)" != "1" ]; then \
		kubectl create namespace kafka 2>/dev/null || true; \
		STRIMZI_URL="https://github.com/strimzi/strimzi-kafka-operator/releases/download/$(STRIMZI_VERSION)/strimzi-$(STRIMZI_VERSION).yaml"; \
		kubectl create -f "$$STRIMZI_URL" -n kafka 2>/dev/null || kubectl replace -f "$$STRIMZI_URL" -n kafka; \
		echo "Waiting for Strimzi operator to be ready..."; \
		kubectl wait --for=condition=Available deployment/strimzi-cluster-operator -n kafka --timeout=120s; \
	else \
		echo "Skipping Strimzi (SKIP_STRIMZI=1)"; \
	fi
	@echo ""
	@echo "=== Creating operator namespace and RBAC ==="
	$(MAKE) deploy
	@echo ""
	@echo "=== Local deployment complete ==="
	@echo "To run a benchmark: kubectl apply -f config/samples/sre-outage-run.yaml"

# --- Smoke Test ---
smoke-test: ## Run a quick validation of the local deployment
	@echo "=== Smoke Test ==="
	@echo ""
	@echo "--- Checking CRD registration ---"
	kubectl get crd incidentbenchruns.incidentbench.io
	@echo ""
	@echo "--- Checking operator pod ---"
	kubectl get pods -n $(NAMESPACE) -l app.kubernetes.io/name=incidentbench-operator
	@echo ""
	@echo "--- Checking operator logs (last 10 lines) ---"
	kubectl logs -n $(NAMESPACE) -l app.kubernetes.io/name=incidentbench-operator --tail=10
	@echo ""
	@echo "--- Checking Strimzi operator ---"
	kubectl get pods -n kafka -l name=strimzi-cluster-operator
	@echo ""
	@echo "--- Applying sample CR in dry-run mode ---"
	kubectl apply -f config/samples/sre-outage-run.yaml --dry-run=server
	@echo ""
	@echo "=== Smoke test passed ==="

# --- Cleanup ---
clean: ## Remove build artifacts and optionally destroy kind cluster
	cargo clean
	rm -f $(CRD_OUTPUT)
	@echo "Build artifacts cleaned."
	@echo "To destroy the kind cluster: kind delete cluster --name $(KIND_CLUSTER)"
