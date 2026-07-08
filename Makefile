# silo — distributed storage system
# `make help` lists available targets. `make` (no args) brings up a local cluster.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

BIN := bin
COMPOSE := docker compose -f deploy/docker-compose.yml
COMPOSE_LOCAL := docker compose -f deploy/docker-compose-local.yml

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

GO_TEST_FLAGS ?= -race -timeout 60s
GO_PKGS := ./...

# Default to an execs budget (Nx) so `make fuzz` stops deterministically and
# exits cleanly; override with a duration for a longer run, e.g.
# `make fuzz FUZZTIME=60s` per target.
FUZZTIME ?= 1000000x

.PHONY: help up up-local down restart build images test test-cover test-integration test-nbd-kernel bench bench-cluster fuzz lint fmt vet clean clean-creds logs status check proto nbd-demo nbd-demo-vm
.DEFAULT_GOAL := up

help: ## Show this help and exit.
	@printf "silo — make targets\n\n"
	@awk -F ':[^#]*## ' '/^[a-zA-Z_-]+:[^#]*## / { printf "  \033[1m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf "\nFirst run? Just type 'make'. It generates a dev key and boots a 3-node cluster.\n"

# --- Local cluster -----------------------------------------------------------

up: deploy/.env ## Build and start a 3-node silo cluster locally; print a ready-to-paste 'siloctl auth init'.
	@SILO_PRINT_BOOTSTRAP_TOKEN=1 $(COMPOSE) up -d --build
	@printf "\nWaiting for silo-a to report healthy…\n"
	@for _ in $$(seq 1 30); do \
	  if curl -sf --max-time 2 http://localhost:7080/healthz >/dev/null 2>&1; then break; fi; \
	  sleep 1; \
	done
	@token=$$($(COMPOSE) logs --no-log-prefix silo-a 2>/dev/null | awk '$$1=="token:"{t=$$2} END{print t}'); \
	 fp=$$($(COMPOSE) logs --no-log-prefix silo-a 2>/dev/null | awk '$$1=="server" && $$2=="fingerprint:"{f=$$3} END{print f}'); \
	 if [ -n "$$token" ] && [ -n "$$fp" ]; then \
	   printf "\nClaim operator credentials (single-use token from silo-a):\n\n"; \
	   printf "  ./bin/siloctl auth init \\\\\n"; \
	   printf "    --token %s \\\\\n" "$$token"; \
	   printf "    --server 127.0.0.1:7001 \\\\\n"; \
	   printf "    --server-fingerprint %s\n" "$$fp"; \
	 else \
	   printf "\nCould not find a bootstrap token in silo-a's recent logs. If silo-a didn't restart on this 'make up', try:\n  %s restart silo-a\nthen re-run 'make up'.\n" "$(COMPOSE)"; \
	 fi
	@printf "\nUseful next steps:\n"
	@printf "  make status     check /healthz on every node\n"
	@printf "  make logs       follow logs across the cluster\n"
	@printf "  make down       stop everything (volumes + creds removed)\n\n"
	@printf "Endpoints:\n"
	@printf "  http://localhost:7080/healthz   silo-a\n"
	@printf "  http://localhost:7081/healthz   silo-b\n"
	@printf "  http://localhost:7082/healthz   silo-c\n"
	@printf "  http://localhost:9090           prometheus\n"
	@printf "  http://localhost:%s           grafana (anonymous viewer; login admin/admin)\n" "$${SILO_GRAFANA_PORT:-3030}"

up-local: deploy/.env ## Run a single silod in the foreground (fast dev loop).
	@$(COMPOSE_LOCAL) up --build

down: ## Stop the cluster, remove its volumes, and wipe local artifacts that are stale once the cluster CA is gone.
	@$(COMPOSE) down -v
	@if [ -x "$(BIN)/siloctl" ]; then \
	  $(BIN)/siloctl auth clean --yes >/dev/null 2>&1 || true; \
	  printf "Wiped cached operator credentials (the cluster CA is gone).\n"; \
	else \
	  printf "(skipping credential wipe: %s not built; run 'make clean-creds' yourself if needed)\n" "$(BIN)/siloctl"; \
	fi
	@if [ -f deploy/.env ]; then \
	  rm -f deploy/.env; \
	  printf "Removed deploy/.env (the SILO_ENCRYPTION_KEY it held has no data left to unwrap).\n"; \
	fi

clean-creds: ## Delete cached operator credentials (prompts; pass YES=1 to skip).
	@if [ ! -x "$(BIN)/siloctl" ]; then \
	  printf "siloctl not built; run 'make build' first\n" >&2; exit 1; \
	fi
	@if [ "$$YES" = "1" ]; then $(BIN)/siloctl auth clean --yes; else $(BIN)/siloctl auth clean; fi

restart: down up ## Stop and restart the cluster.

status: ## Probe /healthz on every node and report.
	@printf "Probing cluster…\n"
	@for port in 7080 7081 7082; do \
	  body=$$(curl -sf --max-time 2 http://localhost:$$port/healthz 2>/dev/null || true); \
	  if [ -n "$$body" ]; then printf "  OK  port %s — %s\n" "$$port" "$$body"; \
	  else printf "  --  port %s — not ready yet (try again in a few seconds)\n" "$$port"; fi; \
	done

logs: ## Follow logs from all silo nodes.
	@$(COMPOSE) logs -f silo-a silo-b silo-c

nbd-demo: deploy/.env ## Mkfs+mount a volume over NBD end to end (LINUX host with the nbd kernel module; not the macOS Docker VM).
	@SILO_PRINT_BOOTSTRAP_TOKEN=1 $(COMPOSE) up -d --build silo-a silo-b silo-c
	@printf "Waiting for silo-a to report healthy…\n"
	@for _ in $$(seq 1 30); do \
	  if curl -sf --max-time 2 http://localhost:7080/healthz >/dev/null 2>&1; then break; fi; \
	  sleep 1; \
	done
	@token=$$($(COMPOSE) logs --no-log-prefix silo-a 2>/dev/null | awk '$$1=="token:"{print $$2; exit}'); \
	 fp=$$($(COMPOSE) logs --no-log-prefix silo-a 2>/dev/null | awk '$$1=="server" && $$2=="fingerprint:"{print $$3; exit}'); \
	 if [ -z "$$token" ] || [ -z "$$fp" ]; then \
	   printf "Could not read the bootstrap token/fingerprint from silo-a's logs.\nThe token is printed once on first boot, so this needs a fresh cluster:\n  make down && make nbd-demo\n" >&2; \
	   exit 1; \
	 fi; \
	 printf "Claiming credentials with token %s… (fingerprint %s)\n" "$$(printf %s "$$token" | cut -c1-8)" "$$fp"; \
	 SILO_DEMO_TOKEN=$$token SILO_DEMO_FP=$$fp $(COMPOSE) run --rm --build nbd-demo

nbd-demo-vm: deploy/.env ## Attach a volume to a throwaway QEMU guest over NBD and mkfs+mount it (macOS-friendly; needs qemu + docker, no nbd kernel module).
	@SILO_PRINT_BOOTSTRAP_TOKEN=1 $(COMPOSE) up -d --build silo-a silo-b silo-c
	@printf "Waiting for silo-a to report healthy…\n"
	@for _ in $$(seq 1 30); do \
	  if curl -sf --max-time 2 http://localhost:7080/healthz >/dev/null 2>&1; then break; fi; \
	  sleep 1; \
	done
	@token=$$($(COMPOSE) logs --no-log-prefix silo-a 2>/dev/null | awk '$$1=="token:"{print $$2; exit}'); \
	 fp=$$($(COMPOSE) logs --no-log-prefix silo-a 2>/dev/null | awk '$$1=="server" && $$2=="fingerprint:"{print $$3; exit}'); \
	 if [ -z "$$token" ] || [ -z "$$fp" ]; then \
	   printf "Could not read the bootstrap token/fingerprint from silo-a's logs.\nThe token is printed once on first boot, so this needs a fresh cluster:\n  make down && make nbd-demo-vm\n" >&2; \
	   exit 1; \
	 fi; \
	 SILO_DEMO_TOKEN=$$token SILO_DEMO_FP=$$fp \
	   SILO_GRPC_ADDR=127.0.0.1:$${SILO_GRPC_HOST_PORT:-7900} \
	   SILO_NBD_HOST_PORT=$${SILO_NBD_HOST_PORT:-10809} \
	   deploy/nbd-demo/vm/run-vm.sh

deploy/.env:
	@printf "# Generated by 'make up' — never commit (see .gitignore).\n# To rotate, delete this file and re-run 'make up'.\nSILO_ENCRYPTION_KEY=%s\nSILO_LOG_LEVEL=info\nSILO_LOG_FORMAT=text\n" "$$(openssl rand -base64 32 | tr -d '\n')" > $@
	@printf "Generated deploy/.env with a fresh development encryption key.\n"

# --- Build & test ------------------------------------------------------------

build: ## Build all Go binaries into bin/.
	@mkdir -p $(BIN)
	@CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN)/silod ./cmd/silod
	@CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN)/siloctl ./cmd/siloctl
	@CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN)/silo-csi ./cmd/silo-csi
	@CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN)/silo-fuse ./cmd/silo-fuse
	@printf "Built %s/{silod,siloctl,silo-csi,silo-fuse} (%s)\n" "$(BIN)" "$(VERSION)"

IMAGE_REGISTRY ?= silo
IMAGE_TAG ?= $(VERSION)

images: ## Build the silod and silo-csi container images (override IMAGE_REGISTRY/IMAGE_TAG).
	@docker build --build-arg VERSION=$(VERSION) -t $(IMAGE_REGISTRY)/silod:$(IMAGE_TAG) -f Dockerfile .
	@docker build --build-arg VERSION=$(VERSION) -t $(IMAGE_REGISTRY)/silo-csi:$(IMAGE_TAG) -f Dockerfile.csi .
	@printf "Built %s/silod:%s and %s/silo-csi:%s\n" "$(IMAGE_REGISTRY)" "$(IMAGE_TAG)" "$(IMAGE_REGISTRY)" "$(IMAGE_TAG)"

test: ## Run unit tests.
	@go test $(GO_TEST_FLAGS) $(GO_PKGS)

test-cover: ## Run unit tests and print coverage (excludes generated code).
	@go test $(GO_TEST_FLAGS) -coverprofile=coverage.out -coverpkg=./internal/...,./cmd/...,./pkg/... $(GO_PKGS)
	@printf "\nOverall coverage: "
	@go tool cover -func=coverage.out | awk '/^total:/ {print $$3}'
	@printf "HTML report: \`go tool cover -html=coverage.out\`\n"

test-integration: ## Run integration tests (require docker, build-tag 'integration').
	@go test $(GO_TEST_FLAGS) -tags=integration ./test/integration/...

test-nbd-kernel: ## Run the NBD attach/reconnect test against a real kernel (privileged docker; needs /dev/nbd*).
	@docker run --rm --privileged --network host \
	  -v "$(CURDIR)":/src -w /src \
	  -v silo-gomod:/go/pkg/mod -v silo-gocache:/root/.cache/go-build \
	  golang:1.25-alpine \
	  go test -tags integration -count=1 -run TestKernel -v ./internal/nbdclient/

bench: ## Run the data-plane benchmarks (crypto, chunk store, placement, peers, volume).
	@go test -run '^$$' -bench=. -benchmem ./internal/crypto/... ./internal/chunkstore/... ./internal/placement/... ./internal/replication/... ./internal/volume/...

bench-cluster: ## Run end-to-end cluster benchmarks (spawns 3 real silod processes; build-tag 'integration').
	@go test -run='^$$' -bench='^BenchmarkCluster' -benchmem -timeout=10m -tags=integration ./test/integration/...

fuzz: ## Fuzz the parse/merge/placement boundaries on demand (override: make fuzz FUZZTIME=60s).
	@go test -run='^$$' -fuzz='^FuzzReadMessage$$'  -fuzztime=$(FUZZTIME) ./internal/gossip/
	@go test -run='^$$' -fuzz='^FuzzDecryptChunk$$' -fuzztime=$(FUZZTIME) ./internal/crypto/
	@go test -run='^$$' -fuzz='^FuzzValidateID$$'   -fuzztime=$(FUZZTIME) ./internal/chunkstore/
	@go test -run='^$$' -fuzz='^FuzzMerge$$'        -fuzztime=$(FUZZTIME) ./internal/membership/
	@go test -run='^$$' -fuzz='^FuzzReplicas$$'     -fuzztime=$(FUZZTIME) ./internal/placement/
	@go test -run='^$$' -fuzz='^FuzzLoadCA$$'       -fuzztime=$(FUZZTIME) ./internal/clustertls/
	@go test -run='^$$' -fuzz='^FuzzClockMonotonic$$' -fuzztime=$(FUZZTIME) ./internal/hlc/
	@go test -run='^$$' -fuzz='^FuzzORSetConverges$$' -fuzztime=$(FUZZTIME) ./internal/crdt/
	@go test -run='^$$' -fuzz='^FuzzLWWMapConverges$$' -fuzztime=$(FUZZTIME) ./internal/crdt/
	@go test -run='^$$' -fuzz='^FuzzNamespaceConverges$$' -fuzztime=$(FUZZTIME) ./internal/namespace/
	@go test -run='^$$' -fuzz='^FuzzNamespaceMergeBytes$$' -fuzztime=$(FUZZTIME) ./internal/namespace/
	@go test -run='^$$' -fuzz='^FuzzNamespacePaths$$' -fuzztime=$(FUZZTIME) ./internal/namespace/
	@go test -run='^$$' -fuzz='^FuzzStoreOpen$$' -fuzztime=$(FUZZTIME) ./internal/bootstraptoken/
	@go test -run='^$$' -fuzz='^FuzzRedeem$$' -fuzztime=$(FUZZTIME) ./internal/bootstraptoken/
	@go test -run='^$$' -fuzz='^FuzzConfigLoad$$' -fuzztime=$(FUZZTIME) ./internal/config/
	@go test -run='^$$' -fuzz='^FuzzLoadAuthConfig$$' -fuzztime=$(FUZZTIME) ./cmd/siloctl/
	@go test -run='^$$' -fuzz='^FuzzChunkIDAlwaysValid$$' -fuzztime=$(FUZZTIME) ./internal/writer/

proto: ## Regenerate protobuf and gRPC code. Requires Docker; uses bufbuild/buf.
	@if ! docker info >/dev/null 2>&1; then \
	  printf "Docker is not running; start Docker Desktop or the docker daemon first.\nProto regeneration uses the bufbuild/buf image so you do not need protoc installed locally.\n" >&2; \
	  exit 1; \
	fi
	@docker run --rm \
	  -v "$(PWD):/work" \
	  -w /work \
	  --user "$$(id -u):$$(id -g)" \
	  -e HOME=/tmp \
	  bufbuild/buf:latest generate
	@printf "Regenerated *.pb.go under api/proto/. Commit the result.\n"

lint: ## Run golangci-lint (install if missing: https://golangci-lint.run/usage/install/).
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
	  printf "golangci-lint is not installed.\nInstall it with:\n  brew install golangci-lint     (macOS)\n  https://golangci-lint.run/usage/install/  (everywhere else)\n" >&2; \
	  exit 1; \
	fi
	@golangci-lint run

fmt: ## Run gofmt on the entire repo.
	@gofmt -s -w .

vet: ## Run go vet.
	@go vet $(GO_PKGS)

check: fmt vet lint test ## Run formatters, linters, and tests.

# --- Housekeeping ------------------------------------------------------------

clean: ## Remove build artifacts and the generated .env.
	@rm -rf $(BIN) coverage.out
	@rm -f deploy/.env
	@printf "Removed bin/, coverage.out, deploy/.env\n"
