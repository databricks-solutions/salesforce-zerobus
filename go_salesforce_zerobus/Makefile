# go_salesforce_zerobus: pure-Go, multi-tenant Salesforce CDC -> Databricks Zerobus.

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE   ?= salesforce-zerobus:$(VERSION)
PROFILE ?=
TARGET  ?= dev
SP      ?= $(DATABRICKS_CLIENT_ID)# service principal application ID the service runs as
CATALOG ?= main# catalog for the tenant secrets schema
BIN      = $(CURDIR)/bin
PG_PORT ?= 55432
PG_DIR  ?= /tmp/sfzb-test-pg
MODULE   = github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus

.PHONY: all build build-linux proto test test-race test-pg test-integration lint docker load demo \
        validate bundle-validate bundle-deploy bootstrap lakebase-env clean

all: lint test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN)/sfzb ./cmd/zerobus
	go build -o $(BIN)/sfzb-loadgen ./cmd/sfzb-loadgen

# Static Linux binary (no CGO, no Rust library): cross-compiles from any OS.
build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN)/sfzb-linux-amd64 ./cmd/zerobus
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN)/sfzb-linux-arm64 ./cmd/zerobus

$(BIN)/protoc-gen-go:
	GOBIN=$(BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
$(BIN)/protoc-gen-go-grpc:
	GOBIN=$(BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.0

proto: $(BIN)/protoc-gen-go $(BIN)/protoc-gen-go-grpc
	PATH=$(BIN):$$PATH protoc -I proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		proto/pubsub_api.proto proto/salesforce_events.proto

test:
	go test ./...

test-race:
	go test -race ./...

# Runs the registry and checkpoint SQL tests against a throwaway local
# Postgres (needs initdb/pg_ctl on PATH).
test-pg:
	rm -rf $(PG_DIR) && initdb -D $(PG_DIR) -U postgres --auth=trust >/dev/null
	pg_ctl -D $(PG_DIR) -o "-p $(PG_PORT) -k /tmp -c listen_addresses=127.0.0.1" -l $(PG_DIR).log -w start >/dev/null
	SFZB_TEST_PG_DSN="host=127.0.0.1 port=$(PG_PORT) user=postgres dbname=postgres sslmode=disable" \
		go test -race ./internal/registry/ ./internal/checkpoint/... ; \
		status=$$?; pg_ctl -D $(PG_DIR) -m fast stop >/dev/null; exit $$status

# Against real Lakebase (needs SFZB_LAKEBASE_* and DATABRICKS_* in the env).
test-integration:
	go run ./cmd/zerobus migrate
	go run ./cmd/zerobus validate

lint:
	@test -z "$$(gofmt -l ./cmd ./internal)" || (echo "gofmt needed:"; gofmt -l ./cmd ./internal; exit 1)
	go vet ./...

docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

# In-process capacity run against simulated orgs (memory sink).
load:
	go run ./cmd/sfzb-loadgen -orgs 2000 -rate 2000 -duration 2m

# Fake Salesforce for N orgs; prints the env to run `bin/sfzb` against it.
demo: build
	$(BIN)/sfzb-loadgen -orgs 20 -rate 5 -serve -tenants-out /tmp/sfzb-demo-tenants.yaml

validate:
	go run ./cmd/zerobus validate

BUNDLE_ARGS = -t $(TARGET) $(if $(PROFILE),--profile $(PROFILE)) --var service_principal_app_id=$(SP) --var catalog=$(CATALOG)

bundle-validate:
	databricks bundle validate $(BUNDLE_ARGS)

bundle-deploy:
	databricks bundle deploy $(BUNDLE_ARGS)

SECRETS_SCHEMA ?= salesforce_zerobus_secrets

# Re-runs the bundle's postdeploy grants (Lakebase role; UC secret access).
bootstrap:
	go run ./cmd/zerobus bootstrap lakebase -sp $(SP) -endpoint "$$(make -s lakebase-env | cut -d= -f2)" $(if $(PROFILE),-profile $(PROFILE))
	go run ./cmd/zerobus bootstrap uc-secrets -sp $(SP) -schema $(CATALOG).$(SECRETS_SCHEMA) $(if $(PROFILE),-profile $(PROFILE))

# Prints the Lakebase settings for the service environment.
lakebase-env:
	@databricks bundle summary $(BUNDLE_ARGS) -o json | \
		python3 -c 'import json,sys; ep=json.load(sys.stdin)["resources"]["postgres_endpoints"]["primary"]; print("SFZB_LAKEBASE_ENDPOINT=" + ep["id"])'

clean:
	rm -rf $(BIN) coverage.out
