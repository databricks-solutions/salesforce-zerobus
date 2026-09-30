.PHONY: proto build vet clean

MODULE := github.com/databricks-solutions/go-salesforce-zerobus

proto:
	protoc \
		--go_out=proto/gen --go_opt=paths=source_relative \
		--go-grpc_out=proto/gen --go-grpc_opt=paths=source_relative \
		-I proto/ \
		proto/pubsub_api.proto proto/salesforce_events.proto

build:
	go build -o salesforce-zerobus ./cmd/zerobus

vet:
	go vet ./...

clean:
	rm -f salesforce-zerobus
	rm -f proto/gen/*.go

test:
	go test ./...
