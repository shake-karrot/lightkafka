.PHONY: unit-test test run

unit-test:
	@echo "Running unit tests..."
	@go test ./... -v

test: unit-test

run:
	@echo "Starting Kafka Engine..."
	@go run cmd/main.go

