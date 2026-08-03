.PHONY: help fmt fmt-check vet test test-integration build tidy check docker-build docker-test docker-up docker-down docker-logs clean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-14s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

fmt: ## Format Go sources
	go fmt ./...

fmt-check: ## Fail if Go sources are not gofmt-clean
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "gofmt: run 'make fmt' and commit" && exit 1)

vet: ## Run go vet
	go vet ./...

test: ## Run unit tests with race detector and coverage
	go test ./... -race -count=1 -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out

test-integration: ## Run broker integration tests (requires RabbitMQ + Kafka + INTEGRATION=1)
	INTEGRATION=1 go test ./... -race -count=1 -run 'TestIntegration_' -timeout 3m

build: ## Compile all packages
	go build ./...

tidy: ## Sync go.mod / go.sum
	go mod tidy

check: fmt-check vet build test ## fmt-check, vet, build, and unit test (CI)

docker-build: ## Build the test image
	docker compose build test

docker-test: ## Run go test ./... inside Docker (with RabbitMQ)
	docker compose run --rm test

docker-up: ## Start RabbitMQ with host ports (:5672, UI :15672)
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d rabbitmq

docker-down: ## Stop Compose services and remove containers
	docker compose -f docker-compose.yml -f docker-compose.dev.yml down

docker-logs: ## Tail RabbitMQ logs
	docker compose logs -f rabbitmq

clean: ## Remove local caches and Compose leftovers
	go clean -cache -testcache
	rm -f coverage.out
	docker compose -f docker-compose.yml -f docker-compose.dev.yml down --volumes --remove-orphans
