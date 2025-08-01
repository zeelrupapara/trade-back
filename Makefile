# High-Performance Trading Backend Makefile

.PHONY: help build run test clean docker-up docker-down docker-logs deps lint format

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Development commands
deps: ## Install dependencies
	go mod download
	go mod tidy

build: ## Build the application
	go build -o bin/trade-back cmd/main.go

run: ## Run the application locally
	go run cmd/main.go server

test: ## Run tests
	go test -v ./...

test-coverage: ## Run tests with coverage
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

benchmark: ## Run benchmarks
	go test -bench=. -benchmem ./tests/performance/

lint: ## Run linter
	golangci-lint run

format: ## Format code
	go fmt ./...
	goimports -w .

clean: ## Clean build artifacts
	rm -rf bin/
	rm -f coverage.out coverage.html

# Docker commands
docker-build: ## Build Docker image
	docker build -t trade-back:latest .

docker-up: ## Start all services with Docker Compose
	docker-compose up -d

docker-down: ## Stop all services
	docker-compose down

docker-logs: ## Show Docker logs
	docker-compose logs -f

docker-restart: ## Restart services
	docker-compose restart

docker-clean: ## Clean Docker containers and volumes
	docker-compose down -v
	docker system prune -f

# Database commands
db-init: ## Initialize databases
	@echo "Waiting for MySQL to be ready..."
	@while ! docker-compose exec mysql mysqladmin ping -h"localhost" --silent; do sleep 1; done
	@echo "MySQL is ready!"
	@echo "Creating InfluxDB retention policies..."
	docker-compose exec influxdb influx setup --skip-verify \
		--bucket trading \
		--org trading-org \
		--password admin123 \
		--username admin \
		--token my-super-secret-auth-token \
		--force || true

db-migrate: ## Run database migrations
	@echo "Running database migrations..."
	go run cmd/main.go migrate up


# Testing commands
test-unit: ## Run unit tests
	go test -v ./tests/unit/...

test-integration: ## Run integration tests (requires Docker)
	docker-compose up -d
	sleep 10
	go test -v ./tests/integration/...
	docker-compose down

test-performance: ## Run performance tests
	go test -v -timeout=5m ./tests/performance/...

# Production commands
deploy-staging: ## Deploy to staging
	@echo "Deploying to staging..."
	docker-compose -f docker-compose.staging.yml up -d

deploy-prod: ## Deploy to production
	@echo "Deploying to production..."
	# Add production deployment commands here

# Monitoring commands
logs: ## Show application logs
	docker-compose logs -f app

monitor: ## Start monitoring stack
	docker-compose --profile monitoring up -d

health-check: ## Check service health
	@echo "Checking service health..."
	@curl -f http://localhost:8080/health || echo "Service is down"
	@curl -f http://localhost:8086/health || echo "InfluxDB is down"
	@curl -f http://localhost:8222/varz || echo "NATS is down"

# Development helpers
dev-setup: deps docker-up db-init db-migrate ## Complete development setup
	@echo "Development environment ready!"
	@echo "Services available at:"
	@echo "  - Trading API: http://localhost:8080"
	@echo "  - InfluxDB: http://localhost:8086"
	@echo "  - MySQL: localhost:3306"
	@echo "  - Redis: localhost:6379"
	@echo "  - NATS: localhost:4222"
	@echo ""
	@echo "CLI Commands available:"
	@echo "  - make run                 # Start the server"
	@echo "  - make db-migrate         # Run migrations"
	@echo "  - go run cmd/main.go --help # Show all commands"

dev-reset: docker-clean docker-up db-init ## Reset development environment
	@echo "Development environment reset complete!"

# Code quality
security-scan: ## Run security scan
	gosec ./...

mod-update: ## Update Go modules
	go get -u ./...
	go mod tidy


# Quick commands for daily use
quick-test: ## Quick test (unit tests only)
	go test -short ./...

quick-build: ## Quick build without optimization
	go build -o bin/server cmd/server/main.go

watch: ## Watch files and restart server
	@which air > /dev/null || go install github.com/cosmtrek/air@latest
	air