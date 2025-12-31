.PHONY: help proto build up down logs test clean

help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

proto: ## Generate protobuf code
	@echo "Generating protobuf code..."
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/user.proto
	@echo "Protobuf generation completed!"

deps: ## Download Go dependencies
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy
	@echo "Dependencies downloaded!"

build: ## Build Docker images
	@echo "Building Docker images..."
	docker compose build
	@echo "Build completed!"

up: ## Start all services
	@echo "Starting services..."
	docker compose up -d
	@echo "Services started! Use 'make logs' to view logs"
	@echo "User Service: localhost:50051"
	@echo "Metrics: localhost:9090"
	@echo "Grafana: localhost:3000"

down: ## Stop all services
	@echo "Stopping services..."
	docker compose down
	@echo "Services stopped!"

down-v: ## Stop all services and remove volumes
	@echo "Stopping services and removing volumes..."
	docker compose down -v
	@echo "Services stopped and volumes removed!"

logs: ## View logs for all services
	docker compose logs -f

logs-user: ## View logs for user service
	docker compose logs -f user-service

logs-analytics: ## View logs for analytics service
	docker compose logs -f analytics-service

restart: down up ## Restart all services

ps: ## Show running services
	docker compose ps

test-create: ## Test create user endpoint
	@echo "Creating test user..."
	grpcurl -plaintext -d '{"name":"Test User","email":"test@example.com","city":"NYC","phone":"1234567890","married":true}' localhost:50051 user.UserService/CreateUser

test-list: ## Test list users endpoint
	@echo "Listing users..."
	grpcurl -plaintext -d '{"page":1,"page_size":10}' localhost:50051 user.UserService/ListUsers

test-search: ## Test search users endpoint
	@echo "Searching users..."
	grpcurl -plaintext -d '{"query":"Test","page":1,"page_size":10}' localhost:50051 user.UserService/SearchUsers

health: ## Check service health
	@echo "Checking service health..."
	grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check

benchmark-create: ## Benchmark create user operation
	@echo "Running benchmark for CreateUser..."
	ghz --insecure \
		--proto api/proto/user.proto \
		--call user.UserService/CreateUser \
		-d '{"name":"Bench User","email":"bench{{.RequestNumber}}@example.com","city":"NYC","phone":"1234567890"}' \
		-c 50 \
		-n 1000 \
		localhost:50051

benchmark-get: ## Benchmark get user operation (requires USER_ID)
	@echo "Running benchmark for GetUser..."
	@echo "Please set USER_ID environment variable"
	ghz --insecure \
		--proto api/proto/user.proto \
		--call user.UserService/GetUser \
		-d '{"id":"$(USER_ID)"}' \
		-c 100 \
		-n 5000 \
		localhost:50051

mongo-shell: ## Connect to MongoDB shell
	docker exec -it user_mongodb mongosh -u admin -p admin123 userdb

redis-cli: ## Connect to Redis CLI
	docker exec -it user_redis redis-cli

kafka-topics: ## List Kafka topics
	docker exec user_kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-consume: ## Consume messages from Kafka
	docker exec user_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic user-events --from-beginning

metrics: ## View Prometheus metrics
	@echo "Opening Prometheus metrics..."
	@echo "Metrics available at: http://localhost:9090/metrics"
	curl -s http://localhost:9090/metrics | grep user_

clean: ## Clean up build artifacts and volumes
	@echo "Cleaning up..."
	docker-compose down -v --rmi all
	rm -rf ./pkg/pb/*.pb.go
	go clean
	@echo "Cleanup completed!"

lint: ## Run golangci-lint
	@echo "Running linter..."
	golangci-lint run ./...

format: ## Format Go code
	@echo "Formatting code..."
	go fmt ./...
	goimports -w .

security-scan: ## Run security scan
	@echo "Running security scan..."
	gosec ./...

docker-prune: ## Remove unused Docker resources
	@echo "Pruning Docker resources..."
	docker system prune -af

init: proto deps ## Initialize project (generate proto + download deps)
	@echo "Project initialized!"

run-local-user: ## Run user service locally (requires MongoDB, Redis, Kafka)
	@echo "Starting user service locally..."
	go run cmd/user-service/main.go

run-local-analytics: ## Run analytics service locally
	@echo "Starting analytics service locally..."
	go run cmd/analytics-service/main.go

# Development helpers
dev-up: ## Start infrastructure only (MongoDB, Redis, Kafka)
	docker-compose up -d mongodb redis zookeeper kafka
	@echo "Infrastructure started. Services can be run locally."

dev-down: ## Stop infrastructure
	docker-compose stop mongodb redis zookeeper kafka

# Testing
unit-test: ## Run unit tests
	@echo "Running unit tests..."
	go test -v -race -coverprofile=coverage.out ./...

integration-test: ## Run integration tests
	@echo "Running integration tests..."
	go test -v -tags=integration ./...

coverage: unit-test ## Generate test coverage report
	@echo "Generating coverage report..."
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Database operations
db-migrate-up: ## Run database migrations
	@echo "Running migrations..."
	docker exec -it user_mongodb mongosh -u admin -p admin123 userdb < scripts/init-mongo.js

db-seed: ## Seed database with test data
	@echo "Seeding database..."
	# Add your seed script here

db-backup: ## Backup MongoDB database
	@echo "Creating backup..."
	docker exec user_mongodb mongodump --uri="mongodb://admin:admin123@localhost:27017/userdb" --out=/tmp/backup
	docker cp user_mongodb:/tmp/backup ./backups/

# Deployment
deploy-k8s: ## Deploy to Kubernetes
	@echo "Deploying to Kubernetes..."
	kubectl apply -f deployments/kubernetes/

# Documentation
docs: ## Generate documentation
	@echo "Generating documentation..."
	godoc -http=:6060
	@echo "Documentation available at http://localhost:6060"

.DEFAULT_GOAL := help