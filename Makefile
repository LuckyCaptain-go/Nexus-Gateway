.PHONY: build run test clean docker-build docker-run

# Build the application
build:
	go build -o bin/nexus-gateway cmd/server/main.go

# Run the application
run:
	go run cmd/server/main.go

# Run tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out coverage.html

# Build Docker image
docker-build:
	docker build -t nexus-gateway:latest .

# Run Docker container
docker-run:
	docker run -p 8080:8080 nexus-gateway:latest

# Run Docker with config volume
docker-run-with-config:
	docker run -p 8080:8080 -v $(PWD)/configs:/root/configs nexus-gateway:latest

# Format code
fmt:
	go fmt ./...

# Vet code
vet:
	go vet ./...

# Download dependencies
deps:
	go mod download
	go mod tidy

# Generate API docs
docs:
	swag init -g cmd/server/main.go -o docs/

# Install dev tools
install-tools:
	go install github.com/swaggo/swag/cmd/swag@latest

# Default target
all: fmt vet test build