export GO111MODULE=on

TEST_PKGS:=./...
APP_EXECUTABLE ?= bin/gridhouse
SHELL := /bin/bash # Use bash syntax
VERSION ?= "dev"
GIT_HASH ?= $(shell git rev-parse HEAD)
BUILD_DATE ?= $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

LDFLAGS += \
-X 'gridhouse/internal/stats.Version=${VERSION}' \
-X 'gridhouse/internal/stats.Commit=${GIT_HASH}' \
-X 'gridhouse/internal/stats.BuildDate=${BUILD_DATE}'

.SILENT:

check-quality: ## runs code quality checks
	make lint
	make fmt
	make vet

build: ## build binaries
	mkdir -p bin/
	CGO_ENABLED=0 GOARCH=amd64 GOOS=darwin go build -ldflags="${LDFLAGS}" -o ${APP_EXECUTABLE}-darwin-amd64 main.go
	CGO_ENABLED=0 GOARCH=arm64 GOOS=darwin go build -ldflags="${LDFLAGS}" -o ${APP_EXECUTABLE}-darwin-arm64 main.go
	CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -ldflags="${LDFLAGS}" -o ${APP_EXECUTABLE}-linux-amd64 main.go
	CGO_ENABLED=0 GOARCH=amd64 GOOS=windows go build -ldflags="${LDFLAGS}" -o ${APP_EXECUTABLE}-windows-amd64 main.go
	@echo "Build passed"

build-current:
	CGO_ENABLED=0 go build -ldflags="${LDFLAGS}" -o ${APP_EXECUTABLE} main.go

lint: ## lint code
	golangci-lint run

vet: ## runs go vet
	go vet ./...

fmt: ## runs go formatter
	go fmt ./...

tidy: ## runs tidy to fix go.mod dependencies
	go mod tidy

test: ## run test code
	make tidy
	go test -v -timeout 10m $(TEST_PKGS) -coverprofile=coverage.out -json > report.json

test-race: ## test race condition
	make tidy
	go test -race $(TEST_PKGS)

bench: ## run go benchmarks
	go test -run=^$ -bench=. -benchmem $(TEST_PKGS)

rbench: ## run redis benchmark (binary must be present)
	redis-benchmark -p 6380 -a bla -q

coverage: ## displays test coverage report in html mode
	make test
	go tool cover -html=coverage.out

clean: ## cleans binary and other generated files
	go clean
	rm -rf out/
	rm -f coverage*.out

.PHONY: all
## All
all: ## runs setup, quality checks and builds
	make check-quality
	make test
	make build

.PHONY: help
## Help
help: ## Show this help.
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) {printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) {printf "  ${CYAN}%s${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)