# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

OS ?= $(shell go env GOOS)
ARCH ?= $(shell go env GOARCH)

##@ Build Dependencies
## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
SQLC ?= $(LOCALBIN)/sqlc
PROTOC ?= $(LOCALBIN)/protoc
PROTOC_GEN_GO ?= $(LOCALBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC ?= $(LOCALBIN)/protoc-gen-go-grpc

## Tool Versions
SQLC_VERSION ?= v1.28.0
PROTOC_VERSION ?= 30.0
PROTOC_GEN_GO_VERSION ?= v1.36.5
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1

.PHONY: sqlc
sqlc: $(SQLC) ## Download sqlc locally if necessary. If wrong version is installed, it will be overwritten.
$(SQLC): $(LOCALBIN)
	test -s $(LOCALBIN)/sqlc && $(LOCALBIN)/sqlc version | grep -q $(SQLC_VERSION) || \
	GOBIN=$(LOCALBIN) go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)

.PHONY: protoc-gen-go
protoc-gen-go: $(PROTOC_GEN_GO) ## Download protoc locally if necessary. If wrong version is installed, it will be overwritten.
$(PROTOC_GEN_GO): $(LOCALBIN)
	test -s $(LOCALBIN)/protoc-gen-go && $(LOCALBIN)/protoc-gen-go --version | grep -q $(PROTOC_GEN_GO_VERSION) || \
	GOBIN=$(LOCALBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

.PHONY: protoc-gen-go-grpc
protoc-gen-go-grpc: $(PROTOC_GEN_GO_GRPC) ## Download protoc locally if necessary. If wrong version is installed, it will be overwritten.
$(PROTOC_GEN_GO_GRPC): $(LOCALBIN)
	test -s $(LOCALBIN)/protoc-gen-go-grpc && $(LOCALBIN)/protoc-gen-go-grpc --version | grep -q $(PROTOC_GEN_GO_GRPC_VERSION) || \
	GOBIN=$(LOCALBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

PROTOC_OS:=$(OS)
PROTOC_ARCH:=-$(ARCH)
ifeq ("$(ARCH)", "amd64")
	PROTOC_ARCH=-x86_64
endif
ifeq ("$(OS)", "darwin")
	PROTOC_OS:=osx
else ifeq ("$(OS)", "windows")
	PROTOC_OS:=win64
	PROTOC_ARCH:=
endif

.PHONY: protoc
protoc: $(PROTOC) ## Download protoc locally if necessary. If wrong version is installed, it will be overwritten.
$(PROTOC): $(LOCALBIN)
	$(shell test -s $(LOCALBIN)/protoc && $(LOCALBIN)/protoc --version | grep -q $(PROTOC_VERSION);)
	if [ "$(.SHELLSTATUS)" = "1" ]; then \
		curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PROTOC_OS)$(PROTOC_ARCH).zip; \
		unzip -p protoc-$(PROTOC_VERSION)-$(PROTOC_OS)$(PROTOC_ARCH).zip bin/protoc >$(LOCALBIN)/protoc; \
		chmod +x $(LOCALBIN)/protoc; \
		rm protoc-$(PROTOC_VERSION)-$(PROTOC_OS)$(PROTOC_ARCH).zip; \
	fi
	

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: generate
generate: sqlc protoc protoc-gen-go protoc-gen-go-grpc ## Run all the generators in the project
	@go generate ./...
	@$(SQLC) generate
	@cp internal/rqlite/sql/db.go.template internal/rqlite/sql/db.go
	@sed -i "/Foreign[[:space:]]\+interface{}[[:space:]]\+\`json:\"foreign\"\`/d" internal/rqlite/sql/models.go

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: run
run: ## Start this project locally with dev configuration
	export PROFILE=DEV; \
	export CONFIG_FILE=$(CURDIR)/conf/zenbpm/conf-dev.yaml; \
	go run cmd/zenbpm/*.go

.PHONY: test
test: ## Run tests
	LOG_LEVEL=INFO go test ./...

.PHONY: bench
bench: ## Run benchmarks
	LOG_LEVEL=ERROR go test ./... -bench=.

##@ Build

.PHONY: build
build: generate ## Build the project
	go build -o zenbpm cmd/zenbpm/main.go
