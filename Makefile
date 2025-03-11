# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

##@ Build Dependencies
## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
SQLC ?= $(LOCALBIN)/sqlc

## Tool Versions
SQLC_VERSION ?= v1.28.0

.PHONY: sqlc
sqlc: $(SQLC) ## Download sqlc locally if necessary. If wrong version is installed, it will be overwritten.
$(SQLC): $(LOCALBIN)
	test -s $(LOCALBIN)/sqlc && $(LOCALBIN)/sqlc version | grep -q $(SQLC_VERSION) || \
	GOBIN=$(LOCALBIN) go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)

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
generate: sqlc ## Run all the generators in the project
	go generate ./...
	$(SQLC) generate
	cp internal/rqlite/sql/db.go.template internal/rqlite/sql/db.go
	sed -i "/Foreign[[:space:]]\+interface{}[[:space:]]\+\`json:\"foreign\"\`/d" internal/rqlite/sql/models.go

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
