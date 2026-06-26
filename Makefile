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
GOSEC ?= $(LOCALBIN)/gosec
CODEQL ?= $(LOCALBIN)/codeql/codeql
STATICCHECK ?= $(LOCALBIN)/staticcheck
ERRCHECK ?= $(LOCALBIN)/errcheck
REVIVE ?= $(LOCALBIN)/revive

## Setup PATH to point to tools binaries
PATH := $(LOCALBIN):$(PATH)

PACKAGE_NAME ?= github.com/pbinitiative/zenbpm
## Tool Versions
SQLC_VERSION ?= v1.29.0
PROTOC_VERSION ?= 33.4
PROTOC_GEN_GO_VERSION ?= v1.36.5
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
GOLANG_CROSS_VERSION ?= v1.26.4
GOSEC_VERSION ?= v2.27.1
GOSEC_FLAGS ?= -exclude-generated -no-fail
GOSEC_REPORT_DIR ?= gosec-reports
GOSEC_SARIF_REPORT ?= $(GOSEC_REPORT_DIR)/gosec.sarif
GOSEC_HTML_REPORT ?= $(GOSEC_REPORT_DIR)/gosec.html
GOSEC_REPORT_FLAGS = $(filter-out -no-fail,$(GOSEC_FLAGS)) -no-fail
# When true, `make sast` exits non-zero if gosec reports any findings (used by sast-strict).
GOSEC_FAIL_ON_FINDINGS ?= false

STATICCHECK_VERSION ?= v0.7.0
STATICCHECK_REPORT_DIR ?= staticcheck-reports
STATICCHECK_JSON_REPORT ?= $(STATICCHECK_REPORT_DIR)/staticcheck.json
STATICCHECK_SARIF_REPORT ?= $(STATICCHECK_REPORT_DIR)/staticcheck.sarif
STATICCHECK_HTML_REPORT ?= $(STATICCHECK_REPORT_DIR)/staticcheck.html

ERRCHECK_VERSION ?= v1.20.0
ERRCHECK_REPORT_DIR ?= errcheck-reports
ERRCHECK_TEXT_REPORT ?= $(ERRCHECK_REPORT_DIR)/errcheck.txt
ERRCHECK_SARIF_REPORT ?= $(ERRCHECK_REPORT_DIR)/errcheck.sarif
ERRCHECK_HTML_REPORT ?= $(ERRCHECK_REPORT_DIR)/errcheck.html

REVIVE_VERSION ?= v1.15.0
REVIVE_REPORT_DIR ?= revive-reports
REVIVE_JSON_REPORT ?= $(REVIVE_REPORT_DIR)/revive.json
REVIVE_SARIF_REPORT ?= $(REVIVE_REPORT_DIR)/revive.sarif
REVIVE_HTML_REPORT ?= $(REVIVE_REPORT_DIR)/revive.html

GO_TOOL_SARIF_CONVERTER ?= scripts/ci/go_tool_report_to_sarif.py

CODEQL_VERSION ?= v2.25.6
CODEQL_REPORT_DIR ?= codeql-reports
CODEQL_SARIF_REPORT ?= $(CODEQL_REPORT_DIR)/codeql.sarif
CODEQL_HTML_REPORT ?= $(CODEQL_REPORT_DIR)/codeql.html
CODEQL_DB ?= $(CODEQL_REPORT_DIR)/codeql-db
# Map GOOS/GOARCH to CodeQL bundle zip naming.
# codeql-cli-binaries only ships a single macOS asset (codeql-osx64.zip, a
# universal binary for both Intel and Apple Silicon), so darwin always maps to
# osx64 regardless of ARCH.
CODEQL_OS ?= linux64
ifeq ("$(OS)", "darwin")
  CODEQL_OS = osx64
endif
ifeq ("$(OS)", "windows")
  CODEQL_OS = win64
endif

.PHONY: sqlc
sqlc: $(SQLC) ## Download sqlc locally if necessary. If wrong version is installed, it will be overwritten.
$(SQLC): $(LOCALBIN)
	@test -s $(LOCALBIN)/sqlc && $(LOCALBIN)/sqlc version | grep -q $(SQLC_VERSION) || \
	GOBIN=$(LOCALBIN) go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)

.PHONY: protoc-gen-go
protoc-gen-go: $(PROTOC_GEN_GO) ## Download protoc locally if necessary. If wrong version is installed, it will be overwritten.
$(PROTOC_GEN_GO): $(LOCALBIN)
	@test -s $(LOCALBIN)/protoc-gen-go && $(LOCALBIN)/protoc-gen-go --version | grep -q $(PROTOC_GEN_GO_VERSION) || \
	GOBIN=$(LOCALBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

.PHONY: protoc-gen-go-grpc
protoc-gen-go-grpc: $(PROTOC_GEN_GO_GRPC) ## Download protoc locally if necessary. If wrong version is installed, it will be overwritten.
$(PROTOC_GEN_GO_GRPC): $(LOCALBIN)
	@test -s $(LOCALBIN)/protoc-gen-go-grpc && $(LOCALBIN)/protoc-gen-go-grpc --version | grep -q $(PROTOC_GEN_GO_GRPC_VERSION) || \
	GOBIN=$(LOCALBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: gosec
gosec: $(GOSEC) ## Download gosec locally if necessary.
$(GOSEC): $(LOCALBIN)
	@test -s $(LOCALBIN)/gosec && go version -m $(LOCALBIN)/gosec | grep -q $(GOSEC_VERSION) || \
	GOBIN=$(LOCALBIN) go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION)

# Install rules for the static-analysis binaries. The user-facing run targets
# (staticcheck/errcheck/revive below) depend on these, so there is intentionally
# no separate download alias target sharing the same name.
$(STATICCHECK): $(LOCALBIN)
	@test -s $(LOCALBIN)/staticcheck && { [ "$(STATICCHECK_VERSION)" = "latest" ] || go version -m $(LOCALBIN)/staticcheck | grep -q $(STATICCHECK_VERSION); } || \
	GOBIN=$(LOCALBIN) go install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)

$(ERRCHECK): $(LOCALBIN)
	@test -s $(LOCALBIN)/errcheck && { [ "$(ERRCHECK_VERSION)" = "latest" ] || go version -m $(LOCALBIN)/errcheck | grep -q $(ERRCHECK_VERSION); } || \
	GOBIN=$(LOCALBIN) go install github.com/kisielk/errcheck@$(ERRCHECK_VERSION)

$(REVIVE): $(LOCALBIN)
	@test -s $(LOCALBIN)/revive && { [ "$(REVIVE_VERSION)" = "latest" ] || go version -m $(LOCALBIN)/revive | grep -q $(REVIVE_VERSION); } || \
	GOBIN=$(LOCALBIN) go install github.com/mgechev/revive@$(REVIVE_VERSION)

.PHONY: codeql-cli
codeql-cli: $(CODEQL) ## Download CodeQL CLI locally if necessary. If wrong version is installed, it will be overwritten.
$(CODEQL): $(LOCALBIN)
	@if [ ! -s "$@" ] || ! "$@" version --format=terse 2>/dev/null | grep -q "$(CODEQL_VERSION:v%=%)"; then \
		echo "Downloading CodeQL CLI $(CODEQL_VERSION) for $(CODEQL_OS)..."; \
		curl -fsSL "https://github.com/github/codeql-cli-binaries/releases/download/$(CODEQL_VERSION)/codeql-$(CODEQL_OS).zip" \
		  -o /tmp/codeql.zip; \
		rm -rf $(LOCALBIN)/codeql; \
		unzip -q -o /tmp/codeql.zip -d $(LOCALBIN); \
		rm /tmp/codeql.zip; \
	fi

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
	@if [ "$(.SHELLSTATUS)" = "1" ]; then \
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
	@PATH=$(LOCALBIN):$(PATH) go generate ./...
	@$(SQLC) generate
	@cp internal/sql/db.go.template internal/sql/db.go
	@perl -i -ne 'print unless /Foreign\s+interface\{\}\s+`json:"foreign"`/' internal/sql/models.go

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: sast
sast: $(GOSEC) ## Run Go source SAST checks (gosec). Reports are written to gosec-reports/.
	@mkdir -p $(GOSEC_REPORT_DIR)
	$(GOSEC) $(GOSEC_REPORT_FLAGS) -fmt sarif -out $(GOSEC_SARIF_REPORT) ./...
	@python3 scripts/ci/sarif_to_html.py $(GOSEC_SARIF_REPORT) $(GOSEC_HTML_REPORT)
	@if [ "$(GOSEC_FAIL_ON_FINDINGS)" = "true" ]; then \
		python3 -c "import json,sys; d=json.load(open('$(GOSEC_SARIF_REPORT)')); n=sum(len(r.get('results',[])) for r in d.get('runs',[])); print(f'gosec findings: {n}'); sys.exit(1 if n else 0)"; \
	fi

.PHONY: sast-strict
sast-strict: GOSEC_FAIL_ON_FINDINGS := true
sast-strict: sast ## Run Go source SAST checks and fail when findings are present.

.PHONY: staticcheck
staticcheck: $(STATICCHECK) ## Run staticcheck and write JSON, SARIF, and HTML reports.
	@mkdir -p $(STATICCHECK_REPORT_DIR)
	@set +e; \
	$(STATICCHECK) -f=json ./... > $(STATICCHECK_JSON_REPORT) 2>$(STATICCHECK_REPORT_DIR)/staticcheck.stderr; \
	status=$$?; \
	set -e; \
	if [ $$status -ne 0 ] && [ ! -s $(STATICCHECK_JSON_REPORT) ]; then \
		cat $(STATICCHECK_REPORT_DIR)/staticcheck.stderr >&2; \
		exit $$status; \
	fi
	@python3 $(GO_TOOL_SARIF_CONVERTER) staticcheck $(STATICCHECK_JSON_REPORT) $(STATICCHECK_SARIF_REPORT)
	@python3 scripts/ci/sarif_to_html.py $(STATICCHECK_SARIF_REPORT) $(STATICCHECK_HTML_REPORT)

.PHONY: errcheck
errcheck: $(ERRCHECK) ## Run errcheck and write text, SARIF, and HTML reports.
	@mkdir -p $(ERRCHECK_REPORT_DIR)
	@set +e; \
	$(ERRCHECK) -ignoregenerated ./... > $(ERRCHECK_TEXT_REPORT) 2>$(ERRCHECK_REPORT_DIR)/errcheck.stderr; \
	status=$$?; \
	set -e; \
	if [ $$status -ne 0 ] && [ ! -s $(ERRCHECK_TEXT_REPORT) ]; then \
		cat $(ERRCHECK_REPORT_DIR)/errcheck.stderr >&2; \
		exit $$status; \
	fi
	@python3 $(GO_TOOL_SARIF_CONVERTER) errcheck $(ERRCHECK_TEXT_REPORT) $(ERRCHECK_SARIF_REPORT)
	@python3 scripts/ci/sarif_to_html.py $(ERRCHECK_SARIF_REPORT) $(ERRCHECK_HTML_REPORT)

.PHONY: revive
revive: $(REVIVE) ## Run revive and write JSON, SARIF, and HTML reports.
	@mkdir -p $(REVIVE_REPORT_DIR)
	@set +e; \
	$(REVIVE) -formatter json ./... > $(REVIVE_JSON_REPORT) 2>$(REVIVE_REPORT_DIR)/revive.stderr; \
	status=$$?; \
	set -e; \
	if [ $$status -ne 0 ] && [ ! -s $(REVIVE_JSON_REPORT) ]; then \
		cat $(REVIVE_REPORT_DIR)/revive.stderr >&2; \
		exit $$status; \
	fi
	@python3 $(GO_TOOL_SARIF_CONVERTER) revive $(REVIVE_JSON_REPORT) $(REVIVE_SARIF_REPORT)
	@python3 scripts/ci/sarif_to_html.py $(REVIVE_SARIF_REPORT) $(REVIVE_HTML_REPORT)

.PHONY: go-static-analysis
go-static-analysis: staticcheck errcheck revive ## Run staticcheck, errcheck, and revive reports.

.PHONY: codeql
codeql: codeql-cli ## Run CodeQL security analysis locally. Reports are written to codeql-reports/.
	@mkdir -p $(CODEQL_REPORT_DIR)
	$(CODEQL) database create $(CODEQL_DB) --language=go --build-mode=autobuild --overwrite
	$(CODEQL) database analyze $(CODEQL_DB) \
		--download \
		codeql/go-queries:codeql-suites/go-security-extended.qls \
		--format=sarif-latest \
		--output=$(CODEQL_SARIF_REPORT)
	@echo "CodeQL SARIF report written to: $(CODEQL_SARIF_REPORT)"
	python3 scripts/ci/sarif_to_html.py $(CODEQL_SARIF_REPORT) $(CODEQL_HTML_REPORT)
	@echo "CodeQL HTML report written to: $(CODEQL_HTML_REPORT)"

.PHONY: run
run: ## Start this project locally with dev configuration
	export PROFILE=DEV; \
	export CONFIG_FILE=$(CURDIR)/conf/zenbpm/conf-dev.yaml; \
	go run cmd/zenbpm/*.go

.PHONY: run1
run1: ## Start 1st node
	export PROFILE=DEV; \
	export CONFIG_FILE=$(CURDIR)/conf/zenbpm/conf-dev-node1.yaml; \
	go run cmd/zenbpm/*.go

.PHONY: run2
run2: ## Start 2nd node
	export PROFILE=DEV; \
	export CONFIG_FILE=$(CURDIR)/conf/zenbpm/conf-dev-node2.yaml; \
	go run cmd/zenbpm/*.go


.PHONY: start-monitoring
start-monitoring: ## Start monitoring stack
	@docker run -d --rm --add-host=host.docker.internal:host-gateway --name jaeger -p 4318:4318 -p 16686:16686 jaegertracing/jaeger:2.6.0
	@docker run -d --rm --add-host=host.docker.internal:host-gateway --name prometheus -p 9101:9090 -v ./scripts/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
	@docker run -d --rm --add-host=host.docker.internal:host-gateway --name=grafana -p 9100:3000 -v ./scripts/grafana_provisioning:/etc/grafana/provisioning grafana/grafana

.PHONY: stop-monitoring
stop-monitoring:
	@docker stop grafana
	@docker stop prometheus
	@docker stop jaeger

.PHONY: test
test: ## Run tests
	export PROFILE=TEST; \
	export CONFIG_FILE=$(CURDIR)/conf/zenbpm/conf-test.yaml; \
	export LOG_LEVEL=INFO; \
	export POLL_TIMER_DELAY_SECONDS=1; \
	go test ./... -coverprofile cover.out

.PHONY: bench
bench: ## Run benchmarks
	LOG_LEVEL=ERROR go test ./... -bench=.

.PHONY: test-e2e
test-e2e:  ## Run end to end tests (tests will repeat 100 times)
	export PROFILE=TEST; \
	export CONFIG_FILE=$(CURDIR)/conf/zenbpm/conf-test.yaml; \
	export LOG_LEVEL=INFO; \
	export POLL_TIMER_DELAY_SECONDS=1; \
	go test -count=1 -v ./test/e2e/...

.PHONY: test-acceptance
test-acceptance: ## Run acceptance end to end tests
	export PROFILE=TEST; \
	export CONFIG_FILE=$(CURDIR)/conf/zenbpm/conf-test.yaml; \
	export LOG_LEVEL=INFO; \
	export POLL_TIMER_DELAY_SECONDS=1; \
	go test -count=1 -v -tags=acceptance ./test/acceptance_tests/...

.PHONY: test-dmntest
test-dmntest:
	export PROFILE=TEST; \
	export CONFIG_FILE=$(CURDIR)/conf/zenbpm/conf-test.yaml; \
	export LOG_LEVEL=INFO; \
	export POLL_TIMER_DELAY_SECONDS=1; \
	go test -tags=dmntest ./pkg/dmn/dmntest/... -v

##@ Build

.PHONY: build
build: generate ## Build the project
	go build -o zenbpm cmd/zenbpm/main.go

.PHONY: release-dry-run
release-dry-run:
	@docker run \
		--rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/$(PACKAGE_NAME) \
		-w /go/src/$(PACKAGE_NAME) \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--clean --skip=validate --skip=publish

.PHONY: release
release:
	@if [ ! -f ".release-env" ]; then \
		echo "\033[91m.release-env is required for release\033[0m";\
		exit 1;\
	fi
	docker run \
		--rm \
		--env-file .release-env \
		-e BUILDX_BUILDER \
		-e DOCKER_BUILDKIT=1 \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v $(HOME)/.docker:/root/.docker \
		-v `pwd`:/go/src/$(PACKAGE_NAME) \
		-w /go/src/$(PACKAGE_NAME) \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--verbose \
		release --clean
