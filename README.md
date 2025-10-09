---
sidebar_position: 1
---

# ZenBPM - Go BPMN Engine

ZenBPM is a next-generation Business Process Management (BPM) engine written in Go, designed to execute BPMN 2.0 process
definitions.

It provides a lightweight, cloud-ready platform for defining, deploying, and executing business processes with minimal
overhead and maximum flexibility.

## What is BPMN?

Business Process Model and Notation (BPMN) is a graphical representation for specifying business processes in a business process model. It provides businesses with a standard notation that is readily understandable by all business stakeholders.

## Key Features

- **Cloud-Native Architecture**: Designed for cloud environments with containerization and orchestration support
- **Lightweight Design**: Minimal resource footprint with fast startup time and efficient execution
- **BPMN 2.0 Support**: Execute standard BPMN 2.0 process definitions
- **REST API**: Comprehensive REST API for process management
- **gRPC Interface**: High-performance gRPC interface for system integration
- **Distributed Architecture**: Support for clustering and distributed execution
- **Process Elements Support**:
  - Start and End Events
  - Service Tasks
  - User Tasks - handled as Service Tasks
  - Exclusive Gateways
  - Inclusive Gateways
  - Parallel Gateways
  - Event-Based Gateways
  - Intermediate Catch and Throw Events
- **Persistence**: Durable state storage using rqlite
- **Observability**: Integrated with OpenTelemetry for tracing and metrics

## Getting Started

### Installation

```bash
# Clone the repository
git clone git@github.com:pbinitiative/zenbpm.git

# Start application
cd zenbpm
make run

# Run tests
make test
```

### Docker

You can run ZenBPM in a Docker container:

```bash
docker build -t zenbpm .
docker run -d -p 8080:8080 -p 9090:9090 --name zenbpm zenbpm
```

## Architecture

ZenBPM is built with a modular architecture:

- **BPMN Engine**: Core component that executes process definitions, manages process instances, and handles task execution
- **DMN Engine**: Decision Model and Notation (DMN) engine that evaluates business rules and decision tables
- **Storage Layer**: Manages persistence of process state
- **API Layer**: Provides REST and gRPC interfaces
- **Cluster Management**: Coordinates distributed execution

## API Reference

ZenBPM provides both REST and gRPC APIs:

- **REST API**: Documented in OpenAPI format: [specification](/openapi/api.yaml) [documentation](https://pbinitiative.github.io/zenbpm-docusaurus/openapi)
- **gRPC API**: Defined in Protocol Buffers format: [specification](/pkg/client/proto/zenbpm.proto)

## Documentation

ZenBPM documentation can be found at [https://zenbpm.pbinitiative.org](https://zenbpm.pbinitiative.org).
