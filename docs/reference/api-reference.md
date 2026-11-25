---
sidebar_position: 100
---

# API Overview

ZenBPM provides comprehensive APIs for interacting with the BPMN engine. This section documents the available APIs and how to use them.

## Available APIs

ZenBPM offers two primary API interfaces:

- **REST API**: HTTP-based API for web applications and general integration
- **gRPC API**: High-performance RPC framework for system-to-system communication

## REST API

The REST API follows standard RESTful principles

- See the [OpenAPI specification](pathname:///redocusaurus/api.yaml)
- See the [OpenApi documentation](/openapi-api)

> ⚠️ **Upgrade note:** releases up to and including 1.10.0 ignored `sortBy=createdAt` on
> `GET /v1/jobs`, `sortBy=name` on `GET /v1/process-definitions` and `sortBy=incidentCount` on
> `GET /v1/process-definitions/statistics`, and listed the newest key first. These sorts now apply,
> ascending unless `sortOrder` says otherwise. A client which sends one of them without `sortOrder`
> therefore gets the oldest job, the first name or the fewest instances with an incident first; send
> `sortOrder=desc` for the opposite.

## gRPC API

The gRPC API provides similar functionality to the REST API but with the performance benefits of gRPC. It's defined using Protocol Buffers.

- See the [Protocol Buffer definitions ](pathname:///zenbpm.proto)
