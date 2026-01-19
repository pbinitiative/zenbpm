---
sidebar_position: 110
---
# Client Libraries

ZenBPM provides officially supported client libraries in multiple programming languages to help developers integrate with the engine more easily.

The versions of the libraries are aligned with the ZenBPM engine versions.

## Go Client

The Go client is part of the ZenBPM engine and is available as package `github.com/pbinitiative/zenbpm/pkg/zenclient`.

The `zenclient` package provides two clients:
- REST (HTTP) client for managing process/decision resources, starting instances, etc.
- gRPC worker client for subscribing to job types and completing/failing jobs via a bidirectional stream.

### Usage examples
#### Deploy a BPMN process definition and start a process instance
Simplified example:
```go
restClient, _ := zenclient.NewClient("http://localhost:8080/v1")

var bodyBuf bytes.Buffer
mw := multipart.NewWriter(&bodyBuf)
...
resp1, _ := restClient.CreateProcessDefinitionWithBody(
    ctx,
    mw.FormDataContentType(),
    &bodyBuf,
)

startBody := zenclient.CreateProcessInstanceJSONRequestBody{
    ProcessDefinitionKey: key,
}
resp2, _ := restClient.CreateProcessInstance(ctx, startBody)
```
#### Register a worker
Simplified example:
```go
conn, _ := grpc.NewClient("127.0.0.1:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
defer conn.Close()

zen := zenclient.NewGrpc(conn)

jobWorker := func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
// ...
}

zen.RegisterWorker(context.Background(), "my-client-id", jobWorker, "my-job-type")
```
## Java Client

The Java client is a lightweight library that wraps the REST and gRPC APIs, providing a type-safe interface for Java applications.

The client is available on GitHub: [zenbpm-java-client](https://github.com/pbinitiative/zenbpm-java-client)

There are 2 artefacts available:
- **zenbpm-client-code:** the core library, can be used independently of Spring Boot and supports old java versions
- **zenbpm-spring-boot-starter:** a Spring Boot starter that provides auto-configuration for the core library and `@JobWorker("jobName")` method annotation.

More information about the Java client can be found in that repository, including usage examples and setup instructions.

## Future Clients

- Python
- JavaScript / TypeScript
