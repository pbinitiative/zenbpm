---
sidebar_position: 110
---
# Client Libraries

ZenBPM provides officially supported client libraries that wrap its REST and gRPC APIs so you don't have to hand-roll HTTP calls or gRPC streams.

Every client does two things:

- **REST** — deploy process/decision resources, start instances, query state.
- **gRPC** — run *job workers*: subscribe to a job type, complete or fail jobs over a bidirectional stream.

## Versioning

The Go client ships as part of the engine module, so it tracks the engine version. The **Java** client is published to **Maven Central** under group `org.pbinitiative.zenbpm`, and its version tracks the engine version too (e.g. `1.4.0`).

## Choosing a client

| Language | Artifact | Use when |
|---|---|---|
| Go | `github.com/pbinitiative/zenbpm/pkg/zenclient` | Any Go application. Ships as part of the engine module. |
| Java | `org.pbinitiative.zenbpm:zenbpm-client-core` | Java apps **not** using Spring Boot. Works on older Java versions. |
| Java | `org.pbinitiative.zenbpm:zenbpm-spring-boot-starter` | Spring Boot apps. Adds auto-configuration and the `@JobWorker` annotation; pulls in `zenbpm-client-core`. |

> The examples below omit error handling for brevity. Handle returned errors/exceptions in real code.

---

## Go Client

The Go client lives in package `github.com/pbinitiative/zenbpm/pkg/zenclient` and provides both a REST client and a gRPC worker client.

### Install

```bash
go get github.com/pbinitiative/zenbpm@latest
```

The client is part of the engine module, so its version follows the engine version.

### Deploy and start an instance (REST)

```go
// The "WithResponses" client returns typed, parsed responses (with JSON201 etc.).
restClient, _ := zenclient.NewClientWithResponses("http://localhost:8080/v1")

// Deploy is a multipart upload of the .bpmn file.
var bodyBuf bytes.Buffer
mw := multipart.NewWriter(&bodyBuf)
// ... write the .bpmn file into the "resource" form field ...
mw.Close()

defResp, _ := restClient.CreateProcessDefinitionWithBodyWithResponse(
    ctx, mw.FormDataContentType(), &bodyBuf,
)
key := defResp.JSON201.ProcessDefinitionKey

// Start an instance from the returned key.
instResp, _ := restClient.CreateProcessInstanceWithResponse(ctx,
    zenclient.CreateProcessInstanceJSONRequestBody{ProcessDefinitionKey: &key},
)
_ = instResp.JSON201 // the started ProcessInstance
```

### Register a worker (gRPC)

```go
conn, _ := grpc.NewClient("127.0.0.1:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
defer conn.Close()

zen := zenclient.NewGrpc(conn)

jobWorker := func(ctx context.Context, job *proto.WaitingJob) (map[string]any, *zenclient.WorkerError) {
    vars := job.GetVariables()

    // ... do the work ...

    // Success: return output variables (or an empty map) and nil.
    return map[string]any{"result": "done"}, nil

    // To fail the job (and trigger a retry) return a WorkerError instead:
    //   return nil, &zenclient.WorkerError{ErrorCode: "BUSINESS_ERROR"}
}

zen.RegisterWorker(context.Background(), "my-client-id", jobWorker, "my-job-type")
```

---

## Java Client

The Java client wraps the REST and gRPC APIs with a type-safe interface (DTOs are generated from the OpenAPI spec). Source: [zenbpm-java-client](https://github.com/pbinitiative/zenbpm-java-client).

### Install

The client is on **Maven Central** (group `org.pbinitiative.zenbpm`), so no extra repository is needed. Set `zenbpm.version` to the version matching your engine.

**Spring Boot** — a complete, copy-pasteable `pom.xml` for a worker application:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.3.4</version>
    <relativePath/>
  </parent>

  <groupId>com.example</groupId>
  <artifactId>my-worker</artifactId>
  <version>1.0.0</version>

  <properties>
    <java.version>17</java.version>
    <zenbpm.version>1.4.0</zenbpm.version>
    <opentelemetry.version>1.58.0</opentelemetry.version>
  </properties>

  <dependencyManagement>
    <dependencies>
      <!-- The ZenBPM starter needs a newer OpenTelemetry than Spring Boot 3.3.x manages. -->
      <dependency>
        <groupId>io.opentelemetry</groupId>
        <artifactId>opentelemetry-bom</artifactId>
        <version>${opentelemetry.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <dependencies>
    <!-- Spring Boot core. The ZenBPM starter declares spring-boot-autoconfigure as optional (not transitive), so the application must provide Spring Boot itself. -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter</artifactId>
    </dependency>
    <!-- ZenBPM Spring Boot starter: provides @JobWorker and auto-connects on startup. -->
    <dependency>
      <groupId>org.pbinitiative.zenbpm</groupId>
      <artifactId>zenbpm-spring-boot-starter</artifactId>
      <version>${zenbpm.version}</version>
    </dependency>
    <!-- Required for gRPC job workers. -->
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-netty-shaded</artifactId>
      <version>1.80.0</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

**Non-Spring (core only)** — if you're not using Spring Boot, depend on the core client instead of the starter (no OpenTelemetry BOM or `spring-boot-starter` needed):

```xml
<dependency>
  <groupId>org.pbinitiative.zenbpm</groupId>
  <artifactId>zenbpm-client-core</artifactId>
  <version>${zenbpm.version}</version>
</dependency>
```

### Configure

The Spring Boot starter is configured through `application.yml`. Minimal configuration to connect to a local engine:

```yaml
zenbpm:
  restUrl: http://localhost:8080/v1
  grpcHost: localhost
  grpcPort: 9090
  grpcPlaintext: true       # local/dev only; use TLS in production
  jobWorkerEnabled: true    # connect job workers on startup
```

See [Configuration reference](#configuration-reference) for all options, including logging.

> Using `zenbpm-client-core` without Spring Boot? You configure the `ApiClient` and worker manager programmatically instead of via `application.yml` — see the [client repository](https://github.com/pbinitiative/zenbpm-java-client) for a plain-Java example.

### Deploy and start an instance (REST)

Inject `ZenbpmClientService` to obtain the `ApiClient`, then use the typed APIs.

```java
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.pbinitiative.zenbpm.rest.ZenbpmClientService;
import org.pbinitiative.zenbpm.client.ApiException;
import org.pbinitiative.zenbpm.client.ApiClient;
import org.pbinitiative.zenbpm.client.api.ProcessDefinitionApi;
import org.pbinitiative.zenbpm.client.api.ProcessInstanceApi;
import org.pbinitiative.zenbpm.client.api.dto.CreateProcessInstanceRequest;

import java.util.Map;

@Service
public class MyService {
  @Autowired
  private ZenbpmClientService zenbpm;

  public Long deployProcess(String bpmnXml) throws ApiException {
    ApiClient apiClient = zenbpm.getApiClient();
    ProcessDefinitionApi defApi = new ProcessDefinitionApi(apiClient);

    // Deploy the BPMN definition; the generated client returns the 201 response body.
    return defApi.createProcessDefinition(bpmnXml).getProcessDefinitionKey();
  }

  public void startProcess(Long definitionKey) throws ApiException {
    ApiClient apiClient = zenbpm.getApiClient();
    ProcessInstanceApi piApi = new ProcessInstanceApi(apiClient);

    CreateProcessInstanceRequest req = new CreateProcessInstanceRequest()
        .processDefinitionKey(definitionKey)
        .variables(Map.of("orderId", 12345L));

    piApi.createProcessInstance(req);
  }
}
```

Available typed APIs include `ProcessDefinitionApi`, `ProcessInstanceApi`, `JobApi`, `MessageApi`, and others. Methods and DTOs come from the generated packages `org.pbinitiative.zenbpm.client.api` and `org.pbinitiative.zenbpm.client.api.dto`.

### Register a worker (gRPC)

Annotate a Spring bean method with `@JobWorker("<job-type>")`. The gRPC worker manager connects on application startup when `zenbpm.jobWorkerEnabled` is `true`.

```java
import org.springframework.stereotype.Component;
import org.pbinitiative.zenbpm.grpc.JobWorker;
import org.pbinitiative.zenbpm.grpc.JobContext;
import java.util.Map;

@Component
public class EmailWorker {
  @JobWorker("send-email")
  public Map<String, Object> handleJob(JobContext ctx) {
    Map<String, Object> vars = ctx.getVariables();
    String to = (String) vars.get("email");

    // ... send the email ...

    // Return value is serialized as output variables on job completion.
    return Map.of("emailSent", true);
    // Throw an exception to fail the job (triggering a retry).
  }
}
```

Accepted `@JobWorker` method parameters (pick one):

- no parameters
- `org.pbinitiative.zenbpm.proto.Zenbpm.WaitingJob`
- `org.pbinitiative.zenbpm.grpc.JobContext`
- `Map<String, Object>` (the job variables)

---

## Configuration reference

Full set of `application.yml` options for the Java Spring Boot starter.

| Key | Purpose |
|---|---|
| `zenbpm.restUrl` | Base URL of the engine REST API (include `/v1`). |
| `zenbpm.grpcHost` | Engine gRPC host. |
| `zenbpm.grpcPort` | Engine gRPC port. |
| `zenbpm.grpcPlaintext` | Use plaintext gRPC (no TLS). Local/dev only. |
| `zenbpm.jobWorkerEnabled` | Connect registered job workers on startup. |
| `zenbpm.otelEnabled` | Enable OpenTelemetry interceptors (REST) and spans (gRPC). |
| `zenbpm.restLoggingEnabled` | Enable request/response logging for the REST client. |
| `zenbpm.grpcLoggingEnabled` | Enable logging for the gRPC client. |

> See the [client repository](https://github.com/pbinitiative/zenbpm-java-client) for the authoritative list of options and their defaults.

Logging verbosity is controlled through standard Spring logging levels, configured **per client** (`org.pbinitiative.zenbpm.rest`, `org.pbinitiative.zenbpm.grpc`):

- `DEBUG` — exposes request/response headers.
- `TRACE` — exposes full request and response **bodies**. **Never use `TRACE` in production**; it can leak sensitive data.

```yaml
logging:
  level:
    org.pbinitiative.zenbpm.rest: DEBUG
    org.pbinitiative.zenbpm.grpc: DEBUG
```

## Building the Java client from source

Most users consume the published Maven Central artifacts and don't need this. To build the Java client from source:

```bash
mvn clean package
```

## Future Clients

Officially supported clients are planned for:

- Python
- JavaScript / TypeScript
