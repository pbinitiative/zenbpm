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

### Features

* Spring Boot auto-configuration (drop-in starter)
* REST client (`ApiClient` + typed APIs generated from OpenAPI)
* gRPC job workers via `@JobWorker` and ZenbpmJobWorkerManager
* OpenTelemetry interceptors for REST and spans for gRPC
* Configurable HTTP/gRPC logging

### Build Java Client
``mvn clean package``

### Getting started

Add the starter to your application and the core client as needed.

Maven:
```xml
<dependency>
  <groupId>org.zenbpm</groupId>
  <artifactId>zenbpm-spring-boot-starter</artifactId>
  <version>${zenbpm.version}</version>
</dependency>
<dependency>
  <groupId>org.zenbpm</groupId>
  <artifactId>zenbpm-client-core</artifactId>
  <version>${zenbpm.version}</version>
</dependency>
```

Configure connection settings in application.yml 

values shown in `zenbpm` section are defaults.

`logging` section configures logging for rest and grpc clients separately.
 - `DEBUG` levels expose headers of calls and responses.
 - `TRACE` level exposes full request and response bodies. **Never use this in production!**
```yaml
zenbpm:
  restUrl: http://localhost:8080/v1
  restLoggingEnabled: true
  grpcHost: localhost
  grpcPort: 9090
  grpcPlaintext: true
  grpcLoggingEnabled: true
  jobWorkerEnabled: true
  otelEnabled: true
  
logging:
  level:
    root: INFO
    org.zenbpm.rest: TRACE
    org.zenbpm.grpc: DEBUG

```

### Working examples

#### 1) Use REST APIs
Inject the provided ZenbpmClientService to obtain the ApiClient, then create a typed API.

```java
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenbpm.rest.ZenbpmClientService;
import org.zenbpm.client.ApiException;
import org.zenbpm.client.ApiClient;
import org.zenbpm.client.api.ProcessDefinitionApi;
import org.zenbpm.client.api.ProcessInstanceApi;
import org.zenbpm.client.api.dto.CreateProcessInstanceRequest;

import java.util.HashMap;
import java.util.Map;

@Service
public class MyService {
  @Autowired
  private ZenbpmClientService zenbpm;

  public Long deployExampleProcess() throws ApiException {
    ApiClient apiClient = zenbpm.getApiClient();
    ProcessDefinitionApi defApi = new ProcessDefinitionApi(apiClient);

    // Example: create a process definition from a BPMN string (adjust to your endpoint contract)
    String bpmnXml = "<definitions ...>...</definitions>";
    Long definitionKey = defApi.createProcessDefinition(bpmnXml).getProcessDefinitionKey();
    return definitionKey;
  }

  public void startMyProcess() throws ApiException {
    ApiClient apiClient = zenbpm.getApiClient();
    ProcessInstanceApi piApi = new ProcessInstanceApi(apiClient);

    Map<String, Object> vars = new HashMap<>();
    vars.put("orderId", 12345L);

    CreateProcessInstanceRequest req = new CreateProcessInstanceRequest()
        .processDefinitionKey(123456L)
        .variables(vars);

    piApi.createProcessInstance(req);
  }
}
```

Notes:
- Available typed APIs include ProcessDefinitionApi, ProcessInstanceApi, JobApi, MessageApi, etc. Construct them with the provided ApiClient.
- Methods and DTOs come from the generated package `org.zenbpm.client.api` and `org.zenbpm.client.api.dto`.

#### 2) Register a gRPC job worker
Create a Spring bean with a method annotated by `@JobWorker`. Accepted method signatures:
- no parameters
- one parameter of type `org.zenbpm.proto.Zenbpm.WaitingJob`
- one parameter of type `org.zenbpm.grpc.JobContext`
- one parameter of type `Map<String, Object>`

Return value can be any object and will be serialized as variables for job completion. Throwing an exception fails the job.

```java
import org.springframework.stereotype.Component;
import org.zenbpm.grpc.JobWorker;
import org.zenbpm.grpc.JobContext;
import java.util.Map;
import java.util.HashMap;

@Component
public class EmailWorker {
  @JobWorker("send-email")
  public Map<String, Object> handleJob(JobContext ctx) {
    Map<String,Object> vars = ctx.getVariables();
    String to = (String) vars.get("email");

    // send email ...

    Map<String, Object> result = new HashMap<>();
    result.put("success", true);
    result.put("message", "Email to " + to + " mocked successfully");
    return result;
  }
}
```

The gRPC worker manager connects on application start if `zenbpm.jobWorkerEnabled` is true.

## Future Clients

- Python
- JavaScript / TypeScript
