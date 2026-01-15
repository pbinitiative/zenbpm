---
sidebar_position: 110
---
# Client Libraries

ZenBPM provides officially supported client libraries in multiple programming languages to help developers integrate with the engine more easily.

The versions of the libraries are aligned with the ZenBPM engine versions.

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
- Go (natively provided by the core package)
