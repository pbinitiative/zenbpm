# ZenBPM Junie Development Guidelines

This document provides comprehensive guidelines for working with the ZenBPM codebase, following the patterns and conventions established in the project. These guidelines are designed to help developers (both human and AI assistants like Junie) understand and contribute effectively to the ZenBPM project.

## Project Overview

**ZenBPM** is a next-generation Business Process Management (BPM) engine written in Go 1.24, designed to execute BPMN 2.0 and DMN process definitions. It provides a lightweight, cloud-ready platform with distributed architecture capabilities.

### Key Characteristics
- **Cloud-Native**: Containerized with Docker support and Kubernetes-ready
- **Distributed**: Built on HashiCorp Raft consensus with rqlite for persistence  
- **Observable**: Integrated OpenTelemetry tracing and Prometheus metrics
- **Dual APIs**: Both REST (OpenAPI) and gRPC interfaces
- **Standards Compliant**: Full BPMN 2.0 and DMN support
- **Early Stage**: Currently not production-ready (as of documentation)

## Architecture Overview

### Core Components

1. **BPMN Engine** (`pkg/bpmn/`)
   - Core process execution engine with functional options pattern
   - Integrates with DMN engine for decision evaluation
   - Supports exporters and pluggable storage backends
   - Comprehensive test coverage with BPMN test cases

2. **DMN Engine** (`pkg/dmn/`)
   - Decision Model and Notation engine for business rules
   - Integrated FEEL expression evaluation
   - Bulk evaluation support for performance

3. **Cluster Management** (`internal/cluster/`)
   - HashiCorp Raft-based consensus
   - Network communication layer
   - Job management and coordination
   - Protocol Buffers for inter-node communication

4. **Storage Layer** (`pkg/storage/`, `internal/sql/`)
   - rqlite distributed SQLite for persistence
   - SQLC for type-safe SQL queries
   - In-memory storage for testing
   - Migration management

5. **API Layer** (`internal/rest/`, `internal/grpc/`)
   - Chi router for REST API with CORS support
   - gRPC server with protobuf definitions
   - OpenAPI specification and code generation
   - Middleware for observability

6. **Configuration** (`internal/config/`)
   - YAML/environment variable configuration with cleanenv
   - Comprehensive validation logic
   - Support for multi-node cluster setup

## Development Workflow

### Prerequisites
- Go 1.24+ (uses Go toolchain 1.24.0)
- Make for build automation
- Docker for containerization
- Protocol Buffers compiler (protoc)

### Essential Commands

```bash
# Development setup
make help                    # Show all available commands
make generate               # Run all code generators (required after changes)
make fmt                    # Format code
make vet                    # Static analysis

# Running the application
make run                    # Single node development
make run1                   # First node in cluster
make run2                   # Second node in cluster

# Testing
make test                   # Unit tests with coverage
make test-e2e              # End-to-end tests
make bench                 # Benchmarks

# Monitoring stack (development)
make start-monitoring      # Start Jaeger, Prometheus, Grafana
make stop-monitoring       # Stop monitoring stack

# Build
make build                 # Build binary
```

### Code Generation

The project uses extensive code generation:
- **SQLC**: Database queries (`internal/sql/queries/`)
- **Protocol Buffers**: gRPC definitions (`pkg/client/proto/`)
- **OpenAPI**: REST API client/server code
- **Go generate**: Various embedded resources and stringer implementations

**Always run `make generate` after modifying:**
- SQL queries or schema
- Proto files
- OpenAPI specifications
- Any files with `//go:generate` directives

## Coding Standards and Patterns

### Core Style Principles (Google Go Style Guide)

Following Google's Go Style Guide, we prioritize these principles in order of importance:

1. **Clarity**: The code's purpose and rationale must be clear to the reader
   - Use descriptive variable names that explain intent
   - Add commentary that explains *why*, not *what*
   - Break up complex code with whitespace and comments
   - Refactor into separate functions when logic becomes complex

2. **Simplicity**: Code should accomplish goals in the simplest way possible
   - Easy to read from top to bottom
   - No unnecessary levels of abstraction
   - Clear propagation of values and decisions
   - Prefer core language constructs over sophisticated machinery

3. **Concision**: Maintain high signal-to-noise ratio
   - Avoid repetitive code patterns
   - Use meaningful names that reduce need for comments  
   - Eliminate extraneous syntax and unnecessary abstraction
   - Leverage common Go idioms (e.g., standard error handling patterns)

4. **Maintainability**: Write code that can be easily maintained
   - Document complex algorithms and performance optimizations
   - Provide useful errors and test failures
   - Use interface-based design for flexibility
   - Include comprehensive tests and examples

5. **Consistency**: Follow established patterns within the codebase
   - Maintain uniformity with existing code style
   - Use standard Go conventions and idioms
   - Apply the same patterns across similar functionality

### Package Organization

```
/cmd/zenbpm/           # Application entry points
/internal/             # Private application packages
  /appcontext/         # Application context management
  /cluster/            # Distributed system components
  /config/             # Configuration management
  /grpc/              # gRPC server implementation
  /otel/              # OpenTelemetry setup
  /rest/              # REST API implementation
  /sql/               # Database layer
/pkg/                  # Public packages (importable)
  /bpmn/              # BPMN engine
  /dmn/               # DMN engine  
  /client/            # gRPC client
  /storage/           # Storage interfaces
```

### Design Patterns

#### 1. Functional Options Pattern (Preferred)
Used throughout for constructors and optional configuration:

```go
// Example from BPMN engine
type EngineOption = func(*Engine)

func NewEngine(options ...EngineOption) Engine {
    engine := Engine{/* defaults */}
    for _, option := range options {
        option(&engine)
    }
    return engine
}

func EngineWithStorage(storage Storage) EngineOption {
    return func(e *Engine) {
        e.storage = storage
    }
}
```

**When to use:** Any constructor with 3+ parameters or APIs that may need expansion.

#### 2. Interface-Based Design
Clean separation of concerns with well-defined interfaces:

```go
// Storage abstraction allows multiple backends
type Storage interface {
    Store(key string, value []byte) error
    Retrieve(key string) ([]byte, error)
}
```

#### 3. Context-First APIs
All major operations accept `context.Context` as first parameter:

```go
func (e *Engine) CreateInstanceByKey(ctx context.Context, key string, variables map[string]interface{}) (*ProcessInstance, error)
```

### Naming Conventions (Google Go Style Guide)

#### Function and Method Names
Follow these conventions to avoid repetition and improve readability:

- **Avoid package name repetition**: Don't repeat the package name in function names
  ```go
  // Bad:
  package yamlconfig
  func ParseYAMLConfig(input string) (*Config, error)
  
  // Good:
  package yamlconfig  
  func Parse(input string) (*Config, error)
  ```

- **Avoid receiver type repetition**: Don't repeat the receiver type in method names
  ```go
  // Bad:
  func (c *Config) WriteConfigTo(w io.Writer) (int64, error)
  
  // Good:
  func (c *Config) WriteTo(w io.Writer) (int64, error)
  ```

- **Use descriptive names**: Functions that return something get noun-like names
  ```go
  // Good:
  func (c *Config) JobName(key string) (value string, ok bool)
  ```

- **Avoid "Get" prefix**: Omit unnecessary "Get" prefixes for accessor methods
  ```go
  // Bad:
  func (c *Config) GetJobName(key string) (value string, ok bool)
  
  // Good:
  func (c *Config) JobName(key string) (value string, ok bool)
  ```

- **Action functions use verbs**: Functions that perform actions get verb-like names
  ```go
  // Good:
  func (c *Config) WriteDetail(w io.Writer) (int64, error)
  ```

#### Variable and Type Names
- Use **camelCase** for variables, functions, and methods
- Use **PascalCase** for exported types and constants
- Keep names **concise but descriptive**
- Avoid **stuttering** (e.g., `config.ConfigFile` → `config.File`)

### Least Mechanism Principle

When multiple approaches exist, prefer simpler solutions:

1. **Core language constructs** first (channels, slices, maps, loops, structs)
2. **Standard library** tools second (HTTP clients, template engines) 
3. **External dependencies** only when necessary

Examples:
- Use `map[string]bool` for set membership instead of specialized set libraries
- Override values directly in tests rather than using `flag.Set`
- Choose boolean-valued maps over complex set types unless advanced operations needed

### Configuration Management

- **Primary**: YAML files with environment variable overrides
- **Library**: `cleanenv` for parsing and validation
- **Validation**: Comprehensive validation in `Config.validate()`
- **Environment**: Use `CONFIG_FILE` environment variable or default to `conf.yaml`

Example structure:
```yaml
httpServer:
  addr: ":8080"
  context: "/"
grpcServer:
  addr: ":9090"
cluster:
  nodeId: "node1"
  addr: ":8091"
  raft:
    dir: "zen_bpm_node_data"
    joinAddresses: []
tracing:
  enabled: false
  endpoint: "http://localhost:4318"
```

### Error Handling

#### Google Go Style Guide Principles
- **Fail fast and clearly**: Return errors immediately when they occur
- **Provide context**: Wrap errors with meaningful information using `fmt.Errorf`
- **Use standard patterns**: Leverage familiar error handling idioms
  ```go
  // Standard pattern - readers recognize this immediately
  if err := doSomething(); err != nil {
      return fmt.Errorf("failed to do something: %w", err)
  }
  
  // Signal boost for unusual patterns with comments
  if err := doSomething(); err == nil { // if NO error
      return fmt.Errorf("expected error but got none")
  }
  ```

#### ZenBPM Specific Patterns
- Use typed errors for domain-specific cases (`pkg/bpmn/error.go`)
- Wrap errors with context using `fmt.Errorf`
- Log errors at appropriate levels using structured logging
- Return errors up the stack, don't log and continue
- Provide useful error messages that help with debugging

### Documentation and Comments (Google Go Style Guide)

#### Effective Commentary
- **Explain WHY, not WHAT**: Focus on rationale and context, not obvious code behavior
- **Document nuances**: Explain language subtleties, business logic complexities, and performance considerations
- **Avoid redundant comments**: Let code speak for itself with descriptive names
- **Keep comments current**: Comments that contradict code are worse than no comments

#### Examples of Good Comments
```go
// validateUser distinguishes between actual users and impersonated users
// for access control checks. This is critical for security audit trails.
func validateUser(ctx context.Context, userID string, isImpersonated bool) error {
    // Complex validation logic here...
}

// This optimization preallocates buffers for performance in hot paths.
// Benchmarks show 40% improvement in high-throughput scenarios.
func processLargeDataset(data []Item) []Result {
    // Performance-critical code with complex buffer management
}
```

#### Documentation Standards
- **Package documentation**: Start with package name and describe its purpose
- **Exported functions**: Document behavior, parameters, and return values
- **Complex algorithms**: Explain the approach and any trade-offs made
- **Interface contracts**: Clearly define expected behavior and error conditions

### Logging

- **Library**: Uber Zap for structured logging
- **Context**: Pass logger through context where needed
- **Levels**: Use appropriate log levels (DEBUG, INFO, WARN, ERROR)
- **Format**: Structured logging with key-value pairs
- **Security**: Never log sensitive data (passwords, tokens, personal information)

### Testing

#### Test Organization
```
*_test.go              # Unit tests alongside source
/test/e2e/            # End-to-end integration tests
/pkg/bpmn/test-cases/ # BPMN process definition test files
```

#### Testing Patterns
- **Table-driven tests** for multiple scenarios
- **Test helpers** for common setup (`internal/cluster/server/servertest/`)
- **In-memory storage** for unit tests (`pkg/storage/inmemory/`)
- **Test fixtures** for BPMN/DMN files
- **Coverage reporting** with `make test`

#### Test Environment Variables
```bash
export PROFILE=TEST
export CONFIG_FILE=conf/zenbpm/conf-test.yaml
export LOG_LEVEL=INFO
```

## Observability

### OpenTelemetry Integration
- **Tracing**: HTTP and internal operation spans
- **Metrics**: Custom business metrics via Prometheus
- **Configuration**: Centralized in `internal/otel/`
- **Exporters**: OTLP HTTP for Jaeger integration

### Development Monitoring
The project includes a complete monitoring stack:
- **Jaeger** (`:16686`): Distributed tracing
- **Prometheus** (`:9101`): Metrics collection  
- **Grafana** (`:9100`): Visualization dashboards

## Database and Persistence

### rqlite Integration
- **Distributed SQLite**: Raft-replicated for HA
- **Schema Management**: Versioned migrations in `internal/sql/migrations/`
- **Query Generation**: SQLC for type-safe queries
- **Caching**: LRU caches for process/decision definitions

### Database Development
1. Modify schema in `internal/sql/migrations/`
2. Update queries in `internal/sql/queries/`
3. Run `make generate` to update generated code
4. Template file `internal/sql/db.go.template` becomes `db.go`

## API Development

### REST API
- **Router**: Chi v5 with middleware
- **Specification**: OpenAPI 3.x in `/openapi/`
- **Generation**: oapi-codegen for server stubs
- **CORS**: Configured for cross-origin requests
- **Endpoints**: Standard RESTful patterns

### gRPC API
- **Definition**: Protocol Buffers in `pkg/client/proto/`
- **Generation**: protoc with Go plugins
- **Client**: Generated client in `pkg/client/`
- **Streaming**: Support for bidirectional streaming

## Clustering and Distribution

### Raft Configuration
```yaml
cluster:
  nodeId: "unique-node-id"
  addr: ":8091"                    # Bind address
  adv: "node1.example.com:8091"    # Advertise address
  raft:
    dir: "zen_bpm_node_data"       # Data directory
    joinAddresses:                 # Bootstrap cluster
      - "node2.example.com:8091"
    bootstrapExpect: 3             # Expected cluster size
```

### Development Clustering
- Use `make run1` and `make run2` for multi-node testing
- Configuration files: `conf/zenbpm/conf-dev-node{1,2}.yaml`
- Network validation prevents common cluster issues

## Contribution Guidelines

### Licensing
- **Dual License**: AGPLv3 and Enterprise License
- **CLA Required**: All contributors must sign CLA via CLA Assistant
- **Headers**: All files must include license headers (`make license.add`)

### Git Workflow
1. Fork the repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Follow **Conventional Commits** specification
4. Ensure tests pass: `make test`
5. Run `make generate && make fmt && make vet`
6. Submit Pull Request
7. Sign CLA when prompted by CLA Assistant

### Code Review Checklist
- [ ] Follows functional options pattern for extensible APIs
- [ ] Includes appropriate tests with good coverage
- [ ] Uses structured logging with context
- [ ] Handles errors appropriately
- [ ] Updates documentation if needed
- [ ] Runs `make generate` after changes
- [ ] License headers present (`make license.check`)

## Common Development Tasks

### Adding a New BPMN Element
1. Define element in `pkg/bpmn/model/bpmn20/`
2. Implement execution logic in `pkg/bpmn/runtime/`
3. Add comprehensive tests with BPMN files
4. Update documentation in `docs/reference/bpmn-engine.md`

### Adding New Configuration
1. Add fields to appropriate struct in `internal/config/config.go`
2. Include YAML tags, JSON tags, and env tags with defaults
3. Add validation logic in `Config.validate()`
4. Update example configuration files
5. Document in `docs/reference/configuration.md`

### Adding New REST Endpoint
1. Update OpenAPI specification in `openapi/`
2. Run `make generate` to update generated code
3. Implement handler in `internal/rest/`
4. Add appropriate middleware
5. Write integration tests

### Adding New Metrics
1. Define metrics in `internal/otel/metrics.go`
2. Instrument code with metric collection
3. Update Grafana dashboards if needed
4. Document in `docs/reference/opentelemetry.md`

## Performance Considerations

### Database Performance
- Use prepared statements via SQLC
- Implement appropriate indexes
- Consider read replicas for scaling
- Monitor query performance

### Memory Management
- Use object pools for frequent allocations
- Profile with `go tool pprof`
- Monitor garbage collection impact
- Consider streaming for large datasets

### Concurrency
- Use context for cancellation
- Prefer channels over shared memory
- Use sync package primitives appropriately
- Test concurrent scenarios thoroughly

## Security Guidelines

### Data Protection
- Never log sensitive data (passwords, tokens)
- Use secure defaults in configuration
- Validate all input at API boundaries
- Follow principle of least privilege

### Network Security
- TLS for production deployments
- Authenticate cluster communications
- Validate join addresses to prevent attacks
- Use secure random generation for IDs

## Deployment Considerations

### Container Deployment
```bash
docker build -t zenbpm .
docker run -d -p 8080:8080 -p 9090:9090 \
  -v $(pwd)/conf:/app/conf \
  -v $(pwd)/data:/app/data \
  --name zenbpm zenbpm
```

### Configuration Management
- Use environment variables for sensitive data
- Mount configuration files as volumes
- Separate configurations per environment
- Use configuration validation in startup

### Monitoring in Production
- Enable OpenTelemetry tracing
- Configure appropriate log levels
- Set up health check endpoints
- Monitor cluster consensus health

## Troubleshooting Common Issues

### Code Generation Problems
```bash
make generate  # Regenerate all generated code
```

### Cluster Join Issues
- Check network connectivity between nodes
- Verify advertise addresses are routable
- Ensure bootstrap configuration is correct
- Check Raft logs for consensus issues

### Database Connection Issues  
- Verify rqlite cluster health
- Check data directory permissions
- Review migration status
- Monitor connection pool metrics

## Additional Resources

- **Main Documentation**: https://pbinitiative.github.io/zenbpm-docusaurus
- **OpenAPI Spec**: `/openapi/api.yaml`
- **gRPC Proto**: `/pkg/client/proto/zenbpm.proto`
- **BPMN Test Cases**: `/pkg/bpmn/test-cases/`
- **Example Configurations**: `/conf/zenbpm/`

---

**Remember**: This project is in early development stages. APIs may change, and the guidelines will evolve. Always check the latest documentation and follow the established patterns in the codebase.