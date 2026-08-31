// Package config defines and validates ZenBPM runtime configuration.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/types"
	"github.com/rqlite/rqlite/v10/cdc"
)

// TODO: add support for discovery modes
const (
	DiscoModeNone     = ""
	DiscoModeConsulKV = "consul-kv"
	DiscoModeEtcdKV   = "etcd-kv"
	DiscoModeDNS      = "dns"
	DiscoModeDNSSRV   = "dns-srv"
)

type Config struct {
	HttpServer HttpServer `yaml:"httpServer" json:"httpServer"` // configuration of the public REST server
	GrpcServer GrpcServer `yaml:"grpcServer" json:"grpcServer"` // configuration of the public GRPC server
	Tracing    Tracing    `yaml:"tracing" json:"tracing"`
	Cluster    Cluster    `yaml:"cluster" json:"cluster"`
}

// TODO: clean up cluster & rqlite configuration
type Cluster struct {
	// BootstrapExpect sets expected number of servers to join into the cluster before bootstrap is called
	NodeId string `yaml:"nodeId" json:"nodeId" env:"CLUSTER_NODE_ID" env-default:"zenbpm"`
	// internal communication bind address
	Addr string `yaml:"addr" json:"addr" env:"CLUSTER_RAFT_ADDR" env-default:"localhost:8090"`
	// inter communication advertise address. If not set, same as internal communication bind address
	Adv         string      `yaml:"adv" json:"adv" env:"CLUSTER_RAFT_ADV" env-default:"localhost:8090"`
	Raft        ClusterRaft `yaml:"raft" json:"raft"`
	CDC         CDC         `yaml:"cdc" json:"cdc"`
	Persistence Persistence `yaml:"persistence" json:"persistence"`
	Script      Script      `yaml:"script" json:"script"`
	Engine      Engine      `yaml:"engine" json:"engine"`
	// PartitionRetryDelay is the initial retry delay for partition lifecycle operations.
	PartitionRetryDelay time.Duration `yaml:"partitionRetryDelay" json:"partitionRetryDelay" env:"CLUSTER_PARTITION_RETRY_DELAY" env-default:"5s"`
}

// Engine configures the behaviour of the BPMN engines running on the node partitions.
type Engine struct {
	// MaxProcessInstanceNestingDepth is the maximum allowed nesting depth of a process instance in the parent-child chain
	// (call activities, sub processes, multi-instance bodies). When a child process instance exceeds the limit,
	// the engine stops its creation and raises an incident describing a potential infinite loop. Values <= 0 disable the check.
	MaxProcessInstanceNestingDepth int64 `yaml:"maxProcessInstanceNestingDepth" json:"maxProcessInstanceNestingDepth" env:"CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH" env-default:"100"`
	// MaxProcessInstanceElementExecutionCount is the maximum total number of flow element executions allowed within
	// one process instance. It guards against infinite sequence-flow loops (e.g. an exclusive gateway
	// looping back forever). When the limit is exceeded, the engine fails the token and raises an
	// incident; resolving that incident resets the instance's execution counter, granting a fresh budget.
	// Values <= 0 disable the check.
	// This constraint is intentionally separate from (and is much larger than) MaxProcessInstanceNestingDepth:
	// legitimate loops may run thousands of iterations while legitimate nesting rarely exceeds double digits.
	MaxProcessInstanceElementExecutionCount int64 `yaml:"maxProcessInstanceElementExecutionCount" json:"maxProcessInstanceElementExecutionCount" env:"CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_ELEMENT_EXECUTION_COUNT" env-default:"10000"`
}

// CDC configures the rqlite change data capture output.
type CDC struct {
	Enabled   bool   `yaml:"enabled" json:"enabled" env:"RQLITE_CDC_ENABLED" env-default:"false"`
	Output    string `yaml:"output" json:"output" env:"RQLITE_CDC_OUTPUT"`
	ServiceID string `yaml:"serviceId" json:"serviceId" env:"RQLITE_CDC_SERVICE_ID"`
}

// ResolveServiceID returns the effective base CDC service identifier. A service
// identifier from an advanced rqlite output file takes precedence over the
// identifier configured directly in ZenBPM.
func (c CDC) ResolveServiceID(advancedServiceID string) (string, error) {
	serviceID := advancedServiceID
	if serviceID == "" {
		serviceID = c.ServiceID
	}
	if strings.TrimSpace(serviceID) == "" {
		return "", errors.New("CDC service ID is required when CDC is enabled")
	}
	return serviceID, nil
}

// ValidateCDC verifies that an enabled CDC output can be constructed.
func (c Cluster) ValidateCDC() error {
	if !c.CDC.Enabled {
		return nil
	}

	cdcOutput := c.CDC.Output
	if cdcOutput == "" {
		return errors.New("CDC output is required when CDC is enabled")
	}

	cdcConfig, err := cdc.NewConfig(cdcOutput)
	if err != nil {
		return fmt.Errorf("failed to load CDC output: %w", err)
	}
	tlsConfig, err := cdcConfig.TLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build CDC output TLS settings: %w", err)
	}
	// NewSink performs the same endpoint check as cdc.NewService. Constructing
	// and closing it does not issue a network request.
	sink, err := cdc.NewSink(cdc.SinkConfig{
		Endpoint:        cdcConfig.Endpoint,
		TLSConfig:       tlsConfig,
		TransmitTimeout: cdcConfig.TransmitTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to validate CDC output endpoint: %w", err)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("failed to close CDC output sink: %w", err)
	}
	if _, err := c.CDC.ResolveServiceID(cdcConfig.ServiceID); err != nil {
		return err
	}
	return nil
}

type ClusterRaft struct {
	// Dir is path to node data. Always set
	Dir string `yaml:"dir" json:"dir" env:"CLUSTER_RAFT_DIR" env-default:"zen_bpm_node_data"`
	// Configure as non-voting node
	NonVoter bool `yaml:"nonVoter" json:"nonVoter" env:"CLUSTER_RAFT_NON_VOTER"`
	// Number of join attempts to make
	JoinAttempts int `yaml:"joinAttempts" json:"joinAttempts" env:"CLUSTER_RAFT_JOIN_ATTEMPTS" env-default:"5"`
	// Period between join attempts
	JoinInterval time.Duration `yaml:"joinInterval" json:"joinInterval" env:"CLUSTER_RAFT_JOIN_INTERVAL" env-default:"2s"`
	// List of nodes, in host:port form, through which a cluster can be joined
	JoinAddresses []string `yaml:"joinAddresses" json:"joinAddresses" env:"CLUSTER_RAFT_JOIN_ADDRESSES" env-default:"localhost:8090"`
	// Minimum number of nodes required for a bootstrap
	BootstrapExpect int `yaml:"bootstrapExpect" json:"bootstrapExpect" env:"CLUSTER_RAFT_BOOTSTRAP_EXPECT" env-default:"1"`
	// Maximum time for bootstrap process
	BootstrapExpectTimeout time.Duration `yaml:"bootstrapExpectTimeout" json:"bootstrapExpectTimeout" env:"CLUSTER_RAFT_EXPECT_BOOTSTRAP_TIMEOUT" env-default:"10s"`
	// Bootstrap bool `yaml:"bootstrap" json:"bootstrap" env:"CLUSTER_RAFT_BOOTSTRAP"`
}

type GrpcServer struct {
	Addr string `yaml:"addr" json:"addr" env:"GRPC_API_ADDR" env-default:":9090"`
}

type HttpServer struct {
	Context string `yaml:"context" json:"context" env:"REST_API_CONTEXT" env-default:"/"`
	Addr    string `yaml:"addr" json:"addr" env:"REST_API_ADDR" env-default:":8080"`
	// MaxRequestBodyBytes bounds request buffering by the OpenAPI validator.
	MaxRequestBodyBytes int64 `yaml:"maxRequestBodyBytes" json:"maxRequestBodyBytes" env:"REST_API_MAX_REQUEST_BODY_BYTES" env-default:"10485760"`
	// LogMode controls request logging: "errors" (default, status >= 400 only),
	// "all" (every request) or "off" (no request logging).
	LogMode string `yaml:"logMode" json:"logMode" env:"REST_API_LOG_MODE" env-default:"errors"`
	// LogBody enables capturing request and response bodies for logged requests.
	// When true, bodies are included for whatever LogMode logs (e.g. failed
	// requests in "errors" mode). Capturing buffers the whole body of every
	// request/response in memory even when it ends up not being logged, so keep
	// this off on busy servers unless you need body diagnostics.
	LogBody bool `yaml:"logBody" json:"logBody" env:"REST_API_LOG_BODY" env-default:"false"`
}

// Request logging modes for HttpServer.LogMode.
const (
	// LogModeErrors logs failed requests only (status >= 400).
	LogModeErrors = "errors"
	// LogModeAll logs every request.
	LogModeAll = "all"
	// LogModeOff disables request logging entirely.
	LogModeOff = "off"
)

type Tracing struct {
	Enabled         bool     `yaml:"enabled" json:"enabled" env:"TRACING_ENABLED" env-default:"false"`
	Name            string   `yaml:"name" json:"name" env:"TRACING_APP_NAME" env-default:"ZenBPM"` // application identifier
	TransferHeaders []string `yaml:"transferHeaders" json:"transferHeaders" env:"TRACING_TRANSFER_HEADERS"`
	Endpoint        string   `yaml:"endpoint" env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	// SamplerRatio controls the fraction of new traces that get sampled (0.0 - 1.0).
	// Child spans follow the sampling decision of their parent (ParentBased sampler).
	SamplerRatio float64 `yaml:"samplerRatio" json:"samplerRatio" env:"TRACING_SAMPLER_RATIO" env-default:"1.0"`
}

type Persistence struct {
	InstanceHistoryTTL types.TTL `yaml:"instanceHistoryTTL" env:"PERSISTENCE_INSTANCE_HISTORY_TTL"`
	ProcDefCacheTTL    types.TTL `yaml:"procDefCacheTTL" env:"PERSISTENCE_PROC_DEF_CACHE_TTL_SECONDS" env-default:"24h"`
	ProcDefCacheSize   int       `yaml:"procDefCacheSize" env:"PERSISTENCE_PROC_DEF_CACHE_SIZE" env-default:"200"`
	DecDefCacheTTL     types.TTL `yaml:"decDefCacheTTL" env:"PERSISTENCE_DEC_DEF_CACHE_TTL_SECONDS" env-default:"24h"`
	DecDefCacheSize    int       `yaml:"decDefCacheSize" env:"PERSISTENCE_DEC_DEF_CACHE_SIZE" env-default:"200"`
	RqLite             *RqLite   `yaml:"rqlite" json:"rqlite"`
	Migration          Migration `yaml:"migration" json:"migration"`
}

type Migration struct {
	Dir string `yaml:"dir" json:"dir" env:"PERSISTENCE_MIGRATION_DIR" env-default:"internal/sql/migrations"`
}

type Script struct {
	Feel ScriptVmPoolConf `yaml:"feel" json:"feel"`
	Js   ScriptVmPoolConf `yaml:"js" json:"js"`
}

type ScriptVmPoolConf struct {
	MaxVmPoolSize int `yaml:"maxVmPoolSize" json:"maxVmPoolSize" env-default:"10"`
	MinVmPoolSize int `yaml:"minVmPoolSize" json:"minVmPoolSize"  env-default:"2"`
}

// validate checks the configuration for internal consistency, and activates
// important zenbpm policies. It must be called at least once on a Config
// object before the Config object is used.
func (c *Config) validate() error {
	switch c.HttpServer.LogMode {
	case "", LogModeErrors, LogModeAll, LogModeOff:
	default:
		return fmt.Errorf("invalid httpServer.logMode %q, supported: %s, %s, %s",
			c.HttpServer.LogMode, LogModeErrors, LogModeAll, LogModeOff)
	}
	if c.HttpServer.MaxRequestBodyBytes <= 0 {
		return fmt.Errorf("httpServer.maxRequestBodyBytes must be greater than zero, got %d", c.HttpServer.MaxRequestBodyBytes)
	}
	if err := c.Cluster.ValidateCDC(); err != nil {
		return err
	}
	if c.Cluster.NodeId == "" {
		c.Cluster.NodeId = c.Cluster.Adv
	}
	if c.Cluster.Raft.Dir == "" {
		c.Cluster.Raft.Dir = c.Cluster.NodeId
	}
	dataPath, err := filepath.Abs(c.Cluster.Raft.Dir)
	if err != nil {
		return fmt.Errorf("failed to determine absolute data path: %s", err.Error())
	}
	c.Cluster.Raft.Dir = dataPath

	err = CheckFilePaths(&c.Cluster.Raft)
	if err != nil {
		return err
	}

	if c.Cluster.Adv == "" {
		c.Cluster.Adv = c.Cluster.Addr
	}

	if _, rp, err := net.SplitHostPort(c.Cluster.Addr); err != nil {
		return errors.New("raft bind address not valid")
	} else if _, err := strconv.Atoi(rp); err != nil {
		return errors.New("raft bind port not valid")
	}

	radv, rp, err := net.SplitHostPort(c.Cluster.Adv)
	if err != nil {
		return errors.New("raft advertised address not valid")
	}
	if addr := net.ParseIP(radv); addr != nil && addr.IsUnspecified() {
		return fmt.Errorf("advertised Raft address is not routable (%s), specify it via cluster.raft.addr or cluster.raft.adv",
			radv)
	}
	if _, err := strconv.Atoi(rp); err != nil {
		return errors.New("raft advertised port is not valid")
	}

	// Enforce bootstrapping policies
	if c.Cluster.Raft.BootstrapExpect > 0 && c.Cluster.Raft.NonVoter {
		return errors.New("bootstrapping only applicable to voting nodes")
	}

	// Join parameters OK?
	if len(c.Cluster.Raft.JoinAddresses) > 0 {
		for _, addr := range c.Cluster.Raft.JoinAddresses {
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return fmt.Errorf("%s is an invalid join address", addr)
			}

			if c.Cluster.Raft.BootstrapExpect == 0 {
				if addr == c.Cluster.Adv || addr == c.Cluster.Addr {
					return errors.New("node cannot join with itself unless bootstrapping")
				}
			}
		}
	}
	err = network.CheckJoinAddrs(c.Cluster.Raft.JoinAddresses)
	if err != nil {
		return fmt.Errorf("invalid join addresses: %w", err)
	}

	return nil
}

func InitConfig() Config {
	c := Config{}
	var fileName string
	confFile := os.Getenv("CONFIG_FILE")
	if confFile == "" {
		wd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		fileName = filepath.Join(wd, "conf.yaml")
	} else {
		fileName = confFile
	}
	var err error
	if _, perr := os.Stat(fileName); errors.Is(perr, os.ErrNotExist) {
		err = cleanenv.ReadEnv(&c)
		fmt.Printf("Configuration file %s not found. Reading config from ENV.\n", fileName)
	} else {
		err = cleanenv.ReadConfig(fileName, &c)
	}
	if err != nil {
		fmt.Printf("Error occurred while reading the configuration: %s\n", err)
		panic(err)
	}
	err = c.validate()
	if err != nil {
		fmt.Printf("Error occurred while validating configuration: %s\n", err)
		panic(err)
	}
	return c
}
