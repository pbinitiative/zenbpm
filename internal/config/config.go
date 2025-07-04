package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
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
	NodeId string `yaml:"nodeId" json:"nodeId" env:"CLUSTER_NODE_ID"`
	// internal communication bind address
	Addr string `yaml:"addr" json:"addr" env:"CLUSTER_RAFT_ADDR" env-default:":8090"`
	// inter communication advertise address. If not set, same as internal communication bind address
	Adv         string      `yaml:"adv" json:"adv" env:"CLUSTER_RAFT_ADV"`
	Raft        ClusterRaft `yaml:"raft" json:"raft"`
	Persistence Persistence `yaml:"persistence" json:"persistence"`
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
	JoinAddresses []string `yaml:"joinAddresses" json:"joinAddresses" env:"CLUSTER_RAFT_JOIN_ADDRESSES"`
	// Minimum number of nodes required for a bootstrap
	BootstrapExpect int `yaml:"bootstrapExpect" json:"bootstrapExpect" env:"CLUSTER_RAFT_BOOTSTRAP_EXPECT" env-default:"0"`
	// Maximum time for bootstrap process
	BootstrapExpectTimeout time.Duration `yaml:"bootstrapExpectTimeout" json:"bootstrapExpectTimeout" env:"CLUSTER_RAFT_EXPECT_BOOTSTRAP_TIMEOUT" env-default:"10s"`
	// Bootstrap              bool `yaml:"bootstrap" json:"bootstrap" env:"CLUSTER_RAFT_BOOTSTRAP"`
}

type GrpcServer struct {
	Addr string `yaml:"addr" json:"addr" env:"GRPC_API_ADDR" env-default:":9090"`
}

type HttpServer struct {
	Context string `yaml:"context" json:"context" env:"REST_API_CONTEXT" env-default:"/"`
	Addr    string `yaml:"addr" json:"addr" env:"REST_API_ADDR" env-default:":8080"`
}

type Tracing struct {
	Enabled         bool     `yaml:"enabled" json:"enabled" env:"TRACING_ENABLED" env-default:"false"`
	Name            string   `yaml:"name" json:"name" env:"TRACING_APP_NAME" env-default:"ZenBPM"` // application identifier
	TransferHeaders []string `yaml:"transferHeaders" json:"transferHeaders" env:"TRACING_TRANSFER_HEADERS"`
	Endpoint        string   `yaml:"endpoint" env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
}

type Persistence struct {
	ProcDefCacheTTL  time.Duration `yaml:"procDefCacheTTL" env:"PERSISTENCE_PROC_DEF_CACHE_TTL_SECONDS" env-default:"24h"`
	ProcDefCacheSize int           `yaml:"procDefCacheSize" env:"PERSISTENCE_PROC_DEF_CACHE_SIZE" env-default:"200"`
	RqLite           *RqLite       `yaml:"rqlite" json:"rqlite"`
}

// validate checks the configuration for internal consistency, and activates
// important zenbpm policies. It must be called at least once on a Config
// object before the Config object is used.
func (c *Config) validate() error {
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
		fileName = fmt.Sprintf("%s/conf.yaml", wd)
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
