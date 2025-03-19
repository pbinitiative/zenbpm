package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/rqlite/rqlite/v8/random"
)

const (
	DiscoModeNone     = ""
	DiscoModeConsulKV = "consul-kv"
	DiscoModeEtcdKV   = "etcd-kv"
	DiscoModeDNS      = "dns"
	DiscoModeDNSSRV   = "dns-srv"
)

type Config struct {
	Server  Server  `yaml:"server" json:"server"` // configuration of the public REST server
	Name    string  `yaml:"name" json:"name"`     // used for OTEL as an application identifier
	Cluster Cluster `yaml:"cluster" json:"cluster"`
}

// TODO: clean up cluster & rqlite configuration
type Cluster struct {
	// BootstrapExpect sets expected number of servers to join into the cluster before bootstrap is called
	BootstrapExpect int `yaml:"bootstrapExpect" json:"bootstrapExpect" env:"CLUSTER_BOOTSTRAP_EXPECT"`
	// Bootstrap
	Bootstrap bool    `yaml:"bootstrap" json:"bootstrap" env:"CLUSTER_BOOTSTRAP"`
	RaftAddr  string  `yaml:"raftAddress" json:"raftAddress" env:"CLUSTER_RAFT_ADDR" env-default:":8090"`
	RaftDir   string  `yaml:"raftDir" json:"raftDir" env:"CLUSTER_RAFT_DIR"`
	NodeId    string  `yaml:"nodeId" json:"nodeId" env:"CLUSTER_NODE_ID"`
	RqLite    *RqLite `yaml:"rqlite" json:"rqlite"`
}

type Server struct {
	Context string `yaml:"context" json:"context" env:"REST_API_CONTEXT" env-default:"/"`
	Addr    string `yaml:"addr" json:"addr" env:"REST_API_ADDR" env-default:":8080"`
}

func (c Config) defaults() Config {
	if c.Cluster.NodeId == "" {
		c.Cluster.NodeId = random.String()
	}
	if c.Cluster.RaftDir == "" {
		c.Cluster.RaftDir = c.Cluster.NodeId
	}
	return c
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
	return c.defaults()
}
