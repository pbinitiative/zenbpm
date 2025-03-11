package rqlite

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/log"
)

var gen *snowflake.Node
var rqlitePersistence *BpmnEnginePersistenceRqlite
var ZenNode *cluster.ZenNode

func SetupTestEnvironment(m *testing.M) {
	// Set environment variables
	os.Setenv("PROFILE", "DEV")
	os.Setenv("CONFIG_FILE", "../../conf/zenbpm/conf-dev.yaml")

	// Setup
	appContext, _ := context.WithCancel(context.Background())

	// setup
	g, err := snowflake.NewNode(1)
	if err != nil {
		log.Errorf(appContext, "Error while initing snowflake: %s", err.Error())
	}
	gen = g

	conf := config.InitConfig()

	zenNode, err := cluster.StartZenNode(appContext, conf)
	if err != nil {
		log.Error("Failed to start Zen node: %s", err)
		os.Exit(1)
	}

	ZenNode = zenNode
	// give time to start
	time.Sleep(time.Second * 2)

	rqlitePersistence = NewBpmnEnginePersistenceRqlite(
		zenNode,
	)
	if err != nil {
		log.Errorf(appContext, "Error while initing persistence: %s", err.Error())
	}
}

func TeardownTestEnvironment(m *testing.M) {
	// Cleanup: Perform any additional cleanup if needed

	// Cleanup: Remove the partition-1 folder
	if err := os.RemoveAll("./partition-1"); err != nil {
		log.Errorf(context.Background(), "Error while removing partition-1 folder: %s", err.Error())
	}
}
