package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/grpc"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/otel"
	"github.com/pbinitiative/zenbpm/internal/rest"
)

var app Application

type ClusterStatus struct {
	ClusterConfig struct {
		DesiredPartitions int64 `json:"desiredPartitions"`
	} `json:"clusterConfig"`
	Nodes map[string]struct {
		Addr       string `json:"addr"`
		ID         string `json:"id"`
		Partitions map[string]struct {
			ID    int64 `json:"id"`
			Role  int64 `json:"role"`
			State int64 `json:"state"`
		} `json:"partitions"`
		Role     int64 `json:"role"`
		State    int64 `json:"state"`
		Suffrage int64 `json:"suffrage"`
	} `json:"nodes"`
	Partitions map[string]struct {
		ID       int64  `json:"id"`
		LeaderID string `json:"leaderId"`
	} `json:"partitions"`
}

func TestMain(m *testing.M) {
	log.Init()
	appContext, ctxCancel := context.WithCancel(context.Background())
	conf := config.InitConfig()
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("zenbpm-e2e-test-%d", rand.Int()))
	conf.Cluster.Raft.Dir = tempDir
	openTelemetry, err := otel.SetupOtel(conf.Tracing)
	if err != nil {
		log.Error("Failed to start Zen node: %s", err)
		os.Exit(1)
	}
	zenNode, err := cluster.StartZenNode(appContext, conf)
	if err != nil {
		log.Error("Failed to start Zen node: %s", err)
		os.Exit(1)
	}
	// Start the public API
	svr := rest.NewServer(zenNode, conf)
	ln := svr.Start()

	app = Application{
		httpAddr: ln.Addr().String(),
		node:     zenNode,
	}

	// Start ZenBpm GRPC API
	grpcSrv := grpc.NewServer(appContext, zenNode, conf.GrpcServer.Addr)
	grpcSrv.Start()
	app.grpcAddr = conf.GrpcServer.Addr

	// wait until node is ready
	timeout := time.Now().Add(30 * time.Second)
	for {
		time.Sleep(1)
		if time.Now().After(timeout) {
			fmt.Println("Node failed to start until timeout was reached")
			os.Exit(1)
		}
		s := ClusterStatus{}
		resp, err := app.NewRequest(nil).WithPath("/system/status").DoOk()
		_ = err
		_ = json.Unmarshal(resp, &s)
		nodePartition := s.Nodes["test-node-1"].Partitions["1"]
		if len(s.Partitions) > 0 && len(s.Nodes) > 0 &&
			nodePartition.Role == int64(state.RoleLeader) &&
			nodePartition.State == int64(state.NodePartitionStateInitialized) {
			break
		}
	}

	code := m.Run()

	// cleanup after the app
	ctxCancel()
	// cleanup
	svr.Stop(appContext)
	grpcSrv.Stop()
	err = zenNode.Stop()
	if err != nil {
		log.Error("failed to properly stop zen node: %s", err)
	}
	openTelemetry.Stop(appContext)
	// os.RemoveAll(tempDir)
	os.Exit(code)
}
