//go:build acceptance

package acceptance_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	igrpc "github.com/pbinitiative/zenbpm/internal/grpc"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/otel"
	"github.com/pbinitiative/zenbpm/internal/rest"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

// app is the shared test application handle used by all acceptance tests.
var app acceptanceApp

type acceptanceApp struct {
	httpAddr   string
	grpcAddr   string
	node       *cluster.ZenNode
	restClient *zenclient.ClientWithResponses
}

func TestMain(m *testing.M) {
	log.Init()
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	appContext, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	conf := config.InitConfig()
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("zenbpm-acceptance-test-%d", rand.Int()))
	if err != nil {
		log.Error("Failed to create temp dir: %s", err)
		return 1
	}
	defer os.RemoveAll(tempDir)
	conf.Cluster.Raft.Dir = tempDir

	openTelemetry, err := otel.SetupOtel(conf.Tracing)
	if err != nil {
		log.Error("Failed to setup otel: %s", err)
		return 1
	}
	defer openTelemetry.Stop(appContext)

	zenNode, err := cluster.StartZenNode(appContext, conf)
	if err != nil {
		log.Error("Failed to start Zen node: %s", err)
		return 1
	}
	defer func() { _ = zenNode.Stop() }()

	svr := rest.NewServer(zenNode, conf, "test")
	ln := svr.Start()
	defer svr.Stop(appContext)

	grpcSrv := igrpc.NewServer(appContext, zenNode, conf.GrpcServer.Addr)
	grpcSrv.Start()
	defer grpcSrv.Stop()

	httpClient := &http.Client{Timeout: 30 * time.Second}
	restClient, err := zenclient.NewClientWithResponses(
		"http://"+ln.Addr().String()+"/v1",
		zenclient.WithHTTPClient(httpClient),
	)
	if err != nil {
		log.Error("Failed to create rest client: %s", err)
		return 1
	}

	app = acceptanceApp{
		httpAddr:   ln.Addr().String(),
		grpcAddr:   conf.GrpcServer.Addr,
		node:       zenNode,
		restClient: restClient,
	}

	timeout := time.Now().Add(30 * time.Second)
	for {
		time.Sleep(100 * time.Millisecond)
		if time.Now().After(timeout) {
			fmt.Println("Cluster failed to become ready before timeout")
			return 1
		}

		s := zenNode.GetStatus()
		if node, ok := s.Nodes[conf.Cluster.NodeId]; ok {
			if partition, ok := node.Partitions[1]; ok {
				if partition.Role == state.RoleLeader &&
					partition.State == state.NodePartitionStateInitialized {
					break
				}
			}
		}
	}

	return m.Run()
}
