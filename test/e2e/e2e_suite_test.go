package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/grpc"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/otel"
	"github.com/pbinitiative/zenbpm/internal/rest"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

var app Application

type testMainWithCleanup struct {
	testMain *testing.M
	cleanup  func()
}

func (m testMainWithCleanup) Run() int {
	exitCode := m.testMain.Run()
	m.cleanup()
	return exitCode
}

type ClusterStatus struct {
	Version       string `json:"version"`
	Commit        string `json:"commit"`
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
	if os.Getenv("POLL_TIMER_DELAY_SECONDS") == "" {
		if err := os.Setenv("POLL_TIMER_DELAY_SECONDS", "1"); err != nil {
			log.Error("Failed to set POLL_TIMER_DELAY_SECONDS: %s", err)
			os.Exit(1)
		}
	}
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
	buildInfo, err := buildinfo.Current()
	if err != nil {
		log.Warn("Failed to resolve build info: %s", err)
	}
	svr := rest.NewServer(zenNode, conf, buildInfo)
	ln := svr.Start()

	// Create rest client
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	client, err := zenclient.NewClientWithResponses(
		"http://"+ln.Addr().String()+"/v1",
		zenclient.WithHTTPClient(httpClient),
	)
	if err != nil {
		log.Error("failed to create rest client: %s", err)
		os.Exit(1)
	}

	app = Application{
		httpAddr:   ln.Addr().String(),
		node:       zenNode,
		restClient: client,
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

	goleak.VerifyTestMain(
		testMainWithCleanup{
			testMain: m,
			cleanup: func() {
				svr.Stop(appContext)
				grpcSrv.Stop()
				if err := zenNode.Stop(); err != nil {
					log.Error("failed to properly stop zen node: %s", err)
				}
				openTelemetry.Stop(appContext)
				ctxCancel()
				err := os.RemoveAll(tempDir)
				if err != nil {
					return
				}
			},
		},
	)
}

func cleanProcessInstances(t *testing.T) {
	cancelInstancesInState(t, "failed")
	cancelInstancesInState(t, "active")
}

// cancelInstancesInStateMaxPages bounds the number of paging iterations
// cancelInstancesInState will perform. It exists so a stuck cancellation
// (e.g. a repeatedly failing CancelProcessInstance call, or eventual
// consistency causing an instance to keep reading as active) turns into a
// failed test instead of a hung CI job.
const cancelInstancesInStateMaxPages = 50

// cancelInstancesInState pages through the process instances in the given
// state (the spec bounds page size to 1..100) and cancels every default-type
// instance until none are left. It is bounded by
// cancelInstancesInStateMaxPages to guarantee termination even if a page
// keeps yielding instances that never leave the queried state.
func cancelInstancesInState(t *testing.T, instanceState zenclient.GetProcessInstancesParamsState) {
	t.Helper()
	for range cancelInstancesInStateMaxPages {
		processInstances, err := app.restClient.GetProcessInstancesWithResponse(context.Background(), &zenclient.GetProcessInstancesParams{
			State: &instanceState,
			Size:  new(int32(100)),
		})
		assert.NoError(t, err)
		if processInstances == nil || processInstances.JSON200 == nil || len(processInstances.JSON200.Partitions) == 0 {
			return
		}
		items := processInstances.JSON200.Partitions[0].Items
		cancelled := 0
		for i := range items {
			if items[i].ProcessType != ("default") {
				continue
			}
			cancelProcessInstanceOrFail(t, items[i].Key, instanceState)
			cancelled++
		}
		if cancelled == 0 {
			assert.Equal(t, 0, len(items))
			return
		}
	}
	t.Fatalf("cancelInstancesInState did not converge after %d pages for state %s", cancelInstancesInStateMaxPages, instanceState)
}

// cancelProcessInstanceOrFail cancels a single process instance and stops the
// test immediately (t.Fatalf) if the call errors or returns a non-2xx
// response, rather than letting the caller keep retrying against the same
// failing instance indefinitely.
func cancelProcessInstanceOrFail(t *testing.T, key int64, instanceState zenclient.GetProcessInstancesParamsState) {
	t.Helper()
	resp, err := app.restClient.CancelProcessInstanceWithResponse(context.Background(), key)
	if !assert.NoError(t, err) || !assert.NotNil(t, resp) {
		t.Fatalf("failed to cancel process instance %v in state %s", key, instanceState)
	}
	if !assert.Nil(t, resp.JSON400) ||
		!assert.Nil(t, resp.JSON500) ||
		!assert.Nil(t, resp.JSON502) {
		t.Fatalf("failed to cancel process instance %v in state %s", key, instanceState)
	}
}
