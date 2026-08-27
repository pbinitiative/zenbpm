package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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

// e2eMaxProcessInstanceNestingDepth is the engine nesting depth limit configured for the e2e node in
// TestMain. It is intentionally small so TestNestingDepthExceededCreatesIncident stays fast,
// while leaving plenty of headroom for the legitimate nesting (max depth 3) used by other tests.
const e2eMaxProcessInstanceNestingDepth = 10

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
	Git           systemStatusGit   `json:"git"`
	Build         systemStatusBuild `json:"build"`
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

type systemStatusGit struct {
	Branch   string `json:"branch"`
	CommitID string `json:"commitId"`
}

type systemStatusBuild struct {
	Version string `json:"version"`
	Time    string `json:"time"`
}

func TestMain(m *testing.M) {
	log.Init()
	if os.Getenv("POLL_TIMER_DELAY_SECONDS") == "" {
		if err := os.Setenv("POLL_TIMER_DELAY_SECONDS", "1"); err != nil {
			log.Error("Failed to set POLL_TIMER_DELAY_SECONDS: %s", err)
			os.Exit(1)
		}
	}
	// This suite asserts the exact limit below, so do not inherit an ambient value.
	if err := os.Setenv("CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH", strconv.Itoa(e2eMaxProcessInstanceNestingDepth)); err != nil {
		log.Error("Failed to set CLUSTER_ENGINE_MAX_PROCESS_INSTANCE_NESTING_DEPTH: %s", err)
		os.Exit(1)
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
	buildInfo := buildinfo.Current()
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
// instance until none are left. Listing has no server-side filter on
// processType, and the default sort order (created_at DESC) puts child
// instances (subprocesses, multi-instance children - created after their
// parents) ahead of their parents. So a page can be entirely non-default
// instances; when that happens we advance to the next page instead of
// bailing out, otherwise a parent buried behind 100+ children would never
// get cancelled. Whenever something *is* cancelled on a page we reset back
// to page 1, since cancelled instances drop out of the "active"/"failed"
// result set and shift subsequent pages. It is bounded by
// cancelInstancesInStateMaxPages to guarantee termination even if a page
// keeps yielding instances that never leave the queried state.
func cancelInstancesInState(t *testing.T, instanceState zenclient.GetProcessInstancesParamsState) {
	t.Helper()
	const pageSize = int32(100)
	page := int32(1)
	for range cancelInstancesInStateMaxPages {
		items := listProcessInstancesInStateOrFail(t, instanceState, page, pageSize)
		if len(items) == 0 {
			return
		}
		cancelled := 0
		for i := range items {
			if items[i].ProcessType != "default" {
				continue
			}
			cancelProcessInstanceOrFail(t, items[i].Key, instanceState)
			cancelled++
		}
		if cancelled == 0 {
			if int32(len(items)) < pageSize {
				// Whole remainder of the result set is non-default; nothing
				// left for us to cancel directly.
				return
			}
			// A full page of non-default instances; look past them.
			page++
			continue
		}
		// Cancelled instances drop out of the result set, shifting later
		// pages back; restart from page 1 to avoid skipping instances.
		page = 1
	}
	t.Fatalf("cancelInstancesInState did not converge after %d pages for state %s", cancelInstancesInStateMaxPages, instanceState)
}

// cancelProcessInstanceOrFail cancels a single process instance. A 404
// (instance not found) or 409 (instance not in cancellable state) is treated
// as "already gone": these are normal races during cleanup, because an
// instance listed as active may finish or terminate on its own (timers,
// message events, receive tasks) before the cancel reaches it. Any other
// non-204 response or a transport error stops the test immediately
// (t.Fatalf), rather than letting the caller keep retrying against the same
// failing instance indefinitely.
func cancelProcessInstanceOrFail(t *testing.T, key int64, instanceState zenclient.GetProcessInstancesParamsState) {
	t.Helper()
	resp, err := app.restClient.CancelProcessInstanceWithResponse(context.Background(), key)
	if !assert.NoError(t, err) || !assert.NotNil(t, resp) {
		t.Fatalf("failed to cancel process instance %v in state %s", key, instanceState)
	}
	switch resp.StatusCode() {
	case http.StatusNoContent:
		// cancelled
	case http.StatusNotFound, http.StatusConflict:
		// instance already finished or is no longer cancellable; treat as gone
	default:
		t.Fatalf("failed to cancel process instance %v in state %s: status %s", key, instanceState, resp.Status())
	}
}

// listProcessInstancesInStateOrFail fetches one page of process instances in
// the given state and fails the test on any transport or non-200 error.
// Returns nil when there are no partitions with instances left.
func listProcessInstancesInStateOrFail(t *testing.T, instanceState zenclient.GetProcessInstancesParamsState, page int32, size int32) []zenclient.ProcessInstancesSimple {
	t.Helper()
	processInstances, err := app.restClient.GetProcessInstancesWithResponse(context.Background(), &zenclient.GetProcessInstancesParams{
		State: &instanceState,
		Page:  &page,
		Size:  &size,
	})
	if !assert.NoError(t, err) || !assert.NotNil(t, processInstances) {
		t.Fatalf("failed to list process instances in state %s", instanceState)
	}
	if processInstances.StatusCode() != http.StatusOK || processInstances.JSON200 == nil {
		t.Fatalf("failed to list process instances in state %s: status %s", instanceState, processInstances.Status())
	}
	if len(processInstances.JSON200.Partitions) == 0 {
		return nil
	}
	return processInstances.JSON200.Partitions[0].Items
}
