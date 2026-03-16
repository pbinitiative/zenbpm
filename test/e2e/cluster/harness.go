//go:build cluster_e2e

package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/grpc"
	"github.com/pbinitiative/zenbpm/internal/rest"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

// TestNode represents a single ZenBPM node in the test cluster.
type TestNode struct {
	ID          string
	ZenNode     *cluster.ZenNode
	RestServer  *rest.Server
	GrpcServer  *grpc.Server
	RestClient  *zenclient.ClientWithResponses
	RestAddr    string
	GrpcAddr    string
	ClusterAddr string // real cluster address
	ProxyAddr   string // address other nodes connect to (through proxy)
	Proxy       *NodeProxy
	TempDir     string
	Config      config.Config
	ctx         context.Context
	cancel      context.CancelFunc
	stopped     bool
}

// TestCluster manages a multi-node ZenBPM cluster for e2e testing.
type TestCluster struct {
	Nodes   []*TestNode
	BaseDir string
	opts    clusterOptions
}

type clusterOptions struct {
	partitions      int
	bootstrapExpect int
}

// ClusterOption configures a TestCluster.
type ClusterOption func(*clusterOptions)

// WithPartitions sets the desired partition count (default: 1).
func WithPartitions(n int) ClusterOption {
	return func(o *clusterOptions) {
		o.partitions = n
	}
}

// WithBootstrapExpect sets the bootstrap-expect count.
// If not set, defaults to the number of nodes.
func WithBootstrapExpect(n int) ClusterOption {
	return func(o *clusterOptions) {
		o.bootstrapExpect = n
	}
}

// NewTestCluster creates and starts a multi-node cluster.
// All nodes start simultaneously using bootstrap-expect for cluster formation.
func NewTestCluster(t *testing.T, nodeCount int, opts ...ClusterOption) *TestCluster {
	t.Helper()

	o := clusterOptions{
		partitions:      1,
		bootstrapExpect: nodeCount,
	}
	for _, opt := range opts {
		opt(&o)
	}

	baseDir := filepath.Join(os.TempDir(), fmt.Sprintf("zenbpm-cluster-e2e-%d", rand.Int()))
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		t.Fatalf("failed to create base temp dir: %s", err)
	}

	tc := &TestCluster{
		BaseDir: baseDir,
		opts:    o,
	}

	// Phase 1: Create all nodes and proxies (but don't start ZenNodes yet).
	// We need to know all proxy addresses before starting, so nodes can reference each other.
	type nodeSetup struct {
		conf    config.Config
		tempDir string
		nodeID  string
		proxy   *NodeProxy
		realLn  net.Listener
	}

	setups := make([]nodeSetup, nodeCount)

	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("test-node-%d", i+1)
		tempDir := filepath.Join(baseDir, nodeID)
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			t.Fatalf("failed to create node temp dir: %s", err)
		}

		// Bind a real listener for the cluster port to get a free port
		realLn, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen for node %s: %s", nodeID, err)
		}
		realAddr := realLn.Addr().String()
		realLn.Close() // Close it; StartZenNode will rebind

		// Create proxy pointing to the real address
		proxy, err := NewNodeProxy(realAddr)
		if err != nil {
			t.Fatalf("failed to create proxy for node %s: %s", nodeID, err)
		}

		setups[i] = nodeSetup{
			tempDir: tempDir,
			nodeID:  nodeID,
			proxy:   proxy,
			realLn:  realLn,
			conf: config.Config{
				Cluster: config.Cluster{
					NodeId: nodeID,
					Addr:   realAddr,
					Adv:    proxy.Addr(), // advertise the proxy address
					Raft: config.ClusterRaft{
						Dir:                   tempDir,
						JoinAttempts:          10,
						JoinInterval:          1 * time.Second,
						BootstrapExpect:       o.bootstrapExpect,
						BootstrapExpectTimeout: 30 * time.Second,
					},
				},
				HttpServer: config.HttpServer{
					Addr: "127.0.0.1:0", // will be overridden by listener
				},
				GrpcServer: config.GrpcServer{
					Addr: "127.0.0.1:0",
				},
			},
		}
	}

	// Phase 2: Set join addresses to all proxy addresses
	var allProxyAddrs []string
	for _, s := range setups {
		allProxyAddrs = append(allProxyAddrs, s.proxy.Addr())
	}
	for i := range setups {
		setups[i].conf.Cluster.Raft.JoinAddresses = allProxyAddrs
	}

	// Phase 3: Start all nodes concurrently.
	// StartZenNode blocks until bootstrap completes, so with bootstrap-expect > 1
	// all nodes must start in parallel.
	type nodeResult struct {
		node *TestNode
		err  error
	}
	results := make([]chan nodeResult, nodeCount)
	for i := range setups {
		results[i] = make(chan nodeResult, 1)
		go func(idx int) {
			s := &setups[idx]
			nodeCtx, nodeCancel := context.WithCancel(context.Background())

			zenNode, err := cluster.StartZenNode(nodeCtx, s.conf)
			if err != nil {
				nodeCancel()
				results[idx] <- nodeResult{err: fmt.Errorf("failed to start node %s: %w", s.nodeID, err)}
				return
			}

			// Start REST server
			restSrv := rest.NewServer(zenNode, s.conf)
			restLn := restSrv.Start()

			// Create REST client
			restClient, err := zenclient.NewClientWithResponses(
				"http://"+restLn.Addr().String()+"/v1",
				zenclient.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
			)
			if err != nil {
				nodeCancel()
				results[idx] <- nodeResult{err: fmt.Errorf("failed to create rest client for node %s: %w", s.nodeID, err)}
				return
			}

			// Start gRPC server on a free port
			grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				nodeCancel()
				results[idx] <- nodeResult{err: fmt.Errorf("failed to listen for grpc on node %s: %w", s.nodeID, err)}
				return
			}
			grpcAddr := grpcLn.Addr().String()
			grpcLn.Close()
			grpcSrv := grpc.NewServer(nodeCtx, zenNode, grpcAddr)
			grpcSrv.Start()

			results[idx] <- nodeResult{
				node: &TestNode{
					ID:          s.nodeID,
					ZenNode:     zenNode,
					RestServer:  restSrv,
					GrpcServer:  grpcSrv,
					RestClient:  restClient,
					RestAddr:    restLn.Addr().String(),
					GrpcAddr:    grpcAddr,
					ClusterAddr: s.conf.Cluster.Addr,
					ProxyAddr:   s.proxy.Addr(),
					Proxy:       s.proxy,
					TempDir:     s.tempDir,
					Config:      s.conf,
					ctx:         nodeCtx,
					cancel:      nodeCancel,
				},
			}
		}(i)
	}

	// Collect results
	for i := 0; i < nodeCount; i++ {
		r := <-results[i]
		if r.err != nil {
			tc.teardownPartial(t)
			t.Fatalf("%s", r.err)
		}
		tc.Nodes = append(tc.Nodes, r.node)
	}

	return tc
}

// AddNode adds a new node to an existing running cluster.
// The new node joins via the existing nodes' proxy addresses.
func (tc *TestCluster) AddNode(t *testing.T) *TestNode {
	t.Helper()

	idx := len(tc.Nodes) + 1
	nodeID := fmt.Sprintf("test-node-%d", idx)
	tempDir := filepath.Join(tc.BaseDir, nodeID)
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatalf("failed to create node temp dir: %s", err)
	}

	// Get a free port for the cluster address
	realLn, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for node %s: %s", nodeID, err)
	}
	realAddr := realLn.Addr().String()
	realLn.Close()

	proxy, err := NewNodeProxy(realAddr)
	if err != nil {
		t.Fatalf("failed to create proxy for node %s: %s", nodeID, err)
	}

	// Join addresses: all existing nodes' proxy addresses
	var joinAddrs []string
	for _, n := range tc.Nodes {
		joinAddrs = append(joinAddrs, n.ProxyAddr)
	}
	// Also include own proxy
	joinAddrs = append(joinAddrs, proxy.Addr())

	conf := config.Config{
		Cluster: config.Cluster{
			NodeId: nodeID,
			Addr:   realAddr,
			Adv:    proxy.Addr(),
			Raft: config.ClusterRaft{
				Dir:                   tempDir,
				JoinAttempts:          10,
				JoinInterval:          1 * time.Second,
				BootstrapExpect:       0, // join existing cluster, don't bootstrap
				BootstrapExpectTimeout: 30 * time.Second,
			},
		},
		HttpServer: config.HttpServer{Addr: "127.0.0.1:0"},
		GrpcServer: config.GrpcServer{Addr: "127.0.0.1:0"},
	}
	conf.Cluster.Raft.JoinAddresses = joinAddrs

	nodeCtx, nodeCancel := context.WithCancel(context.Background())

	zenNode, err := cluster.StartZenNode(nodeCtx, conf)
	if err != nil {
		nodeCancel()
		proxy.Close()
		t.Fatalf("failed to start new node %s: %s", nodeID, err)
	}

	restSrv := rest.NewServer(zenNode, conf)
	restLn := restSrv.Start()

	restClient, err := zenclient.NewClientWithResponses(
		"http://"+restLn.Addr().String()+"/v1",
		zenclient.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
	)
	if err != nil {
		nodeCancel()
		proxy.Close()
		t.Fatalf("failed to create rest client for node %s: %s", nodeID, err)
	}

	grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		nodeCancel()
		proxy.Close()
		t.Fatalf("failed to listen for grpc on node %s: %s", nodeID, err)
	}
	grpcAddr := grpcLn.Addr().String()
	grpcLn.Close()
	grpcSrv := grpc.NewServer(nodeCtx, zenNode, grpcAddr)
	grpcSrv.Start()

	node := &TestNode{
		ID:          nodeID,
		ZenNode:     zenNode,
		RestServer:  restSrv,
		GrpcServer:  grpcSrv,
		RestClient:  restClient,
		RestAddr:    restLn.Addr().String(),
		GrpcAddr:    grpcAddr,
		ClusterAddr: conf.Cluster.Addr,
		ProxyAddr:   proxy.Addr(),
		Proxy:       proxy,
		TempDir:     tempDir,
		Config:      conf,
		ctx:         nodeCtx,
		cancel:      nodeCancel,
	}
	tc.Nodes = append(tc.Nodes, node)
	return node
}

// Leader returns the current base cluster leader node, or nil if no leader.
func (tc *TestCluster) Leader() *TestNode {
	for _, n := range tc.Nodes {
		if n.stopped {
			continue
		}
		status := n.ZenNode.GetStatus()
		for _, node := range status.Nodes {
			if node.Id == n.ID && node.Role == state.RoleLeader {
				return n
			}
		}
	}
	return nil
}

// Followers returns all non-leader, non-stopped nodes.
func (tc *TestCluster) Followers() []*TestNode {
	leader := tc.Leader()
	var followers []*TestNode
	for _, n := range tc.Nodes {
		if n.stopped {
			continue
		}
		if leader != nil && n.ID == leader.ID {
			continue
		}
		followers = append(followers, n)
	}
	return followers
}

// NodeByID returns the TestNode with the given ID.
func (tc *TestCluster) NodeByID(id string) *TestNode {
	for _, n := range tc.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// RunningNodes returns all nodes that are not stopped.
func (tc *TestCluster) RunningNodes() []*TestNode {
	var running []*TestNode
	for _, n := range tc.Nodes {
		if !n.stopped {
			running = append(running, n)
		}
	}
	return running
}

// StopNode gracefully stops a node (it notifies the cluster before leaving).
func (tc *TestCluster) StopNode(t *testing.T, nodeID string) {
	t.Helper()
	n := tc.NodeByID(nodeID)
	if n == nil {
		t.Fatalf("node %s not found", nodeID)
	}
	if n.stopped {
		return
	}
	tc.stopNodeInternal(t, n)
}

// KillNode hard-kills a node (no graceful leave — simulates crash).
func (tc *TestCluster) KillNode(t *testing.T, nodeID string) {
	t.Helper()
	n := tc.NodeByID(nodeID)
	if n == nil {
		t.Fatalf("node %s not found", nodeID)
	}
	if n.stopped {
		return
	}
	// Blackhole the proxy first to simulate sudden disconnect
	n.Proxy.Blackhole()
	tc.stopNodeInternal(t, n)
}

// RestartNode restarts a previously stopped node with the same data directory.
func (tc *TestCluster) RestartNode(t *testing.T, nodeID string) {
	t.Helper()
	n := tc.NodeByID(nodeID)
	if n == nil {
		t.Fatalf("node %s not found", nodeID)
	}
	if !n.stopped {
		t.Fatalf("node %s is not stopped", nodeID)
	}

	// Restore proxy
	n.Proxy.UnblockAll()

	nodeCtx, nodeCancel := context.WithCancel(context.Background())

	// The node already has persisted state, so StartZenNode will detect it
	// and skip bootstrap
	zenNode, err := cluster.StartZenNode(nodeCtx, n.Config)
	if err != nil {
		nodeCancel()
		t.Fatalf("failed to restart node %s: %s", nodeID, err)
	}

	restSrv := rest.NewServer(zenNode, n.Config)
	restLn := restSrv.Start()

	restClient, err := zenclient.NewClientWithResponses(
		"http://"+restLn.Addr().String()+"/v1",
		zenclient.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
	)
	if err != nil {
		nodeCancel()
		t.Fatalf("failed to create rest client for restarted node %s: %s", nodeID, err)
	}

	grpcSrv := grpc.NewServer(nodeCtx, zenNode, n.GrpcAddr)
	grpcSrv.Start()

	n.ZenNode = zenNode
	n.RestServer = restSrv
	n.GrpcServer = grpcSrv
	n.RestClient = restClient
	n.RestAddr = restLn.Addr().String()
	n.ctx = nodeCtx
	n.cancel = nodeCancel
	n.stopped = false
}

// IsolateNode makes a node unreachable from all other nodes (bidirectional).
func (tc *TestCluster) IsolateNode(t *testing.T, nodeID string) {
	t.Helper()
	n := tc.NodeByID(nodeID)
	if n == nil {
		t.Fatalf("node %s not found", nodeID)
	}

	// Block inbound: blackhole this node's proxy
	n.Proxy.Blackhole()

	// Block outbound: tell other nodes' proxies to block traffic from this node
	for _, other := range tc.Nodes {
		if other.ID == nodeID {
			continue
		}
		other.Proxy.BlockPeer(n.ProxyAddr)
	}
}

// PartitionNetwork creates a network split between two groups of nodes.
func (tc *TestCluster) PartitionNetwork(t *testing.T, groupA, groupB []string) {
	t.Helper()
	for _, aID := range groupA {
		a := tc.NodeByID(aID)
		if a == nil {
			t.Fatalf("node %s not found", aID)
		}
		for _, bID := range groupB {
			b := tc.NodeByID(bID)
			if b == nil {
				t.Fatalf("node %s not found", bID)
			}
			a.Proxy.BlockPeer(b.ProxyAddr)
			b.Proxy.BlockPeer(a.ProxyAddr)
		}
	}
}

// HealNetwork restores all network connectivity.
func (tc *TestCluster) HealNetwork(t *testing.T) {
	t.Helper()
	for _, n := range tc.Nodes {
		n.Proxy.UnblockAll()
	}
}

// AddLatency injects latency to a specific node's proxy.
func (tc *TestCluster) AddLatency(t *testing.T, nodeID string, d time.Duration) {
	t.Helper()
	n := tc.NodeByID(nodeID)
	if n == nil {
		t.Fatalf("node %s not found", nodeID)
	}
	n.Proxy.SetLatency(d)
}

// Teardown stops all nodes and cleans up resources.
func (tc *TestCluster) Teardown(t *testing.T) {
	t.Helper()
	for _, n := range tc.Nodes {
		if !n.stopped {
			tc.stopNodeInternal(t, n)
		}
		if n.Proxy != nil {
			n.Proxy.Close()
		}
	}
	os.RemoveAll(tc.BaseDir)
}

func (tc *TestCluster) stopNodeInternal(t *testing.T, n *TestNode) {
	t.Helper()
	n.cancel()
	n.RestServer.Stop(n.ctx)
	n.GrpcServer.Stop()
	if err := n.ZenNode.Stop(); err != nil {
		t.Logf("warning: error stopping node %s: %s", n.ID, err)
	}
	n.stopped = true
}

// teardownPartial cleans up during failed cluster creation.
func (tc *TestCluster) teardownPartial(t *testing.T) {
	t.Helper()
	for _, n := range tc.Nodes {
		if !n.stopped {
			tc.stopNodeInternal(t, n)
		}
		if n.Proxy != nil {
			n.Proxy.Close()
		}
	}
	if tc.BaseDir != "" {
		os.RemoveAll(tc.BaseDir)
	}
}
