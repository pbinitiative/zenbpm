package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestIsExpectedServeError(t *testing.T) {
	if !isExpectedServeError(net.ErrClosed) {
		t.Fatal("net.ErrClosed must be treated as an expected shutdown")
	}
	if !isExpectedServeError(grpc.ErrServerStopped) {
		t.Fatal("grpc.ErrServerStopped must be treated as an expected shutdown")
	}
	if !isExpectedServeError(errors.New("network connection closed")) {
		t.Fatal("rqlite mux close must be treated as an expected shutdown")
	}
	if isExpectedServeError(errors.New("accept failed")) {
		t.Fatal("unexpected listener failures must still be reported")
	}
}

func TestServer(t *testing.T) {
	ctx := t.Context()

	mux, _, err := network.NewNodeMux("")
	if err != nil {
		t.Fatalf("failed to create new mux: %s", err)
	}
	cLn := network.NewZenBpmClusterListener(mux)
	tStore := &testStore{}
	srv := New(cLn, tStore, nil, nil, nil)
	err = srv.Open()
	if err != nil {
		t.Fatalf("failed to start server: %s", err)
	}

	dialer := network.NewZenBpmClusterDialer()
	fmt.Println(cLn.Addr().String())
	grpcClient, err := grpc.NewClient(cLn.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return dialer.DialGRPC(s)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create grpc client: %s", err)
	}
	zsc := proto.NewZenServiceClient(grpcClient)

	_, err = zsc.Notify(ctx, &proto.NotifyRequest{
		Id:      new("123"),
		Address: new("local-1.cluster"),
	})
	if err != nil {
		t.Fatalf("failed to notify server: %s", err)
	}
	if tStore.notify == nil || tStore.notify.GetId() != "123" {
		t.Fatalf("unexpected notify result")
	}

	_, err = zsc.Join(ctx, &proto.JoinRequest{
		Id:      new("123"),
		Address: new("local-1.cluster"),
		Voter:   new(true),
	})
	if err != nil {
		t.Fatalf("failed to notify server: %s", err)
	}
	if tStore.join == nil || tStore.join.GetId() != "123" || tStore.join.GetAddress() != "local-1.cluster" {
		t.Fatalf("unexpected join result")
	}
}

func TestServerTCPHeaderMux(t *testing.T) {
	ctx := t.Context()

	mux, _, err := network.NewNodeMux("")
	if err != nil {
		t.Fatalf("failed to create new mux: %s", err)
	}
	cLn := network.NewZenBpmClusterListener(mux)
	tStore := &testStore{}
	srv := New(cLn, tStore, nil, nil, nil)
	err = srv.Open()
	if err != nil {
		t.Fatalf("failed to start server: %s", err)
	}

	// create bad dialer (server is multiplexed into Cluster header)
	dialer := network.NewZenBpmRaftDialer()
	grpcClient, err := grpc.NewClient(cLn.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return dialer.Dial(s, 1*time.Second)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create grpc client: %s", err)
	}
	zsc := proto.NewZenServiceClient(grpcClient)
	_, err = zsc.Notify(ctx, &proto.NotifyRequest{})
	if err == nil {
		t.Fatalf("expected a failed grpc call with bad dialer: %s", err)
	}

	// create good dialer
	grpcdialer := network.NewZenBpmClusterDialer()
	grpcClient, err = grpc.NewClient(cLn.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return grpcdialer.DialGRPC(s)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create grpc client: %s", err)
	}
	zsc = proto.NewZenServiceClient(grpcClient)
	_, err = zsc.Notify(ctx, &proto.NotifyRequest{})
	if err != nil {
		t.Fatalf("expected a failed grpc call with bad dialer: %s", err)
	}
}

func TestPartitionNodeLeaderChange_WritesLeaderAndDemotesOldLeader(t *testing.T) {
	ctx := t.Context()
	tStore := &testStore{
		clusterState: state.Cluster{
			Partitions: map[uint32]state.Partition{
				1: {Id: 1, LeaderId: "old-node"},
			},
			Nodes: map[string]state.Node{},
		},
	}
	srv := &Server{store: tStore}

	resp, err := srv.PartitionNodeLeaderChange(ctx, &proto.PartitionNodeLeaderChangeRequest{
		Id:        new("new-node"),
		Partition: new(uint32(1)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if len(tStore.partitionChangeWrites) != 2 {
		t.Fatalf("expected 2 writes, got %d", len(tStore.partitionChangeWrites))
	}

	// First write: demote old leader
	demote := tStore.partitionChangeWrites[0]
	if demote.GetNodeId() != "old-node" {
		t.Errorf("first write should demote old-node, got %s", demote.GetNodeId())
	}
	if demote.GetRole() != protoc.Role_ROLE_TYPE_FOLLOWER {
		t.Errorf("first write should be FOLLOWER, got %s", demote.GetRole())
	}
	if demote.GetPartitionId() != 1 {
		t.Errorf("first write should be partition 1, got %d", demote.GetPartitionId())
	}

	// Second write: promote new leader
	promote := tStore.partitionChangeWrites[1]
	if promote.GetNodeId() != "new-node" {
		t.Errorf("second write should promote new-node, got %s", promote.GetNodeId())
	}
	if promote.GetRole() != protoc.Role_ROLE_TYPE_LEADER {
		t.Errorf("second write should be LEADER, got %s", promote.GetRole())
	}
}

func TestPartitionNodeLeaderChange_SameLeaderIsNoOp(t *testing.T) {
	ctx := t.Context()
	tStore := &testStore{
		clusterState: state.Cluster{
			Partitions: map[uint32]state.Partition{
				1: {Id: 1, LeaderId: "same-node"},
			},
			Nodes: map[string]state.Node{},
		},
	}
	srv := &Server{store: tStore}

	_, err := srv.PartitionNodeLeaderChange(ctx, &proto.PartitionNodeLeaderChangeRequest{
		Id:        new("same-node"),
		Partition: new(uint32(1)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Only one write — the leader re-confirmation. No demotion of itself.
	if len(tStore.partitionChangeWrites) != 1 {
		t.Fatalf("expected 1 write, got %d", len(tStore.partitionChangeWrites))
	}
	w := tStore.partitionChangeWrites[0]
	if w.GetNodeId() != "same-node" || w.GetRole() != protoc.Role_ROLE_TYPE_LEADER {
		t.Errorf("expected same-node/LEADER write, got %s/%s", w.GetNodeId(), w.GetRole())
	}
}

func TestPartitionNodeLeaderChange_FirstElection(t *testing.T) {
	ctx := t.Context()
	// Partition not yet tracked — this is the first election after partition creation.
	tStore := &testStore{
		clusterState: state.Cluster{
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
	}
	srv := &Server{store: tStore}

	_, err := srv.PartitionNodeLeaderChange(ctx, &proto.PartitionNodeLeaderChangeRequest{
		Id:        new("node-a"),
		Partition: new(uint32(1)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Only one write — promotion. Nothing to demote.
	if len(tStore.partitionChangeWrites) != 1 {
		t.Fatalf("expected 1 write, got %d", len(tStore.partitionChangeWrites))
	}
	w := tStore.partitionChangeWrites[0]
	if w.GetNodeId() != "node-a" || w.GetRole() != protoc.Role_ROLE_TYPE_LEADER {
		t.Errorf("expected node-a/LEADER write, got %s/%s", w.GetNodeId(), w.GetRole())
	}
}

type testStore struct {
	notify                *proto.NotifyRequest
	join                  *proto.JoinRequest
	writeNodeChange       *protoc.NodeChange
	partitionChangeWrites []*protoc.NodePartitionChange
	clusterState          state.Cluster
}

var _ StoreService = &testStore{}

func (s *testStore) Notify(nr *proto.NotifyRequest) error {
	s.notify = nr
	return nil
}
func (s *testStore) Join(jr *proto.JoinRequest) error {
	s.join = jr
	return nil
}
func (s *testStore) WriteNodeChange(change *protoc.NodeChange) error {
	s.writeNodeChange = change
	return nil
}
func (s *testStore) WritePartitionChange(change *protoc.NodePartitionChange) error {
	s.partitionChangeWrites = append(s.partitionChangeWrites, change)
	return nil
}
func (s *testStore) ClusterState() state.Cluster {
	return s.clusterState
}

func TestTimerStateToActivityState(t *testing.T) {
	tests := []struct {
		name      string
		input     int64
		wantState int64
		wantErr   bool
	}{
		{"created→active", int64(runtime.TimerStateCreated), int64(runtime.ActivityStateActive), false},
		{"triggered→completed", int64(runtime.TimerStateTriggered), int64(runtime.ActivityStateCompleted), false},
		{"cancelled→withdrawn", int64(runtime.TimerStateCancelled), int64(runtime.ActivityStateWithdrawn), false},
		{"unknown→error", 999, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := timerStateToActivityState(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tt.wantState {
				t.Errorf("got %d, want %d", got, tt.wantState)
			}
		})
	}
}

func TestErrorStateToActivityState(t *testing.T) {
	tests := []struct {
		name      string
		input     int64
		wantState int64
		wantErr   bool
	}{
		{"created→active", int64(runtime.ErrorStateCreated), int64(runtime.ActivityStateActive), false},
		{"cancelled→withdrawn", int64(runtime.ErrorStateCancelled), int64(runtime.ActivityStateWithdrawn), false},
		{"unknown→error", 999, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := errorStateToActivityState(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tt.wantState {
				t.Errorf("got %d, want %d", got, tt.wantState)
			}
		})
	}
}

func TestTimerStateRoundTrip(t *testing.T) {
	cases := []struct {
		timer    runtime.TimerState
		activity runtime.ActivityState
	}{
		{runtime.TimerStateCreated, runtime.ActivityStateActive},
		{runtime.TimerStateTriggered, runtime.ActivityStateCompleted},
		{runtime.TimerStateCancelled, runtime.ActivityStateWithdrawn},
	}
	seen := map[int64]runtime.TimerState{}
	for _, c := range cases {
		actState, err := timerStateToActivityState(int64(c.timer))
		if err != nil {
			t.Fatalf("timerStateToActivityState(%v): %v", c.timer, err)
		}
		if prev, exists := seen[actState]; exists {
			t.Errorf("ActivityState %d is shared by TimerState %v and %v", actState, prev, c.timer)
		}
		seen[actState] = c.timer
		if actState != int64(c.activity) {
			t.Errorf("timerStateToActivityState(%v) = %d, want %d", c.timer, actState, c.activity)
		}
	}
}

func TestErrorStateRoundTrip(t *testing.T) {
	cases := []struct {
		errState runtime.ErrorState
		activity runtime.ActivityState
	}{
		{runtime.ErrorStateCreated, runtime.ActivityStateActive},
		{runtime.ErrorStateCancelled, runtime.ActivityStateWithdrawn},
	}
	seen := map[int64]runtime.ErrorState{}
	for _, c := range cases {
		actState, err := errorStateToActivityState(int64(c.errState))
		if err != nil {
			t.Fatalf("errorStateToActivityState(%v): %v", c.errState, err)
		}
		if prev, exists := seen[actState]; exists {
			t.Errorf("ActivityState %d is shared by ErrorState %v and %v", actState, prev, c.errState)
		}
		seen[actState] = c.errState
		if actState != int64(c.activity) {
			t.Errorf("errorStateToActivityState(%v) = %d, want %d", c.errState, actState, c.activity)
		}
	}
}
