package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer(t *testing.T) {
	ctx := t.Context()

	mux, err := network.NewMux("")
	if err != nil {
		t.Fatalf("failed to create new mux: %s", err)
	}
	cLn := network.NewZenBpmClusterListener(mux)
	tStore := &testStore{}
	srv := New(cLn, tStore)
	err = srv.Open()
	if err != nil {
		t.Fatalf("failed to start server: %s", err)
	}

	dialer := network.NewZenBpmClusterDialer()
	fmt.Println(cLn.Addr().String())
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

	_, err = zsc.Notify(ctx, &proto.NotifyRequest{
		Id:      "123",
		Address: "local-1.cluster",
	})
	if err != nil {
		t.Fatalf("failed to notify server: %s", err)
	}
	if tStore.notify == nil || tStore.notify.Id != "123" {
		t.Fatalf("unexpected notify result")
	}

	_, err = zsc.Join(ctx, &proto.JoinRequest{
		Id:      "123",
		Address: "local-1.cluster",
		Voter:   true,
	})
	if err != nil {
		t.Fatalf("failed to notify server: %s", err)
	}
	if tStore.join == nil || tStore.join.Id != "123" || tStore.join.Address != "local-1.cluster" {
		t.Fatalf("unexpected join result")
	}
}

func TestServerTCPHeaderMux(t *testing.T) {
	ctx := t.Context()

	mux, err := network.NewMux("")
	if err != nil {
		t.Fatalf("failed to create new mux: %s", err)
	}
	cLn := network.NewZenBpmClusterListener(mux)
	tStore := &testStore{}
	srv := New(cLn, tStore)
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
	dialer = network.NewZenBpmClusterDialer()
	grpcClient, err = grpc.NewClient(cLn.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return dialer.Dial(s, 1*time.Second)
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

type testStore struct {
	notify               *proto.NotifyRequest
	join                 *proto.JoinRequest
	writeNodeChange      *protoc.NodeChange
	writePartitionChange *protoc.NodePartitionChange
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
	s.writePartitionChange = change
	return nil
}
