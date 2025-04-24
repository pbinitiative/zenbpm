package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/server/servertest"
	"github.com/rqlite/rqlite/v8/cluster"
)

const numAttempts int = 3
const attemptInterval = 1 * time.Second

func Test_SingleJoinOK(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()
	srv.JoinHandler = func(jr *proto.JoinRequest) (*proto.JoinResponse, error) {
		if jr == nil {
			t.Fatal("join request is nil")
		}
		if exp, got := "id0", jr.Id; exp != got {
			t.Fatalf("unexpected id, got %s, exp: %s", got, exp)
		}
		if exp, got := "1.2.3.4", jr.Address; exp != got {
			t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
		}
		return &proto.JoinResponse{}, nil
	}

	clientMgr := client.NewClientManager(nil)
	joiner := NewJoiner(clientMgr, numAttempts, attemptInterval)
	addr, err := joiner.Do(context.Background(), []string{srv.Addr()}, "id0", "1.2.3.4", cluster.Voter)
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := srv.Addr(), addr; exp != got {
		t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
	}
}

func Test_SingleJoinZeroAttempts(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()
	srv.GlobalHandler = func() error {
		t.Fatalf("handler should not have been called")
		return nil
	}

	clientMgr := client.NewClientManager(nil)
	joiner := NewJoiner(clientMgr, 0, attemptInterval)
	_, err := joiner.Do(context.Background(), []string{srv.Addr()}, "id0", "1.2.3.4", cluster.Voter)
	if err != ErrJoinFailed {
		t.Fatalf("Incorrect error returned when zero attempts specified")
	}
}

func Test_SingleJoinFail(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()
	srv.JoinHandler = func(jr *proto.JoinRequest) (*proto.JoinResponse, error) {
		return &proto.JoinResponse{}, fmt.Errorf("bad request")
	}

	clientMgr := client.NewClientManager(nil)
	joiner := NewJoiner(clientMgr, numAttempts, attemptInterval)
	_, err := joiner.Do(context.Background(), []string{srv.Addr()}, "id0", "1.2.3.4", cluster.Voter)
	if err == nil {
		t.Fatalf("expected error when joining bad node")
	}
}

func Test_SingleJoinCancel(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()
	srv.JoinHandler = func(jr *proto.JoinRequest) (*proto.JoinResponse, error) {
		return &proto.JoinResponse{}, fmt.Errorf("bad request")
	}

	// Cancel the join in a few seconds time.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()

	clientMgr := client.NewClientManager(nil)
	joiner := NewJoiner(clientMgr, 10, attemptInterval)
	_, err := joiner.Do(ctx, []string{srv.Addr()}, "id0", "1.2.3.4", cluster.Voter)
	if err != ErrJoinCanceled {
		t.Fatalf("incorrect error returned when canceling: %s", err)
	}
}

func Test_DoubleJoinOKSecondNode(t *testing.T) {
	srv1 := servertest.NewTestServer()
	defer srv1.Close()
	srv1.JoinHandler = func(jr *proto.JoinRequest) (*proto.JoinResponse, error) {
		return &proto.JoinResponse{}, fmt.Errorf("bad request")
	}

	srv2 := servertest.NewTestServer()
	defer srv2.Close()
	srv2.JoinHandler = func(jr *proto.JoinRequest) (*proto.JoinResponse, error) {
		return &proto.JoinResponse{}, nil
	}

	clientMgr := client.NewClientManager(nil)
	joiner := NewJoiner(clientMgr, numAttempts, attemptInterval)
	addr, err := joiner.Do(context.Background(), []string{srv1.Addr(), srv2.Addr()}, "id0", "1.2.3.4", cluster.Voter)
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := srv2.Addr(), addr; exp != got {
		t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
	}
}
