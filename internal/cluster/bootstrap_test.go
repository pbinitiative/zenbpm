package cluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/server/servertest"
	"github.com/rqlite/rqlite/v8/cluster"
)

func Test_AddressProviderString(t *testing.T) {
	a := []string{"a", "b", "c"}
	p := NewAddressProviderString(a)
	b, err := p.Lookup()
	if err != nil {
		t.Fatalf("failed to lookup addresses: %s", err.Error())
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("failed to get correct addresses")
	}
}

func Test_NewBootstrapper(t *testing.T) {
	bs := NewBootstrapper(nil, nil)
	if bs == nil {
		t.Fatalf("failed to create a simple Bootstrapper")
	}
	if exp, got := cluster.BootUnknown, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootDoneImmediately(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()

	srv.JoinHandler = func(jr *proto.JoinRequest) (*proto.JoinResponse, error) {
		t.Fatalf("client made request")
		return &proto.JoinResponse{}, nil
	}

	done := func() bool {
		return true
	}
	p := NewAddressProviderString([]string{srv.Addr()})
	bs := NewBootstrapper(p, nil)
	if err := bs.Boot(context.Background(), "node1", "192.168.1.1:1234", cluster.Voter, done, 10*time.Second); err != nil {
		t.Fatalf("failed to boot: %s", err)
	}
	if exp, got := cluster.BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootTimeout(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()
	srv.JoinHandler = func(jr *proto.JoinRequest) (*proto.JoinResponse, error) {
		return nil, fmt.Errorf("not a cluster")
	}
	srv.NotifyHandler = func(nr *proto.NotifyRequest) (*proto.NotifyResponse, error) {
		time.Sleep(10 * time.Second)
		return nil, fmt.Errorf("some err")
	}

	done := func() bool {
		return false
	}
	p := NewAddressProviderString([]string{srv.Addr()})
	clientMgr := client.NewClientManager(nil)
	bs := NewBootstrapper(p, clientMgr)
	bs.Interval = time.Second
	err := bs.Boot(context.Background(), "node1", "192.168.1.1:1234", cluster.Voter, done, 5*time.Second)
	if err == nil {
		t.Fatalf("no error returned from timed-out boot")
	}
	if !errors.Is(err, cluster.ErrBootTimeout) {
		t.Fatalf("wrong error returned")
	}
	if exp, got := cluster.BootTimeout, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootCanceled(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()

	done := func() bool {
		return false
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := NewAddressProviderString([]string{srv.Addr()})
	clientMgr := client.NewClientManager(nil)
	bs := NewBootstrapper(p, clientMgr)
	bs.Interval = time.Second
	err := bs.Boot(ctx, "node1", "192.168.1.1:1234", cluster.Voter, done, 5*time.Second)
	if err == nil {
		t.Fatalf("no error returned from timed-out boot")
	}
	if !errors.Is(err, cluster.ErrBootCanceled) {
		t.Fatalf("wrong error returned")
	}
	if exp, got := cluster.BootCanceled, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

// Test_BootstrapperBootCanceledDone tests that a boot that is canceled
// but is done does not return an error.
func Test_BootstrapperBootCanceledDone(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()

	done := func() bool {
		return true
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := NewAddressProviderString([]string{srv.Addr()})
	clientMgr := client.NewClientManager(nil)
	bs := NewBootstrapper(p, clientMgr)
	bs.Interval = time.Second
	err := bs.Boot(ctx, "node1", "192.168.1.1:1234", cluster.Voter, done, 5*time.Second)
	if err != nil {
		t.Fatalf("error returned from canceled boot even though it's done: %s", err)
	}
	if bs.nBootCanceled == 0 {
		t.Fatalf("boot not actually canceled")
	}
}

func Test_BootstrapperBootSingleJoin(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()

	srv.JoinHandler = func(req *proto.JoinRequest) (*proto.JoinResponse, error) {
		if req == nil {
			t.Fatal("expected join node request, got nil")
		}
		if req.Address != "192.168.1.1:1234" {
			t.Fatalf("unexpected node address, got %s", req.Address)
		}

		return &proto.JoinResponse{}, nil
	}

	done := func() bool {
		return false
	}

	p := NewAddressProviderString([]string{srv.Addr()})
	clientMgr := client.NewClientManager(nil)
	bs := NewBootstrapper(p, clientMgr)
	bs.Interval = time.Second

	err := bs.Boot(context.Background(), "node1", "192.168.1.1:1234", cluster.Voter, done, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}
	if exp, got := cluster.BootJoin, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

// Test_BootstrapperBootNonVoter tests that a non-voter just attempts
// to join the cluster, and does not send a notify request.
func Test_BootstrapperBootNonVoter(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()
	srv.JoinHandler = func(req *proto.JoinRequest) (*proto.JoinResponse, error) {
		if req == nil {
			t.Fatal("expected join node request, got nil")
		}
		if req.Address != "192.168.1.1:1234" {
			t.Fatalf("unexpected node address, got %s", req.Address)
		}
		// timeout the bootstrapper.
		time.Sleep(5 * time.Second)
		return &proto.JoinResponse{}, nil
	}

	done := func() bool {
		return false
	}

	p := NewAddressProviderString([]string{srv.Addr()})
	clientMgr := client.NewClientManager(nil)
	bs := NewBootstrapper(p, clientMgr)
	bs.Interval = time.Second

	err := bs.Boot(context.Background(), "node1", "192.168.1.1:1234", cluster.NonVoter, done, 3*time.Second)
	if err == nil {
		t.Fatalf("expected error, got none")
	}
	if exp, got := cluster.BootTimeout, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootSingleNotify(t *testing.T) {
	srv := servertest.NewTestServer()
	defer srv.Close()

	var gotNR *proto.NotifyRequest
	srv.JoinHandler = func(jr *proto.JoinRequest) (*proto.JoinResponse, error) {
		return nil, fmt.Errorf("not a cluster")
	}
	srv.NotifyHandler = func(nr *proto.NotifyRequest) (*proto.NotifyResponse, error) {
		gotNR = nr
		return &proto.NotifyResponse{}, nil
	}
	n := -1
	done := func() bool {
		n++
		return n == 5
	}

	p := NewAddressProviderString([]string{srv.Addr()})
	clientMgr := client.NewClientManager(nil)
	bs := NewBootstrapper(p, clientMgr)
	bs.Interval = time.Second

	err := bs.Boot(context.Background(), "node1", "192.168.1.1:1234", cluster.Voter, done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if got, exp := gotNR.Id, "node1"; got != exp {
		t.Fatalf("wrong node ID supplied, exp %s, got %s", exp, got)
	}
	if got, exp := gotNR.Address, "192.168.1.1:1234"; got != exp {
		t.Fatalf("wrong address supplied, exp %s, got %s", exp, got)
	}
	if exp, got := cluster.BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootMultiJoinNotify(t *testing.T) {
	var srv1JoinC int32
	var srv1NotifiedC int32
	srv1 := servertest.NewTestServer()
	defer srv1.Close()

	srv1.JoinHandler = func(nr *proto.JoinRequest) (*proto.JoinResponse, error) {
		atomic.AddInt32(&srv1JoinC, 1)
		return nil, fmt.Errorf("not a cluster")
	}

	srv1.NotifyHandler = func(nr *proto.NotifyRequest) (*proto.NotifyResponse, error) {
		atomic.AddInt32(&srv1NotifiedC, 1)
		return &proto.NotifyResponse{}, nil
	}

	var srv2JoinC int32
	var srv2NotifiedC int32
	srv2 := servertest.NewTestServer()
	defer srv2.Close()

	srv2.JoinHandler = func(nr *proto.JoinRequest) (*proto.JoinResponse, error) {
		atomic.AddInt32(&srv2JoinC, 1)
		return nil, fmt.Errorf("not a cluster")
	}

	srv2.NotifyHandler = func(nr *proto.NotifyRequest) (*proto.NotifyResponse, error) {
		atomic.AddInt32(&srv2NotifiedC, 1)
		return &proto.NotifyResponse{}, nil
	}

	n := -1
	done := func() bool {
		n++
		return n == 5
	}

	p := NewAddressProviderString([]string{srv1.Addr(), srv2.Addr()})
	clientMgr := client.NewClientManager(nil)
	bs := NewBootstrapper(p, clientMgr)
	bs.Interval = time.Second

	err := bs.Boot(context.Background(), "node1", "192.168.1.1:1234", cluster.Voter, done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if atomic.LoadInt32(&srv1JoinC) < 1 || atomic.LoadInt32(&srv2JoinC) < 1 {
		t.Fatalf("all join targets not contacted")
	}
	if atomic.LoadInt32(&srv2JoinC) < 1 || atomic.LoadInt32(&srv2NotifiedC) < 1 {
		t.Fatalf("all notify targets not contacted")
	}
	if exp, got := cluster.BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}
