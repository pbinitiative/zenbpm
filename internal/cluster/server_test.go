// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/server"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func TestFkdUpClient(t *testing.T) {
	t.SkipNow()
	mux, _, err := network.NewNodeMux("127.0.0.1:41231")
	// ln, err := net.Listen("tcp", "localhost:41231")
	if err != nil {
		t.Fatalf("failed to create new mux: %s", err)
	}
	cLn := network.NewZenBpmClusterListener(mux)

	tStore := &StoreSvc{}
	// srv := server.New(ln, tStore)
	srv := server.New(cLn, tStore, nil, nil)
	err = srv.Open()
	if err != nil {
		t.Fatalf("failed to start server: %s", err)
	}
	cm := client.NewClientManager(nil)
	c, _ := cm.For(cLn.Addr().String())
	// c, _ := cm.For(ln.Addr().String())
	now := time.Now()
	fmt.Println(now)
	for i := range 10 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		// conn := cm.GetConn(c)
		// conn.Invoke(ctx, "/cluster.ZenService/NodeCommand", &proto.Command{
		// 	Type: proto.Command_TYPE_NODE_CHANGE,
		// 	Request: &proto.Command_NodeChange{
		// 		NodeChange: &proto.NodeChange{
		// 			NodeId:   "",
		// 			Addr:     "",
		// 			Suffrage: 0,
		// 			State:    0,
		// 			Role:     0,
		// 		},
		// 	},
		// }, nil)
		// cancel()
		// if err != nil {
		// 	t.Fatalf("failed to open new stream: %s", err)
		// }
		// time.Sleep(1 * time.Second)
		// continue
		now = time.Now()
		fmt.Println(now)
		_, err = c.NodeCommand(ctx, &proto.Command{
			Type: proto.Command_TYPE_NODE_PARTITION_CHANGE.Enum(),
			Request: &proto.Command_NodePartitionChange{
				NodePartitionChange: &proto.NodePartitionChange{
					NodeId:      ptr.To("123"),
					PartitionId: ptr.To(uint32(1)),
					State:       proto.NodePartitionState_NODE_PARTITION_STATE_ERROR.Enum(),
					Role:        proto.Role_ROLE_TYPE_FOLLOWER.Enum(),
				},
			},
		})
		cancel()
		time.Sleep(time.Duration(i) * 2 * time.Second)
		if err != nil {
			fmt.Println(now)
			t.Fatalf("%s", err)
		}
	}
}

type listener struct {
	c    chan net.Conn
	addr net.Addr
}

func (ln *listener) Accept() (c net.Conn, err error) {
	conn, ok := <-ln.c
	if !ok {
		return nil, errors.New("network connection closed")
	}
	return conn, nil
}
func (ln *listener) Close() error   { return nil }
func (ln *listener) Addr() net.Addr { return ln.addr }

type Mux struct {
	ln      net.Listener
	addr    net.Addr
	m       map[byte]*listener
	Timeout time.Duration
	wg      sync.WaitGroup
}

func (mux *Mux) Listen(header byte) net.Listener {
	// Ensure two listeners are not created for the same header byte.
	if _, ok := mux.m[header]; ok {
		panic(fmt.Sprintf("listener already registered under header byte: %d", header))
	}

	// Create a new listener and assign it.
	ln := &listener{
		c:    make(chan net.Conn),
		addr: mux.addr,
	}
	mux.m[header] = ln
	return ln
}
func (mux *Mux) Serve() error {
	for {
		conn, err := mux.ln.Accept()
		if err != nil {
			mux.wg.Wait()
			for _, ln := range mux.m {
				close(ln.c)
			}
			return err
		}
		mux.wg.Add(1)
		go mux.handleConn(conn)
	}
}
func (mux *Mux) handleConn(conn net.Conn) {
	defer mux.wg.Done()
	// Set a read deadline so connections with no data don't timeout.
	if err := conn.SetReadDeadline(time.Now().Add(mux.Timeout)); err != nil {
		conn.Close()
		fmt.Printf("cannot set read deadline: %s\n", err)
		return
	}
	// Read first byte from connection to determine handler.
	var typ [1]byte
	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		conn.Close()
		fmt.Printf("cannot read header byte: %s\n", err)
		return
	}
	// Reset read deadline and let the listener handle that.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		conn.Close()
		fmt.Printf("cannot reset set read deadline: %s\n", err)
		return
	}
	// Retrieve handler based on first byte.
	handler := mux.m[typ[0]]
	if handler == nil {
		conn.Close()
		return
	}
	// Send connection to handler.  The handler is responsible for closing the connection.
	handler.c <- conn
}

type Dialer struct {
	header byte
}

// Dial dials the cluster service at the given addr and returns a connection.
func (d *Dialer) Dial(addr string, timeout time.Duration) (conn net.Conn, retErr error) {
	dialer := &net.Dialer{Timeout: timeout}

	conn, retErr = dialer.Dial("tcp", addr)
	if retErr != nil {
		return nil, retErr
	}

	defer func() {
		if retErr != nil && conn != nil {
			conn.Close()
		}
	}()

	// Write a marker byte to indicate message type.
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		// if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("failed to set WriteDeadline for header: %s", err.Error())
	}
	if _, err := conn.Write([]byte{d.header}); err != nil {
		return nil, err
	}
	return conn, nil
}

func TestGRPCHeaderMux(t *testing.T) {
	t.SkipNow()
	muxLn, err := net.Listen("tcp", ":41421")
	if err != nil {
		t.Fatalf("failed to create mux listener: %s", err)
	}
	mux := &Mux{
		ln:      muxLn,
		addr:    muxLn.Addr(),
		m:       make(map[byte]*listener),
		Timeout: 30 * time.Second,
	}
	h1Ln := mux.Listen(byte(1))
	srv := grpc.NewServer()
	go mux.Serve()
	go func() {
		_ = h1Ln
		err := srv.Serve(h1Ln)
		if err != nil {
			fmt.Printf("%s\n", err)
		}
	}()
	defer muxLn.Close()
	conn, err := grpc.NewClient(
		muxLn.Addr().String(),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			h1Dialer := Dialer{
				header: byte(1),
			}
			return h1Dialer.Dial(s, 30*time.Second)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             1 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		t.Fatalf("failed to create grpc connection: %s", err)
	}
	defer conn.Close()

	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := conn.NewStream(ctx, &grpc.StreamDesc{}, "/")
		cancel()
		if err != nil {
			t.Fatalf("failed to open new stream: %s", err)
		}
		if err := stream.SendMsg(nil); err != nil {
			t.Fatalf("failed to send message: %s", err)
		}
		time.Sleep(15 * time.Second)
	}
}

func TestGRPCClient(t *testing.T) {
	ln, err := net.Listen("tcp", ":41421")
	if err != nil {
		t.Fatalf("failed to create mux listener: %s", err)
	}
	srv := grpc.NewServer()
	go func() {
		err := srv.Serve(ln)
		if err != nil {
			fmt.Printf("%s\n", err)
		}
	}()
	defer ln.Close()
	conn, err := grpc.NewClient(
		ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create grpc connection: %s", err)
	}
	defer conn.Close()

	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := conn.NewStream(ctx, &grpc.StreamDesc{}, "/")
		cancel()
		if err != nil {
			t.Fatalf("failed to open new stream: %s", err)
		}
		if err := stream.SendMsg(nil); err != nil {
			t.Fatalf("failed to send message: %s", err)
		}
		time.Sleep(1 * time.Second)
	}
}

type StoreSvc struct {
}

// Join implements server.StoreService.
func (s *StoreSvc) Join(jr *zenproto.JoinRequest) error {
	fmt.Println("Join")
	return nil
}

// Notify implements server.StoreService.
func (s *StoreSvc) Notify(nr *zenproto.NotifyRequest) error {
	fmt.Println("Notify")
	return nil
}

// WriteNodeChange implements server.StoreService.
func (s *StoreSvc) WriteNodeChange(change *proto.NodeChange) error {
	fmt.Println("WriteNodeChange")
	return nil
}

// WritePartitionChange implements server.StoreService.
func (s *StoreSvc) WritePartitionChange(change *proto.NodePartitionChange) error {
	fmt.Println("WritePartitionChange")
	return nil
}

func (s *StoreSvc) ClusterState() state.Cluster {
	return state.Cluster{}
}
