package servertest

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/rqlite/rqlite/v10/tcp"
	"google.golang.org/grpc"
)

type TestServer struct {
	proto.UnimplementedZenServiceServer
	Listener                 net.Listener
	JoinHandler              func(*proto.JoinRequest) (*proto.JoinResponse, error)
	NotifyHandler            func(*proto.NotifyRequest) (*proto.NotifyResponse, error)
	FindActiveMessageHandler func(*proto.FindActiveMessageRequest) (*proto.FindActiveMessageResponse, error)
	GlobalHandler            func() error
	mux                      *tcp.Mux
	muxListener              net.Listener
	grpcServer               *grpc.Server
	closeSignal              chan struct{}
	closeOnce                sync.Once
	closeErr                 error
}

// New returns a new instance of a TestServer
func NewTestServer() *TestServer {
	mux, muxListener, err := network.NewNodeMux("")
	if err != nil {
		panic("service: failed to listen: " + err.Error())
	}
	ln := network.NewZenBpmClusterListener(mux)
	srv := grpc.NewServer()
	s := &TestServer{
		Listener:    ln,
		mux:         mux,
		muxListener: muxListener,
		grpcServer:  srv,
		closeSignal: make(chan struct{}),
	}

	proto.RegisterZenServiceServer(srv, s)
	go func() {
		if err := srv.Serve(ln); err != nil {
			select {
			case <-s.closeSignal:
				return
			default:
				panic(err)
			}
		}
	}()
	return s
}

var _ proto.ZenServiceServer = &TestServer{}

// Close closes the TestServer.
func (s *TestServer) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeSignal)
		s.closeErr = errors.Join(s.mux.Close(), s.muxListener.Close())
		s.grpcServer.Stop()
	})
	return s.closeErr
}

func (s *TestServer) Addr() string {
	return s.Listener.Addr().String()
}

func (s *TestServer) Notify(ctx context.Context, req *proto.NotifyRequest) (*proto.NotifyResponse, error) {
	if s.NotifyHandler != nil {
		return s.NotifyHandler(req)
	}
	if s.GlobalHandler != nil {
		return &proto.NotifyResponse{}, s.GlobalHandler()
	}
	return &proto.NotifyResponse{}, nil
}

func (s *TestServer) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	if s.JoinHandler != nil {
		return s.JoinHandler(req)
	}
	if s.GlobalHandler != nil {
		return &proto.JoinResponse{}, s.GlobalHandler()
	}
	return &proto.JoinResponse{}, nil
}

func (s *TestServer) FindActiveMessage(ctx context.Context, req *proto.FindActiveMessageRequest) (*proto.FindActiveMessageResponse, error) {
	if s.FindActiveMessageHandler != nil {
		return s.FindActiveMessageHandler(req)
	}
	if s.GlobalHandler != nil {
		return &proto.FindActiveMessageResponse{}, s.GlobalHandler()
	}
	return &proto.FindActiveMessageResponse{}, nil
}
