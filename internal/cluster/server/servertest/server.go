// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package servertest

import (
	"context"
	"net"

	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"google.golang.org/grpc"
)

type TestServer struct {
	proto.UnimplementedZenServiceServer
	Listener      net.Listener
	JoinHandler   func(*proto.JoinRequest) (*proto.JoinResponse, error)
	NotifyHandler func(*proto.NotifyRequest) (*proto.NotifyResponse, error)
	GlobalHandler func() error
}

// New returns a new instance of a TestServer
func NewTestServer() *TestServer {
	mux, _, err := network.NewNodeMux("")
	if err != nil {
		panic("service: failed to listen: " + err.Error())
	}
	ln := network.NewZenBpmClusterListener(mux)
	s := &TestServer{
		Listener: ln,
	}

	srv := grpc.NewServer()
	proto.RegisterZenServiceServer(srv, s)
	go func() {
		err := srv.Serve(ln)
		if err != nil {
			panic(err)
		}
	}()
	return s
}

var _ proto.ZenServiceServer = &TestServer{}

// Close closes the TestServer.
func (s *TestServer) Close() error {
	s.Listener.Close()
	return nil
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
