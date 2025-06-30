package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"

	"github.com/pbinitiative/zenbpm/internal/cluster"
	"go.opentelemetry.io/otel"

	"github.com/pbinitiative/zenbpm/internal/grpc/proto"
	"github.com/pbinitiative/zenbpm/internal/log"
	otelpropagation "go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	oteltracing "google.golang.org/grpc/experimental/opentelemetry"
	"google.golang.org/grpc/stats/opentelemetry"
	"google.golang.org/grpc/status"
)

type Server struct {
	proto.UnimplementedZenBpmServer
	proto.UnimplementedGatewayServer
	node   *cluster.ZenNode
	addr   string // Address this server is listening on
	server *grpc.Server
}

// NewServer returns a new instance of ZenBpm GRPC server
func NewServer(node *cluster.ZenNode, addr string) *Server {
	textMapPropagator := otelpropagation.TraceContext{}
	so := opentelemetry.ServerOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: otel.GetMeterProvider()},
		TraceOptions:   oteltracing.TraceOptions{TracerProvider: otel.GetTracerProvider(), TextMapPropagator: textMapPropagator}})

	grpcServer := grpc.NewServer(so)
	server := &Server{
		node:   node,
		addr:   addr,
		server: grpcServer,
	}
	proto.RegisterZenBpmServer(grpcServer, server)
	proto.RegisterGatewayServer(grpcServer, server)

	return server
}

var _ proto.ZenBpmServer = &Server{}
var _ proto.GatewayServer = &Server{}

// Start starts the ZenBPM GRPC server.
func (s *Server) Start() {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Error("failed to listen: %v", err)
	}
	go func() {
		log.Info("ZenBpm GRPC server listening on %s", s.addr)
		err := s.server.Serve(listener)
		if err != nil {
			log.Error("ZenBPM GRPC server startup failed: %s", err)
		}
	}()
}

// Stop stops the ZenBPM GRPC server.
func (s *Server) Stop() {
	s.server.Stop()
}

func (s *Server) ActivateJobs(req *proto.ActivateJobsRequest, stream proto.ZenBpm_ActivateJobsServer) error {
	return errors.New("TODO: Not yet implemented")
}

func (s *Server) CompleteJob(ctx context.Context, req *proto.CompleteJobRequest) (*proto.CompleteJobResponse, error) {
	return nil, errors.New("TODO: Not yet implemented")
}

func (s *Server) FailJob(ctx context.Context, req *proto.FailJobRequest) (*proto.FailJobResponse, error) {
	var variables map[string]interface{}
	if req.Variables != "" {
		if err := json.Unmarshal([]byte(req.Variables), &variables); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid variables JSON: %v", err)
		}
	}

	err := s.node.FailJob(ctx, req.JobKey, req.Retries, req.ErrorMessage, req.RetryBackOff, variables)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fail job: %v", err)
	}

	return &proto.FailJobResponse{}, nil
}
