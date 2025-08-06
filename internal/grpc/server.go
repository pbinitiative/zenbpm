package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/pkg/client/proto"
	"go.opentelemetry.io/otel"

	"github.com/pbinitiative/zenbpm/internal/log"
	otelpropagation "go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	oteltracing "google.golang.org/grpc/experimental/opentelemetry"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats/opentelemetry"
)

type Server struct {
	ctx context.Context
	proto.UnimplementedZenBpmServer
	node   *cluster.ZenNode
	addr   string // Address this server is listening on
	server *grpc.Server
	logger hclog.Logger
}

// NewServer returns a new instance of ZenBpm GRPC server
func NewServer(ctx context.Context, node *cluster.ZenNode, addr string) *Server {
	textMapPropagator := otelpropagation.TraceContext{}
	so := opentelemetry.ServerOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: otel.GetMeterProvider()},
		TraceOptions:   oteltracing.TraceOptions{TracerProvider: otel.GetTracerProvider(), TextMapPropagator: textMapPropagator}})

	grpcServer := grpc.NewServer(so)
	server := &Server{
		node:   node,
		addr:   addr,
		server: grpcServer,
		ctx:    ctx,
		logger: hclog.Default().Named("public-grpc-server"),
	}
	proto.RegisterZenBpmServer(grpcServer, server)

	return server
}

var _ proto.ZenBpmServer = &Server{}

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

func (s *Server) JobStream(stream grpc.BidiStreamingServer[proto.JobStreamRequest, proto.JobStreamResponse]) error {
	clientID := getClientID(stream.Context())
	clientCh := make(chan jobmanager.Job)
	err := s.node.JobManager.AddClient(stream.Context(), clientID, clientCh)
	if err != nil {
		return fmt.Errorf("failed to add client: %w", err)
	}
	go s.recvClientRequests(stream, clientID)
	s.sendClientJobs(stream, clientCh, clientID)
	return nil
}

func (s *Server) recvClientRequests(stream grpc.BidiStreamingServer[proto.JobStreamRequest, proto.JobStreamResponse], clientID jobmanager.ClientID) {
	for {
		clientReq, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return
		}
		if err != nil {
			s.logger.Error("Failed to receive a job stream request", "err", err)
			return
		}
		switch req := clientReq.Request.(type) {
		case *proto.JobStreamRequest_Complete:
			vars := map[string]any{}
			err := json.Unmarshal(req.Complete.Variables, &vars)
			if err != nil {
				_ = stream.Send(&proto.JobStreamResponse{
					Error: &proto.ErrorResult{
						Code:    0,
						Message: fmt.Sprintf("Failed to unmarshal variables: %s", err),
					},
					Job: &proto.WaitingJob{
						Key: req.Complete.Key,
					},
				})
				continue
			}
			err = s.node.JobManager.CompleteJobReq(stream.Context(), clientID, req.Complete.Key, vars)
			if err != nil {
				_ = stream.Send(&proto.JobStreamResponse{
					Error: &proto.ErrorResult{
						Code:    0,
						Message: fmt.Sprintf("Failed to complete job: %s", err),
					},
					Job: &proto.WaitingJob{
						Key: req.Complete.Key,
					},
				})
				continue
			}
		case *proto.JobStreamRequest_Fail:
			vars := map[string]any{}
			err := json.Unmarshal(req.Fail.Variables, &vars)
			if err != nil {
				stream.Send(&proto.JobStreamResponse{
					Error: &proto.ErrorResult{
						Code:    0,
						Message: fmt.Sprintf("Failed to unmarshal variables: %s", err),
					},
					Job: &proto.WaitingJob{
						Key: req.Fail.Key,
					},
				})
				continue
			}
			err = s.node.JobManager.FailJobReq(stream.Context(), clientID, req.Fail.Key, req.Fail.Message, req.Fail.ErrorCode, &vars)
			if err != nil {
				stream.Send(&proto.JobStreamResponse{
					Error: &proto.ErrorResult{
						Code:    0,
						Message: fmt.Sprintf("Failed to fail job: %s", err),
					},
					Job: &proto.WaitingJob{
						Key: req.Fail.Key,
					},
				})
				continue
			}
		case *proto.JobStreamRequest_Subscription:
			switch req.Subscription.Type {
			case proto.StreamSubscriptionRequest_TYPE_UNDEFINED:
			case proto.StreamSubscriptionRequest_TYPE_SUBSCRIBE:
				jobType := jobmanager.JobType(req.Subscription.JobType)
				s.node.JobManager.AddClientJobSub(stream.Context(), clientID, jobType)
			case proto.StreamSubscriptionRequest_TYPE_UNSUBSCRIBE:
				jobType := jobmanager.JobType(req.Subscription.JobType)
				s.node.JobManager.RemoveClientJobSub(stream.Context(), clientID, jobType)
			default:
				panic(fmt.Sprintf("unexpected proto.StreamSubscriptionRequest_Type: %#v", req.Subscription.Type))
			}
		default:
			panic(fmt.Sprintf("unexpected proto.isJobStreamRequest_Request: %#v", clientReq.Request))
		}
	}
}

func (s *Server) sendClientJobs(stream grpc.BidiStreamingServer[proto.JobStreamRequest, proto.JobStreamResponse], clientCh chan jobmanager.Job, clientID jobmanager.ClientID) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-stream.Context().Done():
			s.node.JobManager.RemoveClient(s.ctx, clientID)
			return
		case job := <-clientCh:
			err := stream.Send(&proto.JobStreamResponse{
				Job: &proto.WaitingJob{
					Key:         job.Key,
					InstanceKey: job.InstanceKey,
					Variables:   job.Variables,
					Type:        string(job.Type),
					ElementId:   job.ElementID,
					CreatedAt:   job.CreatedAt,
				},
			})
			if err != nil {
				s.logger.Error("Failed to send message to stream: %w", err)
				continue
			}
		}
	}
}

func getClientID(ctx context.Context) jobmanager.ClientID {
	md, found := metadata.FromIncomingContext(ctx)
	clientID := jobmanager.ClientID("")
	if found {
		clientIDs := md.Get(jobmanager.MetadataClientID)
		if len(clientIDs) == 1 {
			clientID = jobmanager.ClientID(clientIDs[0])
		}
	}
	return clientID
}
