package server

import (
	"context"
	"fmt"
	"net"

	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/log"
	"google.golang.org/grpc"
)

// Server provides information about the node and cluster.
type Server struct {
	proto.UnimplementedZenServiceServer
	ln    net.Listener // Incoming connections to the service
	addr  net.Addr     // Address on which this service is listening
	store StoreService
}

type StoreService interface {
	Notify(nr *proto.NotifyRequest) error
	Join(jr *proto.JoinRequest) error
	WriteNodeChange(change *protoc.NodeChange) error
	WritePartitionChange(change *protoc.NodePartitionChange) error
}

// New returns a new instance of the zen cluster server
func New(ln net.Listener, store StoreService) *Server {
	return &Server{
		ln:    ln,
		addr:  ln.Addr(),
		store: store,
	}
}

var _ proto.ZenServiceServer = &Server{}

// Open opens the Server.
func (s *Server) Open() error {
	log.Info("zen cluster service listening on %s", s.addr)
	srv := grpc.NewServer()
	proto.RegisterZenServiceServer(srv, s)
	go srv.Serve(s.ln)
	return nil
}

// Close closes the Server.
func (s *Server) Close() error {
	fmt.Println(s.ln)
	s.ln.Close()
	return nil
}

func (s *Server) Notify(ctx context.Context, req *proto.NotifyRequest) (*proto.NotifyResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	err := s.store.Notify(req)
	if err != nil {
		return nil, fmt.Errorf("failed to notify store: %w", err)
	}
	return &proto.NotifyResponse{}, nil
}
func (s *Server) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	err := s.store.Join(req)
	if err != nil {
		return nil, fmt.Errorf("failed to notify store: %w", err)
	}
	return nil, nil
}

func (s *Server) NodeCommand(ctx context.Context, req *protoc.Command) (*proto.NodeCommandResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	switch req.Type {
	case protoc.Command_TYPE_NODE_CHANGE:
		err := s.store.WriteNodeChange(req.GetNodeChange())
		if err != nil {
			return nil, fmt.Errorf("failed to write node change to store: %w", err)
		}
		return &proto.NodeCommandResponse{
			Type: protoc.Command_TYPE_NODE_CHANGE,
			Response: &proto.NodeCommandResponse_NodeChange{
				NodeChange: &proto.ClusterNodeChangeResponse{},
			},
		}, nil
	case protoc.Command_TYPE_NODE_PARTITION_CHANGE:
		err := s.store.WritePartitionChange(req.GetNodePartitionChange())
		if err != nil {
			return nil, fmt.Errorf("failed to write node change to store: %w", err)
		}
		return &proto.NodeCommandResponse{
			Type: protoc.Command_TYPE_NODE_PARTITION_CHANGE,
			Response: &proto.NodeCommandResponse_NodePartitionChange{
				NodePartitionChange: &proto.ClusterNodePartitionChangeResponse{},
			},
		}, nil
	case protoc.Command_TYPE_NOOP:
		fallthrough
	case protoc.Command_TYPE_UNKNOWN:
		fallthrough
	default:
		return nil, nil
	}
}

func (s *Server) ClusterBackup(ctx context.Context, req *proto.ClusterBackupRequest) (*proto.ClusterBackupResponse, error) {
	// TODO: implement
	return nil, nil
}

func (s *Server) ClusterRestore(ctx context.Context, req *proto.ClusterRestoreRequest) (*proto.ClusterRestoreResponse, error) {
	// TODO: implement
	return nil, nil
}

func (s *Server) ConfigurationUpdate(ctx context.Context, req *proto.ConfigurationUpdateRequest) (*proto.ConfigurationUpdateResponse, error) {
	// TODO: implement
	return nil, nil
}

func (s *Server) AssignPartition(ctx context.Context, req *proto.AssignPartitionRequest) (*proto.AssignPartitionResponse, error) {
	// TODO: implement
	return nil, nil
}
func (s *Server) UnassignPartition(ctx context.Context, req *proto.UnassignPartitionRequest) (*proto.UnassignPartitionResponse, error) {
	// TODO: implement
	return nil, nil
}
func (s *Server) PartitionBackup(ctx context.Context, req *proto.PartitionBackupRequest) (*proto.PartitionBackupResponse, error) {
	// TODO: implement
	return nil, nil
}
func (s *Server) PartitionRestore(ctx context.Context, req *proto.PartitionRestoreRequest) (*proto.PartitionRestoreResponse, error) {
	// TODO: implement
	return nil, nil
}
