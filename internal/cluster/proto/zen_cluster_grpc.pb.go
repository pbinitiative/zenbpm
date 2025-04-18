// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v6.30.1
// source: zen_cluster.proto

package proto

import (
	context "context"
	proto "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ZenServiceClient is the client API for ZenService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ZenServiceClient interface {
	// Notify notifies this node that a remote node is ready
	// for bootstrapping.
	Notify(ctx context.Context, in *NotifyRequest, opts ...grpc.CallOption) (*NotifyResponse, error)
	// Join joins a remote node to the cluster.
	Join(ctx context.Context, in *JoinRequest, opts ...grpc.CallOption) (*JoinResponse, error)
	ClusterBackup(ctx context.Context, in *ClusterBackupRequest, opts ...grpc.CallOption) (*ClusterBackupResponse, error)
	ClusterRestore(ctx context.Context, in *ClusterRestoreRequest, opts ...grpc.CallOption) (*ClusterRestoreResponse, error)
	ConfigurationUpdate(ctx context.Context, in *ConfigurationUpdateRequest, opts ...grpc.CallOption) (*ConfigurationUpdateResponse, error)
	AssignPartition(ctx context.Context, in *AssignPartitionRequest, opts ...grpc.CallOption) (*AssignPartitionResponse, error)
	UnassignPartition(ctx context.Context, in *UnassignPartitionRequest, opts ...grpc.CallOption) (*UnassignPartitionResponse, error)
	PartitionBackup(ctx context.Context, in *PartitionBackupRequest, opts ...grpc.CallOption) (*PartitionBackupResponse, error)
	PartitionRestore(ctx context.Context, in *PartitionRestoreRequest, opts ...grpc.CallOption) (*PartitionRestoreResponse, error)
	NodeCommand(ctx context.Context, in *proto.Command, opts ...grpc.CallOption) (*NodeCommandResponse, error)
}

type zenServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewZenServiceClient(cc grpc.ClientConnInterface) ZenServiceClient {
	return &zenServiceClient{cc}
}

func (c *zenServiceClient) Notify(ctx context.Context, in *NotifyRequest, opts ...grpc.CallOption) (*NotifyResponse, error) {
	out := new(NotifyResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/Notify", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) Join(ctx context.Context, in *JoinRequest, opts ...grpc.CallOption) (*JoinResponse, error) {
	out := new(JoinResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/Join", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) ClusterBackup(ctx context.Context, in *ClusterBackupRequest, opts ...grpc.CallOption) (*ClusterBackupResponse, error) {
	out := new(ClusterBackupResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/ClusterBackup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) ClusterRestore(ctx context.Context, in *ClusterRestoreRequest, opts ...grpc.CallOption) (*ClusterRestoreResponse, error) {
	out := new(ClusterRestoreResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/ClusterRestore", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) ConfigurationUpdate(ctx context.Context, in *ConfigurationUpdateRequest, opts ...grpc.CallOption) (*ConfigurationUpdateResponse, error) {
	out := new(ConfigurationUpdateResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/ConfigurationUpdate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) AssignPartition(ctx context.Context, in *AssignPartitionRequest, opts ...grpc.CallOption) (*AssignPartitionResponse, error) {
	out := new(AssignPartitionResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/AssignPartition", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) UnassignPartition(ctx context.Context, in *UnassignPartitionRequest, opts ...grpc.CallOption) (*UnassignPartitionResponse, error) {
	out := new(UnassignPartitionResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/UnassignPartition", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) PartitionBackup(ctx context.Context, in *PartitionBackupRequest, opts ...grpc.CallOption) (*PartitionBackupResponse, error) {
	out := new(PartitionBackupResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/PartitionBackup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) PartitionRestore(ctx context.Context, in *PartitionRestoreRequest, opts ...grpc.CallOption) (*PartitionRestoreResponse, error) {
	out := new(PartitionRestoreResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/PartitionRestore", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *zenServiceClient) NodeCommand(ctx context.Context, in *proto.Command, opts ...grpc.CallOption) (*NodeCommandResponse, error) {
	out := new(NodeCommandResponse)
	err := c.cc.Invoke(ctx, "/cluster.ZenService/NodeCommand", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ZenServiceServer is the server API for ZenService service.
// All implementations must embed UnimplementedZenServiceServer
// for forward compatibility
type ZenServiceServer interface {
	// Notify notifies this node that a remote node is ready
	// for bootstrapping.
	Notify(context.Context, *NotifyRequest) (*NotifyResponse, error)
	// Join joins a remote node to the cluster.
	Join(context.Context, *JoinRequest) (*JoinResponse, error)
	ClusterBackup(context.Context, *ClusterBackupRequest) (*ClusterBackupResponse, error)
	ClusterRestore(context.Context, *ClusterRestoreRequest) (*ClusterRestoreResponse, error)
	ConfigurationUpdate(context.Context, *ConfigurationUpdateRequest) (*ConfigurationUpdateResponse, error)
	AssignPartition(context.Context, *AssignPartitionRequest) (*AssignPartitionResponse, error)
	UnassignPartition(context.Context, *UnassignPartitionRequest) (*UnassignPartitionResponse, error)
	PartitionBackup(context.Context, *PartitionBackupRequest) (*PartitionBackupResponse, error)
	PartitionRestore(context.Context, *PartitionRestoreRequest) (*PartitionRestoreResponse, error)
	NodeCommand(context.Context, *proto.Command) (*NodeCommandResponse, error)
	mustEmbedUnimplementedZenServiceServer()
}

// UnimplementedZenServiceServer must be embedded to have forward compatible implementations.
type UnimplementedZenServiceServer struct {
}

func (UnimplementedZenServiceServer) Notify(context.Context, *NotifyRequest) (*NotifyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Notify not implemented")
}
func (UnimplementedZenServiceServer) Join(context.Context, *JoinRequest) (*JoinResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Join not implemented")
}
func (UnimplementedZenServiceServer) ClusterBackup(context.Context, *ClusterBackupRequest) (*ClusterBackupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ClusterBackup not implemented")
}
func (UnimplementedZenServiceServer) ClusterRestore(context.Context, *ClusterRestoreRequest) (*ClusterRestoreResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ClusterRestore not implemented")
}
func (UnimplementedZenServiceServer) ConfigurationUpdate(context.Context, *ConfigurationUpdateRequest) (*ConfigurationUpdateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ConfigurationUpdate not implemented")
}
func (UnimplementedZenServiceServer) AssignPartition(context.Context, *AssignPartitionRequest) (*AssignPartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AssignPartition not implemented")
}
func (UnimplementedZenServiceServer) UnassignPartition(context.Context, *UnassignPartitionRequest) (*UnassignPartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UnassignPartition not implemented")
}
func (UnimplementedZenServiceServer) PartitionBackup(context.Context, *PartitionBackupRequest) (*PartitionBackupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PartitionBackup not implemented")
}
func (UnimplementedZenServiceServer) PartitionRestore(context.Context, *PartitionRestoreRequest) (*PartitionRestoreResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PartitionRestore not implemented")
}
func (UnimplementedZenServiceServer) NodeCommand(context.Context, *proto.Command) (*NodeCommandResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NodeCommand not implemented")
}
func (UnimplementedZenServiceServer) mustEmbedUnimplementedZenServiceServer() {}

// UnsafeZenServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ZenServiceServer will
// result in compilation errors.
type UnsafeZenServiceServer interface {
	mustEmbedUnimplementedZenServiceServer()
}

func RegisterZenServiceServer(s grpc.ServiceRegistrar, srv ZenServiceServer) {
	s.RegisterService(&ZenService_ServiceDesc, srv)
}

func _ZenService_Notify_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NotifyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).Notify(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/Notify",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).Notify(ctx, req.(*NotifyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_Join_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JoinRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).Join(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/Join",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).Join(ctx, req.(*JoinRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_ClusterBackup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClusterBackupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).ClusterBackup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/ClusterBackup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).ClusterBackup(ctx, req.(*ClusterBackupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_ClusterRestore_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClusterRestoreRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).ClusterRestore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/ClusterRestore",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).ClusterRestore(ctx, req.(*ClusterRestoreRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_ConfigurationUpdate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConfigurationUpdateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).ConfigurationUpdate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/ConfigurationUpdate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).ConfigurationUpdate(ctx, req.(*ConfigurationUpdateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_AssignPartition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AssignPartitionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).AssignPartition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/AssignPartition",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).AssignPartition(ctx, req.(*AssignPartitionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_UnassignPartition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UnassignPartitionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).UnassignPartition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/UnassignPartition",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).UnassignPartition(ctx, req.(*UnassignPartitionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_PartitionBackup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PartitionBackupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).PartitionBackup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/PartitionBackup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).PartitionBackup(ctx, req.(*PartitionBackupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_PartitionRestore_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PartitionRestoreRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).PartitionRestore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/PartitionRestore",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).PartitionRestore(ctx, req.(*PartitionRestoreRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ZenService_NodeCommand_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(proto.Command)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ZenServiceServer).NodeCommand(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.ZenService/NodeCommand",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ZenServiceServer).NodeCommand(ctx, req.(*proto.Command))
	}
	return interceptor(ctx, in, info, handler)
}

// ZenService_ServiceDesc is the grpc.ServiceDesc for ZenService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ZenService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "cluster.ZenService",
	HandlerType: (*ZenServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Notify",
			Handler:    _ZenService_Notify_Handler,
		},
		{
			MethodName: "Join",
			Handler:    _ZenService_Join_Handler,
		},
		{
			MethodName: "ClusterBackup",
			Handler:    _ZenService_ClusterBackup_Handler,
		},
		{
			MethodName: "ClusterRestore",
			Handler:    _ZenService_ClusterRestore_Handler,
		},
		{
			MethodName: "ConfigurationUpdate",
			Handler:    _ZenService_ConfigurationUpdate_Handler,
		},
		{
			MethodName: "AssignPartition",
			Handler:    _ZenService_AssignPartition_Handler,
		},
		{
			MethodName: "UnassignPartition",
			Handler:    _ZenService_UnassignPartition_Handler,
		},
		{
			MethodName: "PartitionBackup",
			Handler:    _ZenService_PartitionBackup_Handler,
		},
		{
			MethodName: "PartitionRestore",
			Handler:    _ZenService_PartitionRestore_Handler,
		},
		{
			MethodName: "NodeCommand",
			Handler:    _ZenService_NodeCommand_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "zen_cluster.proto",
}
