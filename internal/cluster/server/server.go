package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/types"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	pkgdmn "github.com/pbinitiative/zenbpm/pkg/dmn"
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"go.opentelemetry.io/otel"
	otelpropagation "go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	oteltracing "google.golang.org/grpc/experimental/opentelemetry"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats/opentelemetry"
	"google.golang.org/grpc/status"
)

// Server provides information about the node and cluster.
type Server struct {
	proto.UnimplementedZenServiceServer
	ln         net.Listener // Incoming connections to the service
	addr       net.Addr     // Address on which this service is listening
	store      StoreService
	controller ControllerService
	jobManager *jobmanager.JobManager
	client     *client.ClientManager
	cpuProfile CpuProfile
}

type CpuProfile struct {
	Running bool
	Server  *http.Server
}

type StoreService interface {
	Notify(nr *proto.NotifyRequest) error
	Join(jr *proto.JoinRequest) error
	WriteNodeChange(change *protoc.NodeChange) error
	ClusterState() state.Cluster
	WritePartitionChange(change *protoc.NodePartitionChange) error
}

type ControllerService interface {
	PartitionEngine(ctx context.Context, partitionId uint32) *bpmn.Engine
	Engines(ctx context.Context) map[uint32]*bpmn.Engine
	PartitionQueries(ctx context.Context, partitionId uint32) *sql.Queries
	GetPartition(ctx context.Context, partitionId uint32) *partition.ZenPartitionNode
}

// New returns a new instance of the zen cluster server
func New(ln net.Listener, store StoreService, controller ControllerService, jobManager *jobmanager.JobManager) *Server {
	return &Server{
		ln:         ln,
		addr:       ln.Addr(),
		store:      store,
		controller: controller,
		jobManager: jobManager,
	}
}

var _ proto.ZenServiceServer = &Server{}

// Open opens the Server.
func (s *Server) Open() error {
	textMapPropagator := otelpropagation.TraceContext{}
	so := opentelemetry.ServerOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: otel.GetMeterProvider()},
		TraceOptions:   oteltracing.TraceOptions{TracerProvider: otel.GetTracerProvider(), TextMapPropagator: textMapPropagator}})

	srv := grpc.NewServer(so, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}))
	proto.RegisterZenServiceServer(srv, s)
	go srv.Serve(s.ln)
	log.Info("zen cluster service listening on %s", s.addr)
	return nil
}

// Close closes the Server.
func (s *Server) Close() error {
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
	switch req.GetType() {
	case protoc.Command_TYPE_NODE_CHANGE:
		err := s.store.WriteNodeChange(req.GetNodeChange())
		if err != nil {
			return nil, fmt.Errorf("failed to write node change to store: %w", err)
		}
		return &proto.NodeCommandResponse{
			Type: protoc.Command_TYPE_NODE_CHANGE.Enum(),
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
			Type: protoc.Command_TYPE_NODE_PARTITION_CHANGE.Enum(),
			Response: &proto.NodeCommandResponse_NodePartitionChange{
				NodePartitionChange: &proto.ClusterNodePartitionChangeResponse{},
			},
		}, nil
	case protoc.Command_TYPE_NOOP:
		fallthrough
	case protoc.Command_TYPE_UNKNOWN:
		fallthrough
	default:
		panic("unexpected protoc.Command_Type")
	}
}

func (s *Server) ClusterBackup(ctx context.Context, req *proto.ClusterBackupRequest) (*proto.ClusterBackupResponse, error) {
	panic("unimplemented")
}

func (s *Server) ClusterRestore(ctx context.Context, req *proto.ClusterRestoreRequest) (*proto.ClusterRestoreResponse, error) {
	panic("unimplemented")
}

func (s *Server) ConfigurationUpdate(ctx context.Context, req *proto.ConfigurationUpdateRequest) (*proto.ConfigurationUpdateResponse, error) {
	panic("unimplemented")
}

func (s *Server) AssignPartition(ctx context.Context, req *proto.AssignPartitionRequest) (*proto.AssignPartitionResponse, error) {
	panic("unimplemented")
}
func (s *Server) UnassignPartition(ctx context.Context, req *proto.UnassignPartitionRequest) (*proto.UnassignPartitionResponse, error) {
	panic("unimplemented")
}
func (s *Server) PartitionBackup(ctx context.Context, req *proto.PartitionBackupRequest) (*proto.PartitionBackupResponse, error) {
	panic("unimplemented")
}
func (s *Server) PartitionRestore(ctx context.Context, req *proto.PartitionRestoreRequest) (*proto.PartitionRestoreResponse, error) {
	panic("unimplemented")
}
func (s *Server) PartitionNodeLeaderChange(context.Context, *proto.PartitionNodeLeaderChangeRequest) (*proto.PartitionNodeLeaderChangeResponse, error) {
	panic("unimplemented")
}
func (s *Server) AddPartitionNode(context.Context, *proto.AddPartitionNodeRequest) (*proto.AddPartitionNodeResponse, error) {
	panic("unimplemented")
}
func (s *Server) RemovePartitionNode(context.Context, *proto.RemovePartitionNodeRequest) (*proto.RemovePartitionNodeResponse, error) {
	panic("unimplemented")
}

func (s *Server) ResumePartitionNode(context.Context, *proto.ResumePartitionNodeRequest) (*proto.ResumePartitionNodeResponse, error) {
	panic("unimplemented")
}

func (s *Server) ShutdownPartitionNode(context.Context, *proto.ShutdownPartitionNodeRequest) (*proto.ShutdownPartitionNodeResponse, error) {
	panic("unimplemented")
}

func (s *Server) CompleteJob(ctx context.Context, req *proto.CompleteJobRequest) (*proto.CompleteJobResponse, error) {
	vars := map[string]any{}
	err := json.Unmarshal(req.Variables, &vars)
	if err != nil {
		zerr := zenerr.TechnicalError(fmt.Errorf("failed to unmarshal job input variables: %w", err))
		return &proto.CompleteJobResponse{Error: zerr.ToProtoError()}, nil
	}
	err = s.jobManager.CompleteJob(ctx, jobmanager.ClientID(req.GetClientId()), req.GetKey(), vars)
	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("job %d not found", req.GetKey()))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to complete job %d: %w", req.GetKey(), err))
		}
		return &proto.CompleteJobResponse{Error: zerr.ToProtoError()}, nil
	}
	return &proto.CompleteJobResponse{}, nil
}

func (s *Server) FailJob(ctx context.Context, req *proto.FailJobRequest) (*proto.FailJobResponse, error) {
	vars := map[string]any{}
	err := json.Unmarshal(req.Variables, &vars)
	if err != nil {
		err := fmt.Errorf("failed to unmarshal job input variables: %w", err)
		return &proto.FailJobResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	err = s.jobManager.FailJob(ctx, jobmanager.ClientID(req.GetClientId()), req.GetKey(), req.GetMessage(), req.ErrorCode, vars)
	if err != nil {
		err := fmt.Errorf("failed to fail job %d: %w", req.Key, err)
		return &proto.FailJobResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	return &proto.FailJobResponse{}, nil
}

func (s *Server) CreateInstance(ctx context.Context, req *proto.CreateInstanceRequest) (*proto.CreateInstanceResponse, error) {
	engine := s.GetRandomEngine(ctx)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("no engine available on this node"))
		return &proto.CreateInstanceResponse{Error: err.ToProtoError()}, nil
	}
	vars := map[string]any{}
	err := json.Unmarshal(req.Variables, &vars)
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to unmarshal process variables: %w", err))
		return &proto.CreateInstanceResponse{Error: err.ToProtoError()}, nil
	}
	if req.HistoryTTL != nil {
		ctx = appcontext.WithHistoryTTL(ctx, types.TTL(*req.HistoryTTL))
	}

	if req.BusinessKey != nil {
		ctx = appcontext.WithBusinessKey(ctx, ptr.Deref(req.BusinessKey, ""))
	}
	var instance runtime.ProcessInstance
	switch startBy := req.StartBy.(type) {
	case *proto.CreateInstanceRequest_DefinitionKey:
		instance, err = engine.CreateInstanceByKey(ctx, startBy.DefinitionKey, vars)
	case *proto.CreateInstanceRequest_LatestProcessId:
		instance, err = engine.CreateInstanceById(ctx, startBy.LatestProcessId, vars)
	}

	if err != nil && instance == nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("process definition %d not found: %w", req.GetDefinitionKey(), err))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to create process instance: %w", err))
		}
		return &proto.CreateInstanceResponse{Error: zerr.ToProtoError()}, nil
	}
	variables, err := json.Marshal(instance.ProcessInstance().VariableHolder.LocalVariables())
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to marshal process instance result: %w", err))
		return &proto.CreateInstanceResponse{Error: err.ToProtoError()}, nil
	}

	return &proto.CreateInstanceResponse{
		Process: &proto.ProcessInstance{
			Key:               &instance.ProcessInstance().Key,
			ProcessId:         &instance.ProcessInstance().Definition.BpmnProcessId,
			Variables:         variables,
			State:             ptr.To(int64(instance.ProcessInstance().State)),
			CreatedAt:         ptr.To(instance.ProcessInstance().CreatedAt.UnixMilli()),
			DefinitionKey:     &instance.ProcessInstance().Definition.Key,
			ParentInstanceKey: nil,
			BusinessKey:       instance.ProcessInstance().BusinessKey,
			Type:              ptr.To(int64(instance.Type())),
		},
	}, nil
}

func (s *Server) StartProcessInstanceOnElements(ctx context.Context, req *proto.StartInstanceOnElementIdsRequest) (*proto.StartInstanceOnElementIdsResponse, error) {
	engine := s.GetRandomEngine(ctx)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("no engine available on this node"))
		return &proto.StartInstanceOnElementIdsResponse{Error: err.ToProtoError()}, nil
	}
	vars := map[string]any{}
	err := json.Unmarshal(req.Variables, &vars)
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to unmarshal process variables: %w", err))
		return &proto.StartInstanceOnElementIdsResponse{Error: err.ToProtoError()}, nil
	}

	instance, err := engine.CreateInstanceWithStartingElements(ctx, req.GetDefinitionKey(), req.StartingElementIds, vars, nil)
	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("process definition %d not found: %w", req.GetDefinitionKey(), err))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to create process instance on elements from process definition %d: %w", req.GetDefinitionKey(), err))
		}
		return &proto.StartInstanceOnElementIdsResponse{Error: zerr.ToProtoError()}, nil
	}

	variables, err := json.Marshal(instance.ProcessInstance().VariableHolder.LocalVariables())
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to marshal process instance result: %w", err))
		return &proto.StartInstanceOnElementIdsResponse{Error: err.ToProtoError()}, nil
	}

	return &proto.StartInstanceOnElementIdsResponse{
		Process: &proto.ProcessInstance{
			Key:               &instance.ProcessInstance().Key,
			ProcessId:         &instance.ProcessInstance().Definition.BpmnProcessId,
			Variables:         variables,
			State:             ptr.To(int64(instance.ProcessInstance().State)),
			CreatedAt:         ptr.To(instance.ProcessInstance().CreatedAt.UnixMilli()),
			DefinitionKey:     &instance.ProcessInstance().Definition.Key,
			ParentInstanceKey: nil,
			Type:              ptr.To(int64(instance.Type())),
		},
	}, nil
}

func (s *Server) ModifyProcessInstance(ctx context.Context, req *proto.ModifyProcessInstanceRequest) (*proto.ModifyProcessInstanceResponse, error) {
	partitionId := zenflake.GetPartitionId(*req.ProcessInstanceKey)
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("engine with partition %d was not found", partitionId))
		return &proto.ModifyProcessInstanceResponse{Error: err.ToProtoError()}, nil
	}
	vars := map[string]any{}
	err := json.Unmarshal(req.Variables, &vars)
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to unmarshal process variables: %w", err))
		return &proto.ModifyProcessInstanceResponse{Error: err.ToProtoError()}, nil
	}
	var instance runtime.ProcessInstance
	instance, tokens, err := engine.ModifyInstance(ctx, *req.ProcessInstanceKey, req.ElementInstanceIdsToTerminate, req.ElementIdsToStartInstance, vars)
	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("process instance %d not found: %w", *req.ProcessInstanceKey, err))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to modify process instance %d: %w", *req.ProcessInstanceKey, err))
		}
		return &proto.ModifyProcessInstanceResponse{Error: zerr.ToProtoError()}, nil
	}
	variables, err := json.Marshal(instance.ProcessInstance().VariableHolder.LocalVariables())
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to marshal process instance result: %w", err))
		return &proto.ModifyProcessInstanceResponse{Error: err.ToProtoError()}, nil
	}

	respTokens := make([]*proto.ExecutionToken, 0, len(tokens))
	for _, token := range tokens {
		respTokens = append(respTokens, &proto.ExecutionToken{
			Key:                &token.Key,
			ElementInstanceKey: &token.ElementInstanceKey,
			ElementId:          &token.ElementId,
			ProcessInstanceKey: &token.ProcessInstanceKey,
			CreatedAt:          ptr.To(token.CreatedAt.UnixMilli()),
			State:              ptr.To(int64(token.State)),
		})
	}

	return &proto.ModifyProcessInstanceResponse{
		Process: &proto.ProcessInstance{
			Key:               &instance.ProcessInstance().Key,
			ProcessId:         &instance.ProcessInstance().Definition.BpmnProcessId,
			Variables:         variables,
			State:             ptr.To(int64(instance.ProcessInstance().State)),
			CreatedAt:         ptr.To(instance.ProcessInstance().CreatedAt.UnixMilli()),
			DefinitionKey:     &instance.ProcessInstance().Definition.Key,
			ParentInstanceKey: instance.GetParentProcessInstanceKey(),
			Type:              ptr.To(int64(instance.Type())),
		},
		ExecutionTokens: respTokens,
	}, nil
}

func (s *Server) DeleteProcessInstanceVariable(ctx context.Context, req *proto.DeleteProcessInstanceVariableRequest) (*proto.DeleteProcessInstanceVariableResponse, error) {
	partitionId := zenflake.GetPartitionId(*req.ProcessInstanceKey)
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("engine with partition %d was not found", partitionId))
		return &proto.DeleteProcessInstanceVariableResponse{Error: err.ToProtoError()}, nil
	}
	instance, err := engine.DeleteInstanceVariable(ctx, *req.ProcessInstanceKey, req.GetVariable())
	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("process instance %d not found: %w", *req.ProcessInstanceKey, err))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to delete process instance variable: %w", err))
		}
		return &proto.DeleteProcessInstanceVariableResponse{Error: zerr.ToProtoError()}, nil
	}
	variables, err := json.Marshal(instance.ProcessInstance().VariableHolder.LocalVariables())
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to marshal process instance result: %w", err))
		return &proto.DeleteProcessInstanceVariableResponse{Error: err.ToProtoError()}, nil
	}

	return &proto.DeleteProcessInstanceVariableResponse{
		Process: &proto.ProcessInstance{
			Key:               &instance.ProcessInstance().Key,
			ProcessId:         &instance.ProcessInstance().Definition.BpmnProcessId,
			Variables:         variables,
			State:             ptr.To(int64(instance.ProcessInstance().State)),
			CreatedAt:         ptr.To(instance.ProcessInstance().CreatedAt.UnixMilli()),
			DefinitionKey:     &instance.ProcessInstance().Definition.Key,
			ParentInstanceKey: instance.GetParentProcessInstanceKey(),
			BusinessKey:       nil,
			Type:              ptr.To(int64(instance.Type())),
		},
	}, nil
}

func (s *Server) CancelProcessInstance(ctx context.Context, req *proto.CancelProcessInstanceRequest) (*proto.CancelProcessInstanceResponse, error) {
	partitionId := zenflake.GetPartitionId(*req.ProcessInstanceKey)
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("engine with partition %d was not found", partitionId))
		return &proto.CancelProcessInstanceResponse{Error: err.ToProtoError()}, nil
	}
	err := engine.CancelInstanceByKey(ctx, *req.ProcessInstanceKey)
	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("process instance %d not found: %w", *req.ProcessInstanceKey, err))
		} else if errors.Is(err, bpmn.InvalidStateError) {
			zerr = zenerr.Conflict(err)
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to cancel process instance %d: %w", *req.ProcessInstanceKey, err))
		}
		return &proto.CancelProcessInstanceResponse{Error: zerr.ToProtoError()}, nil
	}
	return &proto.CancelProcessInstanceResponse{}, nil
}

func (s *Server) EvaluateDecision(ctx context.Context, req *proto.EvaluateDecisionRequest) (*proto.EvaluatedDRDResult, error) {
	engine := s.GetRandomEngine(ctx)
	if engine == nil {
		zerr := zenerr.TechnicalError(fmt.Errorf("no engine available on this node"))
		return &proto.EvaluatedDRDResult{Error: zerr.ToProtoError()}, nil
	}
	vars := map[string]any{}
	err := json.Unmarshal(req.Variables, &vars)
	if err != nil {
		zerr := zenerr.TechnicalError(fmt.Errorf("failed to unmarshal decision variables: %w", err))
		return &proto.EvaluatedDRDResult{Error: zerr.ToProtoError()}, nil
	}

	result, err := engine.GetDmnEngine().FindAndEvaluateDRD(
		ctx,
		req.GetBindingType(),
		req.GetDecisionId(),
		req.GetVersionTag(),
		vars,
	)
	if err != nil {
		var zerr *zenerr.ZenError
		var notFoundErr *pkgdmn.DecisionNotFoundError
		if errors.As(err, &notFoundErr) || isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("decision definition %s not found: %w", req.GetDecisionId(), err))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to evaluate decision: %w", err))
		}
		return &proto.EvaluatedDRDResult{Error: zerr.ToProtoError()}, nil
	}

	decisionOutput, err := json.Marshal(result.DecisionOutput)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal evaluated decision output: %w", err)
	}

	evaluatedDecisions := make([]*proto.EvaluatedDecisionResult, 0, len(result.EvaluatedDecisions))
	for _, evaluatedDecision := range result.EvaluatedDecisions {

		matchedRules := make([]*proto.EvaluatedRule, 0, len(evaluatedDecision.MatchedRules))
		for _, matchedRule := range evaluatedDecision.MatchedRules {

			evaluatedOutputs := make([]*proto.EvaluatedOutput, 0, len(matchedRule.EvaluatedOutputs))
			for _, evaluatedOutput := range matchedRule.EvaluatedOutputs {
				resultEvaluatedOutput := proto.EvaluatedOutput{
					OutputId:    &evaluatedOutput.OutputId,
					OutputName:  &evaluatedOutput.OutputName,
					OutputValue: nil,
				}

				outputValue := make(map[string]interface{})
				outputValue[evaluatedOutput.OutputJsonName] = evaluatedOutput.OutputValue
				resultEvaluatedOutput.OutputValue, err = json.Marshal(outputValue)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal evaluatedOutput.OutputValue: %w", err)
				}
				evaluatedOutputs = append(evaluatedOutputs, &resultEvaluatedOutput)
			}

			resultMatchedRule := proto.EvaluatedRule{
				RuleId:           &matchedRule.RuleId,
				RuleIndex:        ptr.To(int32(matchedRule.RuleIndex)),
				EvaluatedOutputs: evaluatedOutputs,
			}

			matchedRules = append(matchedRules, &resultMatchedRule)
		}

		evaluatedDecisionOutput, err := json.Marshal(evaluatedDecision.DecisionOutput)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal decision output: %w", err)
		}

		evaluatedInputs := make([]*proto.EvaluatedInput, 0, len(evaluatedDecision.EvaluatedInputs))
		for _, evaluatedInput := range evaluatedDecision.EvaluatedInputs {
			marshaledInputValue, err := json.Marshal(evaluatedInput.InputValue)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal evaluated input %s : %w", evaluatedInput.InputValue, err)
			}

			resultEvaluatedInput := proto.EvaluatedInput{
				InputId:         &evaluatedInput.InputId,
				InputName:       &evaluatedInput.InputName,
				InputExpression: &evaluatedInput.InputExpression,
				InputValue:      marshaledInputValue,
			}
			evaluatedInputs = append(evaluatedInputs, &resultEvaluatedInput)
		}

		evaluatedDecisions = append(evaluatedDecisions, &proto.EvaluatedDecisionResult{
			DecisionId:                   &evaluatedDecision.DecisionId,
			DecisionName:                 &evaluatedDecision.DecisionName,
			DecisionType:                 &evaluatedDecision.DecisionType,
			DmnResourceDefinitionVersion: &evaluatedDecision.DecisionDefinitionVersion,
			DmnResourceDefinitionKey:     &evaluatedDecision.DecisionDefinitionKey,
			DmnResourceDefinitionId:      &evaluatedDecision.DmnResourceDefinitionId,
			MatchedRules:                 matchedRules,
			DecisionOutput:               evaluatedDecisionOutput,
			EvaluatedInputs:              evaluatedInputs,
		})
	}

	return &proto.EvaluatedDRDResult{
		Error:               nil,
		EvaluatedDecisions:  evaluatedDecisions,
		DecisionOutput:      decisionOutput,
		DecisionInstanceKey: &result.DecisionInstanceKey,
	}, nil
}

func (s *Server) DeployDmnResourceDefinition(ctx context.Context, req *proto.DeployDmnResourceDefinitionRequest) (*proto.DeployDmnResourceDefinitionResponse, error) {
	var err error

	bpmnEngines := s.controller.Engines(ctx)

	if len(bpmnEngines) == 0 {
		err := zenerr.TechnicalError(fmt.Errorf("no engines available: %w", err))
		return &proto.DeployDmnResourceDefinitionResponse{Error: err.ToProtoError()}, nil
	}

	var definition *dmn.TDefinitions
	for _, bpmnEngine := range bpmnEngines {
		if definition == nil {
			definition, err = bpmnEngine.GetDmnEngine().ParseDmnFromBytes("", req.Data)
			if err != nil {
				return &proto.DeployDmnResourceDefinitionResponse{
					Error: zenerr.TechnicalError(fmt.Errorf("failed to parse request data: %w", err)).ToProtoError(),
				}, nil
			}
		}
		_, _, err = bpmnEngine.GetDmnEngine().SaveDmnResourceDefinition(ctx, definition, req.GetData(), req.GetKey())
		if err != nil {
			return &proto.DeployDmnResourceDefinitionResponse{
				Error: zenerr.TechnicalError(fmt.Errorf("failed to deploy dmn resource definition: %w", err)).ToProtoError(),
			}, nil
		}
	}
	return &proto.DeployDmnResourceDefinitionResponse{}, nil
}

func (s *Server) DeployProcessDefinition(ctx context.Context, req *proto.DeployProcessDefinitionRequest) (*proto.DeployProcessDefinitionResponse, error) {
	engines := s.controller.Engines(ctx)
	var err error
	for _, engine := range engines {
		_, err = engine.LoadFromBytes(req.GetData(), req.GetKey())
		if err != nil {
			err = fmt.Errorf("failed to deploy process definition: %w", err)
			return &proto.DeployProcessDefinitionResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: ptr.To(err.Error()),
				},
			}, err
		}
	}
	return &proto.DeployProcessDefinitionResponse{}, nil
}

func (s *Server) GetProcessInstance(ctx context.Context, req *proto.GetProcessInstanceRequest) (*proto.GetProcessInstanceResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetProcessInstanceKey())
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("engine with partition %d was not found", partitionId))
		return &proto.GetProcessInstanceResponse{Error: err.ToProtoError()}, nil
	}
	instance, err := engine.FindProcessInstance(req.GetProcessInstanceKey())
	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("process instance %d not found: %w", *req.ProcessInstanceKey, err))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to get process instance %d: %w", *req.ProcessInstanceKey, err))
		}
		return &proto.GetProcessInstanceResponse{Error: zerr.ToProtoError()}, nil
	}

	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
		return &proto.GetProcessInstanceResponse{Error: err.ToProtoError()}, nil
	}

	activeStates := []int64{int64(runtime.TokenStateWaiting), int64(runtime.TokenStateRunning), int64(runtime.TokenStateFailed)}
	tokens, err := queries.GetTokensForProcessInstance(ctx, sql.GetTokensForProcessInstanceParams{
		ProcessInstanceKey: req.GetProcessInstanceKey(),
		States:             activeStates,
	})
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to find process instance execution tokens for instance %d", req.GetProcessInstanceKey()))
		return &proto.GetProcessInstanceResponse{Error: err.ToProtoError()}, nil
	}
	respTokens := make([]*proto.ExecutionToken, 0, len(tokens))
	for _, token := range tokens {
		respTokens = append(respTokens, &proto.ExecutionToken{
			Key:                &token.Key,
			ElementInstanceKey: &token.ElementInstanceKey,
			ElementId:          &token.ElementID,
			ProcessInstanceKey: &token.ProcessInstanceKey,
			CreatedAt:          &token.CreatedAt,
			State:              &token.State,
		})
	}

	vars, err := json.Marshal(instance.ProcessInstance().VariableHolder.LocalVariables())
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to marshal variables of process instance %d", req.GetProcessInstanceKey()))
		return &proto.GetProcessInstanceResponse{Error: err.ToProtoError()}, nil
	}

	return &proto.GetProcessInstanceResponse{
		Processes: &proto.ProcessInstance{
			Key:               &instance.ProcessInstance().Key,
			ProcessId:         &instance.ProcessInstance().Definition.BpmnProcessId,
			Variables:         vars,
			State:             ptr.To(int64(instance.ProcessInstance().State)),
			CreatedAt:         ptr.To(instance.ProcessInstance().CreatedAt.UnixMilli()),
			DefinitionKey:     &instance.ProcessInstance().Definition.Key,
			ParentInstanceKey: instance.GetParentProcessInstanceKey(),
			BusinessKey:       instance.ProcessInstance().BusinessKey,
			Type:              ptr.To(int64(instance.Type())),
		},
		ExecutionTokens: respTokens,
	}, nil
}

func (s *Server) GetDecisionInstance(ctx context.Context, req *proto.GetDecisionInstanceRequest) (*proto.GetDecisionInstanceResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetDecisionInstanceKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		zerr := zenerr.TechnicalError(fmt.Errorf("queries for partition %d was not found", partitionId))
		return &proto.GetDecisionInstanceResponse{Error: zerr.ToProtoError()}, nil
	}

	decisionInstance, err := queries.FindDecisionInstanceByKey(ctx, req.GetDecisionInstanceKey())
	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("decision instance %d not found", req.GetDecisionInstanceKey()))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to find decision instance %d: %w", req.GetDecisionInstanceKey(), err))
		}
		return &proto.GetDecisionInstanceResponse{Error: zerr.ToProtoError()}, nil
	}

	var processInstanceKey *int64
	if decisionInstance.ProcessInstanceKey.Valid {
		processInstanceKey = &decisionInstance.ProcessInstanceKey.Int64
	}
	var flowElementInstanceKey *int64
	if decisionInstance.FlowElementInstanceKey.Valid {
		flowElementInstanceKey = &decisionInstance.FlowElementInstanceKey.Int64
	}
	return &proto.GetDecisionInstanceResponse{
		DecisionInstance: &proto.DecisionInstance{
			Key:                      &decisionInstance.Key,
			DmnResourceDefinitionKey: &decisionInstance.DmnResourceDefinitionKey,
			ProcessInstanceKey:       processInstanceKey,
			FlowElementInstanceKey:   flowElementInstanceKey,
			EvaluatedAt:              &decisionInstance.CreatedAt,
			EvaluatedDecisions:       &decisionInstance.EvaluatedDecisions,
			DecisionOutput:           &decisionInstance.OutputVariables,
		},
	}, nil
}

func (s *Server) GetDecisionInstances(ctx context.Context, req *proto.GetDecisionInstancesRequest) (*proto.GetDecisionInstancesResponse, error) {
	resp := make([]*proto.PartitionedDecisionInstances, 0, len(req.Partitions))
	for _, partitionId := range req.Partitions {
		queries := s.controller.PartitionQueries(ctx, partitionId)
		if queries == nil {
			zerr := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
			return &proto.GetDecisionInstancesResponse{Error: zerr.ToProtoError()}, nil
		}
		instances, err := queries.FindDecisionInstancesPage(ctx, sql.FindDecisionInstancesPageParams{
			DmnResourceDefinitionKey: sql.ToNullInt64(req.DmnResourceDefinitionKey),
			DmnResourceDefinitionID:  sql.ToNullString(req.DmnResourceDefinitionId),
			ProcessInstanceKey:       sql.ToNullInt64(req.ProcessInstanceKey),
			EvaluatedFrom:            sql.ToNullInt64(req.EvaluatedFrom),
			EvaluatedTo:              sql.ToNullInt64(req.EvaluatedTo),
			SortByOrder:              sql.ToNullString(req.SortByOrder),
			Offset:                   int64(req.GetSize()) * int64(req.GetPage()-1),
			Size:                     int64(req.GetSize()),
		})
		if err != nil {
			zerr := zenerr.TechnicalError(fmt.Errorf("failed to find decision instances for request %v: %w", req, err))
			return &proto.GetDecisionInstancesResponse{Error: zerr.ToProtoError()}, nil
		}
		totalCount := int32(0)
		if len(instances) > 0 {
			totalCount = int32(instances[0].TotalCount)
		}
		decisionInstances := make([]*proto.DecisionInstance, len(instances))
		for i, di := range instances {
			var processInstanceKey *int64
			if di.ProcessInstanceKey.Valid {
				processInstanceKey = &di.ProcessInstanceKey.Int64
			}
			var flowElementInstanceKey *int64
			if di.FlowElementInstanceKey.Valid {
				flowElementInstanceKey = &di.FlowElementInstanceKey.Int64
			}
			decisionInstances[i] = &proto.DecisionInstance{
				Key:                      &di.Key,
				DmnResourceDefinitionKey: &di.DmnResourceDefinitionKey,
				ProcessInstanceKey:       processInstanceKey,
				EvaluatedAt:              &di.CreatedAt,
				FlowElementInstanceKey:   flowElementInstanceKey,
			}
		}
		resp = append(resp, &proto.PartitionedDecisionInstances{
			PartitionId:       &partitionId,
			DecisionInstances: decisionInstances,
			TotalCount:        ptr.To(totalCount),
		})
	}
	return &proto.GetDecisionInstancesResponse{
		Partitions: resp,
	}, nil
}

func (s *Server) GetProcessInstanceJobs(ctx context.Context, req *proto.GetProcessInstanceJobsRequest) (*proto.GetProcessInstanceJobsResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetProcessInstanceKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
		return &proto.GetProcessInstanceJobsResponse{Error: err.ToProtoError()}, nil
	}
	result, err := queries.FindProcessInstanceJobs(ctx, sql.FindProcessInstanceJobsParams{
		Offset:             int64(req.GetSize()) * int64(req.GetPage()-1),
		Size:               int64(req.GetSize()),
		ProcessInstanceKey: req.GetProcessInstanceKey(),
	})
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to find process instance jobs for instance %d", req.GetProcessInstanceKey()))
		return &proto.GetProcessInstanceJobsResponse{Error: err.ToProtoError()}, nil
	}
	jobs := make([]*proto.Job, len(result))
	totalCount := int32(0)
	if len(result) > 0 {
		totalCount = int32(result[0].TotalCount)
	}
	for i, job := range result {
		var assignee *string
		if job.Assignee.Valid {
			assignee = &job.Assignee.String
		}
		jobs[i] = &proto.Job{
			Key:                &job.Key,
			ElementInstanceKey: &job.ElementInstanceKey,
			ElementId:          &job.ElementID,
			ProcessInstanceKey: &job.ProcessInstanceKey,
			Type:               &job.Type,
			State:              ptr.To(job.State),
			CreatedAt:          &job.CreatedAt,
			Variables:          []byte(job.Variables),
			Assignee:           assignee,
		}
	}
	return &proto.GetProcessInstanceJobsResponse{
		Jobs:       jobs,
		TotalCount: &totalCount,
	}, nil
}

func (s *Server) AssignJobToAssignee(ctx context.Context, req *proto.AssignJobToAssigneeRequest) (*proto.AssignJobToAssigneeResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetKey())
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("engine for partition %d not found", partitionId))
		return &proto.AssignJobToAssigneeResponse{Error: err.ToProtoError()}, nil
	}
	assignee := req.GetAssignee()
	var assigneePtr *string
	if assignee != "" {
		assigneePtr = &assignee
	}
	if err := engine.JobAssignByKey(ctx, req.GetKey(), assigneePtr); err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("job %d not found: %w", req.GetKey(), err))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to assign job %d: %w", req.GetKey(), err))
		}
		return &proto.AssignJobToAssigneeResponse{Error: zerr.ToProtoError()}, nil
	}
	return &proto.AssignJobToAssigneeResponse{}, nil
}

func (s *Server) GetFlowElementHistory(ctx context.Context, req *proto.GetFlowElementHistoryRequest) (*proto.GetFlowElementHistoryResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetProcessInstanceKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
		return &proto.GetFlowElementHistoryResponse{Error: err.ToProtoError()}, nil
	}
	flowElements, err := queries.FindFlowElementInstances(ctx, sql.FindFlowElementInstancesParams{
		ProcessInstanceKey: *req.ProcessInstanceKey,
		Offset:             int64(req.GetSize()) * int64(req.GetPage()-1),
		Limit:              int64(req.GetSize()),
	})
	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to find flow element history for instance %d", req.GetProcessInstanceKey()))
		return &proto.GetFlowElementHistoryResponse{Error: err.ToProtoError()}, nil
	}

	result := make([]*proto.FlowElement, len(flowElements))
	totalCount := int32(0)
	if len(flowElements) > 0 {
		totalCount = int32(flowElements[0].TotalCount)
	}
	for i, flowElement := range flowElements {
		result[i] = &proto.FlowElement{
			Key:                &flowElement.Key,
			ElementId:          &flowElement.ElementID,
			ProcessInstanceKey: &flowElement.ProcessInstanceKey,
			CreatedAt:          &flowElement.CreatedAt,
		}
	}

	return &proto.GetFlowElementHistoryResponse{
		Flow:       result,
		TotalCount: ptr.To(totalCount),
	}, nil
}

func (s *Server) GetJobs(ctx context.Context, req *proto.GetJobsRequest) (*proto.GetJobsResponse, error) {
	resp := make([]*proto.PartitionedJobs, 0, len(req.Partitions))
	for _, partitionId := range req.Partitions {
		queries := s.controller.PartitionQueries(ctx, partitionId)
		if queries == nil {
			err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
			return &proto.GetJobsResponse{Error: err.ToProtoError()}, nil
		}

		dbJobs, err := queries.FindJobs(ctx, sql.FindJobsParams{
			State:              sql.ToNullInt64(req.State),
			Type:               sql.ToNullString(req.JobType),
			Assignee:           sql.ToNullString(req.Assignee),
			ProcessInstanceKey: sql.ToNullInt64(req.ProcessInstanceKey),
			Sort:               sql.ToNullString(req.Sort),
			Offset:             int64(req.GetSize()) * int64(req.GetPage()-1),
			Limit:              int64(req.GetSize()),
		})
		if err != nil {
			err := zenerr.TechnicalError(fmt.Errorf("failed to find jobs list %w", err))
			return &proto.GetJobsResponse{Error: err.ToProtoError()}, nil
		}

		partitionJobs := make([]*proto.Job, len(dbJobs))

		totalCount := int32(0)
		for i, job := range dbJobs {
			if i == 0 {
				totalCount = int32(job.TotalCount)
			}

			var a *string
			if job.Assignee.Valid {
				a = &job.Assignee.String
			}
			partitionJobs[i] = &proto.Job{
				Key:                ptr.To(job.Key),
				ProcessInstanceKey: ptr.To(job.ProcessInstanceKey),
				ElementId:          ptr.To(job.ElementID),
				ElementInstanceKey: ptr.To(job.ElementInstanceKey),
				Type:               ptr.To(job.Type),
				CreatedAt:          ptr.To(job.CreatedAt),
				State:              ptr.To(job.State),
				Assignee:           a,
				Variables:          []byte(job.Variables),
			}
		}

		resp = append(resp, &proto.PartitionedJobs{
			PartitionId: &partitionId,
			Jobs:        partitionJobs,
			TotalCount:  ptr.To(totalCount),
		})
	}
	return &proto.GetJobsResponse{
		Partitions: resp,
	}, nil
}

func (s *Server) GetJob(ctx context.Context, req *proto.GetJobRequest) (*proto.GetJobResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetJobKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
		return &proto.GetJobResponse{Error: err.ToProtoError()}, nil
	}

	job, err := queries.FindJobByJobKey(ctx, *req.JobKey)

	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(fmt.Errorf("job %d not found", req.GetJobKey()))
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to get job %d: %w", req.GetJobKey(), err))
		}
		return &proto.GetJobResponse{Error: zerr.ToProtoError()}, nil
	}

	var assignee *string
	if job.Assignee.Valid {
		assignee = &job.Assignee.String
	}

	return &proto.GetJobResponse{
		Job: &proto.Job{
			Key:                &job.Key,
			ElementInstanceKey: &job.ElementInstanceKey,
			ElementId:          &job.ElementID,
			ProcessInstanceKey: &job.ProcessInstanceKey,
			Type:               &job.Type,
			State:              &job.State,
			CreatedAt:          &job.CreatedAt,
			Assignee:           assignee,
			Variables:          []byte(job.Variables),
		},
	}, nil

}

func (s *Server) GetProcessInstances(ctx context.Context, req *proto.GetProcessInstancesRequest) (*proto.GetProcessInstancesResponse, error) {
	resp := make([]*proto.PartitionedProcessInstances, 0, len(req.Partitions))
	for _, partitionId := range req.Partitions {
		queries := s.controller.PartitionQueries(ctx, partitionId)
		if queries == nil {
			err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
			return &proto.GetProcessInstancesResponse{Error: err.ToProtoError()}, nil
		}

		var filterTypeCallActivity *int64
		var filterTypeMultiInstance *int64
		var filterTypeDefault *int64
		var filterTypeSubProcess *int64

		if req.ShowChildProcesses != nil && req.GetShowChildProcesses() == true {
			filterTypeCallActivity = ptr.To(int64(runtime.ProcessTypeCallActivity))
			filterTypeMultiInstance = ptr.To(int64(runtime.ProcessTypeMultiInstance))
			filterTypeDefault = ptr.To(int64(runtime.ProcessTypeDefault))
			filterTypeSubProcess = ptr.To(int64(runtime.ProcessTypeSubProcess))
		} else {
			filterTypeDefault = ptr.To(int64(runtime.ProcessTypeDefault))
			filterTypeCallActivity = ptr.To(int64(runtime.ProcessTypeCallActivity))
		}

		instances, err := queries.FindProcessInstancesPage(ctx, sql.FindProcessInstancesPageParams{
			SortByOrder:             sql.ToNullString(req.SortByOrder),
			ProcessDefinitionKey:    req.GetDefinitionKey(),
			ParentInstanceKey:       req.GetParentKey(),
			BusinessKey:             sql.ToNullString(req.BusinessKey),
			BpmnProcessID:           sql.ToNullString(req.ProcessId),
			CreatedFrom:             sql.ToNullInt64(req.CreatedFrom),
			CreatedTo:               sql.ToNullInt64(req.CreatedTo),
			State:                   sql.ToNullInt64(req.State),
			FilterTypeCallActivity:  sql.ToNullInt64(filterTypeCallActivity),
			FilterTypeMultiInstance: sql.ToNullInt64(filterTypeMultiInstance),
			FilterTypeDefault:       sql.ToNullInt64(filterTypeDefault),
			FilterTypeSubProcess:    sql.ToNullInt64(filterTypeSubProcess),
			Offset:                  int64(req.GetSize()) * int64(req.GetPage()-1),
			Size:                    int64(req.GetSize()),
			ActivityID:              sql.ToNullString(req.ActivityId),
		})
		if err != nil {
			err := zenerr.TechnicalError(fmt.Errorf("failed to find process instances with definition key %d", req.DefinitionKey))
			return &proto.GetProcessInstancesResponse{Error: err.ToProtoError()}, nil
		}
		totalCount := int32(0)
		if len(instances) > 0 {
			totalCount = int32(instances[0].TotalCount)
		}
		procInstances := make([]*proto.ProcessInstance, len(instances))

		parentTokenKeys := make([]int64, 0, len(instances))
		for _, inst := range instances {
			parentTokenKeys = append(parentTokenKeys, inst.ParentProcessExecutionToken.Int64)
		}
		parentTokens, err := queries.GetTokens(ctx, parentTokenKeys)
		if err != nil {
			return nil, err
		}
		parentTokensMap := make(map[int64]sql.ExecutionToken, len(parentTokens))
		for i, _ := range parentTokens {
			parentTokensMap[parentTokens[i].Key] = parentTokens[i]
		}

		for i, _ := range instances {
			var businessKey *string
			if instances[i].BusinessKey.Valid {
				businessKey = &instances[i].BusinessKey.String
			}

			var parentInstanceKey *int64
			if instances[i].ParentProcessExecutionToken.Valid {
				parentInstanceKey = ptr.To(parentTokensMap[instances[i].ParentProcessExecutionToken.Int64].ProcessInstanceKey)
			}

			procInstances[i] = &proto.ProcessInstance{
				Key:               &instances[i].Key,
				ProcessId:         &instances[i].BpmnProcessID,
				Variables:         []byte(instances[i].Variables),
				State:             ptr.To(instances[i].State),
				CreatedAt:         &instances[i].CreatedAt,
				DefinitionKey:     &instances[i].ProcessDefinitionKey,
				ParentInstanceKey: parentInstanceKey,
				BusinessKey:       businessKey,
				Type:              ptr.To(instances[i].ProcessType),
			}
		}
		resp = append(resp, &proto.PartitionedProcessInstances{
			PartitionId: &partitionId,
			Instances:   procInstances,
			TotalCount:  ptr.To(totalCount),
		})
	}
	return &proto.GetProcessInstancesResponse{
		Partitions: resp,
	}, nil
}

func (s *Server) GetChildProcessInstances(ctx context.Context, req *proto.GetChildProcessInstancesRequest) (*proto.GetChildProcessInstancesResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetParentInstanceKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := fmt.Errorf("queries for partition %d not found", partitionId)
		return &proto.GetChildProcessInstancesResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	var filterTypeDefault *int64
	filterTypeMultiInstance := ptr.To(int64(runtime.ProcessTypeMultiInstance))
	filterTypeCallActivity := ptr.To(int64(runtime.ProcessTypeCallActivity))
	filterTypeSubprocess := ptr.To(int64(runtime.ProcessTypeSubProcess))

	var businessKey *string
	var bpmnProcessID *string
	var createdFrom *int64
	var createdTo *int64

	instances, err := queries.FindProcessInstancesPage(ctx, sql.FindProcessInstancesPageParams{
		SortByOrder:             sql.ToNullString(req.SortByOrder),
		ProcessDefinitionKey:    0,
		ParentInstanceKey:       req.GetParentInstanceKey(),
		BusinessKey:             sql.ToNullString(businessKey),
		BpmnProcessID:           sql.ToNullString(bpmnProcessID),
		CreatedFrom:             sql.ToNullInt64(createdFrom),
		CreatedTo:               sql.ToNullInt64(createdTo),
		State:                   sql.ToNullInt64(req.State),
		FilterTypeCallActivity:  sql.ToNullInt64(filterTypeCallActivity),
		FilterTypeMultiInstance: sql.ToNullInt64(filterTypeMultiInstance),
		FilterTypeDefault:       sql.ToNullInt64(filterTypeDefault),
		FilterTypeSubProcess:    sql.ToNullInt64(filterTypeSubprocess),
		Offset:                  int64(req.GetSize()) * int64(req.GetPage()-1),
		Size:                    int64(req.GetSize()),
	})
	if err != nil {
		err := fmt.Errorf("failed to find child process instances with key %d", req.ParentInstanceKey)
		return &proto.GetChildProcessInstancesResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	totalCount := int32(0)
	if len(instances) > 0 {
		totalCount = int32(instances[0].TotalCount)
	}

	parentTokenKeys := make([]int64, 0, len(instances))
	for _, inst := range instances {
		parentTokenKeys = append(parentTokenKeys, inst.ParentProcessExecutionToken.Int64)
	}
	parentTokens, err := queries.GetTokens(ctx, parentTokenKeys)
	if err != nil {
		return nil, err
	}
	parentTokensMap := make(map[int64]sql.ExecutionToken, len(parentTokens))
	for i, _ := range parentTokens {
		parentTokensMap[parentTokens[i].Key] = parentTokens[i]
	}

	procInstances := make([]*proto.ProcessInstance, len(instances))
	for i, _ := range instances {
		var businessKey *string
		if instances[i].BusinessKey.Valid {
			businessKey = &instances[i].BusinessKey.String
		}

		var parentInstanceKey *int64
		if instances[i].ParentProcessExecutionToken.Valid {
			parentInstanceKey = ptr.To(parentTokensMap[instances[i].ParentProcessExecutionToken.Int64].ProcessInstanceKey)
		}

		procInstances[i] = &proto.ProcessInstance{
			Key:               &instances[i].Key,
			ProcessId:         &instances[i].BpmnProcessID,
			Variables:         []byte(instances[i].Variables),
			State:             ptr.To(instances[i].State),
			CreatedAt:         &instances[i].CreatedAt,
			DefinitionKey:     &instances[i].ProcessDefinitionKey,
			ParentInstanceKey: parentInstanceKey,
			BusinessKey:       businessKey,
			Type:              ptr.To(instances[i].ProcessType),
		}
	}

	return &proto.GetChildProcessInstancesResponse{
		Instances:  procInstances,
		TotalCount: ptr.To(totalCount),
	}, nil
}

func (s *Server) PublishMessage(ctx context.Context, req *proto.PublishMessageRequest) (*proto.PublishMessageResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetKey())
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("engine with partition %d was not found", partitionId))
		return &proto.PublishMessageResponse{Error: err.ToProtoError()}, nil
	}

	vars := map[string]any{}
	if err := json.Unmarshal(req.GetVariables(), &vars); err != nil {
		zerr := zenerr.TechnicalError(fmt.Errorf("failed to unmarshal message input variables: %w", err))
		return &proto.PublishMessageResponse{Error: zerr.ToProtoError()}, nil
	}

	if err := engine.PublishMessage(ctx, req.GetKey(), vars); err != nil {
		zerr := zenerr.TechnicalError(fmt.Errorf("failed to publish message event %d: %w", req.GetKey(), err))
		return &proto.PublishMessageResponse{Error: zerr.ToProtoError()}, nil
	}

	return &proto.PublishMessageResponse{}, nil
}

func (s *Server) SetMessageSubscriptionPointer(ctx context.Context, req *proto.SetMessageSubscriptionPointerRequest) (*proto.SetMessageSubscriptionPointerResponse, error) {
	partition := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partition == nil {
		err := fmt.Errorf("node for partition %d was not found", req.GetPartitionId())
		return &proto.SetMessageSubscriptionPointerResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	partitionId := s.store.ClusterState().GetPartitionIdFromString(req.GetCorrelationKey())
	if partition.PartitionId == partitionId && partition.IsLeader(ctx) {
		err := partition.DB.Queries.SaveMessageSubscriptionPointer(ctx, sql.SaveMessageSubscriptionPointerParams{
			State:                  int64(req.GetState()),
			CreatedAt:              req.GetCreatedAt(),
			Name:                   req.GetName(),
			CorrelationKey:         req.GetCorrelationKey(),
			MessageSubscriptionKey: req.GetMessageSubscriptionKey(),
			ExecutionTokenKey:      req.GetExecutionTokenKey(),
		})
		if err != nil {
			err := fmt.Errorf("failed to save message subscription pointer for message subscription name:%s correlationId:%s : %w", req.GetName(), req.GetCorrelationKey(), err)
			return &proto.SetMessageSubscriptionPointerResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: ptr.To(err.Error()),
				},
			}, err
		}
	} else {
		err := fmt.Errorf("Node is not a leader for partition %d", req.PartitionId)
		return &proto.SetMessageSubscriptionPointerResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	return &proto.SetMessageSubscriptionPointerResponse{}, nil
}

func (s *Server) FindActiveMessage(ctx context.Context, req *proto.FindActiveMessageRequest) (*proto.FindActiveMessageResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetExecutionTokenKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := fmt.Errorf("failed to find partition %d", partitionId)
		return &proto.FindActiveMessageResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	subs, err := queries.FindTokenMessageSubscriptions(ctx, sql.FindTokenMessageSubscriptionsParams{
		ExecutionToken: req.GetExecutionTokenKey(),
		State:          int64(runtime.ActivityStateActive),
	})
	if err != nil && !isErrNotFound(err) {
		err := fmt.Errorf("failed to find message subscriptions %d", partitionId)
		return &proto.FindActiveMessageResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	// if len(subs) == 0{
	// }
	for _, message := range subs {
		if message.CorrelationKey == req.GetCorrelationKey() && message.Name == req.GetName() {
			return &proto.FindActiveMessageResponse{
				Key:                  &message.Key,
				ElementId:            &message.ElementID,
				ProcessDefinitionKey: &message.ProcessDefinitionKey,
				ProcessInstanceKey:   &message.ProcessInstanceKey,
				Name:                 &message.Name,
				State:                &message.State,
				CorrelationKey:       &message.CorrelationKey,
				ExecutionToken:       &message.ExecutionToken,
			}, nil
		}
	}
	err = fmt.Errorf("message subscription %s %s was not found in partition %d", req.GetName(), req.GetCorrelationKey(), partitionId)
	return &proto.FindActiveMessageResponse{
		Error: &proto.ErrorResult{
			Code:    nil,
			Message: ptr.To(err.Error()),
		},
	}, status.Error(codes.NotFound, err.Error())
}

func (s *Server) GetIncidents(ctx context.Context, req *proto.GetIncidentsRequest) (*proto.GetIncidentsResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetProcessInstanceKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
		return &proto.GetIncidentsResponse{Error: err.ToProtoError()}, nil
	}

	incidents, err := queries.FindIncidentsPageByProcessInstanceKey(ctx, sql.FindIncidentsPageByProcessInstanceKeyParams{
		ProcessInstanceKey: req.GetProcessInstanceKey(),
		State:              sql.ToNullString(req.State),
		Offset:             int64(req.GetSize()) * int64(req.GetPage()-1),
		Size:               int64(req.GetSize()),
	})

	if err != nil {
		err := zenerr.TechnicalError(fmt.Errorf("failed to find incidents for instance %d", req.GetProcessInstanceKey()))
		return &proto.GetIncidentsResponse{Error: err.ToProtoError()}, nil
	}
	var totalCount int32

	if len(incidents) > 0 {
		totalCount = int32(incidents[0].TotalCount)
	}

	results := make([]*proto.Incident, len(incidents))
	for i, incident := range incidents {
		results[i] = &proto.Incident{
			Key:                &incident.Key,
			ElementInstanceKey: &incident.ElementInstanceKey,
			ElementId:          &incident.ElementID,
			ProcessInstanceKey: &incident.ProcessInstanceKey,
			Message:            &incident.Message,
			CreatedAt:          &incident.CreatedAt,
			ResolvedAt: func() *int64 {
				if incident.ResolvedAt.Valid {
					return &incident.ResolvedAt.Int64
				}
				return nil
			}(),
			ExecutionToken: &incident.ExecutionToken,
		}
	}
	return &proto.GetIncidentsResponse{
		Incidents:  results,
		TotalCount: &totalCount,
	}, nil
}

func (s *Server) ResolveIncident(ctx context.Context, req *proto.ResolveIncidentRequest) (*proto.ResolveIncidentResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetIncidentKey())
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := zenerr.TechnicalError(fmt.Errorf("engine with partition %d was not found", partitionId))
		return &proto.ResolveIncidentResponse{Error: err.ToProtoError()}, nil
	}
	err := engine.ResolveIncident(ctx, req.GetIncidentKey())
	if err != nil {
		var zerr *zenerr.ZenError
		if isErrNotFound(err) {
			zerr = zenerr.NotFound(err)
		} else {
			zerr = zenerr.TechnicalError(fmt.Errorf("failed to resolve incident %d: %w", req.GetIncidentKey(), err))
		}
		return &proto.ResolveIncidentResponse{Error: zerr.ToProtoError()}, nil
	}
	return &proto.ResolveIncidentResponse{}, nil
}

func (s *Server) SubscribeJob(stream grpc.BidiStreamingServer[proto.SubscribeJobRequest, proto.SubscribeJobResponse]) error {
	return s.jobManager.AddNodeSubscription(stream)
}

func (s *Server) GetRandomEngine(ctx context.Context) *bpmn.Engine {
	engines := s.controller.Engines(ctx)
	if len(engines) == 0 {
		return nil
	}
	index := rand.Intn(len(engines))
	i := 0
	for _, engine := range engines {
		if i == index {
			return engine
		}
		i++
	}
	return nil
}

func (s *Server) GetProcessDefinitionElementStatistics(ctx context.Context, req *proto.GetProcessDefinitionElementStatisticsRequest) (*proto.GetProcessDefinitionElementStatisticsResponse, error) {
	resp := make([]*proto.PartitionedElementStatistics, 0, len(req.Partitions))
	for _, partitionId := range req.Partitions {
		queries := s.controller.PartitionQueries(ctx, partitionId)
		if queries == nil {
			err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
			return &proto.GetProcessDefinitionElementStatisticsResponse{
				Error: err.ToProtoError(),
			}, nil
		}

		rows, err := queries.GetElementStatisticsByProcessDefinitionKey(ctx, req.GetProcessDefinitionKey())
		if err != nil {
			zenErr := zenerr.TechnicalError(fmt.Errorf("failed to get element statistics for process definition %d: %w", req.GetProcessDefinitionKey(), err))
			return &proto.GetProcessDefinitionElementStatisticsResponse{
				Error: zenErr.ToProtoError(),
			}, nil
		}

		statistics := make([]*proto.ElementStatisticEntry, len(rows))
		for i, row := range rows {
			statistics[i] = &proto.ElementStatisticEntry{
				ElementId:     ptr.To(row.ElementID),
				ActiveCount:   ptr.To(int32(row.ActiveCount)),
				IncidentCount: ptr.To(int32(row.IncidentCount)),
			}
		}

		resp = append(resp, &proto.PartitionedElementStatistics{
			PartitionId: &partitionId,
			Statistics:  statistics,
		})
	}
	return &proto.GetProcessDefinitionElementStatisticsResponse{Partitions: resp}, nil
}

func (s *Server) GetProcessDefinitionStatistics(ctx context.Context, req *proto.GetProcessDefinitionStatisticsRequest) (*proto.GetProcessDefinitionStatisticsResponse, error) {
	resp := make([]*proto.PartitionedProcessDefinitionStatistics, 0, len(req.Partitions))
	for _, partitionId := range req.Partitions {
		queries := s.controller.PartitionQueries(ctx, partitionId)
		if queries == nil {
			err := zenerr.TechnicalError(fmt.Errorf("queries for partition %d not found", partitionId))
			return &proto.GetProcessDefinitionStatisticsResponse{
				Error: err.ToProtoError(),
			}, nil
		}

		onlyLatest := int64(0)
		if req.OnlyLatest != nil && *req.OnlyLatest {
			onlyLatest = 1
		}

		var bpmnProcessIDInJson interface{}
		if req.BpmnProcessIdIn != nil {
			b, err := json.Marshal(req.BpmnProcessIdIn)
			if err != nil {
				zenErr := zenerr.TechnicalError(fmt.Errorf("failed to marshal bpmn process id in filter: %w", err))
				return &proto.GetProcessDefinitionStatisticsResponse{
					Error: zenErr.ToProtoError(),
				}, nil
			}
			bpmnProcessIDInJson = string(b)
		}

		var definitionKeyInJson interface{}
		if req.BpmnProcessDefinitionKeyIn != nil {
			b, err := json.Marshal(req.BpmnProcessDefinitionKeyIn)
			if err != nil {
				zenErr := zenerr.TechnicalError(fmt.Errorf("failed to marshal process definition key in filter: %w", err))
				return &proto.GetProcessDefinitionStatisticsResponse{
					Error: zenErr.ToProtoError(),
				}, nil
			}
			definitionKeyInJson = string(b)
		}

		dbStats, err := queries.FindProcessDefinitionStatistics(ctx, sql.FindProcessDefinitionStatisticsParams{
			Sort:                sql.ToNullString((*string)(req.Sort)),
			NameFilter:          sql.ToNullString(req.Name),
			OnlyLatest:          onlyLatest,
			Offset:              int64(req.GetSize()) * int64(req.GetPage()-1),
			Limit:               int64(req.GetSize()),
			BpmnProcessIDInJson: bpmnProcessIDInJson,
			DefinitionKeyInJson: definitionKeyInJson,
		})
		if err != nil {
			zenErr := zenerr.TechnicalError(fmt.Errorf("failed to find process definition statistics: %w", err))
			return &proto.GetProcessDefinitionStatisticsResponse{
				Error: zenErr.ToProtoError(),
			}, nil
		}

		partitionStats := make([]*proto.ProcessDefinitionStatistics, len(dbStats))

		totalCount := int32(0)
		for i, stat := range dbStats {
			if i == 0 {
				totalCount = int32(stat.TotalCount)
			}

			partitionStats[i] = &proto.ProcessDefinitionStatistics{
				Key:             ptr.To(stat.Key),
				Version:         ptr.To(int32(stat.Version)),
				BpmnProcessId:   ptr.To(stat.BpmnProcessID),
				BpmnProcessName: ptr.To(stat.BpmnProcessName),
				InstanceCounts: &proto.InstanceCounts{
					Total:      ptr.To(stat.TotalInstances),
					Active:     ptr.To(int64(stat.ActiveCount)),
					Completed:  ptr.To(int64(stat.CompletedCount)),
					Terminated: ptr.To(int64(stat.TerminatedCount)),
					Failed:     ptr.To(int64(stat.FailedCount)),
				},
			}
		}

		resp = append(resp, &proto.PartitionedProcessDefinitionStatistics{
			PartitionId: &partitionId,
			Statistics:  partitionStats,
			TotalCount:  ptr.To(totalCount),
		})
	}
	return &proto.GetProcessDefinitionStatisticsResponse{
		Partitions: resp,
	}, nil
}

func (s *Server) StartPprofServer(ctx context.Context, r *proto.PprofServerRequest) (*proto.PprofServerStartResult, error) {
	if s.cpuProfile.Running == true {
		err := fmt.Errorf("pprof server already running")
		return &proto.PprofServerStartResult{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	addr, _, _ := strings.Cut(s.addr.String(), ":")

	s.cpuProfile.Server = &http.Server{
		Addr:    addr + ":6060",
		Handler: nil,
	}

	go func() {
		log.Info("%s", s.cpuProfile.Server.ListenAndServe())
	}()

	s.cpuProfile.Running = true

	return &proto.PprofServerStartResult{}, nil
}

func (s *Server) StopPprofServer(context.Context, *proto.PprofServerRequest) (*proto.PprofServerStopResult, error) {
	if s.cpuProfile.Running == false {
		err := fmt.Errorf("pprof server not started")
		return &proto.PprofServerStopResult{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	if err := s.cpuProfile.Server.Shutdown(context.Background()); err != nil {
		log.Info("shutdown failed: %s", err)
	}

	s.cpuProfile.Running = false

	return &proto.PprofServerStopResult{
		Error: nil,
	}, nil
}

func isErrNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows) || errors.Is(err, storage.ErrNotFound)
}
