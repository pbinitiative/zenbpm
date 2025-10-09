package server

import (
	"bytes"
	"context"
	ssql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"runtime/pprof"
	"slices"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
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
	Output  *bytes.Buffer
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
		err := fmt.Errorf("failed to unmarshal job input variables: %w", err)
		return &proto.CompleteJobResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	err = s.jobManager.CompleteJob(ctx, jobmanager.ClientID(req.GetClientId()), req.GetKey(), vars)
	if err != nil {
		err := fmt.Errorf("failed to complete job %d: %w", req.Key, err)
		return &proto.CompleteJobResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
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
		err := fmt.Errorf("no engine available on this node")
		return &proto.CreateInstanceResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	vars := map[string]any{}
	err := json.Unmarshal(req.Variables, &vars)
	if err != nil {
		err := fmt.Errorf("failed to unmarshal process variables: %w", err)
		return &proto.CreateInstanceResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	var instance *runtime.ProcessInstance
	switch startBy := req.StartBy.(type) {
	case *proto.CreateInstanceRequest_DefinitionKey:
		instance, err = engine.CreateInstanceByKey(ctx, startBy.DefinitionKey, req.StartingFlowNodeId, vars)
	case *proto.CreateInstanceRequest_LatestProcessId:
		instance, err = engine.CreateInstanceById(ctx, startBy.LatestProcessId, req.StartingFlowNodeId, vars)
	}
	if err != nil {
		err := fmt.Errorf("failed to create process instance: %w", err)
		return &proto.CreateInstanceResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	variables, err := json.Marshal(instance.VariableHolder.Variables())
	if err != nil {
		err := fmt.Errorf("failed to marshal process instance result: %w", err)
		return &proto.CreateInstanceResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	return &proto.CreateInstanceResponse{
		Process: &proto.ProcessInstance{
			Key:           &instance.Key,
			ProcessId:     &instance.Definition.BpmnProcessId,
			Variables:     variables,
			State:         ptr.To(int64(instance.State)),
			CreatedAt:     ptr.To(instance.CreatedAt.UnixMilli()),
			DefinitionKey: &instance.Definition.Key,
		},
	}, nil
}

func (s *Server) EvaluateDecision(ctx context.Context, req *proto.EvaluateDecisionRequest) (*proto.EvaluatedDRDResult, error) {
	engine := s.GetRandomEngine(ctx)
	if engine == nil {
		err := fmt.Errorf("no engine available on this node")
		return &proto.EvaluatedDRDResult{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	vars := map[string]any{}
	err := json.Unmarshal(req.Variables, &vars)
	if err != nil {
		err := fmt.Errorf("failed to unmarshal decision variables: %w", err)
		return &proto.EvaluatedDRDResult{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	result, err := engine.GetDmnEngine().FindAndEvaluateDRD(
		ctx,
		req.GetBindingType(),
		req.GetDecisionId(),
		req.GetVersionTag(),
		vars,
	)
	if err != nil {
		err := fmt.Errorf("failed to create process instance: %w", err)
		return &proto.EvaluatedDRDResult{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
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

		resultDecisionOutput, err := json.Marshal(result.DecisionOutput)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal decision output: %w", err)
		}

		evaluatedInputs := make([]*proto.EvaluatedInput, 0, len(evaluatedDecision.EvaluatedInputs))
		for _, evaluatedInput := range evaluatedDecision.EvaluatedInputs {
			resultEvaluatedInput := proto.EvaluatedInput{
				InputId:         &evaluatedInput.InputId,
				InputName:       &evaluatedInput.InputName,
				InputExpression: &evaluatedInput.InputExpression,
				InputValue:      nil,
			}

			inputValue := make(map[string]interface{})
			inputValue[evaluatedInput.InputExpression] = evaluatedInput.InputValue
			resultEvaluatedInput.InputValue, err = json.Marshal(inputValue)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal evaluatedInput.InputValue: %w", err)
			}
			evaluatedInputs = append(evaluatedInputs, &resultEvaluatedInput)
		}

		evaluatedDecisions = append(evaluatedDecisions, &proto.EvaluatedDecisionResult{
			DecisionId:                &evaluatedDecision.DecisionId,
			DecisionName:              &evaluatedDecision.DecisionName,
			DecisionType:              &evaluatedDecision.DecisionType,
			DecisionDefinitionVersion: &evaluatedDecision.DecisionDefinitionVersion,
			DecisionDefinitionKey:     &evaluatedDecision.DecisionDefinitionKey,
			DecisionDefinitionId:      &evaluatedDecision.DecisionDefinitionId,
			MatchedRules:              matchedRules,
			DecisionOutput:            resultDecisionOutput,
			EvaluatedInputs:           evaluatedInputs,
		})
	}

	return &proto.EvaluatedDRDResult{
		Error:              nil,
		EvaluatedDecisions: evaluatedDecisions,
		DecisionOutput:     decisionOutput,
	}, nil
}

func (s *Server) DeployDecisionDefinition(ctx context.Context, req *proto.DeployDecisionDefinitionRequest) (*proto.DeployDecisionDefinitionResponse, error) {
	var err error

	bpmnEngines := s.controller.Engines(ctx)

	if len(bpmnEngines) == 0 {
		err = fmt.Errorf("no engines available: %w", err)
		return &proto.DeployDecisionDefinitionResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	var definition *dmn.TDefinitions
	for _, bpmnEngine := range bpmnEngines {
		if definition == nil {
			definition, err = bpmnEngine.GetDmnEngine().ParseDmnFromBytes("", req.Data)
			if err != nil {
				return nil, err
			}
		}
		_, _, err = bpmnEngine.GetDmnEngine().SaveDecisionDefinition(ctx, "", *definition, req.GetData(), req.GetKey())
		if err != nil {
			err = fmt.Errorf("failed to deploy decision definition: %w", err)
			return &proto.DeployDecisionDefinitionResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: ptr.To(err.Error()),
				},
			}, err
		}
	}
	return &proto.DeployDecisionDefinitionResponse{}, nil
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
		err := fmt.Errorf("engine with partition %d was not found", partitionId)
		return &proto.GetProcessInstanceResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	instance, err := engine.FindProcessInstance(req.GetProcessInstanceKey())
	if err != nil {
		err := fmt.Errorf("failed to find process instance %d", req.GetProcessInstanceKey())
		return &proto.GetProcessInstanceResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	vars, err := json.Marshal(instance.VariableHolder.Variables())
	if err != nil {
		err := fmt.Errorf("failed to marshal variables of process instance %d", req.GetProcessInstanceKey())
		return &proto.GetProcessInstanceResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	return &proto.GetProcessInstanceResponse{
		Processes: &proto.ProcessInstance{
			Key:           &instance.Key,
			ProcessId:     &instance.Definition.BpmnProcessId,
			Variables:     vars,
			State:         ptr.To(int64(instance.State)),
			CreatedAt:     ptr.To(instance.CreatedAt.UnixMilli()),
			DefinitionKey: &instance.Definition.Key,
		},
	}, nil
}

func (s *Server) GetProcessInstanceJobs(ctx context.Context, req *proto.GetProcessInstanceJobsRequest) (*proto.GetProcessInstanceJobsResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetProcessInstanceKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := fmt.Errorf("queries for partition %d not found", partitionId)
		return &proto.GetProcessInstanceJobsResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	jobs, err := queries.FindProcessInstanceJobs(ctx, req.GetProcessInstanceKey())
	if err != nil {
		err := fmt.Errorf("failed to find process instance jobs for instance %d", req.GetProcessInstanceKey())
		return &proto.GetProcessInstanceJobsResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	result := make([]*proto.Job, len(jobs))
	for i, job := range jobs {
		result[i] = &proto.Job{
			Key:                &job.Key,
			ElementInstanceKey: &job.ElementInstanceKey,
			ElementId:          &job.ElementID,
			ProcessInstanceKey: &job.ProcessInstanceKey,
			Type:               &job.Type,
			State:              ptr.To(int64(job.State)),
			CreatedAt:          &job.CreatedAt,
			Variables:          []byte(job.Variables),
		}
	}
	return &proto.GetProcessInstanceJobsResponse{
		Jobs: result,
	}, nil
}

func (s *Server) GetFlowElementHistory(ctx context.Context, req *proto.GetFlowElementHistoryRequest) (*proto.GetFlowElementHistoryResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetProcessInstanceKey())
	queries := s.controller.PartitionQueries(ctx, partitionId)
	if queries == nil {
		err := fmt.Errorf("queries for partition %d not found", partitionId)
		return &proto.GetFlowElementHistoryResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	flowElements, err := queries.GetFlowElementHistory(ctx, req.GetProcessInstanceKey())
	if err != nil {
		err := fmt.Errorf("failed to find process instance jobs for instance %d", req.GetProcessInstanceKey())
		return &proto.GetFlowElementHistoryResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	result := make([]*proto.FlowElement, len(flowElements))
	for i, flowElement := range flowElements {
		result[i] = &proto.FlowElement{
			Key:                &flowElement.Key,
			ElementId:          &flowElement.ElementID,
			ProcessInstanceKey: &flowElement.ProcessInstanceKey,
			CreatedAt:          &flowElement.CreatedAt,
		}
	}
	return &proto.GetFlowElementHistoryResponse{
		Flow: result,
	}, nil
}

func (s *Server) GetJobs(ctx context.Context, req *proto.GetJobsRequest) (*proto.GetJobsResponse, error) {
	resp := make([]*proto.PartitionedJobs, 0, len(req.Partitions))
	for _, partitionId := range req.Partitions {
		queries := s.controller.PartitionQueries(ctx, partitionId)
		if queries == nil {
			err := fmt.Errorf("queries for partition %d not found", partitionId)
			return &proto.GetJobsResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: ptr.To(err.Error()),
				},
			}, err
		}
		jobs, err := queries.FindJobsFilter(ctx, sql.FindJobsFilterParams{
			Offset: int64(req.GetSize()) * int64(req.GetPage()-1),
			Size:   int64(req.GetSize()),
			State: ssql.NullInt64{
				Int64: ptr.Deref(req.State, 0),
				Valid: req.State != nil,
			},
			Type: ssql.NullString{
				String: ptr.Deref(req.JobType, ""),
				Valid:  req.JobType != nil,
			},
		})
		if err != nil {
			err := fmt.Errorf("failed to find jobs with filter %+v", req)
			return &proto.GetJobsResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: ptr.To(err.Error()),
				},
			}, err
		}
		partitionJobs := make([]*proto.Job, len(jobs))
		for i, job := range jobs {
			partitionJobs[i] = &proto.Job{
				Key:                &job.Key,
				Variables:          []byte(job.Variables),
				State:              ptr.To(int64(job.State)),
				CreatedAt:          &job.CreatedAt,
				ElementInstanceKey: &job.ElementInstanceKey,
				ElementId:          &job.ElementID,
				ProcessInstanceKey: &job.ProcessInstanceKey,
				Type:               &job.Type,
			}
		}
		resp = append(resp, &proto.PartitionedJobs{
			PartitionId: &partitionId,
			Jobs:        partitionJobs,
		})
	}
	return &proto.GetJobsResponse{
		Partitions: resp,
	}, nil
}

func (s *Server) GetProcessInstances(ctx context.Context, req *proto.GetProcessInstancesRequest) (*proto.GetProcessInstancesResponse, error) {
	resp := make([]*proto.PartitionedProcessInstances, 0, len(req.Partitions))
	for _, partitionId := range req.Partitions {
		queries := s.controller.PartitionQueries(ctx, partitionId)
		if queries == nil {
			err := fmt.Errorf("queries for partition %d not found", partitionId)
			return &proto.GetProcessInstancesResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: ptr.To(err.Error()),
				},
			}, err
		}
		instances, err := queries.FindProcessInstancesPage(ctx, sql.FindProcessInstancesPageParams{
			ProcessDefinitionKey: req.GetDefinitionKey(),
			Offst:                int64(req.GetSize()) * int64(req.GetPage()-1),
			Size:                 int64(req.GetSize()),
		})
		if err != nil {
			err := fmt.Errorf("failed to find process instances with definition key %d", req.DefinitionKey)
			return &proto.GetProcessInstancesResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: ptr.To(err.Error()),
				},
			}, err
		}
		definitionsToLoad := make([]int64, 0)
		for _, inst := range instances {
			if !slices.Contains(definitionsToLoad, inst.ProcessDefinitionKey) {
				definitionsToLoad = append(definitionsToLoad, inst.ProcessDefinitionKey)
			}
		}
		definitions, err := queries.FindProcessDefinitionsByKeys(ctx, definitionsToLoad)
		if err != nil {
			err := fmt.Errorf("failed to find process definitions with definition keys %v", definitionsToLoad)
			return &proto.GetProcessInstancesResponse{
				Error: &proto.ErrorResult{
					Code:    nil,
					Message: ptr.To(err.Error()),
				},
			}, err
		}
		definitionMap := make(map[int64]sql.ProcessDefinition, len(definitions))
		for _, definition := range definitions {
			definitionMap[definition.Key] = definition
		}
		procInstances := make([]*proto.ProcessInstance, len(instances))
		for i, inst := range instances {
			procInstances[i] = &proto.ProcessInstance{
				Key:           &inst.Key,
				ProcessId:     ptr.To(definitionMap[inst.ProcessDefinitionKey].BpmnProcessID),
				Variables:     []byte(inst.Variables),
				State:         ptr.To(int64(inst.State)),
				CreatedAt:     &inst.CreatedAt,
				DefinitionKey: &inst.ProcessDefinitionKey,
			}
		}
		resp = append(resp, &proto.PartitionedProcessInstances{
			PartitionId: &partitionId,
			Instances:   procInstances,
		})
	}
	return &proto.GetProcessInstancesResponse{
		Partitions: resp,
	}, nil
}

func (s *Server) PublishMessage(ctx context.Context, req *proto.PublishMessageRequest) (*proto.PublishMessageResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetKey())
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := fmt.Errorf("engine with partition %d was not found", partitionId)
		return &proto.PublishMessageResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	vars := map[string]any{}
	err := json.Unmarshal(req.GetVariables(), &vars)
	if err != nil {
		err := fmt.Errorf("failed to unmarshal message input variables: %w", err)
		return &proto.PublishMessageResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	err = engine.PublishMessage(ctx,
		req.GetKey(),
		vars,
	)
	if err != nil {
		err := fmt.Errorf("failed to publish message event %d: %w", req.GetKey(), err)
		return &proto.PublishMessageResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
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
		err := fmt.Errorf("queries for partition %d not found", partitionId)
		return &proto.GetIncidentsResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	incidents, err := queries.FindIncidentsByProcessInstanceKey(ctx, req.GetProcessInstanceKey())
	if err != nil {
		err := fmt.Errorf("failed to find incidents for instance %d", req.GetProcessInstanceKey())
		return &proto.GetIncidentsResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
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
		Incidents: results,
	}, nil
}

func (s *Server) ResolveIncident(ctx context.Context, req *proto.ResolveIncidentRequest) (*proto.ResolveIncidentResponse, error) {
	partitionId := zenflake.GetPartitionId(req.GetIncidentKey())
	engine := s.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		err := fmt.Errorf("engine with partition %d was not found", partitionId)
		return &proto.ResolveIncidentResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	err := engine.ResolveIncident(ctx, req.GetIncidentKey())
	if err != nil {
		err := fmt.Errorf("failed to resolve incident %d: %w", req.GetIncidentKey(), err)
		return &proto.ResolveIncidentResponse{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}
	return &proto.ResolveIncidentResponse{}, err
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

func (s *Server) StartCpuProfiler(context.Context, *proto.CpuProfilerRequest) (*proto.CpuProfilerStartResult, error) {
	if s.cpuProfile.Running == true {
		return &proto.CpuProfilerStartResult{}, nil
	}

	s.cpuProfile.Output = &bytes.Buffer{}

	err := pprof.StartCPUProfile(s.cpuProfile.Output)
	if err != nil {
		err := fmt.Errorf("failed to start cpu profiler: %w", err)
		return &proto.CpuProfilerStartResult{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	s.cpuProfile.Running = true

	return nil, nil
}

func (s *Server) StopCpuProfiler(context.Context, *proto.CpuProfilerRequest) (*proto.CpuProfilerStopResult, error) {
	if s.cpuProfile.Running == false {
		err := fmt.Errorf("start cpu profiler not started")
		return &proto.CpuProfilerStopResult{
			Error: &proto.ErrorResult{
				Code:    nil,
				Message: ptr.To(err.Error()),
			},
		}, err
	}

	pprof.StopCPUProfile()
	s.cpuProfile.Running = false

	return &proto.CpuProfilerStopResult{
		Error: nil,
		Pprof: s.cpuProfile.Output.Bytes(),
	}, nil
}
