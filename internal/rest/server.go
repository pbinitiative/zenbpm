package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/types"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/log"
	apierror "github.com/pbinitiative/zenbpm/internal/rest/error"
	"github.com/pbinitiative/zenbpm/internal/rest/middleware"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	PaginationDefaultPage int32 = 1
	PaginationDefaultSize int32 = 10
)

type Server struct {
	sync.RWMutex
	node   *cluster.ZenNode
	addr   string
	server *http.Server
}

// TODO: do we use non strict interface to implement std lib interface directly and use http.Request to reconstruct calls for proxying?
var _ public.StrictServerInterface = (*Server)(nil)

func NewServer(node *cluster.ZenNode, conf config.Config) *Server {
	r := chi.NewRouter()
	s := Server{
		node: node,
		addr: conf.HttpServer.Addr,
		server: &http.Server{
			ReadHeaderTimeout: 3 * time.Second,
			Handler:           r,
			Addr:              conf.HttpServer.Addr,
		},
	}
	r.Use(middleware.Cors())
	r.Use(middleware.Opentelemetry(conf))
	r.Route("/v1", func(r chi.Router) {
		// mount generated handler from open-api
		h := public.Handler(public.NewStrictHandlerWithOptions(&s, []nethttp.StrictHTTPMiddlewareFunc{}, public.StrictHTTPServerOptions{
			RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
				writeError(w, r, http.StatusBadRequest, apierror.ApiError{
					Message: err.Error(),
					Type:    "BAD_REQUEST",
				})
			},
			ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
				writeError(w, r, http.StatusInternalServerError, apierror.ApiError{
					Message: err.Error(),
					Type:    "ERROR",
				})
			},
		}))
		r.Mount("/", h)
	})
	// register system endpoints
	r.Route("/system", func(r chi.Router) {
		r.Get("/metrics", promhttp.Handler().ServeHTTP)
		r.Get("/status", func(w http.ResponseWriter, r *http.Request) {
			state, _ := json.MarshalIndent(node.GetStatus(), "", " ")
			w.Header().Add("Content-Type", "application/json")
			w.Write(state)
			w.WriteHeader(200)
		})
	})
	return &s
}

func (s *Server) Start() net.Listener {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Error("failed to listen: %v", err)
	}
	log.Info("ZenBpm REST server listening on %s", s.addr)
	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Error("Error starting server: %s", err)
		}
	}()
	return listener
}

func (s *Server) Stop(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if err != nil {
		log.Error("Error stopping server: %s", err)
	}
}

// TODO: implement turn off switch in regular usage
func (s *Server) TestStartCpuProfile(ctx context.Context, request public.TestStartCpuProfileRequestObject) (public.TestStartCpuProfileResponseObject, error) {

	err := s.node.StartCpuProfile(ctx, request.NodeId)
	if err != nil {
		return public.TestStartCpuProfile500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.TestStartCpuProfile200Response{}, nil
}

func (s *Server) TestStopCpuProfile(ctx context.Context, request public.TestStopCpuProfileRequestObject) (public.TestStopCpuProfileResponseObject, error) {
	pprof, err := s.node.StopCpuProfile(ctx, request.NodeId)
	if err != nil {
		return public.TestStopCpuProfile500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.TestStopCpuProfile200JSONResponse{
		Pprof: &pprof,
	}, nil
}

func (s *Server) GetDmnResourceDefinitions(ctx context.Context, request public.GetDmnResourceDefinitionsRequestObject) (public.GetDmnResourceDefinitionsResponseObject, error) {
	definitions, err := s.node.GetDmnResourceDefinitions(ctx)
	if err != nil {
		return public.GetDmnResourceDefinitions502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	defaultPagination(&request.Params.Page, &request.Params.Size)
	page := int(*request.Params.Page)
	size := int(*request.Params.Size)

	items := make([]public.DmnResourceDefinitionSimple, 0)
	for _, p := range definitions {
		processDefinitionSimple := public.DmnResourceDefinitionSimple{
			Key:                     p.GetKey(),
			Version:                 int(p.GetVersion()),
			DmnResourceDefinitionId: p.DmnResourceDefinitionId,
			ResourceName:            *p.ResourceName,
		}
		items = append(items, processDefinitionSimple)
	}

	totalCount := len(items)

	startIndex := (page - 1) * size
	endIndex := startIndex + size
	if startIndex >= totalCount {
		result := public.DmnResourceDefinitionsPage{
			Items: []public.DmnResourceDefinitionSimple{},
			PageMetadata: public.PageMetadata{
				Page:       page,
				Size:       size,
				Count:      0,
				TotalCount: totalCount,
			},
		}
		return public.GetDmnResourceDefinitions200JSONResponse(result), nil
	}

	if endIndex > totalCount {
		endIndex = totalCount
	}

	pagedItems := items[startIndex:endIndex]

	result := public.DmnResourceDefinitionsPage{
		Items: pagedItems,
		PageMetadata: public.PageMetadata{
			Page:       page,
			Size:       size,
			Count:      len(pagedItems),
			TotalCount: totalCount,
		},
	}

	return public.GetDmnResourceDefinitions200JSONResponse(result), nil
}

func (s *Server) GetDmnResourceDefinition(ctx context.Context, request public.GetDmnResourceDefinitionRequestObject) (public.GetDmnResourceDefinitionResponseObject, error) {
	definition, err := s.node.GetDmnResourceDefinition(ctx, request.DmnResourceDefinitionKey)
	if err != nil {
		return public.GetDmnResourceDefinition502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.GetDmnResourceDefinition200JSONResponse{
		DmnResourceDefinitionSimple: public.DmnResourceDefinitionSimple{
			DmnResourceDefinitionId: definition.DmnResourceDefinitionId,
			ResourceName:            definition.GetResourceName(),
			Key:                     definition.GetKey(),
			Version:                 int(definition.GetVersion()),
		},
		DmnData: ptr.To(string(definition.GetDefinition())),
	}, nil
}

func (s *Server) CreateDmnResourceDefinition(ctx context.Context, request public.CreateDmnResourceDefinitionRequestObject) (public.CreateDmnResourceDefinitionResponseObject, error) {
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return public.CreateDmnResourceDefinition400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	deployResult, err := s.node.DeployDmnResourceDefinitionToAllPartitions(ctx, data)
	if err != nil {
		return public.CreateDmnResourceDefinition502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	if deployResult.IsDuplicate == true {
		return public.CreateDmnResourceDefinition409JSONResponse{
			Code:    "DUPLICATE",
			Message: fmt.Sprintf("The same dmn resource definition already exists (key: %d)", deployResult.Key),
		}, nil
	}

	return public.CreateDmnResourceDefinition201JSONResponse{
		DmnResourceDefinitionKey: deployResult.Key,
	}, nil
}

func (s *Server) EvaluateDecision(ctx context.Context, request public.EvaluateDecisionRequestObject) (public.EvaluateDecisionResponseObject, error) {
	var decision = request.DecisionId
	if request.Body.DecisionDefinitionId != nil && request.Body.BindingType == public.Latest {
		decision = *request.Body.DecisionDefinitionId + "." + request.DecisionId
	}

	result, err := s.node.EvaluateDecision(
		ctx,
		string(request.Body.BindingType),
		decision,
		ptr.Deref(request.Body.VersionTag, ""),
		ptr.Deref(request.Body.Variables, make(map[string]interface{})),
	)
	if err != nil {
		return public.EvaluateDecision500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	var decisionOutput any
	err = json.Unmarshal(result.GetDecisionOutput(), &decisionOutput)
	if err != nil {
		return public.EvaluateDecision500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	evaluatedDecisions := make([]public.EvaluatedDecisionResult, 0, len(result.GetEvaluatedDecisions()))
	for _, evaluatedDecision := range result.GetEvaluatedDecisions() {

		matchedRules := make([]public.EvaluatedDecisionRule, 0, len(evaluatedDecision.GetMatchedRules()))
		for _, matchedRule := range evaluatedDecision.GetMatchedRules() {

			evaluatedOutputs := make([]public.EvaluatedDecisionOutput, 0, len(matchedRule.GetEvaluatedOutputs()))
			for _, evaluatedOutput := range matchedRule.GetEvaluatedOutputs() {
				resultEvaluatedOutput := public.EvaluatedDecisionOutput{
					OutputId:    evaluatedOutput.GetOutputId(),
					OutputName:  evaluatedOutput.GetOutputName(),
					OutputValue: make(map[string]any),
				}
				err = json.Unmarshal(evaluatedOutput.GetOutputValue(), &resultEvaluatedOutput.OutputValue)
				if err != nil {
					return public.EvaluateDecision500JSONResponse{
						Code:    "TODO",
						Message: err.Error(),
					}, nil
				}
				evaluatedOutputs = append(evaluatedOutputs, resultEvaluatedOutput)
			}

			resultMatchedRule := public.EvaluatedDecisionRule{
				RuleId:           matchedRule.GetRuleId(),
				RuleIndex:        int(matchedRule.GetRuleIndex()),
				EvaluatedOutputs: evaluatedOutputs,
			}
			matchedRules = append(matchedRules, resultMatchedRule)
		}

		resultDecisionOutput := make(map[string]any)
		err = json.Unmarshal(result.GetDecisionOutput(), &resultDecisionOutput)
		if err != nil {
			return public.EvaluateDecision500JSONResponse{
				Code:    "TODO",
				Message: err.Error(),
			}, nil
		}

		evaluatedInputs := make([]public.EvaluatedDecisionInput, 0, len(evaluatedDecision.GetEvaluatedInputs()))
		for _, evaluatedInput := range evaluatedDecision.GetEvaluatedInputs() {
			resultEvaluatedInput := public.EvaluatedDecisionInput{
				InputId:         evaluatedInput.GetInputId(),
				InputName:       evaluatedInput.GetInputName(),
				InputExpression: evaluatedInput.GetInputExpression(),
			}
			err = json.Unmarshal(evaluatedInput.GetInputValue(), &resultEvaluatedInput.InputValue)
			if err != nil {
				return public.EvaluateDecision500JSONResponse{
					Code:    "TODO",
					Message: err.Error(),
				}, nil
			}
			evaluatedInputs = append(evaluatedInputs, resultEvaluatedInput)
		}

		evaluatedDecisions = append(evaluatedDecisions, public.EvaluatedDecisionResult{
			DecisionId:                evaluatedDecision.GetDecisionId(),
			DecisionName:              evaluatedDecision.GetDecisionName(),
			DecisionType:              evaluatedDecision.GetDecisionType(),
			DecisionDefinitionVersion: int(evaluatedDecision.GetDmnResourceDefinitionVersion()),
			DmnResourceDefinitionKey:  evaluatedDecision.GetDmnResourceDefinitionKey(),
			DecisionDefinitionId:      evaluatedDecision.GetDmnResourceDefinitionId(),
			MatchedRules:              matchedRules,
			DecisionOutput:            resultDecisionOutput,
			EvaluatedInputs:           evaluatedInputs,
		})
	}

	return public.EvaluateDecision200JSONResponse{
		DecisionOutput:     decisionOutput,
		EvaluatedDecisions: evaluatedDecisions,
	}, nil
}

func (s *Server) CreateProcessDefinition(ctx context.Context, request public.CreateProcessDefinitionRequestObject) (public.CreateProcessDefinitionResponseObject, error) {
	var data []byte
	var filename string
	var found bool

	// Iterate through multipart parts to find the "resource" field
	for {
		part, err := request.Body.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return public.CreateProcessDefinition400JSONResponse{
				Code:    "TODO",
				Message: fmt.Sprintf("Failed to read multipart form: %s", err.Error()),
			}, nil
		}

		if part.FormName() == "resource" {
			found = true
			filename = part.FileName()

			// Read file data
			data, err = io.ReadAll(part)
			if err != nil {
				return public.CreateProcessDefinition400JSONResponse{
					Code:    "TODO",
					Message: err.Error(),
				}, nil
			}
			part.Close()
			break
		}
		part.Close()
	}

	if !found {
		return public.CreateProcessDefinition400JSONResponse{
			Code:    "TODO",
			Message: "Resource file is required",
		}, nil
	}

	// Deploy with filename
	deployResult, err := s.node.DeployProcessDefinitionToAllPartitions(ctx, data, filename)
	if err != nil {
		return public.CreateProcessDefinition502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	return public.CreateProcessDefinition201JSONResponse{
		ProcessDefinitionKey: deployResult.Key,
	}, nil
}

func (s *Server) CompleteJob(ctx context.Context, request public.CompleteJobRequestObject) (public.CompleteJobResponseObject, error) {
	err := s.node.CompleteJob(ctx, request.Body.JobKey, ptr.Deref(request.Body.Variables, map[string]any{}))
	if err != nil {
		return public.CompleteJob502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.CompleteJob201Response{}, nil
}

func (s *Server) PublishMessage(ctx context.Context, request public.PublishMessageRequestObject) (public.PublishMessageResponseObject, error) {
	err := s.node.PublishMessage(ctx, request.Body.MessageName, request.Body.CorrelationKey, *request.Body.Variables)
	if err != nil {
		return public.PublishMessage502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.PublishMessage201Response{}, nil
}

func (s *Server) GetProcessDefinitions(ctx context.Context, request public.GetProcessDefinitionsRequestObject) (public.GetProcessDefinitionsResponseObject, error) {
	defaultPagination(&request.Params.Page, &request.Params.Size)

	var sortBy *string
	if request.Params.SortBy != nil {
		s := string(*request.Params.SortBy)
		sortBy = &s
	}

	var sortOrder *string
	if request.Params.SortOrder != nil {
		s := string(*request.Params.SortOrder)
		sortOrder = &s
	}

	definitionsPage, err := s.node.GetProcessDefinitions(ctx,
		request.Params.BpmnProcessId,
		request.Params.OnlyLatest,
		sortBy,
		sortOrder,
		*request.Params.Page, *request.Params.Size)

	if err != nil {
		return public.GetProcessDefinitions500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	items := make([]public.ProcessDefinitionSimple, len(definitionsPage.Items))
	result := public.ProcessDefinitionsPage{
		Items: items,
	}
	for i, p := range definitionsPage.Items {
		processDefinitionSimple := public.ProcessDefinitionSimple{
			Key:              p.GetKey(),
			Version:          int(p.GetVersion()),
			BpmnProcessId:    p.GetProcessId(),
			BpmnResourceName: ptr.To(p.GetResourceName()),
			BpmnProcessName:  ptr.To(p.GetProcessName()),
		}
		items[i] = processDefinitionSimple
	}
	result.Items = items
	result.Count = len(items)
	result.Page = int(*request.Params.Page)
	result.Size = int(*request.Params.Size)
	result.TotalCount = int(*definitionsPage.TotalCount)

	return public.GetProcessDefinitions200JSONResponse(result), nil
}

func (s *Server) GetProcessDefinition(ctx context.Context, request public.GetProcessDefinitionRequestObject) (public.GetProcessDefinitionResponseObject, error) {
	definition, err := s.node.GetProcessDefinition(ctx, request.ProcessDefinitionKey)
	if err != nil {
		return public.GetProcessDefinition500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.GetProcessDefinition200JSONResponse{
		ProcessDefinitionSimple: public.ProcessDefinitionSimple{
			BpmnProcessId: definition.GetProcessId(),
			Key:           definition.GetKey(),
			Version:       int(definition.GetVersion()),
		},
		BpmnData: ptr.To(string(definition.GetDefinition())),
	}, nil
}

func (s *Server) CreateProcessInstance(ctx context.Context, request public.CreateProcessInstanceRequestObject) (public.CreateProcessInstanceResponseObject, error) {
	variables := make(map[string]interface{})
	if request.Body.Variables != nil {
		variables = *request.Body.Variables
	}
	var ttl *types.TTL
	if request.Body.HistoryTimeToLive != nil {
		parsedTTL, err := types.ParseTTL(*request.Body.HistoryTimeToLive)
		if err != nil {
			return public.CreateProcessInstance400JSONResponse{
				Code:    "TODO",
				Message: fmt.Sprintf("Failed to parse historyTimeToLive: %s", err),
			}, nil
		}
		ttl = &parsedTTL
	}

	process, err := s.node.CreateInstance(ctx, request.Body.ProcessDefinitionKey, request.Body.BusinessKey, variables, ttl)
	if err != nil {
		return public.CreateProcessInstance502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	processVars := make(map[string]any)
	err = json.Unmarshal(process.GetVariables(), &processVars)
	if err != nil {
		return public.CreateProcessInstance500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.CreateProcessInstance201JSONResponse{
		CreatedAt:            time.UnixMilli(process.GetCreatedAt()),
		Key:                  process.GetKey(),
		ProcessDefinitionKey: process.GetDefinitionKey(),
		// TODO: make sure its the same string
		State:     public.ProcessInstanceState(runtime.ActivityState(process.GetState()).String()),
		Variables: processVars,
	}, nil
}

func (s *Server) StartProcessInstanceOnElements(ctx context.Context, request public.StartProcessInstanceOnElementsRequestObject) (public.StartProcessInstanceOnElementsResponseObject, error) {
	variables := make(map[string]interface{})
	if request.Body.Variables != nil {
		variables = *request.Body.Variables
	}
	process, err := s.node.StartProcessInstanceOnElements(ctx, request.Body.ProcessDefinitionKey, request.Body.StartingElementIds, variables)
	if err != nil {
		return public.StartProcessInstanceOnElements502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	processVars := make(map[string]any)
	err = json.Unmarshal(process.GetVariables(), &processVars)
	if err != nil {
		return public.StartProcessInstanceOnElements500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.StartProcessInstanceOnElements201JSONResponse{
		CreatedAt:            time.UnixMilli(process.GetCreatedAt()),
		Key:                  process.GetKey(),
		ProcessDefinitionKey: process.GetDefinitionKey(),
		// TODO: make sure its the same string
		State:     public.ProcessInstanceState(runtime.ActivityState(process.GetState()).String()),
		Variables: processVars,
	}, nil
}

func (s *Server) GetProcessInstances(ctx context.Context, request public.GetProcessInstancesRequestObject) (public.GetProcessInstancesResponseObject, error) {
	defaultPagination(&request.Params.Page, &request.Params.Size)

	definitionKey := ptr.Deref(request.Params.ProcessDefinitionKey, int64(0))
	parentInstanceKey := ptr.Deref(request.Params.ParentProcessInstanceKey, int64(0))

	partitionedInstances, err := s.node.GetProcessInstances(
		ctx,
		&definitionKey,
		request.Params.BusinessKey,
		parentInstanceKey,
		*request.Params.Page,
		*request.Params.Size,
	)
	if err != nil {
		return public.GetProcessInstances502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	processInstancesPage := public.GetProcessInstances200JSONResponse{
		Partitions: make([]public.PartitionProcessInstances, len(partitionedInstances)),
		PartitionedPageMetadata: public.PartitionedPageMetadata{
			Page: int(*request.Params.Page),
			Size: int(*request.Params.Size),
		},
	}

	count := 0
	totalCount := 0
	for i, partitionInstances := range partitionedInstances {
		processInstancesPage.Partitions[i] = public.PartitionProcessInstances{
			Items:     make([]public.ProcessInstance, len(partitionInstances.GetInstances())),
			Partition: int(partitionInstances.GetPartitionId()),
		}
		count += len(partitionInstances.GetInstances())
		totalCount += int(partitionInstances.GetTotalCount())
		for k, instance := range partitionInstances.GetInstances() {
			vars := map[string]any{}
			err = json.Unmarshal(instance.GetVariables(), &vars)
			if err != nil {
				return public.GetProcessInstances500JSONResponse{
					Code:    "TODO",
					Message: err.Error(),
				}, nil
			}
			processInstancesPage.Partitions[i].Items[k] = public.ProcessInstance{
				CreatedAt:            time.UnixMilli(instance.GetCreatedAt()),
				Key:                  instance.GetKey(),
				ProcessDefinitionKey: instance.GetDefinitionKey(),
				State:                public.ProcessInstanceState(runtime.ActivityState(instance.GetState()).String()),
				BusinessKey:          instance.BusinessKey,
				Variables:            vars,
			}
			if instance.GetParentKey() != 0 {
				processInstancesPage.Partitions[i].Items[k].ParentProcessInstanceKey = ptr.To(instance.GetParentKey())
			}
		}
	}
	processInstancesPage.Count = count
	processInstancesPage.TotalCount = totalCount
	return processInstancesPage, nil
}

func (s *Server) GetProcessInstance(ctx context.Context, request public.GetProcessInstanceRequestObject) (public.GetProcessInstanceResponseObject, error) {
	instance, activeElementInstances, err := s.node.GetProcessInstance(ctx, request.ProcessInstanceKey)
	if err != nil {
		return public.GetProcessInstance502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	vars := map[string]any{}
	err = json.Unmarshal(instance.GetVariables(), &vars)
	if err != nil {
		return public.GetProcessInstance500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	respActiveElementInstances := make([]public.ElementInstance, 0, len(activeElementInstances))
	for _, elementInstance := range activeElementInstances {
		respActiveElementInstances = append(respActiveElementInstances, public.ElementInstance{
			CreatedAt:          time.UnixMilli(elementInstance.GetCreatedAt()),
			ElementId:          elementInstance.GetElementId(),
			ElementInstanceKey: elementInstance.GetElementInstanceKey(),
			State:              runtime.ActivityState(elementInstance.GetState()).String(),
		})
	}

	return &public.GetProcessInstance200JSONResponse{
		ActiveElementInstances: respActiveElementInstances,
		CreatedAt:              time.UnixMilli(instance.GetCreatedAt()),
		Key:                    instance.GetKey(),
		BusinessKey:            instance.BusinessKey,
		ProcessDefinitionKey:   instance.GetDefinitionKey(),
		State:                  getRestProcessInstanceState(runtime.ActivityState(instance.GetState())),
		Variables:              vars,
	}, nil
}

func (s *Server) GetHistory(ctx context.Context, request public.GetHistoryRequestObject) (public.GetHistoryResponseObject, error) {
	defaultPagination(&request.Params.Page, &request.Params.Size)
	flow, err := s.node.GetFlowElementHistory(ctx, *request.Params.Page, *request.Params.Size, request.ProcessInstanceKey)
	if err != nil {
		return public.GetHistory502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	resp := make([]public.FlowElementHistory, len(flow.Flow))
	for i, flowNode := range flow.Flow {
		key := flowNode.GetKey()
		createdAt := time.UnixMilli(flowNode.GetCreatedAt())
		processInstanceKey := flowNode.GetProcessInstanceKey()
		resp[i] = public.FlowElementHistory{
			Key:                key,
			CreatedAt:          createdAt,
			ElementId:          flowNode.GetElementId(),
			ProcessInstanceKey: processInstanceKey,
		}
	}
	return public.GetHistory200JSONResponse(public.FlowElementHistoryPage{
		Items: &resp,
		PageMetadata: public.PageMetadata{
			Count:      len(resp),
			Page:       int(*request.Params.Page),
			Size:       int(*request.Params.Size),
			TotalCount: int(*flow.TotalCount),
		},
	}), nil
}

func (s *Server) GetProcessInstanceJobs(ctx context.Context, request public.GetProcessInstanceJobsRequestObject) (public.GetProcessInstanceJobsResponseObject, error) {
	defaultPagination(&request.Params.Page, &request.Params.Size)
	jobs, err := s.node.GetProcessInstanceJobs(ctx, *request.Params.Page, *request.Params.Size, request.ProcessInstanceKey)
	if err != nil {
		return public.GetProcessInstanceJobs502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	resp := make([]public.Job, len(jobs.Jobs))
	for i, job := range jobs.Jobs {
		vars := map[string]any{}
		err := json.Unmarshal(job.GetVariables(), &vars)
		if err != nil {
			return public.GetProcessInstanceJobs500JSONResponse{
				Code:    "TODO",
				Message: err.Error(),
			}, nil
		}
		resp[i] = public.Job{
			CreatedAt:          time.UnixMilli(job.GetCreatedAt()),
			ElementId:          job.GetElementId(),
			Key:                job.GetKey(),
			ProcessInstanceKey: job.GetProcessInstanceKey(),
			State:              getRestJobState(runtime.ActivityState(job.GetState())),
			Type:               job.GetType(),
			Variables:          vars,
		}

	}
	return public.GetProcessInstanceJobs200JSONResponse{
		Items: resp,
		PageMetadata: public.PageMetadata{
			Count:      len(resp),
			Page:       int(*request.Params.Page),
			Size:       int(*request.Params.Size),
			TotalCount: int(*jobs.TotalCount),
		},
	}, nil
}

func (s *Server) GetJobs(ctx context.Context, request public.GetJobsRequestObject) (public.GetJobsResponseObject, error) {
	defaultPagination(&request.Params.Page, &request.Params.Size)
	var reqState *runtime.ActivityState
	if request.Params.State != nil {
		switch *request.Params.State {
		case public.JobStateActive:
			reqState = ptr.To(runtime.ActivityStateActive)
		case public.JobStateCompleted:
			reqState = ptr.To(runtime.ActivityStateCompleted)
		case public.JobStateTerminated:
			reqState = ptr.To(runtime.ActivityStateActive)
		default:
			panic("unexpected public.JobState")
		}
	}
	jobs, err := s.node.GetJobs(ctx, *request.Params.Page, *request.Params.Size, request.Params.JobType, reqState, request.Params.Assignee, request.Params.ProcessInstanceKey, (*string)(request.Params.SortBy), (*string)(request.Params.SortOrder))
	if err != nil {
		return public.GetJobs502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	jobsPage := public.GetJobs200JSONResponse{
		Partitions: make([]public.PartitionJobs, len(jobs)),
		PartitionedPageMetadata: public.PartitionedPageMetadata{
			Page: int(*request.Params.Page),
			Size: int(*request.Params.Size),
		},
	}

	count := 0
	totalCount := int32(0)
	for i, partitionJobs := range jobs {
		jobsPage.Partitions[i] = public.PartitionJobs{
			Items:     make([]public.Job, len(partitionJobs.GetJobs())),
			Partition: int(partitionJobs.GetPartitionId()),
		}
		count += len(partitionJobs.GetJobs())
		totalCount += *partitionJobs.TotalCount
		for k, job := range partitionJobs.GetJobs() {

			jobsPage.Partitions[i].Items[k] = public.Job{
				CreatedAt:          time.UnixMilli(job.GetCreatedAt()),
				Key:                job.GetKey(),
				State:              getRestJobState(runtime.ActivityState(job.GetState())),
				ElementId:          job.GetElementId(),
				ProcessInstanceKey: job.GetProcessInstanceKey(),
				Type:               job.GetType(),
			}
		}
	}
	jobsPage.Count = count
	jobsPage.TotalCount = int(totalCount)
	return jobsPage, nil
}

func (s *Server) GetJob(ctx context.Context, request public.GetJobRequestObject) (public.GetJobResponseObject, error) {
	job, err := s.node.GetJob(ctx, request.JobKey)
	if err != nil {
		var cerr *cluster.Error
		if errors.As(err, &cerr) && cerr != nil && cerr.Result != nil {
			switch cerr.Result.GetCode() {
			case proto.ErrorResult_NOT_FOUND:
				return public.GetJob404JSONResponse{
					Code:    proto.ErrorResult_NOT_FOUND.String(),
					Message: cerr.Result.GetMessage(),
				}, nil

			case proto.ErrorResult_CLUSTER_ERROR:
				return public.GetJob502JSONResponse{
					Code:    proto.ErrorResult_CLUSTER_ERROR.String(),
					Message: cerr.Result.GetMessage(),
				}, nil

			default:
				return public.GetJob500JSONResponse{
					Code:    proto.ErrorResult_UNSPECIFIED.String(),
					Message: cerr.Result.GetMessage(),
				}, nil
			}
		}

		// Not your cluster error type → treat as internal (or map generically)
		return public.GetJob500JSONResponse{
			Code:    proto.ErrorResult_UNSPECIFIED.String(),
			Message: err.Error(),
		}, nil
	}

	jobVars := make(map[string]any)
	err = json.Unmarshal(job.GetVariables(), &jobVars)
	if err != nil {
		return public.GetJob500JSONResponse{
			Code:    proto.ErrorResult_UNSPECIFIED.String(),
			Message: err.Error(),
		}, nil
	}

	return public.GetJob200JSONResponse{
		CreatedAt:          time.UnixMilli(job.GetCreatedAt()),
		ElementId:          job.GetElementId(),
		Key:                job.GetKey(),
		ProcessInstanceKey: job.GetProcessInstanceKey(),
		State:              getRestJobState(runtime.ActivityState(job.GetState())),
		Type:               job.GetType(),
		Variables:          jobVars,
	}, nil
}

func getRestJobState(state runtime.ActivityState) public.JobState {
	switch state {
	case runtime.ActivityStateActive:
		return public.JobStateActive
	case runtime.ActivityStateCompleted:
		return public.JobStateCompleted
	case runtime.ActivityStateTerminated:
		return public.JobStateTerminated
	case runtime.ActivityStateFailed:
		return public.JobStateFailed
	default:
		panic(fmt.Sprintf("unexpected runtime.ActivityState: %#v", state))
	}
}

func getRestProcessInstanceState(state runtime.ActivityState) public.ProcessInstanceState {
	switch state {
	case runtime.ActivityStateReady:
		return public.ProcessInstanceStateActive
	case runtime.ActivityStateActive:
		return public.ProcessInstanceStateActive
	case runtime.ActivityStateCompleted:
		return public.ProcessInstanceStateCompleted
	case runtime.ActivityStateFailed:
		return public.ProcessInstanceStateActive
	case runtime.ActivityStateTerminated:
		return public.ProcessInstanceStateTerminated
	default:
		panic(fmt.Sprintf("unexpected runtime.ActivityState: %#v", state))
	}
}

func (s *Server) GetIncidents(ctx context.Context, request public.GetIncidentsRequestObject) (public.GetIncidentsResponseObject, error) {
	defaultPagination(&request.Params.Page, &request.Params.Size)
	incidents, err := s.node.GetIncidents(ctx, *request.Params.Page, *request.Params.Size, request.ProcessInstanceKey)
	if err != nil {
		return public.GetIncidents502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	resp := make([]public.Incident, len(incidents.Incidents))
	for i, incident := range incidents.Incidents {
		resp[i] = public.Incident{
			Key:                incident.GetKey(),
			ElementInstanceKey: incident.GetElementInstanceKey(),
			ElementId:          incident.GetElementId(),
			CreatedAt:          time.UnixMilli(incident.GetCreatedAt()),
			ResolvedAt: func() *time.Time {
				if incident.ResolvedAt != nil {
					return ptr.To(time.UnixMilli(incident.GetResolvedAt()))
				}
				return nil
			}(),
			ProcessInstanceKey: incident.GetProcessInstanceKey(),
			Message:            incident.GetMessage(),
			ExecutionToken:     incident.GetExecutionToken(),
		}
	}
	return public.GetIncidents200JSONResponse{
		Items: resp,
		PageMetadata: public.PageMetadata{
			Count:      len(resp),
			Page:       int(*request.Params.Page),
			Size:       int(*request.Params.Size),
			TotalCount: int(*incidents.TotalCount),
		},
	}, nil
}

func (s *Server) ResolveIncident(ctx context.Context, request public.ResolveIncidentRequestObject) (public.ResolveIncidentResponseObject, error) {
	err := s.node.ResolveIncident(ctx, request.IncidentKey)

	if err != nil {
		return public.ResolveIncident502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	return public.ResolveIncident201Response{}, nil
}

func (s *Server) ModifyProcessInstance(ctx context.Context, request public.ModifyProcessInstanceRequestObject) (public.ModifyProcessInstanceResponseObject, error) {
	variables := make(map[string]interface{})
	if request.Body.Variables != nil {
		variables = *request.Body.Variables
	}

	var elementInstancesToStart []string
	if request.Body.ElementInstancesToStart != nil {
		elementInstancesToStart = make([]string, 0, len(*request.Body.ElementInstancesToStart))
		for _, data := range *request.Body.ElementInstancesToStart {
			elementInstancesToStart = append(elementInstancesToStart, data.ElementId)
		}
	}

	var elementInstancesToTerminate []int64
	if request.Body.ElementInstancesToTerminate != nil {
		elementInstancesToTerminate = make([]int64, 0, len(*request.Body.ElementInstancesToTerminate))
		for _, data := range *request.Body.ElementInstancesToTerminate {
			elementInstancesToTerminate = append(elementInstancesToTerminate, data.ElementInstanceKey)
		}
	}

	process, activeElementInstances, err := s.node.ModifyProcessInstance(ctx, request.Body.ProcessInstanceKey, elementInstancesToTerminate, elementInstancesToStart, variables)
	if err != nil {
		return public.ModifyProcessInstance502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	processVars := make(map[string]any)
	err = json.Unmarshal(process.GetVariables(), &processVars)
	if err != nil {
		return public.ModifyProcessInstance500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	respActiveElementInstances := make([]public.ElementInstance, 0, len(activeElementInstances))
	for _, elementInstance := range activeElementInstances {
		respActiveElementInstances = append(respActiveElementInstances, public.ElementInstance{
			CreatedAt:          time.UnixMilli(elementInstance.GetCreatedAt()),
			ElementId:          elementInstance.GetElementId(),
			ElementInstanceKey: elementInstance.GetElementInstanceKey(),
			State:              runtime.ActivityState(elementInstance.GetState()).String(),
		})
	}

	return public.ModifyProcessInstance201JSONResponse{
		ProcessInstance: &public.ProcessInstance{
			CreatedAt:            time.UnixMilli(process.GetCreatedAt()),
			Key:                  process.GetKey(),
			ProcessDefinitionKey: process.GetDefinitionKey(),
			State:                public.ProcessInstanceState(runtime.ActivityState(process.GetState()).String()),
			Variables:            processVars,
		},
		ActiveElementInstances: &respActiveElementInstances,
	}, nil
}

func writeError(w http.ResponseWriter, r *http.Request, status int, resp interface{}) {
	w.WriteHeader(status)
	body, err := json.Marshal(resp)
	if err != nil {
		log.Error("Server error: %s", err)
	} else {
		w.Write(body)
	}
}

func defaultPagination(page **int32, size **int32) {
	if *page == nil {
		p := PaginationDefaultPage
		*page = &p
	}
	if *size == nil {
		s := PaginationDefaultSize
		*size = &s
	}
}
