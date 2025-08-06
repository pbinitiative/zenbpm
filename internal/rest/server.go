package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/log"
	apierror "github.com/pbinitiative/zenbpm/internal/rest/error"
	"github.com/pbinitiative/zenbpm/internal/rest/middleware"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

func getKeyFromString(s string) (int64, error) {
	key, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return key, fmt.Errorf("failed to parse key: %w", err)
	}
	return key, nil
}

func (s *Server) GetDecisionDefinitions(ctx context.Context, request public.GetDecisionDefinitionsRequestObject) (public.GetDecisionDefinitionsResponseObject, error) {
	definitions, err := s.node.GetDecisionDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]public.DecisionDefinitionSimple, 0)
	result := public.DecisionDefinitionsPage{
		Items: items,
	}
	for _, p := range definitions {
		processDefinitionSimple := public.DecisionDefinitionSimple{
			Key:                  fmt.Sprintf("%x", p.Key),
			Version:              int(p.Version),
			DecisionDefinitionId: p.DecisionDefinitionId,
		}
		items = append(items, processDefinitionSimple)
	}
	result.Items = items
	total := len(items)
	result.Count = total
	result.Offset = 0
	result.Size = len(definitions)

	return public.GetDecisionDefinitions200JSONResponse(result), nil
}

func (s *Server) GetDecisionDefinition(ctx context.Context, request public.GetDecisionDefinitionRequestObject) (public.GetDecisionDefinitionResponseObject, error) {
	key, err := getKeyFromString(request.DecisionDefinitionKey)
	if err != nil {
		return public.GetDecisionDefinition400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	definition, err := s.node.GetDecisionDefinition(ctx, key)
	if err != nil {
		return nil, err
	}
	return public.GetDecisionDefinition200JSONResponse{
		DecisionDefinitionSimple: public.DecisionDefinitionSimple{
			DecisionDefinitionId: definition.DecisionDefinitionId,
			Key:                  fmt.Sprintf("%x", definition.Key),
			Version:              int(definition.Version),
		},
		DmnData: ptr.To(string(definition.Definition)),
	}, nil
}

func (s *Server) CreateDecisionDefinition(ctx context.Context, request public.CreateDecisionDefinitionRequestObject) (public.CreateDecisionDefinitionResponseObject, error) {
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	key, err := s.node.DeployDecisionDefinitionToAllPartitions(ctx, data)
	if err != nil {
		return public.CreateDecisionDefinition502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	return public.CreateDecisionDefinition200JSONResponse{
		DecisionDefinitionKey: fmt.Sprintf("%x", key),
	}, nil
}

func (s *Server) EvaluateDecision(ctx context.Context, request public.EvaluateDecisionRequestObject) (public.EvaluateDecisionResponseObject, error) {
	result, err := s.node.EvaluateDecision(
		ctx,
		string(request.Body.BindingType),
		request.Body.DecisionId,
		request.Body.VersionTag,
		request.Body.Variables,
	)
	if err != nil {
		return public.EvaluateDecision500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	
	//TODO: split into mapper methods
	decisionOutput := make(map[string]any)
	err = json.Unmarshal(result.DecisionOutput, &decisionOutput)
	if err != nil {
		return public.EvaluateDecision500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	evaluatedDecisions := make([]public.EvaluatedDecisionResult, 0)
	for _, evaluatedDecision := range result.EvaluatedDecisions {

		matchedRules := make([]public.EvaluatedDecisionRule, 0)
		for _, matchedRule := range evaluatedDecision.MatchedRules {

			evaluatedOutputs := make([]public.EvaluatedDecisionOutput, 0)
			for _, evaluatedOutput := range matchedRule.EvaluatedOutputs {
				resultEvaluatedOutput := public.EvaluatedDecisionOutput{
					OutputId:    evaluatedOutput.OutputId,
					OutputName:  evaluatedOutput.OutputName,
					OutputValue: make(map[string]any),
				}
				err = json.Unmarshal(evaluatedOutput.OutputValue, &resultEvaluatedOutput.OutputValue)
				if err != nil {
					return public.EvaluateDecision500JSONResponse{
						Code:    "TODO",
						Message: err.Error(),
					}, nil
				}
				evaluatedOutputs = append(evaluatedOutputs, resultEvaluatedOutput)
			}

			resultMatchedRule := public.EvaluatedDecisionRule{
				RuleId:           matchedRule.RuleId,
				RuleIndex:        int(matchedRule.RuleIndex),
				EvaluatedOutputs: evaluatedOutputs,
			}
			matchedRules = append(matchedRules, resultMatchedRule)
		}

		resultDecisionOutput := make(map[string]any)
		err = json.Unmarshal(result.DecisionOutput, &resultDecisionOutput)
		if err != nil {
			return public.EvaluateDecision500JSONResponse{
				Code:    "TODO",
				Message: err.Error(),
			}, nil
		}

		evaluatedInputs := make([]public.EvaluatedDecisionInput, 0)
		for _, evaluatedInput := range evaluatedDecision.EvaluatedInputs {
			resultEvaluatedInput := public.EvaluatedDecisionInput{
				InputId:         evaluatedInput.InputId,
				InputName:       evaluatedInput.InputName,
				InputExpression: evaluatedInput.InputExpression,
				InputValue:      make(map[string]any),
			}
			err = json.Unmarshal(evaluatedInput.InputValue, &resultEvaluatedInput.InputValue)
			if err != nil {
				return public.EvaluateDecision500JSONResponse{
					Code:    "TODO",
					Message: err.Error(),
				}, nil
			}
			evaluatedInputs = append(evaluatedInputs, resultEvaluatedInput)
		}

		evaluatedDecisions = append(evaluatedDecisions, public.EvaluatedDecisionResult{
			DecisionId:                evaluatedDecision.DecisionId,
			DecisionName:              evaluatedDecision.DecisionName,
			DecisionType:              evaluatedDecision.DecisionType,
			DecisionDefinitionVersion: int(evaluatedDecision.DecisionDefinitionVersion),
			DecisionDefinitionKey:     fmt.Sprintf("%x", evaluatedDecision.DecisionDefinitionKey),
			DecisionDefinitionId:      evaluatedDecision.DecisionDefinitionId,
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
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	key, err := s.node.DeployProcessDefinitionToAllPartitions(ctx, data)
	if err != nil {
		return public.CreateProcessDefinition502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	return public.CreateProcessDefinition200JSONResponse{
		ProcessDefinitionKey: fmt.Sprintf("%d", key),
	}, nil
}

func (s *Server) CompleteJob(ctx context.Context, request public.CompleteJobRequestObject) (public.CompleteJobResponseObject, error) {
	key, err := getKeyFromString(request.Body.JobKey)
	if err != nil {
		return public.CompleteJob400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	err = s.node.CompleteJob(ctx, key, ptr.Deref(request.Body.Variables, map[string]any{}))
	if err != nil {
		return public.CompleteJob502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.CompleteJob201Response{}, nil
}

func (s *Server) PublishMessage(ctx context.Context, request public.PublishMessageRequestObject) (public.PublishMessageResponseObject, error) {
	key, err := getKeyFromString(request.Body.ProcessInstanceKey)
	if err != nil {
		return public.PublishMessage400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	err = s.node.PublishMessage(ctx, request.Body.MessageName, key, *request.Body.Variables)
	if err != nil {
		return public.PublishMessage502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.PublishMessage201Response{}, nil
}

func (s *Server) GetProcessDefinitions(ctx context.Context, request public.GetProcessDefinitionsRequestObject) (public.GetProcessDefinitionsResponseObject, error) {
	definitions, err := s.node.GetProcessDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]public.ProcessDefinitionSimple, 0)
	result := public.ProcessDefinitionsPage{
		Items: items,
	}
	for _, p := range definitions {
		processDefinitionSimple := public.ProcessDefinitionSimple{
			Key:           fmt.Sprintf("%d", p.Key),
			Version:       int(p.Version),
			BpmnProcessId: p.ProcessId,
		}
		items = append(items, processDefinitionSimple)
	}
	result.Items = items
	total := len(items)
	result.Count = total
	result.Offset = 0
	result.Size = len(definitions)

	return public.GetProcessDefinitions200JSONResponse(result), nil
}

func (s *Server) GetProcessDefinition(ctx context.Context, request public.GetProcessDefinitionRequestObject) (public.GetProcessDefinitionResponseObject, error) {
	key, err := getKeyFromString(request.ProcessDefinitionKey)
	if err != nil {
		return public.GetProcessDefinition400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	definition, err := s.node.GetProcessDefinition(ctx, key)
	if err != nil {
		return nil, err
	}
	return public.GetProcessDefinition200JSONResponse{
		ProcessDefinitionSimple: public.ProcessDefinitionSimple{
			BpmnProcessId: definition.ProcessId,
			Key:           fmt.Sprintf("%d", definition.Key),
			Version:       int(definition.Version),
		},
		BpmnData: ptr.To(string(definition.Definition)),
	}, nil
}

func (s *Server) CreateProcessInstance(ctx context.Context, request public.CreateProcessInstanceRequestObject) (public.CreateProcessInstanceResponseObject, error) {
	key, err := getKeyFromString(request.Body.ProcessDefinitionKey)
	if err != nil {
		return public.CreateProcessInstance400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	variables := make(map[string]interface{})
	if request.Body.Variables != nil {
		variables = *request.Body.Variables
	}
	process, err := s.node.CreateInstance(ctx, key, variables)
	if err != nil {
		return public.CreateProcessInstance502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	processVars := make(map[string]any)
	err = json.Unmarshal(process.Variables, &processVars)
	if err != nil {
		return public.CreateProcessInstance500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.CreateProcessInstance200JSONResponse{
		CreatedAt:            time.UnixMilli(process.CreatedAt),
		Key:                  fmt.Sprintf("%d", process.Key),
		ProcessDefinitionKey: fmt.Sprintf("%d", process.DefinitionKey),
		// TODO: make sure its the same string
		State:     public.ProcessInstanceState(runtime.ActivityState(process.State).String()),
		Variables: processVars,
	}, nil
}

func (s *Server) GetProcessInstances(ctx context.Context, request public.GetProcessInstancesRequestObject) (public.GetProcessInstancesResponseObject, error) {
	page := int32(1)
	if request.Params.Page != nil {
		page = *request.Params.Page
	}
	size := int32(10)
	if request.Params.Size != nil {
		size = *request.Params.Size
	}

	definitionKey, err := getKeyFromString(request.Params.ProcessDefinitionKey)
	if err != nil {
		return public.GetProcessInstances400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	partitionedInstances, err := s.node.GetProcessInstances(ctx, definitionKey, page, size)
	if err != nil {
		return public.GetProcessInstances500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	processInstancesPage := public.GetProcessInstances200JSONResponse{
		Partitions: make([]public.PartitionProcessInstances, len(partitionedInstances)),
		PartitionedPageMetadata: public.PartitionedPageMetadata{
			Page: int(page),
			Size: int(size),
		},
	}

	count := 0
	for i, partitionInstances := range partitionedInstances {
		processInstancesPage.Partitions[i] = public.PartitionProcessInstances{
			Items:     make([]public.ProcessInstance, len(partitionInstances.Instances)),
			Partition: int(partitionInstances.PartitionId),
		}
		count += len(partitionInstances.Instances)
		for k, instance := range partitionInstances.Instances {
			vars := map[string]any{}
			err = json.Unmarshal(instance.Variables, &vars)
			if err != nil {
				return public.GetProcessInstances500JSONResponse{
					Code:    "TODO",
					Message: err.Error(),
				}, nil
			}
			processInstancesPage.Partitions[i].Items[k] = public.ProcessInstance{
				CreatedAt:            time.UnixMilli(instance.CreatedAt),
				Key:                  fmt.Sprintf("%d", instance.Key),
				ProcessDefinitionKey: fmt.Sprintf("%d", instance.DefinitionKey),
				State:                public.ProcessInstanceState(runtime.ActivityState(instance.State).String()),
				Variables:            vars,
			}
		}
	}
	processInstancesPage.Count = count
	return processInstancesPage, nil
}

func (s *Server) GetProcessInstance(ctx context.Context, request public.GetProcessInstanceRequestObject) (public.GetProcessInstanceResponseObject, error) {
	instanceKey, err := getKeyFromString(request.ProcessInstanceKey)
	if err != nil {
		return public.GetProcessInstance400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	instance, err := s.node.GetProcessInstance(ctx, instanceKey)
	if err != nil {
		return public.GetProcessInstance502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	vars := map[string]any{}
	err = json.Unmarshal(instance.Variables, &vars)
	if err != nil {
		return public.GetProcessInstance500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.GetProcessInstance200JSONResponse{
		CreatedAt:            time.UnixMilli(instance.CreatedAt),
		Key:                  fmt.Sprintf("%d", instance.Key),
		ProcessDefinitionKey: fmt.Sprintf("%d", instance.DefinitionKey),
		State:                public.ProcessInstanceState(runtime.ActivityState(instance.State).String()),
		Variables:            vars,
	}, nil
}

func (s *Server) GetActivities(ctx context.Context, request public.GetActivitiesRequestObject) (public.GetActivitiesResponseObject, error) {
	// TODO: we currently do not store activities
	return public.GetActivities200JSONResponse(public.ActivityPage{}), nil
}

func (s *Server) GetHistory(ctx context.Context, request public.GetHistoryRequestObject) (public.GetHistoryResponseObject, error) {
	instanceKey, err := getKeyFromString(request.ProcessInstanceKey)
	if err != nil {
		return public.GetHistory400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	flow, err := s.node.GetFlowElementHistory(ctx, instanceKey)
	if err != nil {
		return public.GetHistory502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	resp := make([]public.FlowElementHistory, len(flow))
	for i, flowNode := range flow {
		key := fmt.Sprintf("%d", flowNode.Key)
		createdAt := time.UnixMilli(flowNode.CreatedAt)
		processInstanceKey := fmt.Sprintf("%d", flowNode.ProcessInstanceKey)
		resp[i] = public.FlowElementHistory{
			Key:                &key,
			CreatedAt:          &createdAt,
			ElementId:          &flowNode.ElementId,
			ProcessInstanceKey: &processInstanceKey,
		}
	}
	return public.GetHistory200JSONResponse(public.FlowElementHistoryPage{
		Items: &resp,
		PageMetadata: public.PageMetadata{
			Count:  len(resp),
			Offset: 0,
			Size:   len(resp),
		},
	}), nil
}

func (s *Server) GetProcessInstanceJobs(ctx context.Context, request public.GetProcessInstanceJobsRequestObject) (public.GetProcessInstanceJobsResponseObject, error) {
	instanceKey, err := getKeyFromString(request.ProcessInstanceKey)
	if err != nil {
		return public.GetProcessInstanceJobs400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	jobs, err := s.node.GetProcessInstanceJobs(ctx, instanceKey)
	if err != nil {
		return public.GetProcessInstanceJobs502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	resp := make([]public.Job, len(jobs))
	for i, job := range jobs {
		vars := map[string]any{}
		err := json.Unmarshal(job.Variables, &vars)
		if err != nil {
			return public.GetProcessInstanceJobs500JSONResponse{
				Code:    "TODO",
				Message: err.Error(),
			}, nil
		}
		resp[i] = public.Job{
			CreatedAt:          time.UnixMilli(job.CreatedAt),
			ElementId:          job.ElementId,
			Key:                fmt.Sprintf("%d", job.Key),
			ProcessInstanceKey: fmt.Sprintf("%d", job.ProcessInstanceKey),
			State:              getRestJobState(runtime.ActivityState(job.State)),
			Type:               job.Type,
			Variables:          vars,
		}

	}
	return public.GetProcessInstanceJobs200JSONResponse{
		Items: resp,
		PageMetadata: public.PageMetadata{
			Count:  len(resp),
			Offset: 0,
			Size:   len(resp),
		},
	}, nil
}

func (s *Server) GetJobs(ctx context.Context, request public.GetJobsRequestObject) (public.GetJobsResponseObject, error) {
	page := int32(1)
	if request.Params.Page != nil {
		page = *request.Params.Page
	}
	size := int32(10)
	if request.Params.Size != nil {
		size = *request.Params.Size
	}

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
	jobs, err := s.node.GetJobs(ctx, page, size, request.Params.JobType, reqState)
	if err != nil {
		return public.GetJobs500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	jobsPage := public.GetJobs200JSONResponse{
		Partitions: make([]public.PartitionJobs, len(jobs)),
		PartitionedPageMetadata: public.PartitionedPageMetadata{
			Page: int(page),
			Size: int(size),
		},
	}

	count := 0
	for i, partitionJobs := range jobs {
		jobsPage.Partitions[i] = public.PartitionJobs{
			Items:     make([]public.Job, len(partitionJobs.Jobs)),
			Partition: int(partitionJobs.PartitionId),
		}
		count += len(partitionJobs.Jobs)
		for k, job := range partitionJobs.Jobs {
			vars := map[string]any{}
			err = json.Unmarshal(job.Variables, &vars)
			if err != nil {
				return public.GetJobs500JSONResponse{
					Code:    "TODO",
					Message: err.Error(),
				}, nil
			}

			jobsPage.Partitions[i].Items[k] = public.Job{
				CreatedAt:          time.UnixMilli(job.CreatedAt),
				Key:                fmt.Sprintf("%d", job.Key),
				State:              getRestJobState(runtime.ActivityState(job.State)),
				Variables:          vars,
				ElementId:          job.ElementId,
				ProcessInstanceKey: fmt.Sprintf("%d", job.ProcessInstanceKey),
				Type:               job.Type,
			}
		}
	}
	jobsPage.Count = count
	return jobsPage, nil
}

func getRestJobState(state runtime.ActivityState) public.JobState {
	switch state {
	case runtime.ActivityStateActive:
		return public.JobStateActive
	case runtime.ActivityStateCompleted:
		return public.JobStateCompleted
	case runtime.ActivityStateTerminated:
		return public.JobStateTerminated
	default:
		panic(fmt.Sprintf("unexpected runtime.ActivityState: %#v", state))
	}
}

func (s *Server) GetIncidents(ctx context.Context, request public.GetIncidentsRequestObject) (public.GetIncidentsResponseObject, error) {
	incidentKey, err := getKeyFromString(request.ProcessInstanceKey)
	if err != nil {
		return public.GetIncidents400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	incidents, err := s.node.GetIncidents(ctx, incidentKey)
	if err != nil {
		return public.GetIncidents500JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	resp := make([]public.Incident, len(incidents))
	for i, incident := range incidents {
		resp[i] = public.Incident{
			Key:                fmt.Sprintf("%d", incident.Key),
			ElementInstanceKey: fmt.Sprintf("%d", incident.ElementInstanceKey),
			ElementId:          incident.ElementId,
			CreatedAt:          time.UnixMilli(incident.CreatedAt),
			ResolvedAt: func() *time.Time {
				if incident.ResolvedAt != nil {
					return ptr.To(time.UnixMilli(*incident.ResolvedAt))
				}
				return nil
			}(),
			ProcessInstanceKey: fmt.Sprintf("%d", incident.ProcessInstanceKey),
			Message:            incident.Message,
			ExecutionToken:     fmt.Sprintf("%d", incident.ExecutionToken),
		}
	}
	// TODO: Paging needs to be implemented properly
	return public.GetIncidents200JSONResponse{
		Items: resp,
		PageMetadata: public.PageMetadata{
			Count:  len(resp),
			Offset: 0,
			Size:   len(resp),
		},
	}, nil
}

func (s *Server) ResolveIncident(ctx context.Context, request public.ResolveIncidentRequestObject) (public.ResolveIncidentResponseObject, error) {
	incidentKey, err := getKeyFromString(request.IncidentKey)
	if err != nil {
		return public.ResolveIncident400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	err = s.node.ResolveIncident(ctx, incidentKey)

	if err != nil {
		return public.ResolveIncident502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	return public.ResolveIncident201Response{}, nil

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
