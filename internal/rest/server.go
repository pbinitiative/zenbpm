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
		addr: conf.Server.Addr,
		server: &http.Server{
			ReadHeaderTimeout: 3 * time.Second,
			Handler:           r,
			Addr:              conf.Server.Addr,
		},
	}
	r.Use(middleware.Cors())
	r.Use(middleware.Opentelemetry(conf))
	r.Route("/v1", func(r chi.Router) {
		// mount generated handler from open-api
		h := public.Handler(public.NewStrictHandlerWithOptions(&s, []nethttp.StrictHTTPMiddlewareFunc{}, public.StrictHTTPServerOptions{
			RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
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
	key, err := strconv.ParseInt(s, 16, 64)
	if err != nil {
		return key, fmt.Errorf("failed to parse key: %w", err)
	}
	return key, nil
}

func (s *Server) CreateProcessDefinition(ctx context.Context, request public.CreateProcessDefinitionRequestObject) (public.CreateProcessDefinitionResponseObject, error) {
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	key, err := s.node.DeployDefinitionToAllPartitions(ctx, data)
	if err != nil {
		return public.CreateProcessDefinition502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}

	keyHex := fmt.Sprintf("%x", key)
	return public.CreateProcessDefinition200JSONResponse{
		ProcessDefinitionKey: keyHex,
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
	err = s.node.CompleteJob(ctx, key, ptr.Deref(request.Body.Variables, map[string]interface{}{}))
	if err != nil {
		return public.CompleteJob502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	return public.CompleteJob201Response{}, nil
}

func (s *Server) ActivateJobs(ctx context.Context, request public.ActivateJobsRequestObject) (public.ActivateJobsResponseObject, error) {
	jobs, err := s.node.ActivateJob(ctx, request.JobType)
	if err != nil {
		return nil, fmt.Errorf("failed to activate jobs: %w", err)
	}

	items := make([]public.Job, 0, len(jobs))
	for _, j := range jobs {
		key := fmt.Sprintf("%x", j.GetKey())
		processInstanceKey := fmt.Sprintf("%x", j.GetInstanceKey())
		variables := map[string]any{}
		err := json.Unmarshal(j.GetVariables(), &variables)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal job variables: %w", err)
		}
		jobSimple := public.Job{
			Key:                key,
			ElementId:          j.GetElementId(),
			CreatedAt:          time.UnixMilli(j.GetCreatedAt()),
			ProcessInstanceKey: processInstanceKey,
			Variables:          variables,
			State:              runtime.ActivityState(j.GetState()).String(),
			Type:               j.GetType(),
		}
		items = append(items, jobSimple)
	}

	return public.ActivateJobs200JSONResponse(items), nil
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
			Key:           fmt.Sprintf("%x", p.Key),
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
			Key:           fmt.Sprintf("%x", definition.Key),
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
		Key:                  fmt.Sprintf("%x", process.Key),
		ProcessDefinitionKey: fmt.Sprintf("%x", process.DefinitionKey),
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
				Key:                  fmt.Sprintf("%x", instance.Key),
				ProcessDefinitionKey: fmt.Sprintf("%x", instance.DefinitionKey),
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
		Key:                  fmt.Sprintf("%x", instance.Key),
		ProcessDefinitionKey: fmt.Sprintf("%x", instance.DefinitionKey),
		State:                public.ProcessInstanceState(runtime.ActivityState(instance.State).String()),
		Variables:            vars,
	}, nil
}

func (s *Server) GetActivities(ctx context.Context, request public.GetActivitiesRequestObject) (public.GetActivitiesResponseObject, error) {
	// TODO: we currently do not store activities
	return public.GetActivities200JSONResponse(public.ActivityPage{}), nil
}

func (s *Server) GetJobs(ctx context.Context, request public.GetJobsRequestObject) (public.GetJobsResponseObject, error) {
	instanceKey, err := getKeyFromString(request.ProcessInstanceKey)
	if err != nil {
		return public.GetJobs400JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	jobs, err := s.node.GetProcessInstanceJobs(ctx, instanceKey)
	if err != nil {
		return public.GetJobs502JSONResponse{
			Code:    "TODO",
			Message: err.Error(),
		}, nil
	}
	resp := make([]public.Job, len(jobs))
	for i, job := range jobs {
		vars := map[string]any{}
		err := json.Unmarshal(job.Variables, &vars)
		if err != nil {
			return public.GetJobs500JSONResponse{
				Code:    "TODO",
				Message: err.Error(),
			}, nil
		}
		resp[i] = public.Job{
			CreatedAt:          time.UnixMilli(job.CreatedAt),
			ElementId:          job.ElementId,
			Key:                fmt.Sprintf("%x", job.Key),
			ProcessInstanceKey: fmt.Sprintf("%x", job.ProcessInstanceKey),
			State:              runtime.ActivityState(job.State).String(),
			Type:               job.Type,
			Variables:          vars,
		}

	}
	return public.GetJobs200JSONResponse{
		Items: resp,
		PageMetadata: public.PageMetadata{
			Count:  len(resp),
			Offset: 0,
			Size:   len(resp),
		},
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
