package rest

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/ascii85"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/log"
	apierror "github.com/pbinitiative/zenbpm/internal/rest/error"
	"github.com/pbinitiative/zenbpm/internal/rest/middleware"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
)

type Server struct {
	sync.RWMutex
	engine *bpmn.Engine // TODO: remove engine
	node   *cluster.ZenNode
	addr   string
	server *http.Server
}

// TODO: do we use non strict interface to implement std lib interface directly and use http.Request to reconstruct calls for proxying?
var _ public.StrictServerInterface = (*Server)(nil)

func NewServer(node *cluster.ZenNode, addr string) *Server {
	r := chi.NewRouter()
	s := Server{
		node: node,
		addr: addr,
		server: &http.Server{
			ReadHeaderTimeout: 3 * time.Second,
			Handler:           r,
			Addr:              addr,
		},
	}
	r.Use(middleware.Cors())
	r.Route("/v1", func(appContext chi.Router) {
		h := public.HandlerFromMux(public.NewStrictHandlerWithOptions(&s, []nethttp.StrictHTTPMiddlewareFunc{}, public.StrictHTTPServerOptions{
			RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			},
			ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
				writeError(w, r, http.StatusInternalServerError, apierror.ApiError{
					Message: err.Error(),
					Type:    "ERROR",
				})
			},
		}), appContext)
		appContext.Mount("/", h)
	})
	return &s
}

func (s *Server) Start() {
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("Error starting server: %s", err)
		}
	}()
}

func (s *Server) Stop(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if err != nil {
		log.Error("Error stopping server: %s", err)
	}
}

func getKeyFromString(s *string) *int64 {
	if s == nil {
		return nil
	}
	key, err := strconv.ParseInt(*s, 10, 64)
	if err != nil {
		return nil
	}
	return &key
}

func (s *Server) CreateProcessDefinition(ctx context.Context, request public.CreateProcessDefinitionRequestObject) (public.CreateProcessDefinitionResponseObject, error) {
	if !s.node.IsAnyPartitionLeader(ctx) {
		// if not leader redirect to leader
		// proxyTheRequestToLeader(ctx, s)
		return nil, fmt.Errorf("not leader")
	}

	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	process, err := s.engine.LoadFromBytes(data)
	if err != nil {
		return nil, err
	}
	return public.CreateProcessDefinition200JSONResponse{
		ProcessDefinitionKey: &process.BpmnProcessId,
	}, nil
}

func (s *Server) CompleteJob(ctx context.Context, request public.CompleteJobRequestObject) (public.CompleteJobResponseObject, error) {
	key := *getKeyFromString(&request.Body.JobKey)
	s.engine.JobCompleteByKey(ctx, key, ptr.Deref(request.Body.Variables, map[string]interface{}{}))
	return public.CompleteJob201Response{}, nil
}

func (s *Server) ActivateJobs(ctx context.Context, request public.ActivateJobsRequestObject) (public.ActivateJobsResponseObject, error) {
	jobs, err := s.engine.ActivateJobs(ctx, request.JobType)
	if err != nil {
		return nil, err
	}

	items := make([]public.Job, 0)
	for _, j := range jobs {
		key := fmt.Sprintf("%d", j.Key())
		processInstanceKey := fmt.Sprintf("%d", j.ProcessInstanceKey())
		//TODO: Needs propper conversion
		vh := j.Variables()
		jobSimple := public.Job{
			Key:                &key,
			ElementId:          ptr.To(j.ElementId()),
			CreatedAt:          ptr.To(j.CreatedAt()),
			ProcessInstanceKey: &processInstanceKey,
			VariableHolder:     ptr.To(vh.Variables()),
		}
		items = append(items, jobSimple)
	}

	return public.ActivateJobs200JSONResponse(items), nil
}

func (s *Server) PublishMessage(ctx context.Context, request public.PublishMessageRequestObject) (public.PublishMessageResponseObject, error) {
	key := *getKeyFromString(&request.Body.ProcessInstanceKey)
	err := s.engine.PublishEventForInstance(ctx, key, request.Body.MessageName, *request.Body.Variables)
	if err != nil {
		// TODO:
		panic(err)
	}
	return public.PublishMessage201Response{}, nil
}

func (s *Server) GetProcessDefinitions(ctx context.Context, request public.GetProcessDefinitionsRequestObject) (public.GetProcessDefinitionsResponseObject, error) {
	store, err := s.node.GetPartitionStore(ctx, 1)
	if err != nil {
		return nil, err
	}
	_ = store
	// processes, err := store.FindAllProcessDefinitions(ctx, nil, nil)
	var processes []runtime.ProcessDefinition
	if err != nil {
		return nil, err
	}
	items := make([]public.ProcessDefinitionSimple, 0)
	result := public.ProcessDefinitionsPage{
		Items: &items,
	}
	for _, p := range processes {
		version := int(p.Version)
		key := fmt.Sprintf("%d", p.Key)
		processDefinitionSimple := public.ProcessDefinitionSimple{
			Key:           &key,
			Version:       &version,
			BpmnProcessId: &p.BpmnProcessId,
		}
		items = append(items, processDefinitionSimple)
	}
	result.Items = &items
	total := len(items)
	result.Count = &total
	result.Offset = nil
	result.Size = nil

	return public.GetProcessDefinitions200JSONResponse(result), nil
}

func (s *Server) GetProcessDefinition(ctx context.Context, request public.GetProcessDefinitionRequestObject) (public.GetProcessDefinitionResponseObject, error) {
	store, err := s.node.GetPartitionStore(ctx, 1)
	if err != nil {
		return nil, err
	}
	_ = store
	// processes, err := store.FindProcesses(ctx, nil, getKeyFromString(&request.ProcessDefinitionKey))
	var processes []runtime.ProcessDefinition
	if err != nil {
		return nil, err
	}
	if len(processes) == 0 {
		return public.GetProcessDefinition200JSONResponse{}, nil
	}

	version := int(processes[0].Version)
	ascii85Reader := ascii85.NewDecoder(bytes.NewBuffer([]byte(processes[0].BpmnData)))
	deflateReader := flate.NewReader(ascii85Reader)
	buffer := bytes.Buffer{}
	_, err = io.Copy(&buffer, deflateReader)
	if err != nil {
		return nil, err
	}
	bpmnData := base64.StdEncoding.EncodeToString(buffer.Bytes())
	processDefinitionDetail := public.ProcessDefinitionDetail{
		ProcessDefinitionSimple: public.ProcessDefinitionSimple{
			BpmnProcessId: &processes[0].BpmnProcessId,
			Key:           ptr.To(fmt.Sprintf("%d", processes[0].Key)),
			Version:       &version,
		},
		BpmnData: &bpmnData,
	}
	return public.GetProcessDefinition200JSONResponse(processDefinitionDetail), nil
}

func (s *Server) CreateProcessInstance(ctx context.Context, request public.CreateProcessInstanceRequestObject) (public.CreateProcessInstanceResponseObject, error) {
	variables := make(map[string]interface{})
	if request.Body.Variables != nil {
		variables = *request.Body.Variables
	}
	process, err := s.engine.CreateInstanceByKey(ctx, *getKeyFromString(&request.Body.ProcessDefinitionKey), variables)
	if err != nil {
		return nil, err
	}
	instanceDetail, err := s.getProcessInstance(ctx, process.Key)
	if err != nil {
		return nil, err
	}
	return public.CreateProcessInstance200JSONResponse(*instanceDetail), nil
}

func (s *Server) GetProcessInstances(ctx context.Context, request public.GetProcessInstancesRequestObject) (public.GetProcessInstancesResponseObject, error) {
	store, err := s.node.GetPartitionStore(ctx, 1)
	if err != nil {
		return nil, err
	}
	_ = store
	// processInstances, err := store.FindProcessInstances(ctx, nil, getKeyFromString(request.Params.ProcessDefinitionKey))
	processInstances := []runtime.ProcessInstance{}
	if err != nil {
		return nil, err
	}

	processInstancesPage := public.ProcessInstancePage{
		Items: &[]public.ProcessInstance{},
	}
	for _, pi := range processInstances {
		processDefintionKey := fmt.Sprintf("%d", pi.Definition.Key)
		state := public.ProcessInstanceState(fmt.Sprintf("%d", pi.State))
		processInstanceSimple := public.ProcessInstance{
			Key:                  fmt.Sprintf("%d", pi.Key),
			ProcessDefinitionKey: processDefintionKey,
			State:                state,
			CreatedAt:            &pi.CreatedAt,
			// CaughtEvents:         &pi.CaughtEvents,
			// VariableHolder:       &pi.VariableHolder,
			// Activities:           &pi.Activities,
		}
		*processInstancesPage.Items = append(*processInstancesPage.Items, processInstanceSimple)
	}
	total := len(*processInstancesPage.Items)
	processInstancesPage.Count = &total
	processInstancesPage.Offset = nil
	processInstancesPage.Size = nil
	return public.GetProcessInstances200JSONResponse{
		ProcessInstances: &[]public.ProcessInstancePage{processInstancesPage},
		Total:            total,
	}, nil
}

func (s *Server) getProcessInstance(ctx context.Context, key int64) (*public.ProcessInstance, error) {
	store, err := s.node.GetPartitionStore(ctx, 1)
	if err != nil {
		return nil, err
	}
	_ = store
	// processInstances, err := store.FindProcessInstances(ctx, &key, nil)
	processInstances := []runtime.ProcessInstance{}
	if err != nil {
		return nil, err
	}
	if len(processInstances) == 0 {
		return nil, fmt.Errorf("process instance with key %d not found", key)
	}
	pi := processInstances[0]
	processDefintionKey := fmt.Sprintf("%d", pi.Definition.Key)
	state := public.ProcessInstanceState(fmt.Sprintf("%d", pi.State))
	processInstanceSimple := public.ProcessInstance{
		Key:                  fmt.Sprintf("%d", pi.Key),
		ProcessDefinitionKey: processDefintionKey,
		State:                state,
		CreatedAt:            &pi.CreatedAt,
		// CaughtEvents:         &pi.CaughtEvents,
		// VariableHolder:       &pi.VariableHolder,
		// Activities:           &pi.Activities,
	}
	return &processInstanceSimple, nil
}

func (s *Server) GetProcessInstance(ctx context.Context, request public.GetProcessInstanceRequestObject) (public.GetProcessInstanceResponseObject, error) {
	processInstance, err := s.getProcessInstance(ctx, *getKeyFromString(&request.ProcessInstanceKey))
	if err != nil {
		return nil, err
	}
	if processInstance == nil {
		return nil, fmt.Errorf("process instance with key %s not found", request.ProcessInstanceKey)
	}

	return public.GetProcessInstance200JSONResponse(*processInstance), nil
}

func (s *Server) GetActivities(ctx context.Context, request public.GetActivitiesRequestObject) (public.GetActivitiesResponseObject, error) {
	store, err := s.node.GetPartitionStore(ctx, 1)
	if err != nil {
		return nil, err
	}
	_ = store
	// activities, err := store.FindActivitiesByProcessInstanceKey(ctx, getKeyFromString(&request.ProcessInstanceKey))
	// activities := []runtime.Activity
	// if err != nil {
	// 	return nil, err
	// }
	// items := make([]public.Activity, 0)
	// result := public.ActivityPage{
	// 	Items: &items,
	// }
	// for _, a := range activities {
	// 	key := fmt.Sprintf("%d", a.Key)
	// 	createdAt := time.Unix(a.CreatedAt, 0)
	// 	processInstanceKey := fmt.Sprintf("%d", a.ProcessInstanceKey)
	// 	processDefinitionKey := fmt.Sprintf("%d", a.ProcessDefinitionKey)
	// 	activitySimple := public.Activity{
	// 		Key:                  &key,
	// 		ElementId:            &a.ElementID,
	// 		CreatedAt:            &createdAt,
	// 		BpmnElementType:      &a.BpmnElementType,
	// 		ProcessDefinitionKey: &processDefinitionKey,
	// 		ProcessInstanceKey:   &processInstanceKey,
	// 		State:                &a.State,
	// 	}
	// 	items = append(items, activitySimple)
	// }
	// result.Items = &items
	// l := len(items)
	// result.Count = &l
	// result.Offset = nil
	// result.Size = nil
	// return public.GetActivities200JSONResponse(result), nil
	return public.GetActivities200JSONResponse(public.ActivityPage{}), nil
}

func (s *Server) getJobItems(ctx context.Context, elementId *string, jobType *string, processInstanceKey *string, states ...string) ([]public.Job, error) {
	store, err := s.node.GetPartitionStore(ctx, 1)
	if err != nil {
		return nil, err
	}
	_ = store
	// jobs, err := store.FindJobs(ctx, elementId, jobType, getKeyFromString(processInstanceKey), nil, states)
	jobs := []runtime.Job{}
	if err != nil {
		return nil, err
	}
	items := make([]public.Job, 0)
	for _, j := range jobs {
		key := fmt.Sprintf("%d", j.GetKey())
		processInstanceKey := fmt.Sprintf("%d", j.ProcessInstanceKey)
		elementInstanceKey := fmt.Sprintf("%d", j.ElementInstanceKey)
		//TODO: Needs propper conversion
		state := fmt.Sprintf("%d", j.GetState())
		jobSimple := public.Job{
			Key: &key,
			// ElementId:          &j.ElementID,
			// Type:               &j.Type,
			ElementInstanceKey: &elementInstanceKey,
			CreatedAt:          &j.CreatedAt,
			State:              &state,
			ProcessInstanceKey: &processInstanceKey,
		}
		items = append(items, jobSimple)
	}
	return items, nil
}

func (s *Server) GetJobs(ctx context.Context, request public.GetJobsRequestObject) (public.GetJobsResponseObject, error) {
	items, err := s.getJobItems(ctx, nil, nil, &request.ProcessInstanceKey)

	if err != nil {
		return nil, err
	}
	result := public.JobPage{
		Items: &items,
	}
	l := len(items)
	result.Count = &l
	result.Offset = nil
	result.Size = nil
	return public.GetJobs200JSONResponse(result), nil
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
