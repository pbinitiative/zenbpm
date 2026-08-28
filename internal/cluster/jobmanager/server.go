package jobmanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	MetadataNodeID   string = "node_id"
	MetadataClientID string = "client_id"
	// each job will remain assigned to client until this duration expires
	jobLockDuration               time.Duration = 30 * time.Second
	emptyDistributionCounterSleep int           = 100 // counter that puts job loader to sleep for 1 second
	maxActiveJobsPerClient        int64         = 10
)

type JobLoader interface {
	// LoadJobsToDistribute loads at most count jobs, sorted from oldest, across all partitions led by the node.
	LoadJobsToDistribute(jobTypes []string, idsToSkip []int64, count int64) ([]sql.Job, error)
}

type JobCompleter interface {
	JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]any) error
	JobFailByKey(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]any) error
}

type distributedJob struct {
	sentTime time.Time
	client   ClientID
	jobKey   int64
}

type nodeSub struct {
	nodeID NodeId
	stream grpc.BidiStreamingServer[proto.SubscribeJobRequest, proto.SubscribeJobResponse]
}

type jobTypeData struct {
	index   int
	clients []ClientID
}

type jobServer struct {
	ctx      context.Context
	nodeID   NodeId
	nodeMu   *sync.RWMutex
	nodeSubs map[NodeId]*nodeSub

	clientMu      *sync.RWMutex
	subscriptions map[JobType]map[ClientID]*nodeSub
	jobTypes      map[JobType]jobTypeData

	loader    JobLoader
	completer JobCompleter

	maxJobLoadCount          int64
	distributedJobs          []distributedJob
	distributedJobsMu        *sync.Mutex
	emptyDistributionCounter int

	logger hclog.Logger
}

func newJobServer(
	nodeID NodeId,
	jobLoader JobLoader,
	jobCompleter JobCompleter,
) *jobServer {
	return &jobServer{
		nodeMu:            &sync.RWMutex{},
		nodeSubs:          map[NodeId]*nodeSub{},
		nodeID:            nodeID,
		distributedJobs:   []distributedJob{},
		distributedJobsMu: &sync.Mutex{},
		subscriptions:     map[JobType]map[ClientID]*nodeSub{},
		jobTypes:          map[JobType]jobTypeData{},
		clientMu:          &sync.RWMutex{},
		logger:            hclog.Default().Named("job-manager-server"),
		loader:            jobLoader,
		maxJobLoadCount:   300,
		completer:         jobCompleter,
	}
}

func (s *jobServer) startServer(ctx context.Context) {
	s.ctx = ctx
	safego.Go("jobserver-distribute", s.logger, func() {
		s.distributeJobs()
	})
	s.logger.Info("Started server")
}

func (s *jobServer) distributeJobs() {
	for {
		if s.ctx.Err() != nil {
			s.nodeMu.Lock()
			nodeSubs := s.nodeSubs
			s.nodeSubs = make(map[NodeId]*nodeSub)
			s.nodeMu.Unlock()
			for nodeID, sub := range nodeSubs {
				// best-effort: send empty message to close the stream; the stream may already be gone during shutdown
				if err := sub.stream.Send(&proto.SubscribeJobResponse{}); err != nil {
					s.logger.Debug("failed to send stream close message to node", "nodeID", nodeID, "err", err)
				}
			}
			s.logger.Info("Stopping job distribution", "err", s.ctx.Err())
			return
		}
		s.clientMu.RLock()
		clients := make(map[ClientID]int64)
		jobTypeClients := make(map[JobType][]ClientID, len(s.jobTypes))
		for jobType, jobTypeData := range s.jobTypes {
			jobTypeClients[jobType] = slices.Clone(jobTypeData.clients)
			for _, client := range jobTypeData.clients {
				clients[client] = maxActiveJobsPerClient
			}
		}
		s.clientMu.RUnlock()
		s.distributedJobsMu.Lock()
		currentKeys := make([]int64, 0, len(s.distributedJobs))
		now := time.Now()
		for i := len(s.distributedJobs) - 1; i >= 0; i-- {
			job := s.distributedJobs[i]
			if job.sentTime.Add(jobLockDuration).Before(now) {
				// fmt.Println("releasing job", job)
				s.distributedJobs = append(s.distributedJobs[:i], s.distributedJobs[i+1:]...)
				continue
			}
			// only track capacity for clients that are still subscribed,
			// jobs of already removed clients must not create phantom entries
			if _, ok := clients[job.client]; ok {
				clients[job.client]--
			}
			currentKeys = append(currentKeys, job.jobKey)
		}
		s.distributedJobsMu.Unlock()

		jobTypes := make([]string, 0, len(jobTypeClients))
		for jobType, typeClients := range jobTypeClients {
			for _, clientID := range typeClients {
				if clients[clientID] > 0 {
					jobTypes = append(jobTypes, string(jobType))
					break
				}
			}
		}
		sort.Strings(jobTypes)

		jobsToLoad := int64(0)
		for _, numberOfSlots := range clients {
			if numberOfSlots > 0 {
				jobsToLoad += numberOfSlots
			}
		}
		if jobsToLoad <= 0 {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if jobsToLoad > s.maxJobLoadCount {
			jobsToLoad = s.maxJobLoadCount
		}
		jobs, err := s.loader.LoadJobsToDistribute(jobTypes, currentKeys, jobsToLoad)
		if err != nil {
			s.logger.Error("Failed to load new batch of jobs to distribute", "err", err)
			// give it some time not to overwhelm the node we might not be a leader anymore
			time.Sleep(1 * time.Second)
			continue
		}
		if len(jobs) == 0 {
			// wait for something to happen
			s.emptyDistributionCounter++
			if s.emptyDistributionCounter >= emptyDistributionCounterSleep {
				time.Sleep(1 * time.Second)
			} else {
				time.Sleep(100*time.Millisecond + time.Duration(s.emptyDistributionCounter)*time.Millisecond)
			}
			continue
		}
		s.emptyDistributionCounter = 0
		assignedJobs := 0
		for _, job := range jobs {
			s.clientMu.Lock()
			jType := JobType(job.Type)
			jobTypeData := s.jobTypes[jType]
			// check if there are any clients able to process
			if len(jobTypeData.clients) == 0 {
				s.clientMu.Unlock()
				continue
			}
			// round robin: starting from the client after the last used index,
			// pick the first client that still has remaining capacity
			numClients := len(jobTypeData.clients)
			var clientID ClientID
			var nodeStream *nodeSub
			for offset := 1; offset <= numClients; offset++ {
				idx := (jobTypeData.index + offset) % numClients
				candidateID := jobTypeData.clients[idx]
				// clients subscribed after the capacity snapshot was taken have
				// no known capacity in this round and are treated as saturated
				if clients[candidateID] <= 0 {
					continue
				}
				candidateStream, ok := s.subscriptions[jType][candidateID]
				if !ok {
					continue
				}
				jobTypeData.index = idx
				clientID = candidateID
				nodeStream = candidateStream
				clients[clientID]--
				break
			}
			if nodeStream == nil {
				// every client for this job type is saturated, the job stays
				// in the database and will be picked up in a later round
				s.clientMu.Unlock()
				continue
			}
			s.jobTypes[jType] = jobTypeData // set the updated index
			s.distributedJobsMu.Lock()
			s.distributedJobs = append(s.distributedJobs, distributedJob{
				sentTime: time.Now(),
				client:   clientID,
				jobKey:   job.Key,
			})
			s.distributedJobsMu.Unlock()
			s.clientMu.Unlock()
			// this might be bottleneck for now...in the future we might want
			// to have something that will allow us to send jobs to clients on
			// non blocked stream or use a pool of GRPC connections to handle jobs
			err := nodeStream.stream.Send(&proto.SubscribeJobResponse{
				JobType:  &job.Type,
				ClientId: new(string(clientID)),
				Job: &proto.InternalJob{
					Key:            &job.Key,
					InstanceKey:    &job.ProcessInstanceKey,
					InputVariables: []byte(job.InputVariables),
					Type:           &job.Type,
					State:          &job.State,
					ElementId:      &job.ElementID,
					CreatedAt:      &job.CreatedAt,
					ElementType:    &job.ElementType,
				},
			})
			if err != nil {
				s.distributedJobsMu.Lock()
				s.distributedJobs = slices.DeleteFunc(s.distributedJobs, func(distributed distributedJob) bool {
					return distributed.jobKey == job.Key && distributed.client == clientID
				})
				s.distributedJobsMu.Unlock()
				s.logger.Error("Failed to send job to node", "jobType", jType, "key", job.Key, "err", err)
				continue
			}
			assignedJobs++
			JobsDistributed.Add(s.ctx, 1, metric.WithAttributes(
				attribute.String("type", job.Type),
				attribute.String("client", string(clientID)),
			))
			if JobActivationLatency != nil && job.CreatedAt > 0 {
				latencyMs := float64(time.Now().UnixMilli() - job.CreatedAt)
				if latencyMs < 0 {
					latencyMs = 0
				}
				JobActivationLatency.Record(s.ctx, latencyMs, metric.WithAttributes(
					attribute.String("type", job.Type),
				))
			}
		}
		if assignedJobs == 0 {
			// every loaded job was skipped (saturated or unavailable clients),
			// back off to avoid a tight database-query loop until capacity changes
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (s *jobServer) addNodeSubscription(stream grpc.BidiStreamingServer[proto.SubscribeJobRequest, proto.SubscribeJobResponse]) error {
	md, found := metadata.FromIncomingContext(stream.Context())
	if !found {
		return fmt.Errorf("expected metadata to be present in SubscribeJob stream")
	}
	nodeIds := md.Get(MetadataNodeID)
	if len(nodeIds) != 1 {
		return fmt.Errorf("expected nodeId to be present in metadata in SubscribeJob stream")
	}
	nodeID := NodeId(nodeIds[0])
	nodeSub := &nodeSub{
		nodeID: nodeID,
		stream: stream,
	}
	s.nodeMu.Lock()
	s.nodeSubs[nodeID] = nodeSub
	s.nodeMu.Unlock()
	s.handleJobStreamRecv(nodeSub)
	return nil
}

func (s *jobServer) handleJobStreamRecv(stream *nodeSub) {
	for {
		req, err := stream.stream.Recv()
		if err == io.EOF || errors.Is(err, context.Canceled) {
			// read done.
			s.removeNode(stream)
			s.logger.Debug("Stream closed", "err", err)
			return
		}
		if err != nil {
			s.logger.Error("Failed to receive a job subscription request", "err", err, "streamNodeId", stream.nodeID)
			return
		}
		switch req.GetType() {
		case proto.SubscribeJobRequest_TYPE_SUBSCRIBE:
			s.subscribeClient(stream.nodeID, ClientID(req.GetClientId()), JobType(req.GetJobType()))
		case proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE:
			s.unsubscribeClient(ClientID(req.GetClientId()), JobType(req.GetJobType()))
		case proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE_ALL:
			s.removeClient(ClientID(req.GetClientId()))
		default:
			s.logger.Error("received unexpected SubscribeJob request type, ignoring",
				"type", req.GetType(), "streamNodeId", stream.nodeID)
			continue
		}
	}
}

func (s *jobServer) removeNode(closing *nodeSub) {
	s.removeNodeSubscription(closing)

	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	removedClients := make(map[ClientID]struct{})
	for jobType, subs := range s.subscriptions {
		removed := make(map[ClientID]struct{}, len(subs))
		for clientID, nodeSub := range subs {
			if nodeSub != closing {
				continue
			}
			delete(s.subscriptions[jobType], clientID)
			removed[clientID] = struct{}{}
			removedClients[clientID] = struct{}{}
		}
		if len(removed) == 0 {
			continue
		}
		// clients of the removed node have to be dropped from the round robin
		// list as well, otherwise a subscription replay after a reconnect would
		// register them twice
		jobTypeData, ok := s.jobTypes[jobType]
		if !ok {
			continue
		}
		jobTypeData.clients = slices.DeleteFunc(jobTypeData.clients, func(clientID ClientID) bool {
			_, ok := removed[clientID]
			return ok
		})
		if len(jobTypeData.clients) == 0 {
			delete(s.jobTypes, jobType)
			continue
		}
		if jobTypeData.index >= len(jobTypeData.clients) {
			jobTypeData.index = 0
		}
		s.jobTypes[jobType] = jobTypeData
	}
	if len(removedClients) > 0 {
		s.distributedJobsMu.Lock()
		s.distributedJobs = slices.DeleteFunc(s.distributedJobs, func(job distributedJob) bool {
			_, removed := removedClients[job.client]
			return removed
		})
		s.distributedJobsMu.Unlock()
	}
}

func (s *jobServer) removeNodeSubscription(closing *nodeSub) {
	s.nodeMu.Lock()
	defer s.nodeMu.Unlock()

	if current, ok := s.nodeSubs[closing.nodeID]; ok && current == closing {
		delete(s.nodeSubs, closing.nodeID)
	}
}

func (s *jobServer) subscribeClient(clientsNodeID NodeId, clientID ClientID, jType JobType) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	s.nodeMu.RLock()
	clientsNode, ok := s.nodeSubs[clientsNodeID]
	s.nodeMu.RUnlock()
	if !ok {
		s.logger.Error("Failed to subscribe client. Clients node is not subscribed.")
		return
	}
	if _, ok := s.subscriptions[jType]; !ok {
		s.subscriptions[jType] = map[ClientID]*nodeSub{}
	}
	if _, ok := s.jobTypes[jType]; !ok {
		s.jobTypes[jType] = jobTypeData{
			index:   0,
			clients: make([]ClientID, 0, 10),
		}
	}
	jobTypeData := s.jobTypes[jType]
	if _, alreadySubscribed := s.subscriptions[jType][clientID]; alreadySubscribed {
		// resubscribing the same client (e.g. a replay after a stream was
		// reopened) must not register it twice in the round robin list
		s.subscriptions[jType][clientID] = clientsNode
		return
	}
	s.subscriptions[jType][clientID] = clientsNode
	jobTypeData.clients = append(jobTypeData.clients, clientID)
	s.jobTypes[jType] = jobTypeData
}

func (s *jobServer) unsubscribeClient(clientID ClientID, jType JobType) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	delete(s.subscriptions[jType], clientID)
	index := -1
	for i, client := range s.jobTypes[jType].clients {
		if client == clientID {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	jobTypeData := s.jobTypes[jType]
	jobTypeData.clients = append(jobTypeData.clients[:index], jobTypeData.clients[index+1:]...)
	s.jobTypes[jType] = jobTypeData
}

func (s *jobServer) removeClient(clientID ClientID) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	for jobType := range s.subscriptions {
		delete(s.subscriptions[jobType], clientID)
	}
	for jType, jobTypeData := range s.jobTypes {
		index := -1
		for k, client := range jobTypeData.clients {
			if client == clientID {
				index = k
				break
			}
		}
		if index >= 0 {
			jobTypeData := s.jobTypes[jType]
			jobTypeData.clients = append(jobTypeData.clients[:index], jobTypeData.clients[index+1:]...)
			s.jobTypes[jType] = jobTypeData
		}
		if len(s.jobTypes[jType].clients) == 0 {
			delete(s.jobTypes, jType)
		}
	}
	s.distributedJobsMu.Lock()
	s.distributedJobs = slices.DeleteFunc(s.distributedJobs, func(job distributedJob) bool {
		return job.client == clientID
	})
	s.distributedJobsMu.Unlock()
}

func (s *jobServer) completeJob(ctx context.Context, clientID ClientID, jobKey int64, variables map[string]any) error {
	err := s.completer.JobCompleteByKey(ctx, jobKey, variables)
	if err != nil {
		return fmt.Errorf("failed to complete job %d: %w", jobKey, err)
	}
	s.distributedJobsMu.Lock()
	for i, job := range s.distributedJobs {
		if job.jobKey != jobKey {
			continue
		}
		s.distributedJobs = append(s.distributedJobs[:i], s.distributedJobs[i+1:]...)
		break
	}
	s.distributedJobsMu.Unlock()
	return nil
}

func (s *jobServer) failJob(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables map[string]interface{}) error {
	err := s.completer.JobFailByKey(ctx, jobKey, message, errorCode, variables)
	if err != nil {
		return fmt.Errorf("failed to fail job %d: %w", jobKey, err)
	}
	s.distributedJobsMu.Lock()
	for i, job := range s.distributedJobs {
		if job.jobKey != jobKey {
			continue
		}
		s.distributedJobs = append(s.distributedJobs[:i], s.distributedJobs[i+1:]...)
		break
	}
	s.distributedJobsMu.Unlock()
	return nil
}

func (s *jobServer) onJobRejected(ctx context.Context, jobKey int64) {
	// TODO: unlock the job and assign to new node, if there is no new node we need to remove the type from currently needed jobTypes
}
