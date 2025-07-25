package jobmanager

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	metadataNodeId   string = "node_id"
	metadataClientID string = "client_id"
	// each job will remain assigned to client until this duration expires
	jobLockDuration time.Duration = 30 * time.Second
	jobLoadCount    int           = 100
)

type JobLoader interface {
	// LoadJobsToDistribute loads a number of jobs (sorted from oldest) from each partition that the node is leader on
	LoadJobsToDistribute(jobTypes []string, idsToSkip []int64, count int) ([]sql.Job, error)
}

type JobCompleter interface {
	JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]any) error
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

type jobServer struct {
	ctx      context.Context
	nodeID   NodeId
	nodeMu   *sync.RWMutex
	nodeSubs map[NodeId]*nodeSub

	clientMu      *sync.RWMutex
	subscriptions map[JobType]map[ClientID]*nodeSub
	jobClients    map[JobType][]ClientID
	jobTypeIdx    map[JobType]int64

	loader    JobLoader
	completer JobCompleter

	jobLoadCount      int
	distributedJobs   []distributedJob
	distributedJobsMu *sync.RWMutex

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
		distributedJobsMu: &sync.RWMutex{},
		subscriptions:     map[JobType]map[ClientID]*nodeSub{},
		jobClients:        map[JobType][]ClientID{},
		jobTypeIdx:        map[JobType]int64{},
		clientMu:          &sync.RWMutex{},
		logger:            hclog.Default().Named("job-manager-server"),
		loader:            jobLoader,
		jobLoadCount:      jobLoadCount,
		completer:         jobCompleter,
	}
}

func (s *jobServer) startServer(ctx context.Context) {
	s.ctx = ctx
	go s.distributeJobs()
}

func (s *jobServer) distributeJobs() {
	for {
		if s.ctx.Err() != nil {
			for _, sub := range s.nodeSubs {
				// send empty message to close the stream
				sub.stream.Send(&proto.SubscribeJobResponse{})
			}
			s.nodeSubs = make(map[NodeId]*nodeSub)
			return
		}
		jobTypes := make([]string, 0, len(s.jobTypeIdx))
		for jobType := range s.jobTypeIdx {
			jobTypes = append(jobTypes, string(jobType))
		}
		s.distributedJobsMu.RLock()
		currentKeys := make([]int64, len(s.distributedJobs))
		now := time.Now()
		for i := len(s.distributedJobs) - 1; i >= 0; i-- {
			job := s.distributedJobs[i]
			if job.sentTime.Add(jobLockDuration).Before(now) {
				s.distributedJobs = append(s.distributedJobs[:i], s.distributedJobs[i+1:]...)
				continue
			}
			currentKeys[i] = job.jobKey
		}
		s.distributedJobsMu.RUnlock()
		jobs, err := s.loader.LoadJobsToDistribute(jobTypes, currentKeys, s.jobLoadCount)
		if err != nil {
			s.logger.Error("Failed to load new batch of jobs to distribute", "err", err)
			continue
		}
		for _, job := range jobs {
			s.clientMu.RLock()
			jType := JobType(job.Type)
			// check if there are any clients able to process
			if len(s.jobClients[jType]) == 0 {
				s.clientMu.RUnlock()
				continue
			}
			s.jobTypeIdx[jType]++
			// index overflow
			if int64(len(s.jobClients[jType])) <= s.jobTypeIdx[jType] {
				s.jobTypeIdx[jType] = 0
			}
			clientIdx := s.jobTypeIdx[jType]
			clientID := s.jobClients[jType][clientIdx]
			nodeStream, ok := s.subscriptions[jType][clientID]
			if !ok {
				s.logger.Warn("Stream for job was not found", "jobType", jType, "key", job.Key)
				s.clientMu.RUnlock()
				continue
			}
			s.distributedJobsMu.Lock()
			s.distributedJobs = append(s.distributedJobs, distributedJob{
				sentTime: time.Now(),
				client:   clientID,
				jobKey:   job.Key,
			})
			s.distributedJobsMu.Unlock()
			s.clientMu.RUnlock()
			// this might be bottleneck for now...in the future we might want
			// to have something that will allow us to send jobs to clients on
			// non blocked stream or use a pool of GRPC connections to handle jobs
			err := nodeStream.stream.Send(&proto.SubscribeJobResponse{
				JobType:  job.Type,
				ClientId: string(clientID),
				Job: &proto.InternalJob{
					Key:         job.Key,
					InstanceKey: job.ProcessInstanceKey,
					Variables:   []byte(job.Variables),
					Type:        job.Type,
					State:       job.State,
					ElementId:   job.ElementID,
					CreatedAt:   job.CreatedAt,
				},
			})
			if err != nil {
				s.logger.Error("Failed to send job to node", "jobType", jType, "key", job.Key, "err", err)
				continue
			}
		}
	}
}

func (s *jobServer) addNodeSubscription(stream grpc.BidiStreamingServer[proto.SubscribeJobRequest, proto.SubscribeJobResponse]) error {
	md, found := metadata.FromIncomingContext(stream.Context())
	if !found {
		return fmt.Errorf("expected metadata to be present in SubscribeJob stream")
	}
	nodeIds := md.Get(metadataNodeId)
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
		if err == io.EOF {
			// read done.
			s.removeNode(stream.nodeID)
			return
		}
		if err != nil {
			s.logger.Error("Failed to receive a job subscription request", "err", err, "streamNodeId", stream.nodeID)
			return
		}
		switch req.Type {
		case proto.SubscribeJobRequest_TYPE_SUBSCRIBE:
			s.subscribeClient(stream.nodeID, ClientID(req.ClientId), JobType(req.JobType))
		case proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE:
			s.unsubscribeClient(ClientID(req.ClientId), JobType(req.JobType))
		case proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE_ALL:
			s.removeClient(ClientID(req.ClientId))
		default:
			panic(fmt.Sprintf("unexpected proto.SubscribeJobRequest_Type: %#v", req.Type))
		}
	}
}

func (s *jobServer) removeNode(nodeId NodeId) {
	s.nodeMu.Lock()
	delete(s.nodeSubs, nodeId)
	s.nodeMu.Unlock()
	s.clientMu.Lock()
	for jobType, subs := range s.subscriptions {
		for clientID, nodeSub := range subs {
			if nodeSub.nodeID != nodeId {
				continue
			}
			delete(s.subscriptions[jobType], clientID)
		}
	}
	s.clientMu.Unlock()
}

func (s *jobServer) subscribeClient(clientsNodeID NodeId, clientID ClientID, jobType JobType) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	s.nodeMu.RLock()
	clientsNode, ok := s.nodeSubs[clientsNodeID]
	if !ok {
		s.logger.Error("Failed to subscribe client. Clients node is not subscribed.")
		return
	}
	s.nodeMu.RUnlock()
	if _, ok := s.subscriptions[jobType]; !ok {
		s.subscriptions[jobType] = map[ClientID]*nodeSub{}
	}
	if _, ok := s.jobClients[jobType]; !ok {
		s.jobClients[jobType] = make([]ClientID, 0)
	}
	s.subscriptions[jobType][clientID] = clientsNode
	s.jobClients[jobType] = append(s.jobClients[jobType], clientID)
}

func (s *jobServer) unsubscribeClient(clientID ClientID, jobType JobType) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	delete(s.subscriptions[jobType], clientID)
	index := -1
	for i, client := range s.jobClients[jobType] {
		if client == clientID {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	s.jobClients[jobType] = append(s.jobClients[jobType][:index], s.jobClients[jobType][index+1:]...)
}

func (s *jobServer) removeClient(clientID ClientID) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	for jobType := range s.subscriptions {
		delete(s.subscriptions[jobType], clientID)
	}
	for jobType, clients := range s.jobClients {
		index := -1
		for k, client := range clients {
			if client == clientID {
				index = k
				break
			}
		}
		if index >= 0 {
			s.jobClients[jobType] = append(s.jobClients[jobType][:index], s.jobClients[jobType][index+1:]...)
		}
	}
}

func (s *jobServer) completeJob(ctx context.Context, clientID ClientID, jobKey int64, variables map[string]any) error {
	var dJob distributedJob
	err := s.completer.JobCompleteByKey(ctx, jobKey, variables)
	if err != nil {
		return fmt.Errorf("failed to complete job %d: %w", jobKey, err)
	}
	s.distributedJobsMu.Lock()
	for i, job := range s.distributedJobs {
		if job.jobKey != jobKey {
			continue
		}
		dJob = job
		s.distributedJobs = append(s.distributedJobs[:i], s.distributedJobs[i+1:]...)
		break
	}
	s.distributedJobsMu.Unlock()
	if dJob.jobKey == 0 {
		return fmt.Errorf("job with key %d was not distributed for this client", jobKey)
	}
	return nil
}

func (s *jobServer) onJobRejected(ctx context.Context, jobKey int64) {
	// TODO: unlock the job and assign to new node, if there is no new node we need to remove the type from currently needed jobTypes
}
