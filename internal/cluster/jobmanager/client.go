package jobmanager

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
)

type clientSub struct {
	ctx      context.Context
	ch       chan Job
	clientId ClientId
	jobTypes []JobType
}

type jobClient struct {
	clientSubs     map[ClientId]*clientSub
	jobTypeClients map[JobType][]ClientId
	clientIdx      map[JobType]int // iterates over clientIds to provide round robin
	clientMu       *sync.RWMutex

	currentNodeTypes  []JobType
	store             Store
	nodeClientManager *client.ClientManager
	nodeStreams       []*nodeStream
	nodeMu            *sync.RWMutex
	// jobs are streamed in here by a server and distributed to clients
	jobsChan chan Job

	logger hclog.Logger
	ctx    context.Context
}

func newJobClient(ctx context.Context, store Store, clientManager *client.ClientManager) *jobClient {
	return &jobClient{
		clientSubs:        map[ClientId]*clientSub{},
		jobTypeClients:    map[JobType][]ClientId{},
		clientIdx:         map[JobType]int{},
		clientMu:          &sync.RWMutex{},
		currentNodeTypes:  []JobType{},
		store:             store,
		nodeClientManager: clientManager,
		nodeStreams:       []*nodeStream{},
		nodeMu:            &sync.RWMutex{},
		jobsChan:          make(chan Job),
		logger:            hclog.Default().Named("job-manager-client"),
		ctx:               ctx,
	}
}

// subscribeNode subscribes current node to all partition leaders
func (m *jobClient) subscribeNode(ctx context.Context, jobType ...JobType) {
	clusterState := m.store.ClusterState()
	for _, partition := range clusterState.Partitions {
		m.subscribeNodeToPartition(ctx, partition.Id, jobType...)
	}
}

func (m *jobClient) subscribeNodeToPartition(ctx context.Context, partition uint32, jobType ...JobType) {
	m.nodeMu.Lock()
	defer m.nodeMu.Unlock()
	lClient, nodeId, err := m.nodeClientManager.PartitionLeaderWithID(partition)
	if err != nil {
		m.logger.Error(fmt.Sprintf("failed to create client for partition %d leader", partition))
	}
	stream, err := lClient.SubscribeJob(ctx)
	if err != nil {
		m.logger.Error(fmt.Sprintf("failed to open stream for partition %d leader", partition))
	}
	nodeStream := nodeStream{
		stream:      stream,
		nodeId:      nodeId,
		partitionId: partition,
	}
	m.nodeStreams = append(m.nodeStreams, &nodeStream)
	go m.handleJobStreamRecv(&nodeStream)
}

func (m *jobClient) handleJobStreamRecv(stream *nodeStream) {
	for {
		job, err := stream.stream.Recv()
		if err == io.EOF {
			// read done.
			return
		}
		if err != nil {
			m.logger.Error("Failed to receive a job", "err", err, "streamNodeId", stream.nodeId)
		}
		m.jobsChan <- Job{
			Key:         job.Key,
			InstanceKey: job.InstanceKey,
			Variables:   job.Variables,
			Type:        JobType(job.Type),
			State:       job.State,
			ElementId:   job.ElementId,
			CreatedAt:   job.CreatedAt,
		}
	}
}

// unsubscribeNode unsubscribes current node from open jobType on current streams
func (m *jobClient) unsubscribeNode(ctx context.Context, jobType JobType) error {
	m.nodeMu.Lock()
	defer m.nodeMu.Unlock()
	for i := len(m.nodeStreams) - 1; i >= 0; i-- {
		stream := m.nodeStreams[i]
		err := stream.stream.Send(&proto.SubscribeJobRequest{
			JobType: string(jobType),
			Type:    proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE,
		})
		stream.jobTypes = slices.DeleteFunc(stream.jobTypes, func(a JobType) bool {
			return a == jobType
		})
		m.nodeStreams[i] = stream
		if err == io.EOF {
			m.nodeStreams = slices.Delete(m.nodeStreams, i, i+1)
			// stream was closed we need to reconnect to partition leader
			go m.subscribeNodeToPartition(ctx, stream.partitionId, stream.jobTypes...)
			continue
		}
		if err != nil {
			m.logger.Error(fmt.Sprintf("failed to unsubscribe job", "jobType", jobType, "streamNodeId", stream.nodeId, "err", err))
		}
	}
	return nil
}

func (c *jobClient) distributeToClients() {
	for {
		select {
		case job := <-c.jobsChan:
			c.sendJobToClient(job)
		case <-c.ctx.Done():
			c.logger.Info("Closing job client. Context cancelled.")
			return
		}
	}
}

func (c *jobClient) sendJobToClient(job Job) {
	c.clientMu.RLock()
	idx := c.clientIdx[job.Type]
	jobTypeClients := c.jobTypeClients[job.Type]
	idx++
	if idx >= len(jobTypeClients) {
		idx = 0
	}
	c.clientIdx[job.Type] = idx
	pickedClientId := jobTypeClients[idx]
	pickedClient := c.clientSubs[pickedClientId]
	if pickedClient.ctx.Err() != nil {
		// TODO: the client can already be disconnected we need to:
		//   try and pick another client or notify node that job is unprocessed
		c.clientMu.RUnlock()
		c.removeClient(pickedClient.ctx, pickedClient.clientId)
		return
	}
	pickedClient.ch <- job
	c.clientMu.RUnlock()
}

func (c *jobClient) startClient() {
	go c.distributeToClients()
}

func (c *jobClient) addClient(ctx context.Context, clientId ClientId) error {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	if _, ok := c.clientSubs[clientId]; ok {
		return fmt.Errorf("client with this id is already subscribed")
	}
	c.clientSubs[clientId] = &clientSub{
		ctx:      ctx,
		ch:       make(chan Job),
		clientId: clientId,
		jobTypes: []JobType{},
	}
	return nil
}

func (c *jobClient) removeClient(ctx context.Context, clientId ClientId) {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	sub, subFound := c.clientSubs[clientId]
	if subFound {
		delete(c.clientSubs, clientId)
		for _, jobType := range sub.jobTypes {
			c.removeJobSub(ctx, clientId, jobType)
		}
		close(sub.ch)
	}
}

func (m *jobClient) addJobSub(ctx context.Context, clientId ClientId, jobType JobType) error {
	m.clientMu.Lock()
	defer m.clientMu.Unlock()

	if _, ok := m.clientSubs[clientId]; !ok {
		return fmt.Errorf("client with id %s not subscribed", clientId)
	}
	m.clientSubs[clientId].jobTypes = append(m.clientSubs[clientId].jobTypes, jobType)

	if jobClients, ok := m.jobTypeClients[jobType]; !ok {
		m.jobTypeClients[jobType] = []ClientId{clientId}
	} else {
		if !slices.Contains(jobClients, clientId) {
			m.jobTypeClients[jobType] = append(jobClients, clientId)
		}
	}

	if !slices.Contains(m.currentNodeTypes, jobType) {
		m.currentNodeTypes = append(m.currentNodeTypes, jobType)
		m.subscribeNode(ctx, jobType)
	}
	return nil
}

func (m *jobClient) removeJobSub(ctx context.Context, clientId ClientId, jobType JobType) error {
	m.clientMu.Lock()
	defer m.clientMu.Unlock()
	if _, ok := m.clientSubs[clientId]; !ok {
		return fmt.Errorf("client with id %s is not subscribed", clientId)
	}
	m.clientSubs[clientId].jobTypes = slices.DeleteFunc(
		m.clientSubs[clientId].jobTypes,
		func(a JobType) bool {
			return a == jobType
		},
	)
	jobTypeSubscribed := false
pubSubs:
	for _, sub := range m.clientSubs {
		for _, t := range sub.jobTypes {
			if t == jobType {
				jobTypeSubscribed = true
				break pubSubs
			}
		}
	}
	if !jobTypeSubscribed {
		m.currentNodeTypes = slices.DeleteFunc(m.currentNodeTypes, func(a JobType) bool {
			return a == jobType
		})
		err := m.unsubscribeNode(ctx, jobType)
		if err != nil {
			m.logger.Error(fmt.Sprintf("failed to unsubscribe type %s", jobType), "err", err)
		}
	}
	return nil
}
