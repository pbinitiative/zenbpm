package jobmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type clientSub struct {
	ctx      context.Context
	ch       chan Job
	clientID ClientID
}

type clientNodeStream struct {
	stream     grpc.BidiStreamingClient[proto.SubscribeJobRequest, proto.SubscribeJobResponse]
	nodeID     string
	partitions []uint32
}

type jobClient struct {
	// TODO: add mechanism to handle max_active_jobs and lock_duration
	clientSubs map[ClientID]*clientSub
	clientMu   *sync.RWMutex

	store             Store
	nodeID            NodeId
	nodeClientManager *client.ClientManager
	nodeStreams       []*clientNodeStream
	nodeMu            *sync.RWMutex
	// jobs are streamed in here by a server and distributed to clients
	jobsChan chan Job

	logger hclog.Logger
	ctx    context.Context
}

func (c *jobClient) updateNodeSubs(ctx context.Context, leaders []string) {
	c.nodeMu.Lock()
	partitionsToSubscribe := []uint32{}
	for i := len(c.nodeStreams) - 1; i >= 0; i-- {
		stream := c.nodeStreams[i]
		if !slices.Contains(leaders, stream.nodeID) {
			err := stream.stream.CloseSend()
			if err != nil {
				c.logger.Error("Failed to close stream", "nodeID", stream.nodeID, "err", err)
			}
			c.nodeStreams = append(c.nodeStreams[:i], c.nodeStreams[i+1:]...)
			partitionsToSubscribe = append(partitionsToSubscribe, stream.partitions...)
		}
	}
	c.nodeMu.Unlock()
	for _, partition := range partitionsToSubscribe {
		c.subscribeNodeToPartition(ctx, partition)
	}
}

func newJobClient(ctx context.Context, nodeID NodeId, store Store, clientManager *client.ClientManager) *jobClient {
	return &jobClient{
		clientSubs:        map[ClientID]*clientSub{},
		clientMu:          &sync.RWMutex{},
		store:             store,
		nodeID:            nodeID,
		nodeClientManager: clientManager,
		nodeStreams:       []*clientNodeStream{},
		nodeMu:            &sync.RWMutex{},
		jobsChan:          make(chan Job),
		logger:            hclog.Default().Named("job-manager-client"),
		ctx:               ctx,
	}
}

// subscribeNode subscribes current node to all partition leaders
func (c *jobClient) subscribeNode(ctx context.Context) {
	clusterState := c.store.ClusterState()
	for _, partition := range clusterState.Partitions {
		c.subscribeNodeToPartition(ctx, partition.Id)
	}
}

func (c *jobClient) subscribeNodeToPartition(ctx context.Context, partition uint32) {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	lClient, nodeID, err := c.nodeClientManager.PartitionLeaderWithID(partition)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to create client for partition %d leader", partition))
		return
	}
	md := metadata.New(map[string]string{
		MetadataNodeID: string(c.nodeID),
	})
	ctx = metadata.NewOutgoingContext(ctx, md)
	stream, err := lClient.SubscribeJob(ctx)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to open stream for partition %d leader", partition), "err", err)
		return
	}
	nodeStream := clientNodeStream{
		stream: stream,
		nodeID: nodeID,
	}
	c.nodeStreams = append(c.nodeStreams, &nodeStream)
	go c.handleJobStreamRecv(&nodeStream)
}

func (c *jobClient) handleJobStreamRecv(stream *clientNodeStream) {
	for {
		resp, err := stream.stream.Recv()
		if err == io.EOF {
			// read done.
			return
		}
		if err != nil {
			c.logger.Error("Failed to receive a job", "err", err, "streamNodeId", stream.nodeID)
			return
		}
		if resp.Job == nil {
			c.logger.Error("closing stream", "err", err, "streamNodeId", stream.nodeID)
			return
		}
		c.jobsChan <- Job{
			Key:         resp.Job.Key,
			InstanceKey: resp.Job.InstanceKey,
			Variables:   resp.Job.Variables,
			Type:        JobType(resp.Job.Type),
			State:       resp.Job.State,
			ElementID:   resp.Job.ElementId,
			CreatedAt:   resp.Job.CreatedAt,
			ClientID:    ClientID(resp.ClientId),
		}
	}
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
	pickedClient := c.clientSubs[job.ClientID]
	if pickedClient == nil {
		// TODO send msg to server to free the job
		return
	}
	if pickedClient.ctx.Err() != nil {
		c.clientMu.RUnlock()
		c.removeClient(pickedClient.ctx, pickedClient.clientID)
		return
	}
	pickedClient.ch <- job
	c.clientMu.RUnlock()
}

func (c *jobClient) startClient() {
	c.subscribeNode(c.ctx)
	go c.distributeToClients()
}

func (c *jobClient) broadcastToNodes(req *proto.SubscribeJobRequest) error {
	var errJoin error
	c.nodeMu.RLock()
	defer c.nodeMu.RUnlock()
	for _, stream := range c.nodeStreams {
		err := stream.stream.Send(req)
		if err != nil {
			errJoin = errors.Join(errJoin, fmt.Errorf("failed to send request to nodeID %s: %w", stream.nodeID, err))
		}
	}
	return errJoin
}

func (c *jobClient) addClient(ctx context.Context, clientID ClientID, clientRcv chan Job) error {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	if _, ok := c.clientSubs[clientID]; ok {
		return fmt.Errorf("client with this id is already subscribed")
	}
	c.clientSubs[clientID] = &clientSub{
		ctx:      ctx,
		ch:       clientRcv,
		clientID: clientID,
	}
	return nil
}

func (c *jobClient) removeClient(ctx context.Context, clientID ClientID) {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	sub, subFound := c.clientSubs[clientID]
	if subFound {
		delete(c.clientSubs, clientID)
		close(sub.ch)
		err := c.broadcastToNodes(&proto.SubscribeJobRequest{
			Type:     proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE_ALL,
			ClientId: string(clientID),
		})
		if err != nil {
			c.logger.Error("failed to remove client from nodes", "clientID", clientID, "err", err)
		}
	}
}

func (c *jobClient) addJobSub(ctx context.Context, clientID ClientID, jobType JobType) error {
	err := c.broadcastToNodes(&proto.SubscribeJobRequest{
		JobType:  string(jobType),
		Type:     proto.SubscribeJobRequest_TYPE_SUBSCRIBE,
		ClientId: string(clientID),
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe client %s to jobType %s: %w", clientID, jobType, err)
	}
	return nil
}

func (c *jobClient) completeJob(ctx context.Context, clientID ClientID, jobKey int64, variables map[string]any) error {
	partitionId := zenflake.GetPartitionId(jobKey)
	lClient, err := c.nodeClientManager.PartitionLeader(partitionId)
	if err != nil {
		return fmt.Errorf("failed to retrieve client for partition %d leader: %w", partitionId, err)
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables for job completion: %w", err)
	}
	_, err = lClient.CompleteJob(ctx, &proto.CompleteJobRequest{
		Key:       jobKey,
		Variables: vars,
	})
	if err != nil {
		return fmt.Errorf("failed to complete job %d from client: %w", jobKey, err)
	}
	return nil
}

func (c *jobClient) failJob(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables *map[string]interface{}) error {
	partitionId := zenflake.GetPartitionId(jobKey)
	lClient, err := c.nodeClientManager.PartitionLeader(partitionId)
	if err != nil {
		return fmt.Errorf("failed to retrieve client for partition %d leader: %w", partitionId, err)
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables for job failure: %w", err)
	}
	_, err = lClient.FailJob(ctx, &proto.FailJobRequest{
		Key:       jobKey,
		Message:   message,
		ErrorCode: errorCode,
		Variables: vars,
	})
	if err != nil {
		return fmt.Errorf("failed to fail job %d from client: %w", jobKey, err)
	}
	return nil
}

func (c *jobClient) removeJobSub(ctx context.Context, clientID ClientID, jobType JobType) error {
	err := c.broadcastToNodes(&proto.SubscribeJobRequest{
		JobType:  string(jobType),
		Type:     proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE,
		ClientId: string(clientID),
	})
	if err != nil {
		return fmt.Errorf("failed to unsubscribe client %s from jobType %s: %w", clientID, jobType, err)
	}
	return nil
}
