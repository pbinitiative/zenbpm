package jobmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/safego"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type clientSub struct {
	ctx      context.Context
	ch       chan Job
	clientID ClientID
	// jobTypes the client is subscribed to, kept so the subscriptions can be
	// replayed to node streams that are opened later.
	jobTypes map[JobType]struct{}
}

type clientNodeStream struct {
	stream grpc.BidiStreamingClient[proto.SubscribeJobRequest, proto.SubscribeJobResponse]
	// sendMu serializes sends on the stream. grpc-go does not support concurrent
	// SendMsg/CloseSend calls on the same client stream.
	sendMu    sync.Mutex
	nodeID    string
	partition uint32
}

func (s *clientNodeStream) send(req *proto.SubscribeJobRequest) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(req)
}

func (s *clientNodeStream) closeSend() error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.CloseSend()
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
	// subscribeMu serializes node stream reconciliation
	subscribeMu sync.Mutex
	// jobs are streamed in here by a server and distributed to clients
	jobsChan chan Job

	logger hclog.Logger
	ctx    context.Context
}

// updateNodeSubs reconciles the open job streams with the current cluster state.
// Streams pointing to a node that is no longer the initialized leader of the
// partition are closed and streams are opened for every partition that does not
// have one yet. It is safe to call repeatedly: partitions that already have a
// healthy stream are skipped.
func (c *jobClient) updateNodeSubs() {
	// Serialize reconciliations so two concurrent calls cannot open two streams
	// for the same partition (streams are opened outside of nodeMu).
	c.subscribeMu.Lock()
	defer c.subscribeMu.Unlock()

	leaders := map[uint32]string{}
	s := c.store.ClusterState()
	for _, partition := range s.Partitions {
		partitionLeader := partition.LeaderId
		partitionNode, ok := s.Nodes[partitionLeader]
		if !ok {
			continue
		}
		// if partition is not initialized yet skip it for now
		if partitionNode.Partitions[partition.Id].State != state.NodePartitionStateInitialized {
			continue
		}
		leaders[partition.Id] = partition.LeaderId
	}
	partitionsToSubscribe := slices.Collect(maps.Keys(leaders))
	slices.Sort(partitionsToSubscribe)

	c.nodeMu.Lock()
	for i := len(c.nodeStreams) - 1; i >= 0; i-- {
		stream := c.nodeStreams[i]
		assignedLeader, ok := leaders[stream.partition]
		if ok && assignedLeader == stream.nodeID && stream.stream.Context().Err() == nil {
			// the stream still points to the current partition leader
			partitionsToSubscribe = slices.DeleteFunc(partitionsToSubscribe, func(a uint32) bool {
				return a == stream.partition
			})
			continue
		}
		// the stream node is not the partition leader anymore or the stream died
		if err := stream.closeSend(); err != nil {
			c.logger.Error("Failed to close stream", "nodeID", stream.nodeID, "err", err)
		}
		c.nodeStreams = append(c.nodeStreams[:i], c.nodeStreams[i+1:]...)
	}
	c.nodeMu.Unlock()

	for _, partition := range partitionsToSubscribe {
		c.subscribeNodeToPartition(partition)
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
func (c *jobClient) subscribeNode() {
	c.updateNodeSubs()
}

// subscribeNodeToPartition opens a job stream to the leader of the partition.
// The stream is bound to the job client context (not to the context of the
// cluster state notification that triggered the reconciliation), because the
// notification context is cancelled as soon as the notification is handled.
func (c *jobClient) subscribeNodeToPartition(partition uint32) {
	lClient, nodeID, err := c.nodeClientManager.PartitionLeaderWithID(partition)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to create client for partition %d leader", partition), "err", err)
		return
	}
	md := metadata.New(map[string]string{
		MetadataNodeID: string(c.nodeID),
	})
	streamCtx := metadata.NewOutgoingContext(c.ctx, md)
	stream, err := lClient.SubscribeJob(streamCtx)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to open stream for partition %d leader", partition), "err", err)
		return
	}
	nodeStream := clientNodeStream{
		stream:    stream,
		nodeID:    nodeID,
		partition: partition,
	}
	// Registering the stream and replaying the current subscriptions happens
	// under clientMu so that a concurrent subscription change either completes
	// before the snapshot is taken (and is therefore part of the replay) or is
	// broadcast after the stream is registered (and is therefore delivered to
	// it). Otherwise a removed subscription could be replayed after its
	// UNSUBSCRIBE was already broadcast.
	c.clientMu.RLock()
	c.nodeMu.Lock()
	c.nodeStreams = append(c.nodeStreams, &nodeStream)
	c.nodeMu.Unlock()
	// A stream opened after clients already registered (e.g. a partition that
	// became available later) does not know about their job types yet.
	c.resendClientSubscriptions(&nodeStream)
	c.clientMu.RUnlock()
	safego.Go("jobclient-stream-recv", c.logger, func() {
		c.handleJobStreamRecv(&nodeStream)
	})
}

// resendClientSubscriptions replays the job subscriptions of all locally
// registered clients to a newly opened node stream.
// The caller must hold clientMu.
func (c *jobClient) resendClientSubscriptions(stream *clientNodeStream) {
	requests := make([]*proto.SubscribeJobRequest, 0, len(c.clientSubs))
	for clientID, sub := range c.clientSubs {
		for jobType := range sub.jobTypes {
			requests = append(requests, &proto.SubscribeJobRequest{
				JobType:  new(string(jobType)),
				Type:     proto.SubscribeJobRequest_TYPE_SUBSCRIBE.Enum(),
				ClientId: new(string(clientID)),
			})
		}
	}
	for _, req := range requests {
		if err := stream.send(req); err != nil {
			c.logger.Error("Failed to resend client job subscription", "nodeID", stream.nodeID, "err", err)
		}
	}
}

func (c *jobClient) handleJobStreamRecv(stream *clientNodeStream) {
	for {
		resp, err := stream.stream.Recv()
		if err == io.EOF || errors.Is(err, context.Canceled) {
			// read done.
			c.logger.Debug("Stream closed", "err", err)
			// TODO: reconnect when stream is closed
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
			Key:            resp.Job.GetKey(),
			InstanceKey:    resp.Job.GetInstanceKey(),
			InputVariables: resp.Job.GetInputVariables(),
			Type:           JobType(resp.Job.GetType()),
			State:          resp.Job.GetState(),
			ElementID:      resp.Job.GetElementId(),
			CreatedAt:      resp.Job.GetCreatedAt(),
			ClientID:       ClientID(resp.GetClientId()),
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
		c.clientMu.RUnlock()
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
	c.subscribeNode()
	safego.Go("jobclient-distribute", c.logger, func() {
		c.distributeToClients()
	})
	c.logger.Info("Started client")
}

// broadcastToNodes sends the request to all open node streams.
// The caller must hold clientMu so that subscription changes stay ordered with
// the subscription replay done for newly opened streams.
func (c *jobClient) broadcastToNodes(req *proto.SubscribeJobRequest) error {
	var errJoin error
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	healthyStreams := make([]*clientNodeStream, 0, len(c.nodeStreams))
	for _, stream := range c.nodeStreams {
		if err := stream.send(req); err != nil {
			errJoin = errors.Join(errJoin, fmt.Errorf("failed to send request to nodeID %s: %w", stream.nodeID, err))
			if closeErr := stream.closeSend(); closeErr != nil {
				errJoin = errors.Join(errJoin, fmt.Errorf("failed to close subscription stream to nodeID %s: %w", stream.nodeID, closeErr))
			}
			continue
		}
		healthyStreams = append(healthyStreams, stream)
	}
	c.nodeStreams = healthyStreams
	return errJoin
}

func (c *jobClient) reconcileNodeSubscriptions() {
	if c.nodeClientManager != nil {
		c.updateNodeSubs()
	}
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
		jobTypes: map[JobType]struct{}{},
	}
	return nil
}

func (c *jobClient) removeClient(ctx context.Context, clientID ClientID) {
	c.clientMu.Lock()
	sub, subFound := c.clientSubs[clientID]
	if !subFound {
		c.clientMu.Unlock()
		return
	}
	err := c.broadcastToNodes(&proto.SubscribeJobRequest{
		Type:     proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE_ALL.Enum(),
		ClientId: new(string(clientID)),
	})
	delete(c.clientSubs, clientID)
	close(sub.ch)
	c.clientMu.Unlock()
	if err != nil {
		c.logger.Error("failed to remove client from nodes", "clientID", clientID, "err", err)
		c.reconcileNodeSubscriptions()
	}
}

func (c *jobClient) addJobSub(ctx context.Context, clientID ClientID, jobType JobType) error {
	c.clientMu.Lock()
	sub, ok := c.clientSubs[clientID]
	if !ok {
		c.clientMu.Unlock()
		return fmt.Errorf("client %s is not registered", clientID)
	}
	sub.jobTypes[jobType] = struct{}{}
	err := c.broadcastToNodes(&proto.SubscribeJobRequest{
		JobType:  new(string(jobType)),
		Type:     proto.SubscribeJobRequest_TYPE_SUBSCRIBE.Enum(),
		ClientId: new(string(clientID)),
	})
	c.clientMu.Unlock()
	if err != nil {
		c.reconcileNodeSubscriptions()
		return fmt.Errorf("failed to subscribe client %s to jobType %s: %w", clientID, jobType, err)
	}
	return nil
}

func (c *jobClient) completeJob(ctx context.Context, clientID ClientID, jobKey int64, variables map[string]any) error {
	partitionID := zenflake.GetPartitionId(jobKey)
	lClient, err := c.nodeClientManager.PartitionLeader(partitionID)
	if err != nil {
		return fmt.Errorf("failed to retrieve client for partition %d leader: %w", partitionID, err)
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables for job completion: %w", err)
	}
	_, err = lClient.CompleteJob(ctx, &proto.CompleteJobRequest{
		Key:       new(jobKey),
		Variables: vars,
		ClientId:  new(string(clientID)),
	})
	if err != nil {
		return fmt.Errorf("failed to complete job %d from client: %w", jobKey, err)
	}
	return nil
}

func (c *jobClient) failJob(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables map[string]interface{}) error {
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
		Key:       &jobKey,
		Message:   &message,
		ErrorCode: errorCode,
		Variables: vars,
	})
	if err != nil {
		return fmt.Errorf("failed to fail job %d from client: %w", jobKey, err)
	}
	return nil
}

func (c *jobClient) removeJobSub(ctx context.Context, clientID ClientID, jobType JobType) error {
	c.clientMu.Lock()
	sub, ok := c.clientSubs[clientID]
	if !ok {
		c.clientMu.Unlock()
		return fmt.Errorf("client %s is not registered", clientID)
	}
	delete(sub.jobTypes, jobType)
	err := c.broadcastToNodes(&proto.SubscribeJobRequest{
		JobType:  new(string(jobType)),
		Type:     proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE.Enum(),
		ClientId: new(string(clientID)),
	})
	c.clientMu.Unlock()
	if err != nil {
		c.reconcileNodeSubscriptions()
		return fmt.Errorf("failed to unsubscribe client %s from jobType %s: %w", clientID, jobType, err)
	}
	return nil
}
