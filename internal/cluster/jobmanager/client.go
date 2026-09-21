package jobmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

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
	// jobTypes the client is subscribed to with the settings it asked for,
	// kept so the subscriptions can be replayed to node streams that are opened later.
	jobTypes map[JobType]SubscriptionSettings
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
	clientSubs map[ClientID]*clientSub
	clientMu   *sync.RWMutex

	store             Store
	nodeID            NodeId
	nodeClientManager *client.ClientManager
	nodeStreams       []*clientNodeStream
	nodeMu            *sync.RWMutex
	// subscribeMu serializes node stream reconciliation
	subscribeMu sync.Mutex
	// reconcileCh coalesces requests to restore streams without blocking job
	// distribution or public gRPC request handling.
	reconcileCh chan struct{}
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
func (c *jobClient) updateNodeSubs() bool {
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

	allSubscribed := true
	for _, partition := range partitionsToSubscribe {
		if !c.subscribeNodeToPartition(partition) {
			allSubscribed = false
		}
	}
	return allSubscribed
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
		reconcileCh:       make(chan struct{}, 1),
		jobsChan:          make(chan Job),
		logger:            hclog.Default().Named("job-manager-client"),
		ctx:               ctx,
	}
}

// subscribeNode subscribes current node to all partition leaders
func (c *jobClient) subscribeNode() {
	if !c.updateNodeSubs() {
		c.reconcileNodeSubscriptions()
	}
}

// subscribeNodeToPartition opens a job stream to the leader of the partition.
// The stream is bound to the job client context (not to the context of the
// cluster state notification that triggered the reconciliation), because the
// notification context is cancelled as soon as the notification is handled.
func (c *jobClient) subscribeNodeToPartition(partition uint32) bool {
	lClient, nodeID, err := c.nodeClientManager.PartitionLeaderWithID(partition)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to create client for partition %d leader", partition), "err", err)
		return false
	}
	md := metadata.New(map[string]string{
		MetadataNodeID: string(c.nodeID),
	})
	streamCtx := metadata.NewOutgoingContext(c.ctx, md)
	stream, err := lClient.SubscribeJob(streamCtx)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to open stream for partition %d leader", partition), "err", err)
		return false
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
	if !c.resendClientSubscriptions(&nodeStream) {
		c.removeNodeStream(&nodeStream)
		c.clientMu.RUnlock()
		if err := nodeStream.closeSend(); err != nil {
			c.logger.Error("Failed to close stream after subscription replay failure", "nodeID", nodeStream.nodeID, "err", err)
		}
		return false
	}
	c.clientMu.RUnlock()
	safego.Go("jobclient-stream-recv", c.logger, func() {
		c.handleJobStreamRecv(&nodeStream)
	})
	return true
}

// resendClientSubscriptions replays the job subscriptions of all locally
// registered clients to a newly opened node stream.
// The caller must hold clientMu.
func (c *jobClient) resendClientSubscriptions(stream *clientNodeStream) bool {
	requests := make([]*proto.SubscribeJobRequest, 0, len(c.clientSubs))
	for clientID, sub := range c.clientSubs {
		for jobType, settings := range sub.jobTypes {
			requests = append(requests, subscribeRequest(clientID, jobType, settings))
		}
	}
	for _, req := range requests {
		if err := stream.send(req); err != nil {
			c.logger.Error("Failed to resend client job subscription", "nodeID", stream.nodeID, "err", err)
			return false
		}
	}
	return true
}

func (c *jobClient) handleJobStreamRecv(stream *clientNodeStream) {
	for {
		resp, err := stream.stream.Recv()
		if err == io.EOF || errors.Is(err, context.Canceled) {
			// read done.
			c.logger.Debug("Stream closed", "err", err)
			c.removeNodeStream(stream)
			c.reconcileNodeSubscriptions()
			return
		}
		if err != nil {
			c.logger.Error("Failed to receive a job", "err", err, "streamNodeId", stream.nodeID)
			c.removeNodeStream(stream)
			c.reconcileNodeSubscriptions()
			return
		}
		if resp.Job == nil {
			c.logger.Error("closing stream", "err", err, "streamNodeId", stream.nodeID)
			c.removeNodeStream(stream)
			c.reconcileNodeSubscriptions()
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
			ElementType:    resp.Job.GetElementType(),
			ClientID:       ClientID(resp.GetClientId()),
			LockUntil:      resp.Job.GetLockUntil(),
		}
	}
}

func (c *jobClient) removeNodeStream(closing *clientNodeStream) {
	c.nodeMu.Lock()
	defer c.nodeMu.Unlock()
	c.nodeStreams = slices.DeleteFunc(c.nodeStreams, func(stream *clientNodeStream) bool {
		return stream == closing
	})
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
	c.clientMu.RUnlock()
	if pickedClient == nil {
		// TODO send msg to server to free the job
		return
	}
	if pickedClient.ctx.Err() != nil {
		safego.Go("jobclient-remove-disconnected", c.logger, func() {
			c.removeClient(pickedClient.ctx, pickedClient.clientID)
		})
		return
	}
	select {
	case pickedClient.ch <- job:
	case <-pickedClient.ctx.Done():
		safego.Go("jobclient-remove-disconnected", c.logger, func() {
			c.removeClient(pickedClient.ctx, pickedClient.clientID)
		})
	case <-c.ctx.Done():
	}
}

func (c *jobClient) startClient() {
	safego.Go("jobclient-reconcile", c.logger, c.reconcileNodeSubscriptionsLoop)
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
	if c.nodeClientManager == nil {
		return
	}
	select {
	case c.reconcileCh <- struct{}{}:
	default:
	}
}

func (c *jobClient) reconcileNodeSubscriptionsLoop() {
	const retryDelay = time.Second
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.reconcileCh:
		}

		for !c.updateNodeSubs() {
			timer := time.NewTimer(retryDelay)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-c.reconcileCh:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
			}
		}
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
		jobTypes: map[JobType]SubscriptionSettings{},
	}
	return nil
}

func (c *jobClient) removeClient(_ context.Context, clientID ClientID) {
	var err error
	removed := false
	func() {
		c.clientMu.Lock()
		defer c.clientMu.Unlock()
		if _, found := c.clientSubs[clientID]; !found {
			return
		}
		delete(c.clientSubs, clientID)
		removed = true
		err = c.broadcastToNodes(&proto.SubscribeJobRequest{
			Type:     proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE_ALL.Enum(),
			ClientId: new(string(clientID)),
		})
	}()
	if !removed {
		return
	}
	if err != nil {
		c.logger.Error("failed to remove client from nodes", "clientID", clientID, "err", err)
		c.reconcileNodeSubscriptions()
	}
}

func (c *jobClient) addJobSub(_ context.Context, clientID ClientID, jobType JobType, settings SubscriptionSettings) error {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	sub, ok := c.clientSubs[clientID]
	if !ok {
		return fmt.Errorf("client %s is not registered", clientID)
	}
	sub.jobTypes[jobType] = settings
	err := c.broadcastToNodes(subscribeRequest(clientID, jobType, settings))
	if err != nil {
		c.logger.Error("failed to broadcast client job subscription; desired state will be replayed", "clientID", clientID, "jobType", jobType, "err", err)
		c.reconcileNodeSubscriptions()
	}
	return nil
}

// subscribeRequest is the subscription as relayed to a partition leader; the
// settings travel verbatim, defaults and caps are the leader's business.
func subscribeRequest(clientID ClientID, jobType JobType, settings SubscriptionSettings) *proto.SubscribeJobRequest {
	return &proto.SubscribeJobRequest{
		JobType:        new(string(jobType)),
		Type:           proto.SubscribeJobRequest_TYPE_SUBSCRIBE.Enum(),
		ClientId:       new(string(clientID)),
		LockDurationMs: new(settings.LockDuration.Milliseconds()),
		MaxActiveJobs:  new(activeJobsForWire(settings.MaxActiveJobs)),
	}
}

// activeJobsForWire narrows an active-job count to the int32 the stream
// carries without wrapping: a count above what the wire can hold saturates,
// which the leader's cap then lowers, and a count below zero becomes zero,
// the request for the engine default.
func activeJobsForWire(count int) int32 {
	if count > math.MaxInt32 {
		return math.MaxInt32
	}
	if count < 0 {
		return 0
	}
	return int32(count)
}

// extendLock asks the leader of the job's partition to move the lock deadline
// of the job to now plus duration. A refusal comes back as ErrLockNotHeld or
// ErrLockHeldByOtherClient so the caller can report it by code.
func (c *jobClient) extendLock(ctx context.Context, clientID ClientID, jobKey int64, duration time.Duration) (time.Time, error) {
	partitionID := zenflake.GetPartitionId(jobKey)
	lClient, err := c.nodeClientManager.PartitionLeader(partitionID)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to retrieve client for partition %d leader: %w", partitionID, err)
	}
	resp, err := lClient.ExtendJobLock(ctx, &proto.ExtendJobLockRequest{
		Key:            new(jobKey),
		ClientId:       new(string(clientID)),
		LockDurationMs: new(duration.Milliseconds()),
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to extend lock of job %d from client: %w", jobKey, err)
	}
	switch resp.GetRefusal() {
	case proto.LockRefusal_LOCK_REFUSAL_NOT_HELD:
		return time.Time{}, ErrLockNotHeld
	case proto.LockRefusal_LOCK_REFUSAL_HELD_BY_OTHER_CLIENT:
		return time.Time{}, ErrLockHeldByOtherClient
	}
	if resp.Error != nil {
		return time.Time{}, fmt.Errorf("failed to extend lock of job %d: %s", jobKey, resp.Error.GetMessage())
	}
	return time.UnixMilli(resp.GetLockUntil()), nil
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
		ClientId:  new(string(clientID)),
	})
	if err != nil {
		return fmt.Errorf("failed to fail job %d from client: %w", jobKey, err)
	}
	return nil
}

func (c *jobClient) removeJobSub(_ context.Context, clientID ClientID, jobType JobType) error {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	sub, ok := c.clientSubs[clientID]
	if !ok {
		return fmt.Errorf("client %s is not registered", clientID)
	}
	delete(sub.jobTypes, jobType)
	err := c.broadcastToNodes(&proto.SubscribeJobRequest{
		JobType:  new(string(jobType)),
		Type:     proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE.Enum(),
		ClientId: new(string(clientID)),
	})
	if err != nil {
		c.logger.Error("failed to broadcast client job unsubscription; desired state will be replayed", "clientID", clientID, "jobType", jobType, "err", err)
		c.reconcileNodeSubscriptions()
	}
	return nil
}
