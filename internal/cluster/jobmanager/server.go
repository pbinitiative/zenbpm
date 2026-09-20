package jobmanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/config"
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
	// counter that puts job loader to sleep for 1 second
	emptyDistributionCounterSleep int = 100
)

var (
	// ErrLockNotHeld is returned by a lock extension for a job which is not
	// distributed at the moment: its lock lapsed, it was completed or failed,
	// or it was never delivered by this leader.
	ErrLockNotHeld = errors.New("job lock is not held")
	// ErrLockHeldByOtherClient is returned by a lock extension for a job which
	// is currently locked for a different client.
	ErrLockHeldByOtherClient = errors.New("job lock is held by another client")
)

type JobLoader interface {
	// LoadJobsToDistribute loads at most count jobs, sorted from oldest, across all partitions led by the node.
	LoadJobsToDistribute(jobTypes []string, idsToSkip []int64, count int64) ([]sql.Job, error)
}

type JobCompleter interface {
	JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]any) error
	JobFailByKey(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]any) error
}

// LockLimits are the engine-wide defaults and caps applied to what a client
// asks for in a subscription (see config.JobManager).
type LockLimits struct {
	DefaultLockDuration  time.Duration
	MaxLockDuration      time.Duration
	DefaultMaxActiveJobs int
	MaxActiveJobsCap     int
}

// DefaultLockLimits are the limits used when none are configured: 30 seconds
// per delivery, at most 24 hours, ten jobs per client and job type, at most a thousand.
func DefaultLockLimits() LockLimits {
	return LockLimits{
		DefaultLockDuration:  30 * time.Second,
		MaxLockDuration:      24 * time.Hour,
		DefaultMaxActiveJobs: 10,
		MaxActiveJobsCap:     1000,
	}
}

// SubscriptionSettings is what a client asks for when it subscribes to a job
// type. A zero value means "the engine's default"; a value above the engine's
// cap is lowered to the cap.
type SubscriptionSettings struct {
	LockDuration  time.Duration
	MaxActiveJobs int
}

// DurationFromMillis converts a millisecond count taken from the wire or the
// configuration into a duration without wrapping: a count above what a
// duration can hold saturates at the maximum (which the caps then lower), a
// count of zero or less becomes zero, the request for the engine default.
func DurationFromMillis(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	if ms > config.MaxLockDurationMillis {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ms) * time.Millisecond
}

// distributedJob is a job delivered to a client whose lock has not lapsed.
type distributedJob struct {
	client  ClientID
	jobKey  int64
	jobType JobType
	// lockUntil is when the delivery stops reserving the job for the client.
	lockUntil time.Time
	// lockDuration is the subscription's lock duration at delivery time; an
	// extension which names no duration uses it.
	lockDuration time.Duration
}

// clientAndType is the granularity at which active jobs are capped.
type clientAndType struct {
	client  ClientID
	jobType JobType
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
	// settings holds the effective (defaults applied, caps enforced) settings
	// of every subscription, keyed like subscriptions.
	settings map[JobType]map[ClientID]SubscriptionSettings
	// settingsVersion counts subscription changes, so a distribution round
	// can tell that the capacity it snapshotted is stale.
	settingsVersion uint64
	jobTypes        map[JobType]jobTypeData

	loader    JobLoader
	completer JobCompleter
	limits    LockLimits

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
	limits LockLimits,
) *jobServer {
	return &jobServer{
		nodeMu:            &sync.RWMutex{},
		nodeSubs:          map[NodeId]*nodeSub{},
		nodeID:            nodeID,
		distributedJobs:   []distributedJob{},
		distributedJobsMu: &sync.Mutex{},
		subscriptions:     map[JobType]map[ClientID]*nodeSub{},
		settings:          map[JobType]map[ClientID]SubscriptionSettings{},
		jobTypes:          map[JobType]jobTypeData{},
		clientMu:          &sync.RWMutex{},
		logger:            hclog.Default().Named("job-manager-server"),
		loader:            jobLoader,
		maxJobLoadCount:   300,
		completer:         jobCompleter,
		limits:            limits,
	}
}

// effectiveSettings applies the engine defaults to zero values and lowers
// values above the caps, so that every stored subscription is usable as is.
func (s *jobServer) effectiveSettings(requested SubscriptionSettings) SubscriptionSettings {
	effective := requested
	if effective.LockDuration <= 0 {
		effective.LockDuration = s.limits.DefaultLockDuration
	}
	effective.LockDuration = min(effective.LockDuration, s.limits.MaxLockDuration)
	if effective.MaxActiveJobs <= 0 {
		effective.MaxActiveJobs = s.limits.DefaultMaxActiveJobs
	}
	effective.MaxActiveJobs = min(effective.MaxActiveJobs, s.limits.MaxActiveJobsCap)
	return effective
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
		capacity, jobTypeClients, currentKeys := s.capacityLocked(time.Now())
		settingsVersion := s.settingsVersion
		s.clientMu.RUnlock()

		jobTypes := make([]string, 0, len(jobTypeClients))
		for jobType, typeClients := range jobTypeClients {
			for _, clientID := range typeClients {
				if capacity[clientAndType{client: clientID, jobType: jobType}] > 0 {
					jobTypes = append(jobTypes, string(jobType))
					break
				}
			}
		}
		sort.Strings(jobTypes)

		// the free slots are summed only up to the batch size, so the sum
		// can neither overflow nor exceed what one round loads
		jobsToLoad := int64(0)
		for _, numberOfSlots := range capacity {
			if numberOfSlots > 0 {
				jobsToLoad = min(jobsToLoad+int64(numberOfSlots), s.maxJobLoadCount)
			}
			if jobsToLoad >= s.maxJobLoadCount {
				break
			}
		}
		if jobsToLoad <= 0 {
			s.pause(20 * time.Millisecond)
			continue
		}
		jobs, err := s.loader.LoadJobsToDistribute(jobTypes, currentKeys, jobsToLoad)
		if err != nil {
			s.logger.Error("Failed to load new batch of jobs to distribute", "err", err)
			// give it some time not to overwhelm the node we might not be a leader anymore
			s.pause(1 * time.Second)
			continue
		}
		if len(jobs) == 0 {
			// wait for something to happen
			s.emptyDistributionCounter++
			if s.emptyDistributionCounter >= emptyDistributionCounterSleep {
				s.pause(1 * time.Second)
			} else {
				s.pause(100*time.Millisecond + time.Duration(s.emptyDistributionCounter)*time.Millisecond)
			}
			continue
		}
		s.emptyDistributionCounter = 0
		assignedJobs := 0
		for _, job := range jobs {
			s.clientMu.Lock()
			if s.settingsVersion != settingsVersion {
				// a subscription changed since the snapshot: a lowered cap
				// must bind the deliveries of this batch, not the next one
				capacity, _, _ = s.capacityLocked(time.Now())
				settingsVersion = s.settingsVersion
			}
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
				slot := clientAndType{client: candidateID, jobType: jType}
				if capacity[slot] <= 0 {
					continue
				}
				candidateStream, ok := s.subscriptions[jType][candidateID]
				if !ok {
					continue
				}
				jobTypeData.index = idx
				clientID = candidateID
				nodeStream = candidateStream
				capacity[slot]--
				break
			}
			if nodeStream == nil {
				// every client for this job type is saturated, the job stays
				// in the database and will be picked up in a later round
				s.clientMu.Unlock()
				continue
			}
			s.jobTypes[jType] = jobTypeData // set the updated index
			lockDuration := s.settingsLocked(jType, clientID).LockDuration
			lockUntil := time.Now().Add(lockDuration)
			s.distributedJobsMu.Lock()
			s.distributedJobs = append(s.distributedJobs, distributedJob{
				client:       clientID,
				jobKey:       job.Key,
				jobType:      jType,
				lockUntil:    lockUntil,
				lockDuration: lockDuration,
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
					LockUntil:      new(lockUntil.UnixMilli()),
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
			s.pause(100 * time.Millisecond)
		}
	}
}

// settingsLocked returns the effective settings of a subscription. The
// settings are kept in step with the round robin list under clientMu, so an
// entry is never missing; should a bookkeeping slip ever make one missing,
// the engine defaults apply rather than a zero lock duration, which would
// redeliver the job on every round. The caller holds clientMu.
func (s *jobServer) settingsLocked(jobType JobType, client ClientID) SubscriptionSettings {
	if settings, ok := s.settings[jobType][client]; ok {
		return settings
	}
	s.logger.Warn("subscription has no settings, applying the engine defaults", "jobType", jobType, "client", client)
	return s.effectiveSettings(SubscriptionSettings{})
}

// capacityLocked drops every lapsed lock and returns how many more jobs each
// (client, job type) may be sent, the clients of every job type, and the keys
// of the jobs still locked. A job type never eats into another type's slots.
// The caller holds clientMu.
func (s *jobServer) capacityLocked(now time.Time) (map[clientAndType]int, map[JobType][]ClientID, []int64) {
	capacity := make(map[clientAndType]int)
	jobTypeClients := make(map[JobType][]ClientID, len(s.jobTypes))
	for jobType, jobTypeData := range s.jobTypes {
		jobTypeClients[jobType] = slices.Clone(jobTypeData.clients)
		for _, client := range jobTypeData.clients {
			capacity[clientAndType{client: client, jobType: jobType}] = s.settingsLocked(jobType, client).MaxActiveJobs
		}
	}
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	currentKeys := make([]int64, 0, len(s.distributedJobs))
	for i := len(s.distributedJobs) - 1; i >= 0; i-- {
		job := s.distributedJobs[i]
		if job.lockUntil.Before(now) {
			s.distributedJobs = append(s.distributedJobs[:i], s.distributedJobs[i+1:]...)
			continue
		}
		// only track capacity for clients that are still subscribed,
		// jobs of already removed clients must not create phantom entries
		slot := clientAndType{client: job.client, jobType: job.jobType}
		if _, ok := capacity[slot]; ok {
			capacity[slot]--
		}
		currentKeys = append(currentKeys, job.jobKey)
	}
	return capacity, jobTypeClients, currentKeys
}

// pause delays the distribution loop for d, returning early when the server
// context ends so that shutdown never waits for a back-off to elapse.
func (s *jobServer) pause(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-s.ctx.Done():
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
	// A stream arriving once the server context ended must be refused rather
	// than registered: the distribution loop is gone (or about to close every
	// registered stream under this same lock), so the stream would never see
	// a job nor the close message and the client would keep it as its healthy
	// stream to the partition leader for good.
	if s.ctx == nil || s.ctx.Err() != nil {
		s.nodeMu.Unlock()
		return fmt.Errorf("job server of node %s is not distributing jobs: %w", s.nodeID, NodeIsNotALeader)
	}
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
			s.subscribeClient(stream.nodeID, ClientID(req.GetClientId()), JobType(req.GetJobType()), SubscriptionSettings{
				LockDuration:  DurationFromMillis(req.GetLockDurationMs()),
				MaxActiveJobs: int(req.GetMaxActiveJobs()),
			})
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
			delete(s.settings[jobType], clientID)
			s.settingsVersion++
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

// subscribeClient registers the client for the job type with the requested
// settings; defaults and caps are applied here, once. A resubscription of the
// same client and type replaces the settings, jobs already delivered keep the
// deadline they were delivered with.
func (s *jobServer) subscribeClient(clientsNodeID NodeId, clientID ClientID, jType JobType, requested SubscriptionSettings) {
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
		s.settings[jType] = map[ClientID]SubscriptionSettings{}
	}
	if _, ok := s.jobTypes[jType]; !ok {
		s.jobTypes[jType] = jobTypeData{
			index:   0,
			clients: make([]ClientID, 0, 10),
		}
	}
	s.settings[jType][clientID] = s.effectiveSettings(requested)
	s.settingsVersion++
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
	delete(s.settings[jType], clientID)
	s.settingsVersion++
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
		delete(s.settings[jobType], clientID)
	}
	s.settingsVersion++
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

// extendLock moves the lock deadline of a distributed job to now plus
// duration. Zero duration means the lock duration of the subscription the job
// was delivered under; a duration above the engine cap is lowered to the cap.
// The deadline is relative to now rather than to the previous deadline, so a
// client renewing periodically keeps a stable lead instead of accumulating one.
// A lock whose deadline passed is gone even if no distribution round has
// dropped the entry yet: the published deadline decides, not the cleanup.
func (s *jobServer) extendLock(clientID ClientID, jobKey int64, duration time.Duration) (time.Time, error) {
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	index := slices.IndexFunc(s.distributedJobs, func(job distributedJob) bool {
		return job.jobKey == jobKey
	})
	if index < 0 {
		return time.Time{}, ErrLockNotHeld
	}
	now := time.Now()
	job := &s.distributedJobs[index]
	if job.lockUntil.Before(now) {
		s.distributedJobs = slices.Delete(s.distributedJobs, index, index+1)
		return time.Time{}, ErrLockNotHeld
	}
	if job.client != clientID {
		return time.Time{}, ErrLockHeldByOtherClient
	}
	if duration <= 0 {
		duration = job.lockDuration
	}
	duration = min(duration, s.limits.MaxLockDuration)
	job.lockUntil = now.Add(duration)
	return job.lockUntil, nil
}

func (s *jobServer) completeJob(ctx context.Context, clientID ClientID, jobKey int64, variables map[string]any) error {
	err := s.completer.JobCompleteByKey(ctx, jobKey, variables)
	if err != nil {
		return fmt.Errorf("failed to complete job %d: %w", jobKey, err)
	}
	s.releaseLock(clientID, jobKey, "completed")
	return nil
}

func (s *jobServer) failJob(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables map[string]interface{}) error {
	err := s.completer.JobFailByKey(ctx, jobKey, message, errorCode, variables)
	if err != nil {
		return fmt.Errorf("failed to fail job %d: %w", jobKey, err)
	}
	s.releaseLock(clientID, jobKey, "failed")
	return nil
}

// releaseLock drops the distributed entry of a job the engine no longer waits
// for. Completion is deliberately not bound to the lock holder: a REST client
// which never held the lock may complete a job. The mismatch is logged so that
// a future ownership check has data to look at.
func (s *jobServer) releaseLock(clientID ClientID, jobKey int64, outcome string) {
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	for i, job := range s.distributedJobs {
		if job.jobKey != jobKey {
			continue
		}
		if job.client != clientID {
			s.logger.Debug("job "+outcome+" by a client other than the lock holder",
				"jobKey", jobKey, "lockHolder", job.client, "client", clientID)
		}
		s.distributedJobs = append(s.distributedJobs[:i], s.distributedJobs[i+1:]...)
		return
	}
}

func (s *jobServer) onJobRejected(_ context.Context, _ int64) {
	// TODO: unlock the job and assign to new node, if there is no new node we need to remove the type from currently needed jobTypes
}
