package jobmanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
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
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/storage"
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
	// ErrLeaderUnavailable is returned by a lock extension or a job failure
	// which could not reach the leader of the job's partition, or reached a
	// node which does not lead it any more: a transient condition of the
	// cluster, the request may be retried in a moment.
	ErrLeaderUnavailable = errors.New("leader of the job's partition is unavailable")
	// ErrInvalidJobRequest is returned by a job request the leader refused for
	// what it asks, such as negative retries; repeating it gets the same answer.
	ErrInvalidJobRequest = errors.New("invalid job request")
	// ErrJobNotFound is returned by a job request for a key which names no job.
	ErrJobNotFound = errors.New("job not found")
	// ErrJobInTerminalState is returned by a job request for a job which no
	// longer waits for a worker: it was completed, terminated or already failed,
	// or, for a failure naming its delivery, it was handed out again after it.
	ErrJobInTerminalState = errors.New("job no longer waits for a worker")
)

// InvalidJobRequestError is ErrInvalidJobRequest together with the reason the
// leader gave, which names the field and the value and is what the worker is
// told.
type InvalidJobRequestError struct {
	Reason string
}

func (e *InvalidJobRequestError) Error() string {
	return ErrInvalidJobRequest.Error() + ": " + e.Reason
}

func (e *InvalidJobRequestError) Unwrap() error {
	return ErrInvalidJobRequest
}

type JobLoader interface {
	// LoadJobsToDistribute loads at most count jobs, sorted from oldest, across all partitions led by the node.
	LoadJobsToDistribute(jobTypes []string, idsToSkip []int64, count int64) ([]sql.Job, error)
	// RecordDeliveries persists a delivery of every loaded job, one write per
	// partition, and returns the jobs whose delivery it recorded, each carrying
	// the token of its delivery. A job which no longer waits for a worker, or
	// for which another delivery was recorded since it was loaded, is left
	// out, and so are the jobs of a partition whose write failed, which the
	// error names.
	RecordDeliveries(jobs []sql.Job) ([]sql.Job, error)
	// WithdrawDelivery takes back the token of a delivery which was recorded
	// but never sent, so that the delivery before it counts again. A delivery
	// recorded since is left alone.
	WithdrawDelivery(ctx context.Context, jobKey int64, deliveryToken int64) error
}

type JobCompleter interface {
	JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]any) error
	JobFailByKey(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]any, retries *int32, retryBackoff *time.Duration, deliveryToken *int64) error
	JobUpdateRetriesByKey(ctx context.Context, jobKey int64, retries int32, retryAt *time.Time) error
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

// RetryBackoffFromMillis converts the optional backoff of a fail request taken
// from the wire. Absent stays absent, the request for the task definition's
// policy; a negative count stays negative, so that the engine refuses it
// instead of taking it for "at once".
func RetryBackoffFromMillis(ms *int64) *time.Duration {
	if ms == nil {
		return nil
	}
	if *ms < 0 {
		return new(time.Duration(max(*ms, minDurationMillis)) * time.Millisecond)
	}
	return new(DurationFromMillis(*ms))
}

// minDurationMillis is the most negative count of milliseconds a time.Duration
// holds; a count below it is clamped so that the conversion cannot overflow.
const minDurationMillis = math.MinInt64 / int64(time.Millisecond)

// RetryBackoffToMillis puts the optional backoff of a fail request on the wire.
// Milliseconds are truncated towards zero, which would turn a backoff just
// below zero into "at once"; a negative backoff stays negative instead, so
// that the engine refuses it as it refuses every other negative one.
func RetryBackoffToMillis(backoff *time.Duration) *int64 {
	if backoff == nil {
		return nil
	}
	ms := backoff.Milliseconds()
	if *backoff < 0 && ms == 0 {
		ms = -1
	}
	return &ms
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
	// deliveryToken is the persisted token of the delivery, which the worker
	// names in the failure it reports; 0 while the delivery is being recorded.
	// A failure naming another token belongs to another delivery and leaves
	// this lock alone.
	deliveryToken int64
}

// jobMutations tells the distribution loop which jobs of a loaded batch were
// changed after the batch was read. A batch is a snapshot of the database: a
// failure which commits a backoff, or a retry update which postpones a job,
// between the query and the delivery would otherwise be overtaken by a
// delivery of the stale snapshot. Every change runs between beginMutation and
// endMutation, and every event advances seq; a job counts as changed when a
// change of it is in progress or its last event came after the batch was read.
type jobMutations struct {
	seq       uint64
	inFlight  map[int64]int
	changedAt map[int64]uint64
}

func newJobMutations() jobMutations {
	return jobMutations{inFlight: map[int64]int{}, changedAt: map[int64]uint64{}}
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

	maxJobLoadCount int64
	// maxQueryParameters is how many parameters the query loading a batch may
	// carry: one per locked job key, one per job type and one for the limit.
	// It is sql.MaxQueryParameters; tests lower it.
	maxQueryParameters int
	// distributedJobs are the jobs delivered and still locked, by job key, so
	// that a renewal, a completion or a failure finds its entry without a
	// scan of every lock the leader holds.
	distributedJobs   map[int64]*distributedJob
	distributedJobsMu *sync.Mutex
	// handingOut are the deliveries reserved and not yet sent, by job key,
	// guarded by distributedJobsMu, see handOut and beginMutation.
	handingOut map[int64]*handOut
	// mutations tracks the jobs a completion, failure or retry update is
	// changing, guarded by distributedJobsMu, see beginMutation.
	mutations                jobMutations
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
		nodeMu:             &sync.RWMutex{},
		nodeSubs:           map[NodeId]*nodeSub{},
		nodeID:             nodeID,
		distributedJobs:    map[int64]*distributedJob{},
		distributedJobsMu:  &sync.Mutex{},
		mutations:          newJobMutations(),
		handingOut:         map[int64]*handOut{},
		subscriptions:      map[JobType]map[ClientID]*nodeSub{},
		settings:           map[JobType]map[ClientID]SubscriptionSettings{},
		jobTypes:           map[JobType]jobTypeData{},
		clientMu:           &sync.RWMutex{},
		logger:             hclog.Default().Named("job-manager-server"),
		loader:             jobLoader,
		maxJobLoadCount:    300,
		maxQueryParameters: sql.MaxQueryParameters,
		completer:          jobCompleter,
		limits:             limits,
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
		// no batch is being handed out between two rounds, so the changes
		// which ended by now concern nobody any more; a leader which loads
		// no batches, say one whose jobs are finished over REST, would
		// otherwise remember every job it ever changed
		s.forgetFinishedChanges()
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
		// the query loading a batch carries one parameter per requested job
		// type, one per locked key (a placeholder when nothing is locked), one
		// for the current time and one for the limit, and SQLite refuses a query with more parameters
		// than maxQueryParameters; so a round delivers no more jobs than the
		// query of the next round can still exclude, whatever the
		// subscriptions ask for
		lockBudget := s.maxQueryParameters - 2 - len(jobTypes) - max(1, len(currentKeys))
		if lockBudget <= 0 {
			s.logger.Warn("leader holds as many locked jobs as one query can exclude, waiting for locks to lapse or jobs to complete",
				"lockedJobs", len(currentKeys), "requestedJobTypes", len(jobTypes), "maxQueryParameters", s.maxQueryParameters)
			s.pause(1 * time.Second)
			continue
		}
		jobsToLoad = min(jobsToLoad, int64(lockBudget))
		loadedAt := s.startLoad()
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
		round := &distributionRound{capacity: capacity, settingsVersion: settingsVersion, loadedAt: loadedAt, recorded: make(chan struct{})}
		reserved := make([]reservedDelivery, 0, len(jobs))
		for _, job := range jobs {
			headers, err := sql.JobHeadersFromJSON(job.Headers)
			if err != nil {
				// SaveJobWith always stores headers as a JSON object, so a parse
				// failure means a corrupt value: log it and skip the job
				s.logger.Error("Failed to parse job headers", "jobType", job.Type, "key", job.Key, "err", err)
				continue
			}
			if delivery, ok := s.reserveDelivery(round, job); ok {
				delivery.headers = headers
				reserved = append(reserved, delivery)
			}
		}
		recorded, err := s.recordDeliveries(round, reserved)
		s.logRecordingError(err)
		assignedJobs := 0
		for _, delivery := range recorded {
			if s.sendRecordedDelivery(delivery) {
				assignedJobs++
			}
		}
		if err != nil && len(recorded) == 0 {
			// give it some time not to overwhelm the node we might not be a leader anymore
			s.pause(1 * time.Second)
		} else if assignedJobs == 0 {
			// every loaded job was skipped (saturated or unavailable clients),
			// back off to avoid a tight database-query loop until capacity changes
			s.pause(100 * time.Millisecond)
		}
	}
}

// distributionRound is what one round of the distribution loop knows while it
// hands out the batch it loaded: the free slots of every client and job type,
// the subscription changes they reflect, the position in the sequence of job
// changes the batch reflects (see startLoad), and the channel it closes once the
// delivery tokens of its reservations are written or failed to be.
type distributionRound struct {
	capacity        map[clientAndType]int
	settingsVersion uint64
	loadedAt        uint64
	recorded        chan struct{}
}

// reservedDelivery is a job reserved for a client and not yet sent to it. Its
// job carries the token of the delivery once the delivery is recorded.
type reservedDelivery struct {
	job          sql.Job
	headers      map[string]string
	handOut      *handOut
	client       ClientID
	stream       *nodeSub
	lockUntil    time.Time
	lockDuration time.Duration
}

// reserveDelivery picks the client a job of the round's batch goes to, round
// robin among the clients of its type with a free slot, and locks the job for
// it. It takes clientMu and distributedJobsMu and releases both before it
// returns, so that no lock is held while the job is sent. A job is not
// reserved when no client of its type has a free slot, or when it changed
// after the batch was read: a later round reloads it if it is still
// deliverable. A reserved job counts as being recorded until the round wrote
// the delivery tokens of its reservations, see recordDeliveries.
func (s *jobServer) reserveDelivery(round *distributionRound, job sql.Job) (reservedDelivery, bool) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	if s.settingsVersion != round.settingsVersion {
		// a subscription changed since the snapshot: a lowered cap
		// must bind the deliveries of this batch, not the next one
		round.capacity, _, _ = s.capacityLocked(time.Now())
		round.settingsVersion = s.settingsVersion
	}
	jType := JobType(job.Type)
	jobTypeData := s.jobTypes[jType]
	// round robin: starting from the client after the last used index,
	// pick the first client that still has remaining capacity
	numClients := len(jobTypeData.clients)
	delivery := reservedDelivery{job: job, handOut: &handOut{round: round, sent: make(chan struct{})}}
	for offset := 1; offset <= numClients; offset++ {
		idx := (jobTypeData.index + offset) % numClients
		candidateID := jobTypeData.clients[idx]
		if round.capacity[clientAndType{client: candidateID, jobType: jType}] <= 0 {
			continue
		}
		candidateStream, ok := s.subscriptions[jType][candidateID]
		if !ok {
			continue
		}
		jobTypeData.index = idx
		delivery.client = candidateID
		delivery.stream = candidateStream
		break
	}
	if delivery.stream == nil {
		// no client for this job type, or every one is saturated: the job
		// stays in the database and will be picked up in a later round
		return reservedDelivery{}, false
	}
	delivery.lockDuration = s.settingsLocked(jType, delivery.client).LockDuration
	// the deadline taken here reserves the job while it is being sent
	// and is what the worker is told; the leader's own deadline is
	// restarted once the send completed (see restartLockAfterSend)
	delivery.lockUntil = time.Now().Add(delivery.lockDuration)

	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	if s.changedSinceLocked(job.Key, round.loadedAt) {
		// the snapshot is stale, and the client's slot stays free
		return reservedDelivery{}, false
	}
	round.capacity[clientAndType{client: delivery.client, jobType: jType}]--
	s.jobTypes[jType] = jobTypeData // set the updated index
	s.distributedJobs[job.Key] = &distributedJob{
		client:       delivery.client,
		jobKey:       job.Key,
		jobType:      jType,
		lockUntil:    delivery.lockUntil,
		lockDuration: delivery.lockDuration,
	}
	s.handingOut[job.Key] = delivery.handOut
	return delivery, true
}

// handOutStage is how far a reserved delivery got on its way to the worker.
type handOutStage int

const (
	// deliveryBeingRecorded: its round writes the token; a change of the job
	// waits for the write
	deliveryBeingRecorded handOutStage = iota
	// deliveryRecorded: the token is written and the send has not begun; a
	// change of the job withdraws the delivery instead of waiting for it
	deliveryRecorded
	// deliveryBeingSent: a change of the job waits for the send
	deliveryBeingSent
)

// handOut follows a reserved delivery from its reservation until it is sent,
// dropped or withdrawn, so that a change of its job neither overtakes the write
// of its token nor finds a token which never reached a worker. A failure the
// engine checks against the job's token must see the token of a delivery which
// was sent: the failure of the delivery before it is then superseded for good
// reason. Guarded by distributedJobsMu.
type handOut struct {
	round   *distributionRound
	stage   handOutStage
	written int64
	// sent is closed once the delivery left this server, or will not
	sent chan struct{}
}

// endHandOutLocked forgets a delivery which was sent, dropped or withdrawn and
// lets the changes waiting for it go on. It does nothing for a delivery ended
// before, so that each is ended once. The caller holds distributedJobsMu.
func (s *jobServer) endHandOutLocked(key int64, delivery *handOut) {
	if s.handingOut[key] != delivery {
		return
	}
	delete(s.handingOut, key)
	close(delivery.sent)
}

// dropReservationLocked releases the lock a reservation took, unless the lock
// meanwhile belongs to another client. The caller holds distributedJobsMu.
func (s *jobServer) dropReservationLocked(delivery reservedDelivery) {
	if locked, ok := s.distributedJobs[delivery.job.Key]; ok && locked.client == delivery.client {
		delete(s.distributedJobs, delivery.job.Key)
	}
}

// recordDeliveries persists the token of every reserved delivery of the round,
// in one write per partition, and returns the deliveries it recorded, each job
// carrying its token. A reservation which was not recorded is dropped: its job
// changed since it was loaded, or the write failed, and a later round hands the
// job out if it still waits. The changes of the reserved jobs which waited for
// the write go on once it is done, even when the write panicked.
func (s *jobServer) recordDeliveries(round *distributionRound, reserved []reservedDelivery) (recorded []reservedDelivery, err error) {
	var tokens map[int64]int64
	defer func() {
		s.distributedJobsMu.Lock()
		defer s.distributedJobsMu.Unlock()
		recorded = make([]reservedDelivery, 0, len(tokens))
		for _, delivery := range reserved {
			key := delivery.job.Key
			token, ok := tokens[key]
			if !ok {
				s.dropReservationLocked(delivery)
				s.endHandOutLocked(key, delivery.handOut)
				continue
			}
			if locked, ok := s.distributedJobs[key]; ok && locked.client == delivery.client {
				locked.deliveryToken = token
			}
			delivery.handOut.stage = deliveryRecorded
			delivery.handOut.written = token
			delivery.job.DeliveryToken = token
			recorded = append(recorded, delivery)
		}
		close(round.recorded)
	}()
	if len(reserved) == 0 {
		return nil, nil
	}
	jobs := make([]sql.Job, len(reserved))
	for i, delivery := range reserved {
		jobs[i] = delivery.job
	}
	recordedJobs, err := s.loader.RecordDeliveries(jobs)
	tokens = make(map[int64]int64, len(recordedJobs))
	for _, job := range recordedJobs {
		tokens[job.Key] = job.DeliveryToken
	}
	return nil, err
}

// logRecordingError reports a write of delivery tokens which failed. A
// partition which is not led here any more is expected during a leader change
// and only warned about; the jobs of both are handed out by a later round.
func (s *jobServer) logRecordingError(err error) {
	switch {
	case err == nil:
	case errors.Is(err, NodeIsNotALeader):
		s.logger.Warn("Could not record the delivery of jobs of a partition this node no longer leads", "err", err)
	default:
		s.logger.Error("Failed to record the delivery of jobs, a later round hands them out", "err", err)
	}
}

// sendRecordedDelivery sends a recorded delivery unless a change of its job
// withdrew it meanwhile, see beginMutation, and reports whether it was sent.
// The changes of the job which arrive during the send wait for it.
func (s *jobServer) sendRecordedDelivery(delivery reservedDelivery) bool {
	key := delivery.job.Key
	s.distributedJobsMu.Lock()
	if s.handingOut[key] != delivery.handOut {
		s.distributedJobsMu.Unlock()
		return false
	}
	delivery.handOut.stage = deliveryBeingSent
	s.distributedJobsMu.Unlock()
	defer func() {
		s.distributedJobsMu.Lock()
		defer s.distributedJobsMu.Unlock()
		s.endHandOutLocked(key, delivery.handOut)
	}()
	return s.sendReservedJob(delivery)
}

// sendReservedJob sends a reserved job to its client, holding no lock while
// the stream may block, and reports whether it was sent. A job which could not
// be sent loses its reservation, so that a later round hands it out again.
func (s *jobServer) sendReservedJob(delivery reservedDelivery) bool {
	job := delivery.job
	// this might be bottleneck for now...in the future we might want
	// to have something that will allow us to send jobs to clients on
	// non blocked stream or use a pool of GRPC connections to handle jobs
	err := delivery.stream.stream.Send(&proto.SubscribeJobResponse{
		JobType:  &job.Type,
		ClientId: new(string(delivery.client)),
		Job: &proto.InternalJob{
			Key:            &job.Key,
			InstanceKey:    &job.ProcessInstanceKey,
			InputVariables: []byte(job.InputVariables),
			Type:           &job.Type,
			State:          &job.State,
			ElementId:      &job.ElementID,
			CreatedAt:      &job.CreatedAt,
			ElementType:    &job.ElementType,
			LockUntil:      new(delivery.lockUntil.UnixMilli()),
			Retries:        new(int32(job.Retries)), // #nosec G115 -- the engine writes this column from an int32 field
			Attempt:        new(attemptOfDelivery(job)),
			DeliveryToken:  &job.DeliveryToken,
			Headers:        delivery.headers,
		},
	})
	if err != nil {
		s.distributedJobsMu.Lock()
		if locked, ok := s.distributedJobs[job.Key]; ok && locked.client == delivery.client {
			delete(s.distributedJobs, job.Key)
		}
		s.distributedJobsMu.Unlock()
		s.logger.Error("Failed to send job to node", "jobType", job.Type, "key", job.Key, "err", err)
		return false
	}
	s.restartLockAfterSend(job.Key, delivery.client, delivery.lockDuration)
	JobsDistributed.Add(s.ctx, 1, metric.WithAttributes(
		attribute.String("type", job.Type),
		attribute.String("client", string(delivery.client)),
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
	return true
}

// attemptOfDelivery is the attempt a job is handed out for: the one after the
// attempts whose failure the job recorded.
func attemptOfDelivery(job sql.Job) int32 {
	return int32(job.Attempts) + 1 // #nosec G115 -- the engine writes this column from an int32 field
}

// restartLockAfterSend moves the deadline of a just delivered job to now plus
// its lock duration. A send blocked by a node whose workers stopped reading
// can take a good part of the lock duration; counting the lock from the end
// of the send keeps such a delay from handing the job to another client while
// the worker just started on it. The deadline is never moved backwards, so an
// extension which arrived meanwhile stands, and a worker which already
// completed the job finds no entry to move. The worker keeps the earlier
// deadline it was sent, which is conservative.
func (s *jobServer) restartLockAfterSend(jobKey int64, clientID ClientID, lockDuration time.Duration) {
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	locked, ok := s.distributedJobs[jobKey]
	if !ok || locked.client != clientID {
		return
	}
	sentUntil := time.Now().Add(lockDuration)
	if sentUntil.After(locked.lockUntil) {
		locked.lockUntil = sentUntil
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
	for key, job := range s.distributedJobs {
		if job.lockUntil.Before(now) {
			delete(s.distributedJobs, key)
			continue
		}
		// only track capacity for clients that are still subscribed,
		// jobs of already removed clients must not create phantom entries
		slot := clientAndType{client: job.client, jobType: job.jobType}
		if _, ok := capacity[slot]; ok {
			capacity[slot]--
		}
		currentKeys = append(currentKeys, key)
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
		maps.DeleteFunc(s.distributedJobs, func(_ int64, job *distributedJob) bool {
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
	if len(jobTypeData.clients) == 0 {
		// a job type nobody subscribes to any more must go, as when its
		// last client or node leaves; kept, it would count in every round
		delete(s.jobTypes, jType)
		return
	}
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
	maps.DeleteFunc(s.distributedJobs, func(_ int64, job *distributedJob) bool {
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
// dropped the entry yet: the published deadline decides, not the cleanup. An
// extension which names its delivery extends only the lock of that delivery:
// the worker still at work on a delivery whose lock lapsed learns that it lost
// the job, instead of keeping the next delivery to its client locked.
func (s *jobServer) extendLock(clientID ClientID, jobKey int64, duration time.Duration, deliveryToken *int64) (time.Time, error) {
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	job, ok := s.distributedJobs[jobKey]
	if !ok {
		return time.Time{}, ErrLockNotHeld
	}
	now := time.Now()
	if job.lockUntil.Before(now) {
		delete(s.distributedJobs, jobKey)
		return time.Time{}, ErrLockNotHeld
	}
	if job.client != clientID {
		return time.Time{}, ErrLockHeldByOtherClient
	}
	if deliveryToken != nil && (*deliveryToken < 1 || *deliveryToken != job.deliveryToken) {
		return time.Time{}, ErrLockNotHeld
	}
	if duration <= 0 {
		duration = job.lockDuration
	}
	duration = min(duration, s.limits.MaxLockDuration)
	job.lockUntil = now.Add(duration)
	return job.lockUntil, nil
}

// startLoad returns the position in the sequence of job changes a batch loaded
// from now on reflects, and forgets the changes which ended before it: the
// query sees what they committed.
func (s *jobServer) startLoad() uint64 {
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	s.forgetFinishedChangesLocked()
	return s.mutations.seq
}

// forgetFinishedChanges drops the record of every change which ended. It is
// for the moments at which no loaded batch waits to be handed out: nothing
// then needs to know about a change any more.
func (s *jobServer) forgetFinishedChanges() {
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	s.forgetFinishedChangesLocked()
}

// forgetFinishedChangesLocked is forgetFinishedChanges for a caller which
// holds distributedJobsMu.
func (s *jobServer) forgetFinishedChangesLocked() {
	for jobKey := range s.mutations.changedAt {
		if s.mutations.inFlight[jobKey] == 0 {
			delete(s.mutations.changedAt, jobKey)
		}
	}
}

// changedSinceLocked reports whether the job is being changed or was changed
// after a batch loaded at loadedAt read it. The caller holds distributedJobsMu.
func (s *jobServer) changedSinceLocked(jobKey int64, loadedAt uint64) bool {
	return s.mutations.inFlight[jobKey] > 0 || s.mutations.changedAt[jobKey] > loadedAt
}

// beginMutation marks the job as being changed until the returned function is
// called. A delivery reserved before stands, unless it is still to be sent; one
// not yet reserved is skipped. A failure the engine checks against the job's
// delivery token must see the token of a delivery which reached a worker, so a
// change waits while the token of a delivery of the job is being written or the
// delivery is being sent, and withdraws a delivery which is written but not yet
// sent: the job is not sent, and its token is taken back.
func (s *jobServer) beginMutation(ctx context.Context, jobKey int64) (endMutation func(), err error) {
	var withdrawn *handOut
	for {
		s.distributedJobsMu.Lock()
		delivery, handingOut := s.handingOut[jobKey]
		if !handingOut {
			break
		}
		if delivery.stage == deliveryRecorded {
			withdrawn = delivery
			if locked, ok := s.distributedJobs[jobKey]; ok && locked.deliveryToken == delivery.written {
				delete(s.distributedJobs, jobKey)
			}
			s.endHandOutLocked(jobKey, delivery)
			break
		}
		progress := delivery.sent
		if delivery.stage == deliveryBeingRecorded {
			progress = delivery.round.recorded
		}
		s.distributedJobsMu.Unlock()
		select {
		case <-progress:
		case <-ctx.Done():
			return nil, fmt.Errorf("job %d is being handed out: %w", jobKey, ctx.Err())
		}
	}
	s.mutations.seq++
	s.mutations.inFlight[jobKey]++
	s.mutations.changedAt[jobKey] = s.mutations.seq
	s.distributedJobsMu.Unlock()
	if withdrawn != nil {
		s.withdrawDelivery(ctx, jobKey, withdrawn.written)
	}
	return func() {
		s.distributedJobsMu.Lock()
		defer s.distributedJobsMu.Unlock()
		s.mutations.seq++
		s.mutations.changedAt[jobKey] = s.mutations.seq
		if s.mutations.inFlight[jobKey]--; s.mutations.inFlight[jobKey] <= 0 {
			delete(s.mutations.inFlight, jobKey)
		}
	}, nil
}

// withdrawDelivery takes back the token of a delivery a change withdrew before
// it was sent, see beginMutation. When that fails the job keeps the token, and
// a failure of the delivery before it is refused as superseded although the
// job reached nobody since; the job is handed out again all the same.
func (s *jobServer) withdrawDelivery(ctx context.Context, jobKey int64, deliveryToken int64) {
	if err := s.loader.WithdrawDelivery(ctx, jobKey, deliveryToken); err != nil {
		s.logger.Warn("Failed to take back the token of a delivery which was never sent",
			"jobKey", jobKey, "deliveryToken", deliveryToken, "err", err)
	}
}

func (s *jobServer) completeJob(ctx context.Context, clientID ClientID, jobKey int64, variables map[string]any) error {
	endMutation, err := s.beginMutation(ctx, jobKey)
	if err != nil {
		return fmt.Errorf("failed to complete job %d: %w", jobKey, err)
	}
	defer endMutation()
	err = s.completer.JobCompleteByKey(ctx, jobKey, variables)
	if err != nil {
		s.releaseLockOfEndedJob(jobKey, err)
		return fmt.Errorf("failed to complete job %d: %w", jobKey, err)
	}
	s.releaseLock(clientID, jobKey, "completed")
	return nil
}

// failJob fails the job through the engine. A job left active for another
// attempt is handed out again by the next round once its backoff has passed.
// A failure naming its delivery releases the lock of that delivery, whoever
// reports it, and leaves the lock of any other delivery alone, see
// releaseLockOfFailedDelivery.
func (s *jobServer) failJob(ctx context.Context, clientID ClientID, jobKey int64, message string, errorCode *string, variables map[string]interface{}, retries *int32, retryBackoff *time.Duration, deliveryToken *int64) error {
	endMutation, err := s.beginMutation(ctx, jobKey)
	if err != nil {
		return fmt.Errorf("failed to fail job %d: %w", jobKey, err)
	}
	defer endMutation()
	err = s.completer.JobFailByKey(ctx, jobKey, message, errorCode, variables, retries, retryBackoff, deliveryToken)
	if err != nil {
		s.releaseLockOfEndedJob(jobKey, err)
		return fmt.Errorf("failed to fail job %d: %w", jobKey, err)
	}
	s.releaseLockOfFailedDelivery(clientID, jobKey, deliveryToken)
	return nil
}

// updateJobRetries sets the retries of a job through the engine. It is a
// change like a failure: a batch loaded before must not deliver the job,
// which may have been moved into a backoff. A lock held on the job stands.
func (s *jobServer) updateJobRetries(ctx context.Context, jobKey int64, retries int32, retryAt *time.Time) error {
	endMutation, err := s.beginMutation(ctx, jobKey)
	if err != nil {
		return fmt.Errorf("failed to update retries of job %d: %w", jobKey, err)
	}
	defer endMutation()
	if err := s.completer.JobUpdateRetriesByKey(ctx, jobKey, retries, retryAt); err != nil {
		return fmt.Errorf("failed to update retries of job %d: %w", jobKey, err)
	}
	return nil
}

// releaseLock drops the distributed entry of a job the engine no longer waits
// for. Completion is deliberately not bound to the lock holder: a REST client
// which never held the lock may complete a job. The mismatch is logged so that
// a future ownership check has data to look at.
func (s *jobServer) releaseLock(clientID ClientID, jobKey int64, outcome string) {
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	job, ok := s.distributedJobs[jobKey]
	if !ok {
		return
	}
	if job.client != clientID {
		s.logger.Debug("job "+outcome+" by a client other than the lock holder",
			"jobKey", jobKey, "lockHolder", job.client, "client", clientID)
	}
	delete(s.distributedJobs, jobKey)
}

// releaseLockOfFailedDelivery drops the lock a failure the engine accepted
// ended. A failure naming its delivery ends that delivery, whoever reports it,
// and leaves the lock of any other delivery alone: the engine answered it as
// the repeat of a failure it recorded before, which says nothing about the
// delivery now running. A failure naming none releases the lock only when the
// reporting client holds it: another client's lock stands, and so does a lock
// on a job a REST client which named no client id failed, until the holder
// reports or the lock lapses.
func (s *jobServer) releaseLockOfFailedDelivery(clientID ClientID, jobKey int64, deliveryToken *int64) {
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	job, ok := s.distributedJobs[jobKey]
	if !ok {
		return
	}
	if deliveryToken != nil && job.deliveryToken != *deliveryToken {
		s.logger.Debug("failure of another delivery than the locked one, which keeps its lock",
			"jobKey", jobKey, "lockHolder", job.client, "client", clientID, "deliveryToken", *deliveryToken, "lockedDeliveryToken", job.deliveryToken)
		return
	}
	if deliveryToken == nil && job.client != clientID {
		s.logger.Debug("job failed by a client other than the lock holder, which keeps its lock",
			"jobKey", jobKey, "lockHolder", job.client, "client", clientID)
		return
	}
	delete(s.distributedJobs, jobKey)
}

// releaseLockOfEndedJob drops the distributed entry of a job the engine
// refused a completion or failure of because the job no longer waits for a
// worker or no longer exists: nobody works on it any more, and the entry
// would hold its client's slot until it lapsed.
func (s *jobServer) releaseLockOfEndedJob(jobKey int64, refusal error) {
	if !errors.Is(refusal, bpmn.ErrJobInTerminalState) && !errors.Is(refusal, storage.ErrNotFound) {
		return
	}
	s.distributedJobsMu.Lock()
	defer s.distributedJobsMu.Unlock()
	delete(s.distributedJobs, jobKey)
}

func (s *jobServer) onJobRejected(_ context.Context, _ int64) {
	// TODO: unlock the job and assign to new node, if there is no new node we need to remove the type from currently needed jobTypes
}
