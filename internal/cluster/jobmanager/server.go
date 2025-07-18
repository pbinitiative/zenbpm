package jobmanager

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type JobLoader interface {
	// LoadJobsToDistribute loads a number of jobs (sorted from oldest) from each partition that the node is leader on
	LoadJobsToDistribute(jobTypes []string, idsToSkip []int64, count int) ([]runtime.Job, error)
}

type JobCompleter interface {
	JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]any) error
}

type distributedJob struct {
	sentTime time.Time
	client   ClientId
	job      Job
}

type jobServer struct {
	jobTypes map[JobType][]*nodeSubs
	nodeSubs map[NodeId]*nodeSubs
	nodeId   NodeId
	mu       *sync.RWMutex

	loader    JobLoader
	completer JobCompleter

	jobLoadCount    int
	distributedJobs []distributedJob

	logger hclog.Logger
}

func newJobServer(
	ctx context.Context,
	nodeId NodeId,
	jobLoader JobLoader,
	jobCompleter JobCompleter,
) *jobServer {
	return &jobServer{
		jobTypes:        map[JobType][]*nodeSubs{},
		nodeSubs:        map[NodeId]*nodeSubs{},
		nodeId:          nodeId,
		distributedJobs: []distributedJob{},
		loader:          jobLoader,
		mu:              &sync.RWMutex{},
		logger:          hclog.Default().Named("job-manager-server"),
	}
}

func (s *jobServer) startServer() {
	go s.jobMonitor()
}

func (s *jobServer) jobMonitor() {
	for {
		s.mu.RLock()
		jobTypes := make([]string, 0, len(s.jobTypes))
		for jobType := range s.jobTypes {
			jobTypes = append(jobTypes, string(jobType))
		}
		currentKeys := make([]int64, len(s.distributedJobs))
		for i, job := range s.distributedJobs {
			currentKeys[i] = job.job.Key
		}
		jobs, err := s.loader.LoadJobsToDistribute(jobTypes, currentKeys, s.jobLoadCount)

	}
}

func (s *jobServer) completeJob(ctx context.Context, jobKey int64, variables map[string]any) error {
	return s.completer.JobCompleteByKey(ctx, jobKey, variables)
}

func (s *jobServer) onJobRejected(ctx context.Context, jobKey int64) {
	// TODO unlock the job and assign to new node, if there is no new node we need to remove the type from currently needed jobTypes
}
