package jobmanager

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// lockJobs enters the jobs into the lock table of a server which does not
// run, or whose lock table the caller holds.
func lockJobs(server *jobServer, jobs ...distributedJob) {
	for _, job := range jobs {
		locked := job
		server.distributedJobs[job.jobKey] = &locked
	}
}

// theLockedJob is the single entry of the lock table; the test fails when
// there is none or more than one. The caller holds the lock table of a
// running server.
func theLockedJob(t *testing.T, server *jobServer) *distributedJob {
	t.Helper()
	require.Len(t, server.distributedJobs, 1, "expected exactly one locked job")
	for _, job := range server.distributedJobs {
		return job
	}
	panic("unreachable")
}

// lockedJobs are copies of every entry of the lock table, taken under its lock.
func lockedJobs(server *jobServer) []distributedJob {
	server.distributedJobsMu.Lock()
	defer server.distributedJobsMu.Unlock()
	jobs := make([]distributedJob, 0, len(server.distributedJobs))
	for _, job := range server.distributedJobs {
		jobs = append(jobs, *job)
	}
	return jobs
}
