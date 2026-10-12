package cluster

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadJobsWithGlobalLimitAcrossSources(t *testing.T) {
	sources := [][]sql.Job{
		{{Key: 1}, {Key: 2}},
		{{Key: 3}, {Key: 4}},
		{{Key: 5}, {Key: 6}},
	}
	requestedLimits := make([]int64, 0, len(sources))

	jobs, err := loadJobsWithGlobalLimit(len(sources), 3, func(index int, limit int64) ([]sql.Job, error) {
		requestedLimits = append(requestedLimits, limit)
		jobs := sources[index]
		if int64(len(jobs)) > limit {
			jobs = jobs[:limit]
		}
		return jobs, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []int64{3, 3, 3}, requestedLimits, "every partition must get an opportunity to contribute jobs")
	assert.Equal(t, []sql.Job{{Key: 1}, {Key: 2}, {Key: 3}}, jobs)
}

func TestLoadJobsWithGlobalLimitSelectsOldestJobsAcrossSources(t *testing.T) {
	sources := [][]sql.Job{
		{{Key: 1, CreatedAt: 10}, {Key: 2, CreatedAt: 30}},
		{{Key: 3, CreatedAt: 20}, {Key: 4, CreatedAt: 40}},
	}

	jobs, err := loadJobsWithGlobalLimit(len(sources), 2, func(index int, _ int64) ([]sql.Job, error) {
		return sources[index], nil
	})

	require.NoError(t, err)
	assert.Equal(t, []sql.Job{{Key: 1, CreatedAt: 10}, {Key: 3, CreatedAt: 20}}, jobs)
}

func TestLoadJobsWithGlobalLimitClampsUnexpectedPerSourceResults(t *testing.T) {
	calls := 0
	jobs, err := loadJobsWithGlobalLimit(2, 2, func(index int, _ int64) ([]sql.Job, error) {
		calls++
		firstKey := int64(index*3 + 1)
		return []sql.Job{{Key: firstKey}, {Key: firstKey + 1}, {Key: firstKey + 2}}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []sql.Job{{Key: 1}, {Key: 2}}, jobs)
	assert.Equal(t, 2, calls, "all partitions must be queried even if an earlier source over-returns")
}

// TestTheDeliveriesOfEveryPartitionAreRecordedAtOnce shows the writes of the
// partitions do not wait for each other: every write here waits until all of
// them started, which writes made one after the other never get to.
func TestTheDeliveriesOfEveryPartitionAreRecordedAtOnce(t *testing.T) {
	partitions := [][]sql.Job{{{Key: 1}}, {{Key: 2}, {Key: 3}}, {{Key: 4}}}
	var started sync.WaitGroup
	started.Add(len(partitions))
	allStarted := make(chan struct{})
	go func() {
		started.Wait()
		close(allStarted)
	}()

	recorded, err := writePartitionsAtOnce("record-job-deliveries", partitions, func(jobs []sql.Job) ([]sql.Job, error) {
		started.Done()
		select {
		case <-allStarted:
			return jobs, nil
		case <-time.After(5 * time.Second):
			return nil, errors.New("the other partitions did not start their writes")
		}
	})

	require.NoError(t, err)
	assert.Equal(t, []sql.Job{{Key: 1}, {Key: 2}, {Key: 3}, {Key: 4}}, recorded, "in the order of the partitions")
}

// TestAPartitionWhoseWriteFailsRecordsNoneOfItsJobs shows the jobs of the
// other partitions are recorded all the same, and the error names the failed
// write, a panic included.
func TestAPartitionWhoseWriteFailsRecordsNoneOfItsJobs(t *testing.T) {
	partitions := [][]sql.Job{{{Key: 1}}, {{Key: 2}}, {{Key: 3}}}

	recorded, err := writePartitionsAtOnce("record-job-deliveries", partitions, func(jobs []sql.Job) ([]sql.Job, error) {
		switch jobs[0].Key {
		case 2:
			return nil, errors.New("partition 2 lost its quorum")
		case 3:
			panic("partition 3 broke")
		}
		return jobs, nil
	})

	assert.Equal(t, []sql.Job{{Key: 1}}, recorded)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partition 2 lost its quorum")
	assert.Contains(t, err.Error(), "partition 3 broke")
}
