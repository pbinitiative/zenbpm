package cluster

import (
	"testing"

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
