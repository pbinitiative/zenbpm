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
	assert.Equal(t, []int64{3, 1}, requestedLimits)
	assert.Equal(t, []sql.Job{{Key: 1}, {Key: 2}, {Key: 3}}, jobs)
}

func TestLoadJobsWithGlobalLimitClampsUnexpectedSourceResults(t *testing.T) {
	calls := 0
	jobs, err := loadJobsWithGlobalLimit(2, 2, func(_ int, _ int64) ([]sql.Job, error) {
		calls++
		return []sql.Job{{Key: 1}, {Key: 2}, {Key: 3}}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []sql.Job{{Key: 1}, {Key: 2}}, jobs)
	assert.Equal(t, 1, calls, "later partitions must not be queried after the global limit is reached")
}
