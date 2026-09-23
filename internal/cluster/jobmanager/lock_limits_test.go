package jobmanager

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestLockLimitsFromConfigTakesEveryConfiguredValue(t *testing.T) {
	limits := LockLimitsFromConfig(config.JobManager{
		DefaultLockDurationMs: 5000,
		MaxLockDurationMs:     60000,
		DefaultMaxActiveJobs:  4,
		MaxActiveJobsCap:      40,
	})

	assert.Equal(t, LockLimits{
		DefaultLockDuration:  5 * time.Second,
		MaxLockDuration:      time.Minute,
		DefaultMaxActiveJobs: 4,
		MaxActiveJobsCap:     40,
	}, limits)
}

// TestLockLimitsFromConfigFillsOmittedFieldsWithTheDefaults shows a
// configuration built as a struct literal without a job manager section, as
// the cluster test harness builds it, keeps the engine defaults instead of
// installing zero limits under which no job is ever delivered.
func TestLockLimitsFromConfigFillsOmittedFieldsWithTheDefaults(t *testing.T) {
	assert.Equal(t, DefaultLockLimits(), LockLimitsFromConfig(config.JobManager{}))

	partial := LockLimitsFromConfig(config.JobManager{MaxActiveJobsCap: 3})
	assert.Equal(t, 3, partial.MaxActiveJobsCap)
	assert.Equal(t, DefaultLockLimits().DefaultMaxActiveJobs, partial.DefaultMaxActiveJobs)
	assert.Equal(t, DefaultLockLimits().DefaultLockDuration, partial.DefaultLockDuration)
	assert.Equal(t, DefaultLockLimits().MaxLockDuration, partial.MaxLockDuration)
}

func TestSubscriptionUnderOmittedLimitsHasCapacity(t *testing.T) {
	server := newJobServer("node-1", nil, nil, LockLimitsFromConfig(config.JobManager{}))

	effective := server.effectiveSettings(SubscriptionSettings{})

	assert.Equal(t, DefaultLockLimits().DefaultMaxActiveJobs, effective.MaxActiveJobs, "a client must get slots, else nothing is ever delivered")
	assert.Equal(t, DefaultLockLimits().DefaultLockDuration, effective.LockDuration)
}
