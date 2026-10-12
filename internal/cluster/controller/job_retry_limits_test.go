package controller

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobRetryLimitsOfAConfigurationBuiltInCodeAreTheDefaults shows a node
// started with a configuration which leaves the jobs section out, as the
// cluster test harness does, gets the engine defaults instead of failing to start.
func TestJobRetryLimitsOfAConfigurationBuiltInCodeAreTheDefaults(t *testing.T) {
	limits, err := JobRetryLimits(config.Jobs{})

	require.NoError(t, err)
	assert.Equal(t, int32(1), limits.DefaultRetries)
	assert.Equal(t, int32(100), limits.MaxRetries)
	assert.Equal(t, []time.Duration{0}, limits.DefaultRetryBackoff)
	assert.Equal(t, 24*time.Hour, limits.MaxRetryBackoff)
}

func TestJobRetryLimitsTakeTheConfiguredValues(t *testing.T) {
	limits, err := JobRetryLimits(config.Jobs{DefaultRetries: 3, MaxRetries: 5, DefaultRetryBackoff: "PT10S,PT1M", MaxRetryBackoff: "PT1H"})

	require.NoError(t, err)
	assert.Equal(t, bpmn.JobRetryLimits{
		DefaultRetries:      3,
		MaxRetries:          5,
		DefaultRetryBackoff: []time.Duration{10 * time.Second, time.Minute},
		MaxRetryBackoff:     time.Hour,
	}, limits)
}

func TestJobRetryLimitsRefuseAnInconsistentSection(t *testing.T) {
	_, err := JobRetryLimits(config.Jobs{DefaultRetries: 7, MaxRetries: 5})

	assert.ErrorContains(t, err, "jobs.defaultRetries")
}
