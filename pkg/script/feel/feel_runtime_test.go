package feel

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/script"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var runtime script.FeelRuntime

func TestMain(m *testing.M) {
	runtime = NewFeelinRuntime(1, 1)
	m.Run()
}

func TestFeelinRuntime_Evaluate_ReturnsNilInsteadOfFunctionReference(t *testing.T) {
	result, err := runtime.Evaluate("=product", nil)
	require.NoError(t, err)
	assert.Equal(t, nil, result)
}

func TestFeelinRuntime_Evaluate_StringNow(t *testing.T) {
	result, err := runtime.Evaluate("string(now())", nil)
	require.NoError(t, err)
	s, ok := result.(string)
	require.True(t, ok, "string(now()) should evaluate to a string, got %T = %v", result, result)
	require.NotEmpty(t, s, "string(now()) should not be empty")
	// Expect ISO-8601 prefix like "YYYY-MM-DDTHH" -- enough to confirm it is a real timestamp.
	require.Regexp(t, `^\d{4}-\d{2}-\d{2}T\d{2}`, s)
}

func TestFeelinRuntime_Evaluate_StringNow_Distinct(t *testing.T) {
	first, err := runtime.Evaluate("string(now())", nil)
	require.NoError(t, err)
	// Two now() evaluations should eventually differ as the wall-clock advances. Using
	// require.Eventually keeps the test rule-compliant (no time.Sleep) and as fast as the
	// underlying runtime allows.
	require.Eventually(t, func() bool {
		second, err := runtime.Evaluate("string(now())", nil)
		return err == nil && second != first
	}, 3*time.Second, 50*time.Millisecond,
		"two consecutive now() evaluations should eventually differ")
}
