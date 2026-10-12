package extensions

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetriesAreANonNegativeInteger(t *testing.T) {
	retries, err := ParseRetries(" 3 ")
	require.NoError(t, err)
	assert.Equal(t, int32(3), retries)

	retries, err = ParseRetries("0")
	require.NoError(t, err)
	assert.Zero(t, retries)

	for _, invalid := range []string{"-1", "three", "2.5", "", "99999999999"} {
		_, err := ParseRetries(invalid)
		assert.Error(t, err, "%q must be refused", invalid)
	}
}

func TestRetriesBeyondAnInt32AreReportedForWhatTheyAre(t *testing.T) {
	_, err := ParseRetries("99999999999")
	assert.EqualError(t, err, `retries "99999999999" is larger than 2147483647`)

	_, err = ParseRetries("-99999999999")
	assert.EqualError(t, err, `retries "-99999999999" must not be negative`)
}

func TestRetryBackoffIsADurationOrAListOfThem(t *testing.T) {
	policy, err := ParseRetryBackoff("PT10S")
	require.NoError(t, err)
	assert.Equal(t, []time.Duration{10 * time.Second}, policy)

	policy, err = ParseRetryBackoff("PT10S, PT1M ,PT1H30M,P1D,P1W,PT0S")
	require.NoError(t, err)
	assert.Equal(t, []time.Duration{10 * time.Second, time.Minute, 90 * time.Minute, 24 * time.Hour, 7 * 24 * time.Hour, 0}, policy)
}

func TestRetryBackoffRefusesWhatHasNoFixedLength(t *testing.T) {
	for _, invalid := range []string{"10s", "", "P", "PT", "PT10S,", "P1M", "P1Y", "R3/PT5M", "-PT1S"} {
		_, err := ParseRetryBackoff(invalid)
		assert.Error(t, err, "%q must be refused", invalid)
	}
	_, err := ParseRetryBackoff("P1M")
	assert.ErrorContains(t, err, "months")
}

func TestRetryBackoffSaturatesInsteadOfWrapping(t *testing.T) {
	backoff, err := ParseBackoffDuration("PT9999999999999H")
	require.NoError(t, err)
	assert.Equal(t, time.Duration(math.MaxInt64), backoff)
}

func TestRetryBackoffIsWrittenTheWayItIsRead(t *testing.T) {
	policy := []time.Duration{0, 10 * time.Second, 90 * time.Minute, 25*time.Hour + time.Second}
	formatted := FormatRetryBackoff(policy)
	assert.Equal(t, "PT0S,PT10S,PT1H30M,PT25H1S", formatted)

	parsed, err := ParseRetryBackoff(formatted)
	require.NoError(t, err)
	assert.Equal(t, policy, parsed)
	assert.Empty(t, FormatRetryBackoff(nil))
}

func TestExpressionsStartWithAnEqualsSign(t *testing.T) {
	assert.True(t, IsExpression("=retries"))
	assert.True(t, IsExpression("  = 1 + 2"))
	assert.False(t, IsExpression("3"))
	assert.False(t, IsExpression(""))
}
