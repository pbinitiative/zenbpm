package rest

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLockDuration(t *testing.T) {
	tests := []struct {
		name     string
		raw      *string
		expected time.Duration
	}{
		{"absent means the subscription's duration", nil, 0},
		{"blank means the subscription's duration", new("  "), 0},
		{"minutes", new("PT5M"), 5 * time.Minute},
		{"hours, minutes and seconds", new(" PT1H30M15S "), time.Hour + 30*time.Minute + 15*time.Second},
		{"a day", new("P1D"), 24 * time.Hour},
		{"weeks, days and a time part", new("P2W1DT1H"), 15*24*time.Hour + time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLockDuration(tt.raw)
			require.NoError(t, err)
			if tt.name == "a day" || tt.name == "weeks, days and a time part" {
				// a calendar day is resolved against now, so around a
				// daylight saving change it is an hour shorter or longer
				assert.InDelta(t, tt.expected, got, float64(time.Hour))
				return
			}
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestParseLockDurationSaturatesInsteadOfWrapping shows a duration longer
// than a time.Duration can hold becomes the maximum, which the engine cap
// then lowers, rather than wrapping to a short lock: PT5124096H wrapped comes
// out as some twenty-five minutes.
func TestParseLockDurationSaturatesInsteadOfWrapping(t *testing.T) {
	for _, raw := range []string{
		"PT5124096H", "PT9223372036855S", "P1000000Y", "P300YT2562047H", "PT9223372036854775807S",
		// the weeks times seven wrap to minus two, the days bring that to zero
		// and the one second would be all that is left
		"P2635249153387078802W2DT1S",
		"P9223372036854775807D", "P4000M", "P1000000W", "P200Y1200MT1H",
	} {
		t.Run(raw, func(t *testing.T) {
			got, err := parseLockDuration(new(raw))
			require.NoError(t, err)
			assert.Equal(t, time.Duration(math.MaxInt64), got)
		})
	}
	longest, err := parseLockDuration(new("PT2562047H47M16S"))
	require.NoError(t, err)
	assert.Equal(t, 2562047*time.Hour+47*time.Minute+16*time.Second, longest, "the longest representable time part is converted exactly")
}

func TestParseLockDurationRefusesWhatIsNotAPositiveDuration(t *testing.T) {
	tests := []struct {
		raw     string
		message string
	}{
		{"5 minutes", "ISO-8601"},
		{"PT0S", "must be positive"},
		{"P0D", "must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			_, err := parseLockDuration(new(tt.raw))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.message)
		})
	}
}
