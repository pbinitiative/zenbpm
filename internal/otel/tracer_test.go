package otel

import (
	"math"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSamplerRatio(t *testing.T) {
	tests := []struct {
		name    string
		ratio   float64
		wantErr bool
	}{
		{name: "zero disables root sampling", ratio: 0, wantErr: false},
		{name: "half", ratio: 0.5, wantErr: false},
		{name: "one samples everything", ratio: 1, wantErr: false},
		{name: "negative", ratio: -0.1, wantErr: true},
		{name: "above one", ratio: 1.1, wantErr: true},
		{name: "NaN", ratio: math.NaN(), wantErr: true},
		{name: "positive infinity", ratio: math.Inf(1), wantErr: true},
		{name: "negative infinity", ratio: math.Inf(-1), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSamplerRatio(tt.ratio)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSetupOtelRejectsInvalidSamplerRatioEvenWhenTracingDisabled(t *testing.T) {
	// an invalid sampler ratio must fail fast at startup even with tracing
	// disabled, so the misconfiguration does not surface on the day tracing
	// gets switched on
	o, err := SetupOtel(config.Tracing{
		Enabled:      false,
		Name:         "test",
		SamplerRatio: 2.0,
	})
	require.Error(t, err)
	assert.Nil(t, o)
}
