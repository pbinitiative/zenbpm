package feel

import (
	"github.com/pbinitiative/zenbpm/pkg/script"
	"github.com/stretchr/testify/assert"
	"testing"
)

var runtime script.FeelRuntime

func TestMain(m *testing.M) {
	runtime = NewFeelinRuntime(1, 1)
	m.Run()
}

func TestFeelinRuntime_Evaluate_ReturnsNilInsteadOfFunctionReference(t *testing.T) {
	result, err := runtime.Evaluate("=product", nil)
	assert.NoError(t, err)
	assert.Equal(t, nil, result)
}
