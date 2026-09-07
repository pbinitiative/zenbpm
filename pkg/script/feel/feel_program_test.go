// Package feel provides FEEL (Friendly Enough Expression Language) evaluation
// support backed by the Goja JavaScript runtime, including compilation and
// reuse of FEEL programs across independent runners.
package feel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompiledProgramsAreSharedAcrossCalls(t *testing.T) {
	first := compiledPrograms()
	second := compiledPrograms()

	require.Len(t, first, 3, "polyfill, feelin bundle, and extensions must all be compiled")
	require.Len(t, second, len(first))
	for i := range first {
		require.NotNil(t, first[i])
		// The bundle must be compiled only once: every call returns the very same immutable *goja.Program instances.
		require.Same(t, first[i], second[i], "program %d must be the shared compiled instance", i)
	}
}

func TestNewRunnersShareCompiledProgramsButOwnIndependentRuntimes(t *testing.T) {
	firstRunner := newFeelRunner()
	secondRunner := newFeelRunner()

	require.NotSame(t, firstRunner.vm, secondRunner.vm, "each runner must own an independent goja.Runtime")

	// Both runners must be fully functional after executing the shared compiled programs.
	firstResult, err := (*firstRunner.evalFunc)("1 + 1", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 2, firstResult)

	secondResult, err := (*secondRunner.evalFunc)("2 + 3", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 5, secondResult)

	// State written into one runtime must never leak into the other.
	_, err = (*firstRunner.evalFunc)("a", map[string]any{"a": 42})
	require.NoError(t, err)
	leaked, err := (*secondRunner.evalFunc)("a", nil)
	require.NoError(t, err)
	assert.Nil(t, leaked, "variable context of one runner must not leak into another runner")
}
