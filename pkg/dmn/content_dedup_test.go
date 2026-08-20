package dmn

import (
	"bytes"
	"crypto/md5"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveDmnResourceDefinition_FormattingOnlyRedeployReusesDefinition(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()

	original, err := os.ReadFile(filepath.Join("test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	require.NoError(t, err)
	formatted := bytes.Replace(
		original,
		[]byte(">\n  <decision"),
		[]byte(">\n\n\n  <decision"),
		1,
	)
	require.NotEqual(t, original, formatted, "test fixture must contain the formatting insertion point")
	require.NotEqual(t, md5.Sum(original), md5.Sum(formatted), "test inputs must exercise the fallback")

	originalDefinition, err := engine.ParseDmnFromBytes("original.dmn", original)
	require.NoError(t, err)
	formattedDefinition, err := engine.ParseDmnFromBytes("formatted.dmn", formatted)
	require.NoError(t, err)

	first, firstDecisions, err := engine.SaveDmnResourceDefinition(
		t.Context(),
		originalDefinition,
		original,
		engine.generateKey(),
	)
	require.NoError(t, err)
	require.NotEmpty(t, firstDecisions)

	second, secondDecisions, err := engine.SaveDmnResourceDefinition(
		t.Context(),
		formattedDefinition,
		formatted,
		engine.generateKey(),
	)
	require.NoError(t, err)

	assert.Equal(t, first.Key, second.Key)
	assert.Equal(t, int64(1), second.Version)
	assert.Equal(t, original, second.DmnData)
	assert.Equal(t, md5.Sum(original), second.DmnChecksum, "the stored checksum must remain the raw MD5")
	assert.Empty(t, secondDecisions)

	definitions, err := store.FindDmnResourceDefinitionsById(t.Context(), originalDefinition.Id)
	require.NoError(t, err)
	assert.Len(t, definitions, 1)
}
