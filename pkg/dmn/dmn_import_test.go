package dmn

import (
	"path/filepath"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportDmnResourceDefinitionPreservesKeyAndVersions(t *testing.T) {
	store := inmemory.NewStorage()
	dmnEngine.persistence = store
	ctx := t.Context()
	definition, xmldata, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	require.NoError(t, err)

	// version 2 is present, version 1 is imported afterwards
	resource, decisions, err := dmnEngine.ImportDmnResourceDefinition(ctx, definition, xmldata, 200, 2, map[string]int64{"example_canAutoLiquidateRule": 2})
	require.NoError(t, err)
	assert.Equal(t, int64(200), resource.Key)
	assert.Equal(t, int64(2), resource.Version)
	require.Len(t, decisions, 1)
	assert.Equal(t, int64(2), decisions[0].Version)
	assert.Equal(t, int64(200), decisions[0].DmnResourceDefinitionKey)

	resource, decisions, err = dmnEngine.ImportDmnResourceDefinition(ctx, definition, xmldata, 100, 1, map[string]int64{"example_canAutoLiquidateRule": 1})
	require.NoError(t, err)
	assert.Equal(t, int64(100), resource.Key)
	assert.Equal(t, int64(1), resource.Version, "the version is copied, not assigned")
	require.Len(t, decisions, 1)
	assert.Equal(t, int64(1), decisions[0].Version)

	latest, err := store.FindLatestDmnResourceDefinitionById(ctx, "example_canAutoLiquidate")
	require.NoError(t, err)
	assert.Equal(t, int64(200), latest.Key, "importing a historical version must not make it the latest one")

	// a decision without a given version gets the next one, as a deployment would
	resource, decisions, err = dmnEngine.ImportDmnResourceDefinition(ctx, definition, xmldata, 300, 3, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), resource.Version)
	require.Len(t, decisions, 1)
	assert.Equal(t, int64(3), decisions[0].Version)

	// importing the same key again is a no-op
	again, decisions, err := dmnEngine.ImportDmnResourceDefinition(ctx, definition, xmldata, 100, 1, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(100), again.Key)
	assert.Empty(t, decisions)
	all, err := store.FindDmnResourceDefinitionsById(ctx, "example_canAutoLiquidate")
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

func TestImportDmnResourceDefinitionRefusesDivergedVersionHistories(t *testing.T) {
	store := inmemory.NewStorage()
	dmnEngine.persistence = store
	ctx := t.Context()
	definition, xmldata, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	require.NoError(t, err)

	_, _, err = dmnEngine.ImportDmnResourceDefinition(ctx, definition, xmldata, 100, 1, map[string]int64{"example_canAutoLiquidateRule": 1})
	require.NoError(t, err)

	_, _, err = dmnEngine.ImportDmnResourceDefinition(ctx, definition, xmldata, 101, 1, map[string]int64{"example_canAutoLiquidateRule": 2})
	require.ErrorIs(t, err, storage.ErrUniqueConstraint, "another key already holds resource version 1")

	_, _, err = dmnEngine.ImportDmnResourceDefinition(ctx, definition, xmldata, 102, 2, map[string]int64{"example_canAutoLiquidateRule": 1})
	require.ErrorIs(t, err, storage.ErrUniqueConstraint, "another decision definition already holds decision version 1")

	all, err := store.FindDmnResourceDefinitionsById(ctx, "example_canAutoLiquidate")
	require.NoError(t, err)
	assert.Len(t, all, 1, "nothing is written on a conflict")
	decisions, err := store.GetDecisionDefinitionsById(ctx, "example_canAutoLiquidateRule")
	require.NoError(t, err)
	assert.Len(t, decisions, 1)
}
