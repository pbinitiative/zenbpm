package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaReady(t *testing.T) {
	t.Run("ready after all migrations applied", func(t *testing.T) {
		partition, _, _, _, _ := prepareTestSetup(t, false)
		defer partition.Stop()

		ready, err := partition.DB.SchemaReady(t.Context())
		assert.NoError(t, err)
		assert.True(t, ready, "schema should be ready once all migrations are applied")
	})

	t.Run("not ready while migrations are pending", func(t *testing.T) {
		partition, _, _, _, _ := prepareTestSetupWithTestMigration(t)
		defer partition.Stop()

		ready, err := partition.DB.SchemaReady(t.Context())
		assert.NoError(t, err)
		assert.False(t, ready, "schema should not be ready while migrations are still pending")
	})
}
