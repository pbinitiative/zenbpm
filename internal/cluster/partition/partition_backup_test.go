package partition

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/assert"
)

func TestSchemaVersion(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer partition.Stop()

	db := partition.DB

	version, err := db.SchemaVersion(t.Context())
	assert.NoError(t, err)

	migs, err := sql.GetUpMigrations(db.migrationDir)
	assert.NoError(t, err)

	var latest string
	for _, m := range migs {
		if m.Filename > latest {
			latest = m.Filename
		}
	}
	assert.Equal(t, latest, version)
}

func TestDataStats(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer partition.Stop()

	db := partition.DB

	definitions, instances, err := db.DataStats(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), definitions)
	assert.Equal(t, int64(0), instances)
}
