package partition

import (
	ssql "database/sql"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/sql"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
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

func TestListActiveMessageSubscriptionsAndRebuildPointers(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer partition.Stop()

	ctx := t.Context()
	db := partition.DB

	// seed a process definition so FK constraint is satisfied
	err := db.Queries.SaveProcessDefinition(ctx, sql.SaveProcessDefinitionParams{
		Key: 1, Version: 1, BpmnProcessID: "test", BpmnData: "<test/>", BpmnChecksum: []byte{1}, BpmnProcessName: "test",
	})
	assert.NoError(t, err)

	// two ACTIVE subscriptions, one COMPLETED (must not be listed)
	saveSub := func(key int64, name, ck string, state int64, createdAt int64) {
		err := db.Queries.SaveMessageSubscription(ctx, sql.SaveMessageSubscriptionParams{
			Key:                  key,
			ElementID:            "el",
			ProcessDefinitionKey: 1,
			ProcessInstanceKey:   ssql.NullInt64{Valid: false},
			Name:                 name,
			State:                state,
			CreatedAt:            createdAt,
			CorrelationKey:       ssql.NullString{String: ck, Valid: ck != ""},
			ExecutionToken:       ssql.NullInt64{Int64: 1, Valid: true},
			Type:                 1,
		})
		assert.NoError(t, err)
	}
	active := int64(bpmnruntime.ActivityStateActive)
	saveSub(101, "m1", "ck-a", active, 1000)
	saveSub(102, "m2", "ck-b", active, 2000)
	saveSub(103, "m3", "ck-c", int64(bpmnruntime.ActivityStateCompleted), 3000)

	rows, err := db.ListActiveMessageSubscriptions(ctx)
	assert.NoError(t, err)
	assert.Len(t, rows, 2)

	// pre-existing stale pointer must be wiped by rebuild
	err = db.Queries.SaveMessageSubscriptionPointer(ctx, sql.SaveMessageSubscriptionPointerParams{
		State: active, CreatedAt: 1, Name: "stale", CorrelationKey: "gone", MessageSubscriptionKey: 999,
	})
	assert.NoError(t, err)

	err = db.RebuildMessageSubscriptionPointers(ctx, rows)
	assert.NoError(t, err)

	// stale pointer gone
	_, err = db.Queries.FindMessageSubscriptionPointer(ctx, sql.FindMessageSubscriptionPointerParams{
		FilterState: active, CorrelationKey: "gone", Name: "stale",
	})
	assert.Error(t, err) // no rows

	// rebuilt pointers resolve
	ptr, err := db.Queries.FindMessageSubscriptionPointer(ctx, sql.FindMessageSubscriptionPointerParams{
		FilterState: active, CorrelationKey: "ck-a", Name: "m1",
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(101), ptr.MessageSubscriptionKey)
}
