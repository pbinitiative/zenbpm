package partition

import (
	ssql "database/sql"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
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

func TestListDefinitionRefs(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer partition.Stop()

	ctx := t.Context()
	db := partition.DB

	// empty initially
	refs, err := db.ListDefinitionRefs(ctx)
	assert.NoError(t, err)
	assert.Empty(t, refs)

	// seed one process definition
	err = db.Queries.SaveProcessDefinition(ctx, sql.SaveProcessDefinitionParams{
		Key: 42, Version: 1, BpmnProcessID: "proc-1", BpmnData: "<process/>", BpmnChecksum: []byte{1}, BpmnProcessName: "proc-1",
	})
	assert.NoError(t, err)

	// seed one dmn resource definition
	err = db.Queries.SaveDmnResourceDefinition(ctx, sql.SaveDmnResourceDefinitionParams{
		Key: 99, Version: 1, DmnResourceDefinitionID: "dmn-1", DmnData: "<dmn/>", DmnChecksum: []byte{2}, DmnDefinitionName: "dmn-1",
	})
	assert.NoError(t, err)

	refs, err = db.ListDefinitionRefs(ctx)
	assert.NoError(t, err)
	assert.Len(t, refs, 2)

	keys := map[int64]proto.DefinitionType{}
	for _, r := range refs {
		keys[r.GetKey()] = r.GetType()
	}
	assert.Equal(t, proto.DefinitionType_DEFINITION_TYPE_PROCESS, keys[42])
	assert.Equal(t, proto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE, keys[99])
}

func TestGetDefinitionResource(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	defer partition.Stop()

	ctx := t.Context()
	db := partition.DB

	// seed process definition
	err := db.Queries.SaveProcessDefinition(ctx, sql.SaveProcessDefinitionParams{
		Key: 10, Version: 1, BpmnProcessID: "my-process", BpmnData: "<bpmn-xml/>", BpmnChecksum: []byte{1}, BpmnProcessName: "My Process",
	})
	assert.NoError(t, err)

	data, resourceName, err := db.GetDefinitionResource(ctx, 10, proto.DefinitionType_DEFINITION_TYPE_PROCESS)
	assert.NoError(t, err)
	assert.Equal(t, []byte("<bpmn-xml/>"), data)
	assert.Equal(t, "my-process.bpmn", resourceName)

	// seed dmn resource definition
	err = db.Queries.SaveDmnResourceDefinition(ctx, sql.SaveDmnResourceDefinitionParams{
		Key: 20, Version: 1, DmnResourceDefinitionID: "my-dmn", DmnData: "<dmn-xml/>", DmnChecksum: []byte{2}, DmnDefinitionName: "My DMN",
	})
	assert.NoError(t, err)

	data, resourceName, err = db.GetDefinitionResource(ctx, 20, proto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE)
	assert.NoError(t, err)
	assert.Equal(t, []byte("<dmn-xml/>"), data)
	assert.Equal(t, "", resourceName)

	// not found returns error
	_, _, err = db.GetDefinitionResource(ctx, 9999, proto.DefinitionType_DEFINITION_TYPE_PROCESS)
	assert.Error(t, err)

	// unknown type returns error
	_, _, err = db.GetDefinitionResource(ctx, 10, proto.DefinitionType_DEFINITION_TYPE_UNKNOWN)
	assert.Error(t, err)
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
