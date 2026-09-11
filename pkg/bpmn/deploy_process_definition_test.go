package bpmn

import (
	"crypto/md5"
	"strings"
	"sync"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeployProcessDefinitionStoresAllocatedKeyAndVersion(t *testing.T) {
	store := inmemory.NewStorage()
	recorder := &processEventRecorder{}
	engine := NewEngine(EngineWithStorage(store), EngineWithExporter(recorder))
	defer engine.Stop()
	ctx := t.Context()

	// the cluster allocated version 2 first; the partition must not assign 1
	v2, err := engine.DeployProcessDefinition(ctx, timerStartXML("allocated-process", "PT2H"), 200, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(200), v2.Key)
	assert.Equal(t, int32(2), v2.Version)
	assert.Len(t, recorder.events, 1, "a deployment is exported")
	assert.Equal(t, int32(2), recorder.events[0].Version)

	// the older allocation arrives later (a retry that lost the race): it
	// fills its version and leaves the newest version's subscriptions alone
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(ctx, 200))
	v1, err := engine.DeployProcessDefinition(ctx, timerStartXML("allocated-process", "PT1H"), 100, 1)
	require.NoError(t, err)
	assert.Equal(t, int32(1), v1.Version)
	assert.Len(t, timersOf(t, store, 200), 1)

	latest, err := store.FindLatestProcessDefinitionById(ctx, "allocated-process")
	require.NoError(t, err)
	assert.Equal(t, int64(200), latest.Key)

	// a newer version retires the definition-level subscriptions of the
	// previously newest one, as any deployment does
	v3, err := engine.DeployProcessDefinition(ctx, timerStartXML("allocated-process", "PT3H"), 300, 3)
	require.NoError(t, err)
	assert.Equal(t, int32(3), v3.Version)
	assert.Empty(t, timersOf(t, store, 200))
	assert.Len(t, recorder.events, 3)
}

func TestDeployProcessDefinitionIsIdempotentOnKey(t *testing.T) {
	store := inmemory.NewStorage()
	recorder := &processEventRecorder{}
	engine := NewEngine(EngineWithStorage(store), EngineWithExporter(recorder))
	defer engine.Stop()
	ctx := t.Context()
	xml := timerStartXML("retried-process", "PT1H")

	first, err := engine.DeployProcessDefinition(ctx, xml, 100, 1)
	require.NoError(t, err)

	// a retry after a partial failure or a leader change sends the same
	// allocation again
	again, err := engine.DeployProcessDefinition(ctx, xml, 100, 1)
	require.NoError(t, err)
	assert.Equal(t, first.Key, again.Key)
	assert.Equal(t, first.Version, again.Version)
	all, err := store.FindProcessDefinitionsById(ctx, "retried-process")
	require.NoError(t, err)
	assert.Len(t, all, 1)
	assert.Len(t, recorder.events, 1, "a retry is not exported twice")
}

func TestDeployProcessDefinitionSerialisesConcurrentIdenticalDeployments(t *testing.T) {
	// two nodes deploying the same content at once share one allocation and
	// both reach this partition; the second must find the first one's row
	// instead of saving the key twice
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	xml := timerStartXML("raced-process", "PT1H")

	const deployments = 8
	errs := make([]error, deployments)
	var wg sync.WaitGroup
	for i := range deployments {
		wg.Go(func() {
			_, errs[i] = engine.DeployProcessDefinition(t.Context(), xml, 100, 1)
		})
	}
	wg.Wait()
	for i, err := range errs {
		assert.NoError(t, err, "deployment %d", i)
	}
	all, err := store.FindProcessDefinitionsById(t.Context(), "raced-process")
	require.NoError(t, err)
	assert.Len(t, all, 1)
}

func TestDeployProcessDefinitionDoesNotDeduplicateContent(t *testing.T) {
	// the cluster decided that identical content is a new version (it was
	// deployed again after a different version); the partition stores what
	// was allocated instead of answering with the older definition
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	ctx := t.Context()
	xml := timerStartXML("redeployed-process", "PT1H")

	_, err := engine.DeployProcessDefinition(ctx, xml, 100, 1)
	require.NoError(t, err)
	_, err = engine.DeployProcessDefinition(ctx, timerStartXML("redeployed-process", "PT2H"), 200, 2)
	require.NoError(t, err)
	v3, err := engine.DeployProcessDefinition(ctx, xml, 300, 3)
	require.NoError(t, err)
	assert.Equal(t, int64(300), v3.Key)
	assert.Equal(t, int32(3), v3.Version)
	assert.Equal(t, md5.Sum(xml), v3.BpmnChecksum)
}

func TestDeployProcessDefinitionRefusesDivergedPartition(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	ctx := t.Context()

	_, err := engine.DeployProcessDefinition(ctx, timerStartXML("diverged-process", "PT1H"), 100, 1)
	require.NoError(t, err)

	_, err = engine.DeployProcessDefinition(ctx, timerStartXML("diverged-process", "PT2H"), 101, 1)
	require.ErrorIs(t, err, storage.ErrUniqueConstraint, "another key already holds version 1")

	_, err = engine.DeployProcessDefinition(ctx, timerStartXML("diverged-process", "PT2H"), 102, 0)
	require.Error(t, err, "a version has to be positive")

	all, err := store.FindProcessDefinitionsById(ctx, "diverged-process")
	require.NoError(t, err)
	assert.Len(t, all, 1, "nothing is written on a conflict")
}

func TestDeployProcessDefinitionRefusesReusedVersionTag(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	ctx := t.Context()
	tagged := func(name string) []byte {
		return []byte(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="tagged-process" name="` + name + `" isExecutable="true">
    <bpmn:extensionElements><zenbpm:versionTag value="stable" /></bpmn:extensionElements>
    <bpmn:startEvent id="start" />
  </bpmn:process>
</bpmn:definitions>`)
	}

	_, err := engine.DeployProcessDefinition(ctx, tagged("first"), 100, 1)
	require.NoError(t, err)
	_, err = engine.DeployProcessDefinition(ctx, tagged("second"), 200, 2)
	require.ErrorIs(t, err, storage.ErrUniqueConstraint)
}

func TestParseProcessDefinitionIdentity(t *testing.T) {
	xml := []byte(strings.TrimSpace(`
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="identity-process" isExecutable="true">
    <bpmn:extensionElements><zenbpm:versionTag value="1.2.3" /></bpmn:extensionElements>
    <bpmn:startEvent id="start" />
  </bpmn:process>
</bpmn:definitions>`))
	identity, err := ParseProcessDefinitionIdentity(xml)
	require.NoError(t, err)
	assert.Equal(t, "identity-process", identity.ProcessId)
	assert.Equal(t, "1.2.3", identity.VersionTag)
	assert.Equal(t, md5.Sum(xml), identity.Checksum)

	_, err = ParseProcessDefinitionIdentity([]byte("not xml"))
	require.Error(t, err)
}

// processEventRecorder collects the deployment events an engine exports.
type processEventRecorder struct {
	events []exporter.ProcessEvent
}

func (r *processEventRecorder) NewProcessEvent(event *exporter.ProcessEvent) {
	r.events = append(r.events, *event)
}

func (r *processEventRecorder) EndProcessEvent(*exporter.ProcessInstanceEvent)         {}
func (r *processEventRecorder) NewProcessInstanceEvent(*exporter.ProcessInstanceEvent) {}
func (r *processEventRecorder) NewElementEvent(*exporter.ProcessInstanceEvent, *exporter.ElementInfo) {
}
