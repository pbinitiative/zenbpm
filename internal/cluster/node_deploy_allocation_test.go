package cluster

import (
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is a content fingerprint for change detection, not a security primitive
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentDeploymentsAgreeOnVersionsAcrossPartitions is the regression
// test for concurrent deployments of one process id: two different revisions
// are deployed at the same time and reach the two partitions in opposite
// orders. Because the cluster allocates (version, key) once before the
// fan-out, every partition still ends up with the same
// (process id, version) → definition mapping.
func TestConcurrentDeploymentsAgreeOnVersionsAcrossPartitions(t *testing.T) {
	fc := newFakeDeployCluster(t, 2)
	revisionA := deployTestBPMN("concurrent-process", "revision A")
	revisionB := deployTestBPMN("concurrent-process", "revision B")

	// partition 1 stores A before B, partition 2 stores B before A: whichever
	// revision arrives first must not decide the version on its own
	aOnPartition1 := make(chan struct{})
	bOnPartition2 := make(chan struct{})
	fc.beforeDeploy = func(partitionId uint32, req *proto.DeployProcessDefinitionRequest) {
		switch {
		case partitionId == 1 && string(req.GetData()) == string(revisionB):
			<-aOnPartition1
		case partitionId == 2 && string(req.GetData()) == string(revisionA):
			<-bOnPartition2
		}
	}
	fc.afterDeploy = func(partitionId uint32, req *proto.DeployProcessDefinitionRequest) {
		switch {
		case partitionId == 1 && string(req.GetData()) == string(revisionA):
			close(aOnPartition1)
		case partitionId == 2 && string(req.GetData()) == string(revisionB):
			close(bOnPartition2)
		}
	}

	type outcome struct {
		key            int64
		alreadyExisted bool
		err            error
	}
	outcomes := make([]outcome, 2)
	var wg sync.WaitGroup
	for i, data := range [][]byte{revisionA, revisionB} {
		wg.Go(func() {
			key, alreadyExisted, err := fc.deployer().Deploy(context.Background(), data, "process.bpmn")
			outcomes[i] = outcome{key: key, alreadyExisted: alreadyExisted, err: err}
		})
	}
	wg.Wait()

	for i, o := range outcomes {
		require.NoError(t, o.err, "deployment %d", i)
		assert.False(t, o.alreadyExisted)
		assert.NotZero(t, o.key)
	}
	assert.NotEqual(t, outcomes[0].key, outcomes[1].key)

	for _, req := range fc.requests() {
		assert.Positive(t, req.GetVersion(), "every partition deployment carries the allocated version")
	}

	mappings := fc.definitionMappings(t, "concurrent-process")
	require.Len(t, mappings[1], 2, "partition 1 holds both revisions")
	assert.Equal(t, mappings[1], mappings[2], "every partition maps (process id, version) to the same definition")
	assert.Equal(t, definitionRef{key: outcomes[0].key, checksum: md5.Sum(revisionA)}, mappings[1][mappings[1].versionOf(outcomes[0].key)])
	assert.Equal(t, definitionRef{key: outcomes[1].key, checksum: md5.Sum(revisionB)}, mappings[1][mappings[1].versionOf(outcomes[1].key)])

	// the allocation is what the partitions stored
	latest := fc.clusterState().ProcessDefinitions["concurrent-process"].Latest
	assert.Equal(t, int32(2), latest.Version)
	assert.Equal(t, latest.Key, mappings[1][2].key)
}

// TestRetriedDeploymentReusesAllocation verifies that a deployment which
// failed on one partition is completed by a retry with the same key and
// version instead of allocating again or failing on the version that the
// other partition already holds.
func TestRetriedDeploymentReusesAllocation(t *testing.T) {
	fc := newFakeDeployCluster(t, 2)
	revision := deployTestBPMN("retried-process", "revision A")

	failOnce := true
	fc.beforeDeploy = func(partitionId uint32, _ *proto.DeployProcessDefinitionRequest) {
		if partitionId == 2 && failOnce {
			failOnce = false
			panic(zenerr.TechnicalError(errors.New("partition 2 went away")))
		}
	}

	key, alreadyExisted, err := fc.deployer().Deploy(context.Background(), revision, "process.bpmn")
	require.Error(t, err, "the first attempt fails on partition 2")
	require.NotZero(t, key)
	assert.False(t, alreadyExisted)
	require.Len(t, fc.definitionMappings(t, "retried-process")[2], 0)

	retriedKey, alreadyExisted, err := fc.deployer().Deploy(context.Background(), revision, "process.bpmn")
	require.NoError(t, err, "the retry must not fail with a unique constraint error")
	assert.Equal(t, key, retriedKey, "the retry reuses the allocated key")
	assert.True(t, alreadyExisted, "the allocation already existed")

	mappings := fc.definitionMappings(t, "retried-process")
	assert.Equal(t, mappings[1], mappings[2])
	assert.Len(t, mappings[1], 1)
	assert.Equal(t, int32(1), fc.clusterState().ProcessDefinitions["retried-process"].Latest.Version, "no second version was allocated")

	// a repeated deployment of content every partition already holds keeps
	// answering with the same definition
	sameKey, alreadyExisted, err := fc.deployer().Deploy(context.Background(), revision, "process.bpmn")
	require.NoError(t, err)
	assert.Equal(t, key, sameKey)
	assert.True(t, alreadyExisted)
	assert.Equal(t, int32(1), fc.clusterState().ProcessDefinitions["retried-process"].Latest.Version)
	assert.Equal(t, mappings, fc.definitionMappings(t, "retried-process"))
}

// TestRetryAfterAnotherRevisionCompletesTheOriginalAllocation verifies that a
// deployment which failed on one partition is completed by a retry with its
// original key and version even though another revision was deployed in the
// meantime, so every partition ends up with the same version history.
func TestRetryAfterAnotherRevisionCompletesTheOriginalAllocation(t *testing.T) {
	for _, tag := range []string{"", "stable"} {
		t.Run("tag="+tag, func(t *testing.T) {
			fc := newFakeDeployCluster(t, 2)
			revisionA := deployTestBPMNWithTag("interleaved-process", "revision A", tag)
			revisionB := deployTestBPMN("interleaved-process", "revision B")

			failOnce := true
			fc.beforeDeploy = func(partitionId uint32, req *proto.DeployProcessDefinitionRequest) {
				if partitionId == 2 && failOnce && string(req.GetData()) == string(revisionA) {
					failOnce = false
					panic(zenerr.TechnicalError(errors.New("partition 2 went away")))
				}
			}
			keyA, _, err := fc.deployer().Deploy(context.Background(), revisionA, "process.bpmn")
			require.Error(t, err)
			keyB, _, err := fc.deployer().Deploy(context.Background(), revisionB, "process.bpmn")
			require.NoError(t, err)

			retriedKey, alreadyExisted, err := fc.deployer().Deploy(context.Background(), revisionA, "process.bpmn")
			require.NoError(t, err, "the retry must neither fail nor be rejected for its own version tag")
			assert.Equal(t, keyA, retriedKey)
			assert.True(t, alreadyExisted)

			mappings := fc.definitionMappings(t, "interleaved-process")
			assert.Equal(t, mappings[1], mappings[2], "partition 2 caught up with revision A")
			assert.Equal(t, definitionRef{key: keyA, checksum: md5.Sum(revisionA)}, mappings[2][1])
			assert.Equal(t, definitionRef{key: keyB, checksum: md5.Sum(revisionB)}, mappings[2][2])
			assert.Equal(t, keyB, fc.clusterState().ProcessDefinitions["interleaved-process"].Latest.Key, "revision B stays the latest version")
			assert.Eventually(t, func() bool {
				return len(fc.clusterState().ProcessDefinitions["interleaved-process"].Incomplete) == 0
			}, 5*time.Second, 10*time.Millisecond, "every allocation is confirmed in the background")
		})
	}
}

// TestConcurrentIdenticalDeploymentsShareOneDefinition verifies that two
// concurrent deployments of the same content end up with a single definition
// on every partition and both callers receive its key.
func TestConcurrentIdenticalDeploymentsShareOneDefinition(t *testing.T) {
	fc := newFakeDeployCluster(t, 2)
	revision := deployTestBPMN("identical-process", "revision A")

	keys := make([]int64, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range keys {
		wg.Go(func() {
			keys[i], _, errs[i] = fc.deployer().Deploy(context.Background(), revision, "process.bpmn")
		})
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Equal(t, keys[0], keys[1])
	mappings := fc.definitionMappings(t, "identical-process")
	assert.Len(t, mappings[1], 1)
	assert.Equal(t, mappings[1], mappings[2])
}

// TestDeploymentContinuesVersionsDeployedBeforeAllocation verifies the upgrade
// path: a process deployed before allocations were replicated exists on the
// partitions only, and the next deployment continues its version sequence.
func TestDeploymentContinuesVersionsDeployedBeforeAllocation(t *testing.T) {
	fc := newFakeDeployCluster(t, 2)
	legacy := deployTestBPMN("legacy-process", "revision A")
	for _, engine := range fc.engines {
		_, err := engine.LoadFromBytes(context.Background(), legacy, 77)
		require.NoError(t, err)
	}

	key, alreadyExisted, err := fc.deployer().Deploy(context.Background(), legacy, "process.bpmn")
	require.NoError(t, err)
	assert.True(t, alreadyExisted)
	assert.Equal(t, int64(77), key, "the legacy definition is recognised as the latest one")

	key, alreadyExisted, err = fc.deployer().Deploy(context.Background(), deployTestBPMN("legacy-process", "revision B"), "process.bpmn")
	require.NoError(t, err)
	assert.False(t, alreadyExisted)
	mappings := fc.definitionMappings(t, "legacy-process")
	assert.Equal(t, mappings[1], mappings[2])
	assert.Equal(t, key, mappings[1][2].key, "the new revision became version 2")
}

func TestDeployRejectsResourcesWithoutProcessId(t *testing.T) {
	fc := newFakeDeployCluster(t, 1)
	_, _, err := fc.deployer().Deploy(context.Background(), []byte("<definitions/>"), "process.bpmn")
	var zerr *zenerr.ZenError
	require.ErrorAs(t, err, &zerr)
	assert.Equal(t, zenerr.BadRequestCode, zerr.Code)
	assert.Zero(t, fc.allocations)
}

// fakeDeployCluster is a cluster of engine-backed partitions whose
// allocations go through the replicated cluster state, as the raft FSM
// applies them, instead of the raft log.
type fakeDeployCluster struct {
	t       *testing.T
	engines map[uint32]*bpmn.Engine
	stores  map[uint32]*inmemory.Storage

	mu          sync.Mutex
	cluster     state.Cluster
	allocations int
	sent        []*proto.DeployProcessDefinitionRequest

	beforeDeploy func(partitionId uint32, req *proto.DeployProcessDefinitionRequest)
	afterDeploy  func(partitionId uint32, req *proto.DeployProcessDefinitionRequest)
}

func newFakeDeployCluster(t *testing.T, partitions int) *fakeDeployCluster {
	fc := &fakeDeployCluster{
		t:       t,
		engines: map[uint32]*bpmn.Engine{},
		stores:  map[uint32]*inmemory.Storage{},
	}
	for p := 1; p <= partitions; p++ {
		store := inmemory.NewStorage()
		engine := bpmn.NewEngine(bpmn.EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		fc.stores[uint32(p)] = store // #nosec G115 -- test partition ids are tiny
		fc.engines[uint32(p)] = &engine
	}
	return fc
}

func (fc *fakeDeployCluster) deployer() *processDefinitionDeployer {
	return &processDefinitionDeployer{
		observeLatest: fc.observeLatest,
		allocate:      fc.allocate,
		partitions:    fc.partitions,
		deploy:        fc.deploy,
		confirm:       fc.confirm,
		logger:        hclog.NewNullLogger(),
	}
}

// clusterState reads the replicated state under the lock the detached
// confirmation writes it with.
func (fc *fakeDeployCluster) clusterState() state.Cluster {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return *fc.cluster.DeepCopy()
}

func (fc *fakeDeployCluster) confirm(_ context.Context, processId string, key int64) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	next := *fc.cluster.DeepCopy()
	next.ConfirmProcessDefinition(processId, key)
	fc.cluster = next
	return nil
}

// observeLatest reads partition 1, as a node reads one partition.
func (fc *fakeDeployCluster) observeLatest(ctx context.Context, processId string) (*observedProcessDefinition, error) {
	definitions, err := fc.stores[1].FindProcessDefinitionsById(ctx, processId)
	if err != nil {
		return nil, err
	}
	var latest *runtime.ProcessDefinition
	for i := range definitions {
		if latest == nil || latest.Version < definitions[i].Version {
			latest = &definitions[i]
		}
	}
	if latest == nil {
		return nil, nil
	}
	return &observedProcessDefinition{
		key:        latest.Key,
		version:    latest.Version,
		checksum:   latest.BpmnChecksum[:],
		versionTag: latest.VersionTag,
		data:       []byte(latest.BpmnData),
	}, nil
}

// allocate applies the command the way the FSM does: on a copy of the state
// that replaces it only when the allocation was accepted, with the command's
// position as the sequence.
func (fc *fakeDeployCluster) allocate(_ context.Context, req *protoc.ProcessDefinitionAllocation) (int64, int32, bool, error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.allocations++
	next := *fc.cluster.DeepCopy()
	request := state.ProcessDefinitionAllocationRequest{
		ProcessID:  req.GetProcessId(),
		Checksum:   req.GetChecksum(),
		VersionTag: req.GetVersionTag(),
		Sequence:   uint64(fc.allocations), // #nosec G115 -- a test counter
		NowMillis:  req.GetTimestampMillis(),
	}
	if observed := req.GetObservedLatest(); observed != nil {
		request.ObservedLatest = &state.ProcessDefinitionAllocation{
			Key: observed.GetKey(), Version: observed.GetVersion(), Checksum: observed.GetChecksum(), VersionTag: observed.GetVersionTag(),
		}
	}
	allocation, existing, err := next.AllocateProcessDefinition(request)
	if err != nil {
		return 0, 0, false, zenerr.BadRequest(err)
	}
	fc.cluster = next
	return allocation.Key, allocation.Version, existing, nil
}

func (fc *fakeDeployCluster) partitions(string) ([]uint32, uint32, error) {
	ids := make([]uint32, 0, len(fc.engines))
	for id := range fc.engines {
		ids = append(ids, id)
	}
	return ids, 1, nil
}

func (fc *fakeDeployCluster) deploy(ctx context.Context, partitionId uint32, req *proto.DeployProcessDefinitionRequest) (err error) {
	fc.mu.Lock()
	fc.sent = append(fc.sent, req)
	fc.mu.Unlock()
	defer func() {
		// a hook panicking with an error simulates the partition failing
		if r := recover(); r != nil {
			var ok bool
			if err, ok = r.(error); !ok {
				panic(r)
			}
		}
	}()
	if fc.beforeDeploy != nil {
		fc.beforeDeploy(partitionId, req)
	}
	engine := fc.engines[partitionId]
	if _, err := engine.DeployProcessDefinition(ctx, req.GetData(), req.GetKey(), req.GetVersion()); err != nil {
		return zenerr.TechnicalError(fmt.Errorf("failed to deploy process definition: %w", err))
	}
	if req.GetRegisterProcessDefinitionSubscriptions() {
		if err := engine.RegisterProcessDefinitionSubscriptions(ctx, req.GetKey()); err != nil {
			return zenerr.TechnicalError(err)
		}
	}
	if fc.afterDeploy != nil {
		fc.afterDeploy(partitionId, req)
	}
	return nil
}

func (fc *fakeDeployCluster) requests() []*proto.DeployProcessDefinitionRequest {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return append([]*proto.DeployProcessDefinitionRequest(nil), fc.sent...)
}

// definitionRef identifies the definition a partition stores for a version.
type definitionRef struct {
	key      int64
	checksum [16]byte
}

// versionMapping is the (version → definition) mapping of one process on one
// partition.
type versionMapping map[int32]definitionRef

func (m versionMapping) versionOf(key int64) int32 {
	for version, ref := range m {
		if ref.key == key {
			return version
		}
	}
	return 0
}

// definitionMappings reads, per partition, which definition holds every
// version of the process.
func (fc *fakeDeployCluster) definitionMappings(t *testing.T, processId string) map[uint32]versionMapping {
	t.Helper()
	mappings := map[uint32]versionMapping{}
	for partitionId, store := range fc.stores {
		definitions, err := store.FindProcessDefinitionsById(context.Background(), processId)
		require.NoError(t, err)
		mapping := versionMapping{}
		for _, definition := range definitions {
			_, duplicate := mapping[definition.Version]
			require.False(t, duplicate, "partition %d holds version %d twice", partitionId, definition.Version)
			mapping[definition.Version] = definitionRef{key: definition.Key, checksum: definition.BpmnChecksum}
		}
		mappings[partitionId] = mapping
	}
	return mappings
}

// deployTestBPMN is a minimal executable process whose content varies with
// the name, including the diagram interchange every fixture carries.
func deployTestBPMN(processId string, name string) []byte {
	return deployTestBPMNWithTag(processId, name, "")
}

func deployTestBPMNWithTag(processId string, name string, versionTag string) []byte {
	extension := ""
	if versionTag != "" {
		extension = `<bpmn:extensionElements><zenbpm:versionTag value="` + versionTag + `" /></bpmn:extensionElements>`
	}
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0" id="defs-%[1]s" targetNamespace="urn:test">
  <bpmn:process id="%[1]s" name="%[2]s" isExecutable="true">%[3]s
    <bpmn:startEvent id="start"><bpmn:outgoing>to-end</bpmn:outgoing></bpmn:startEvent>
    <bpmn:endEvent id="end"><bpmn:incoming>to-end</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="to-end" sourceRef="start" targetRef="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="diagram-%[1]s">
    <bpmndi:BPMNPlane id="plane-%[1]s" bpmnElement="%[1]s">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start"><dc:Bounds x="100" y="100" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end"><dc:Bounds x="200" y="100" width="36" height="36" /></bpmndi:BPMNShape>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`, processId, name, extension))
}
