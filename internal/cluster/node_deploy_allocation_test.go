package cluster

import (
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is a content fingerprint for change detection, not a security primitive
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
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

// TestRedeployingOlderContentAfterAnotherRevisionMakesItTheLatestVersion
// verifies that deploying A, then B, then A again ends with A as the latest
// version on every partition, whether or not the first deployment of A
// reached every partition: an older revision is deduplicated against the
// latest version only.
func TestRedeployingOlderContentAfterAnotherRevisionMakesItTheLatestVersion(t *testing.T) {
	for _, firstDeploymentFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("firstDeploymentFails=%t", firstDeploymentFails), func(t *testing.T) {
			fc := newFakeDeployCluster(t, 2)
			revisionA := deployTestBPMN("interleaved-process", "revision A")
			revisionB := deployTestBPMN("interleaved-process", "revision B")

			failOnce := firstDeploymentFails
			fc.beforeDeploy = func(partitionId uint32, req *proto.DeployProcessDefinitionRequest) {
				if partitionId == 2 && failOnce && string(req.GetData()) == string(revisionA) {
					failOnce = false
					panic(zenerr.TechnicalError(errors.New("partition 2 went away")))
				}
			}
			keyA, _, err := fc.deployer().Deploy(context.Background(), revisionA, "process.bpmn")
			if firstDeploymentFails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			keyB, _, err := fc.deployer().Deploy(context.Background(), revisionB, "process.bpmn")
			require.NoError(t, err)

			keyA2, alreadyExisted, err := fc.deployer().Deploy(context.Background(), revisionA, "process.bpmn")
			require.NoError(t, err)
			assert.False(t, alreadyExisted, "revision A is deployed again as a new version")
			assert.NotEqual(t, keyA, keyA2)

			mappings := fc.definitionMappings(t, "interleaved-process")
			for partitionId, mapping := range mappings {
				assert.Equal(t, definitionRef{key: keyB, checksum: md5.Sum(revisionB)}, mapping[2], "partition %d", partitionId)
				assert.Equal(t, definitionRef{key: keyA2, checksum: md5.Sum(revisionA)}, mapping[3], "partition %d", partitionId)
			}
			assert.Equal(t, keyA2, fc.clusterState().ProcessDefinitions["interleaved-process"].Latest.Key, "revision A is the latest version again")
			if !firstDeploymentFails {
				assert.Equal(t, mappings[1], mappings[2])
			}
		})
	}
}

// TestTaggedRetryAfterAnotherRevisionCompletesTheOriginalAllocation verifies
// that a tagged deployment which failed on one partition is completed by a
// retry with its original key and version even though another revision was
// deployed in the meantime: the tag pins the content to that version, so the
// retry is neither rejected for reusing its own tag nor allocated again.
func TestTaggedRetryAfterAnotherRevisionCompletesTheOriginalAllocation(t *testing.T) {
	fc := newFakeDeployCluster(t, 2)
	revisionA := deployTestBPMNWithTag("tagged-process", "revision A", "stable")
	revisionB := deployTestBPMN("tagged-process", "revision B")

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

	mappings := fc.definitionMappings(t, "tagged-process")
	assert.Equal(t, mappings[1], mappings[2], "partition 2 caught up with revision A")
	assert.Equal(t, definitionRef{key: keyA, checksum: md5.Sum(revisionA)}, mappings[2][1])
	assert.Equal(t, definitionRef{key: keyB, checksum: md5.Sum(revisionB)}, mappings[2][2])
	assert.Equal(t, keyB, fc.clusterState().ProcessDefinitions["tagged-process"].Latest.Key, "revision B stays the latest version")

	// other content under the tag is refused before anything is allocated
	_, _, err = fc.deployer().Deploy(context.Background(), deployTestBPMNWithTag("tagged-process", "revision C", "stable"), "process.bpmn")
	var zerr *zenerr.ZenError
	require.ErrorAs(t, err, &zerr)
	assert.Equal(t, zenerr.BadRequestCode, zerr.Code)
	assert.Equal(t, mappings, fc.definitionMappings(t, "tagged-process"))
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

// TestDeploymentObservesEveryPartitionDeployedBeforeAllocation covers the
// upgrade path of a cluster whose partitions disagree on a process: before
// allocations were replicated, concurrent deployments could map one version
// to different definitions on different partitions. The allocation continues
// from the newest version any partition holds, so the next version is free on
// every partition, and every version tag any partition holds is reserved, so
// no deployment is allocated a tag a partition would refuse.
func TestDeploymentObservesEveryPartitionDeployedBeforeAllocation(t *testing.T) {
	fc := newFakeDeployCluster(t, 2)
	ctx := context.Background()
	legacyA := deployTestBPMNWithTag("diverged-process", "revision A", "legacy")
	legacyB := deployTestBPMN("diverged-process", "revision B")
	// partition 1 holds A as version 1 and B as version 2, partition 2 holds
	// B as version 1
	_, err := fc.engines[1].LoadFromBytes(ctx, legacyA, 71)
	require.NoError(t, err)
	_, err = fc.engines[1].LoadFromBytes(ctx, legacyB, 72)
	require.NoError(t, err)
	_, err = fc.engines[2].LoadFromBytes(ctx, legacyB, 73)
	require.NoError(t, err)

	// a tag only partition 1 holds is taken
	_, _, err = fc.deployer().Deploy(ctx, deployTestBPMNWithTag("diverged-process", "revision C", "legacy"), "process.bpmn")
	var zerr *zenerr.ZenError
	require.ErrorAs(t, err, &zerr)
	assert.Equal(t, zenerr.BadRequestCode, zerr.Code)

	key, alreadyExisted, err := fc.deployer().Deploy(ctx, deployTestBPMN("diverged-process", "revision C"), "process.bpmn")
	require.NoError(t, err)
	assert.False(t, alreadyExisted)
	mappings := fc.definitionMappings(t, "diverged-process")
	assert.Equal(t, definitionRef{key: key, checksum: md5.Sum(deployTestBPMN("diverged-process", "revision C"))}, mappings[1][3], "the new revision continues after the newest version any partition holds")
	assert.Equal(t, mappings[1][3], mappings[2][3])
	assert.Equal(t, int32(3), fc.clusterState().ProcessDefinitions["diverged-process"].Latest.Version)
}

func TestMergeObservedProcessDefinitionsUnitesPartitionsByKey(t *testing.T) {
	merged := mergeObservedProcessDefinitions([][]observedProcessDefinition{
		{{key: 2, version: 2, checksum: []byte("b"), data: []byte("B")}},
		{{key: 2, version: 2, checksum: []byte("b")}, {key: 1, version: 1, checksum: []byte("a"), versionTag: "t", data: []byte("A")}},
		nil,
	})
	assert.ElementsMatch(t, []observedProcessDefinition{
		{key: 2, version: 2, checksum: []byte("b"), data: []byte("B")},
		{key: 1, version: 1, checksum: []byte("a"), versionTag: "t", data: []byte("A")},
	}, merged)
	latest := latestObservedProcessDefinition(merged)
	require.NotNil(t, latest)
	assert.Equal(t, int64(2), latest.key)
	assert.Nil(t, latestObservedProcessDefinition(nil))
	// partitions holding different definitions at the same version are told
	// apart by key, like the allocation does
	sameVersion := latestObservedProcessDefinition([]observedProcessDefinition{{key: 3, version: 1}, {key: 9, version: 1}})
	require.NotNil(t, sameVersion)
	assert.Equal(t, int64(9), sameVersion.key)
}

func TestDeployRejectsResourcesWithoutProcessId(t *testing.T) {
	fc := newFakeDeployCluster(t, 1)
	_, _, err := fc.deployer().Deploy(context.Background(), []byte("<definitions/>"), "process.bpmn")
	var zerr *zenerr.ZenError
	require.ErrorAs(t, err, &zerr)
	assert.Equal(t, zenerr.BadRequestCode, zerr.Code)
	assert.Zero(t, fc.allocations)
}

func TestAllocatedDeploymentCannotCrossRestore(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial-fanout=%t", partial), func(t *testing.T) {
			fc := newFakeDeployCluster(t, 2)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			const processID = "restore-deployment"
			a := deployTestBPMN(processID, "A")
			b := deployTestBPMN(processID, "B")
			keyA, _, err := fc.deployer().Deploy(ctx, a, "a.bpmn")
			require.NoError(t, err)
			paused := make(chan struct{}, 2)
			firstDeployed := make(chan struct{}, 1)
			resume := make(chan struct{})
			fc.beforeDeploy = func(id uint32, req *proto.DeployProcessDefinitionRequest) {
				if string(req.GetData()) != string(b) || (partial && id == 1) {
					return
				}
				paused <- struct{}{}
				select {
				case <-resume:
				case <-ctx.Done():
					panic(ctx.Err())
				}
			}
			fc.afterDeploy = func(id uint32, req *proto.DeployProcessDefinitionRequest) {
				if partial && id == 1 && string(req.GetData()) == string(b) {
					firstDeployed <- struct{}{}
				}
			}
			type outcome struct {
				key      int64
				existing bool
				err      error
			}
			done := make(chan outcome, 1)
			go func() {
				key, existing, err := fc.deployer().Deploy(ctx, b, "b.bpmn")
				done <- outcome{key: key, existing: existing, err: err}
			}()
			wait := func(ch <-chan struct{}) {
				select {
				case <-ch:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			wait(paused)
			if partial {
				wait(firstDeployed)
			} else {
				wait(paused)
			}
			obsolete := fc.clusterState().ProcessDefinitions[processID].Latest
			require.Equal(t, int32(2), obsolete.Version)
			token := partition.RestoreToken{OperationID: "restore", Epoch: 1}
			for id, guard := range fc.guards {
				guard.EnterRestoreFence(token)
				// Restore the backup's A-only content on fresh engines, as a real
				// restore stops the old engines before replacing their databases.
				fc.engines[id].Stop()
				storage := inmemory.NewStorage()
				engine := bpmn.NewEngine(bpmn.EngineWithStorage(storage))
				t.Cleanup(engine.Stop)
				_, err := engine.DeployProcessDefinition(ctx, a, keyA, 1)
				require.NoError(t, err)
				fc.stores[id], fc.engines[id] = storage, &engine
			}
			fc.mu.Lock()
			fc.cluster.Restore = state.RestoreOperation{ID: token.OperationID, Epoch: token.Epoch, Status: state.RestoreStatusActive, Phase: state.RestorePhaseReconciling}
			checksum := md5.Sum(a)
			err = fc.cluster.ResetProcessDefinitions([]state.ObservedProcessDefinition{{ProcessID: processID, Key: keyA, Version: 1, Checksum: hex.EncodeToString(checksum[:])}}, token.OperationID, token.Epoch)
			fc.cluster.Restore.Status, fc.cluster.Restore.Phase = state.RestoreStatusCompleted, state.RestorePhaseDone
			fc.mu.Unlock()
			require.NoError(t, err)
			for _, guard := range fc.guards {
				guard.LeaveRestoreFence(token)
			}
			close(resume)
			// the restored partitions refuse the obsolete fan-out; the deployer
			// observes and allocates again and completes against them
			result := <-done
			require.NoError(t, result.err, "the deployment is repeated from a fresh observation")
			assert.False(t, result.existing)
			assert.NotEqual(t, obsolete.Key, result.key, "the obsolete allocation is never stored")
			mappings := fc.definitionMappings(t, processID)
			for partitionID, mapping := range mappings {
				require.Len(t, mapping, 2, "partition %d", partitionID)
				assert.Equal(t, keyA, mapping[1].key, "the restored content is kept")
				assert.Equal(t, result.key, mapping[2].key)
			}
			assert.Equal(t, result.key, fc.clusterState().ProcessDefinitions[processID].Latest.Key)
			fc.beforeDeploy, fc.afterDeploy = nil, nil
			sameKey, alreadyExisted, err := fc.deployer().Deploy(ctx, b, "b.bpmn")
			require.NoError(t, err)
			assert.True(t, alreadyExisted, "a repeat is answered with the fresh definition")
			assert.Equal(t, result.key, sameKey)
		})
	}
}

func TestDeployerAndAllocatorAgreeOnEqualVersionObservations(t *testing.T) {
	for _, observations := range [][]observedProcessDefinition{
		{{key: 3, version: 1, checksum: []byte("a")}, {key: 9, version: 1, checksum: []byte("b")}},
		{{key: 9, version: 1, checksum: []byte("b")}, {key: 3, version: 1, checksum: []byte("a")}},
	} {
		latest := latestObservedProcessDefinition(observations)
		require.NotNil(t, latest)
		var cluster state.Cluster
		req := state.ProcessDefinitionAllocationRequest{ProcessID: "tie", Checksum: hex.EncodeToString(latest.checksum), NowMillis: time.Now().UnixMilli()}
		for _, observed := range observations {
			req.Observed = append(req.Observed, state.ProcessDefinitionAllocation{Key: observed.key, Version: observed.version, Checksum: hex.EncodeToString(observed.checksum)})
		}
		allocated, existing, err := cluster.AllocateProcessDefinition(req)
		require.NoError(t, err)
		assert.True(t, existing)
		assert.Equal(t, latest.key, allocated.Key)
		assert.Equal(t, latest.version, allocated.Version)
	}
}

// fakeDeployCluster is a cluster of engine-backed partitions whose
// allocations go through the replicated cluster state, as the raft FSM
// applies them, instead of the raft log.
type fakeDeployCluster struct {
	t       *testing.T
	engines map[uint32]*bpmn.Engine
	stores  map[uint32]*inmemory.Storage
	guards  map[uint32]*partition.DB

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
		guards:  map[uint32]*partition.DB{},
	}
	for p := 1; p <= partitions; p++ {
		store := inmemory.NewStorage()
		engine := bpmn.NewEngine(bpmn.EngineWithStorage(store))
		t.Cleanup(engine.Stop)
		fc.stores[uint32(p)] = store // #nosec G115 -- test partition ids are tiny
		fc.engines[uint32(p)] = &engine
		fc.guards[uint32(p)] = &partition.DB{}
	}
	return fc
}

func (fc *fakeDeployCluster) deployer() *processDefinitionDeployer {
	return &processDefinitionDeployer{
		observe:  fc.observe,
		allocate: fc.allocate,
		deploy:   fc.deploy,
	}
}

// clusterState reads the replicated state under the lock concurrent allocations write it with.
func (fc *fakeDeployCluster) clusterState() state.Cluster {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return *fc.cluster.DeepCopy()
}

// observe reads every partition, as a node reads every partition leader:
// the latest version with its bytes and the tagged versions, merged by key,
// under the restore generation of the cluster state.
func (fc *fakeDeployCluster) observe(ctx context.Context, processId string) (processObservation, error) {
	restore := fc.clusterState().Restore
	observation := processObservation{restoreID: restore.ID, restoreEpoch: restore.Epoch}
	observation.partitionIDs, observation.subscriptionPartition, _ = fc.partitions(processId)
	var perPartition [][]observedProcessDefinition
	for _, store := range fc.stores {
		definitions, err := store.FindProcessDefinitionsById(ctx, processId)
		if err != nil {
			return processObservation{}, err
		}
		var latest *runtime.ProcessDefinition
		for i := range definitions {
			if latest == nil || latest.Version < definitions[i].Version {
				latest = &definitions[i]
			}
		}
		var observed []observedProcessDefinition
		for i := range definitions {
			definition := &definitions[i]
			if definition != latest && definition.VersionTag == "" {
				continue
			}
			entry := observedProcessDefinition{
				key:        definition.Key,
				version:    definition.Version,
				checksum:   definition.BpmnChecksum[:],
				versionTag: definition.VersionTag,
			}
			if definition == latest {
				entry.data = []byte(definition.BpmnData)
			}
			observed = append(observed, entry)
		}
		perPartition = append(perPartition, observed)
	}
	observation.definitions = mergeObservedProcessDefinitions(perPartition)
	return observation, nil
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
		ProcessID:    req.GetProcessId(),
		Checksum:     req.GetChecksum(),
		VersionTag:   req.GetVersionTag(),
		Sequence:     uint64(fc.allocations), // #nosec G115 -- a test counter
		RestoreID:    req.GetRestoreOperationId(),
		RestoreEpoch: req.GetRestoreEpoch(),
		NowMillis:    req.GetTimestampMillis(),
	}
	for _, observed := range req.GetObserved() {
		request.Observed = append(request.Observed, state.ProcessDefinitionAllocation{
			Key: observed.GetKey(), Version: observed.GetVersion(), Checksum: observed.GetChecksum(), VersionTag: observed.GetVersionTag(),
		})
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
	err = fc.guards[partitionId].RunDeployment(ctx, partition.RestoreToken{OperationID: req.GetRestoreOperationId(), Epoch: req.GetRestoreEpoch()}, func() error {
		engine := fc.engines[partitionId]
		if _, err := engine.DeployProcessDefinition(ctx, req.GetData(), req.GetKey(), req.GetVersion()); err != nil {
			return zenerr.TechnicalError(fmt.Errorf("failed to deploy process definition: %w", err))
		}
		if req.GetRegisterProcessDefinitionSubscriptions() {
			if err := engine.RegisterProcessDefinitionSubscriptions(ctx, req.GetKey()); err != nil {
				return zenerr.TechnicalError(err)
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, partition.ErrDeploymentGenerationChanged) {
			// what the server answers for a fan-out that predates a restore
			return zenerr.Conflict(err)
		}
		return err
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
