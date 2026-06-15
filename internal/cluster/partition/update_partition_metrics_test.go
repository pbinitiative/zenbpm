package partition

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	internalsql "github.com/pbinitiative/zenbpm/internal/sql"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type recordedMeasurement struct {
	value int64
	attrs []attribute.KeyValue
}

// fakeGauge embeds noop.Int64Gauge to satisfy the embedded.Int64Gauge marker
// interface, which contains an unexported method that cannot be implemented
// externally.
type fakeGauge struct {
	noop.Int64Gauge
	mu           sync.Mutex
	measurements []recordedMeasurement
}

func (g *fakeGauge) Record(_ context.Context, value int64, opts ...metric.RecordOption) {
	set := metric.NewRecordConfig(opts).Attributes()
	kvs := make([]attribute.KeyValue, 0, set.Len())
	for i := 0; i < set.Len(); i++ {
		kv, _ := set.Get(i)
		kvs = append(kvs, kv)
	}
	g.mu.Lock()
	g.measurements = append(g.measurements, recordedMeasurement{value: value, attrs: kvs})
	g.mu.Unlock()
}

func (g *fakeGauge) recorded() []recordedMeasurement {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]recordedMeasurement, len(g.measurements))
	copy(out, g.measurements)
	return out
}

func injectFakeMetrics(zpn *ZenPartitionNode) (jobsGauge, instancesGauge *fakeGauge) {
	jobsGauge = &fakeGauge{}
	instancesGauge = &fakeGauge{}
	zpn.metrics.jobsWaiting = jobsGauge
	zpn.metrics.processInstancesActive = instancesGauge
	return
}

// saveWaitingJob inserts the full FK chain required by the schema:
//
//	process_definition → process_instance (state=active) → job (state=waiting)
func saveWaitingJob(t *testing.T, zpn *ZenPartitionNode, key int64) {
	t.Helper()
	ctx := context.Background()

	if err := zpn.DB.Queries.SaveProcessDefinition(ctx, internalsql.SaveProcessDefinitionParams{
		Key:             key,
		Version:         1,
		BpmnProcessID:   "test-process",
		BpmnData:        "<definitions/>",
		BpmnChecksum:    []byte("checksum"),
		BpmnProcessName: "Test Process",
	}); err != nil {
		t.Fatalf("saveWaitingJob/SaveProcessDefinition: %v", err)
	}

	if err := zpn.DB.Queries.SaveProcessInstance(ctx, internalsql.SaveProcessInstanceParams{
		Key:                  key,
		ProcessDefinitionKey: key,
		CreatedAt:            time.Now().UnixMilli(),
		State:                1, // active
		Variables:            "{}",
	}); err != nil {
		t.Fatalf("saveWaitingJob/SaveProcessInstance: %v", err)
	}

	if err := zpn.DB.Queries.SaveJob(ctx, internalsql.SaveJobParams{
		Key:                key,
		ElementID:          "test-element",
		ElementInstanceKey: key,
		ProcessInstanceKey: key,
		Type:               "test-job",
		State:              1, // waiting
		CreatedAt:          time.Now().UnixMilli(),
		InputVariables:     "{}",
		ExecutionToken:     key,
		Assignee:           sql.NullString{},
	}); err != nil {
		t.Fatalf("saveWaitingJob/SaveJob: %v", err)
	}
}

// saveActiveProcessInstance does not insert a job row; it is used to test
// instance counts independently from job counts.
func saveActiveProcessInstance(t *testing.T, zpn *ZenPartitionNode, key int64) {
	t.Helper()
	ctx := context.Background()

	if err := zpn.DB.Queries.SaveProcessDefinition(ctx, internalsql.SaveProcessDefinitionParams{
		Key:             key,
		Version:         1,
		BpmnProcessID:   "test-process",
		BpmnData:        "<definitions/>",
		BpmnChecksum:    []byte("checksum"),
		BpmnProcessName: "Test Process",
	}); err != nil {
		t.Fatalf("saveActiveProcessInstance/SaveProcessDefinition: %v", err)
	}

	if err := zpn.DB.Queries.SaveProcessInstance(ctx, internalsql.SaveProcessInstanceParams{
		Key:                  key,
		ProcessDefinitionKey: key,
		CreatedAt:            time.Now().UnixMilli(),
		State:                1, // active
		Variables:            "{}",
	}); err != nil {
		t.Fatalf("saveActiveProcessInstance/SaveProcessInstance: %v", err)
	}
}

func TestUpdatePartitionMetrics_EmptyDB(t *testing.T) {
	zpn, _, _, _, _ := prepareTestSetup(t, false)
	defer zpn.Stop()

	jobsGauge, instancesGauge := injectFakeMetrics(zpn)

	zpn.updatePartitionMetrics()

	jobsMeasurements := jobsGauge.recorded()
	if len(jobsMeasurements) != 1 {
		t.Fatalf("expected 1 jobs measurement, got %d", len(jobsMeasurements))
	}
	if jobsMeasurements[0].value != 0 {
		t.Errorf("jobs waiting: want 0, got %d", jobsMeasurements[0].value)
	}

	instancesMeasurements := instancesGauge.recorded()
	if len(instancesMeasurements) != 1 {
		t.Fatalf("expected 1 instances measurement, got %d", len(instancesMeasurements))
	}
	if instancesMeasurements[0].value != 0 {
		t.Errorf("active instances: want 0, got %d", instancesMeasurements[0].value)
	}
}

func TestUpdatePartitionMetrics_CountsReflectData(t *testing.T) {
	zpn, _, _, _, _ := prepareTestSetup(t, false)
	defer zpn.Stop()

	saveWaitingJob(t, zpn, 1001)
	saveWaitingJob(t, zpn, 1002)
	saveWaitingJob(t, zpn, 1003)
	saveActiveProcessInstance(t, zpn, 2001)
	saveActiveProcessInstance(t, zpn, 2002)

	jobsGauge, instancesGauge := injectFakeMetrics(zpn)

	zpn.updatePartitionMetrics()

	jobsMeasurements := jobsGauge.recorded()
	if len(jobsMeasurements) != 1 {
		t.Fatalf("expected 1 jobs measurement, got %d", len(jobsMeasurements))
	}
	if jobsMeasurements[0].value != 3 {
		t.Errorf("jobs waiting: want 3, got %d", jobsMeasurements[0].value)
	}

	instancesMeasurements := instancesGauge.recorded()
	if len(instancesMeasurements) != 1 {
		t.Fatalf("expected 1 instances measurement, got %d", len(instancesMeasurements))
	}
	if instancesMeasurements[0].value != 5 {
		t.Errorf("active instances: want 5, got %d", instancesMeasurements[0].value)
	}
}

func TestUpdatePartitionMetrics_PartitionAttributeIsSet(t *testing.T) {
	zpn, _, _, _, _ := prepareTestSetup(t, false)
	defer zpn.Stop()

	jobsGauge, instancesGauge := injectFakeMetrics(zpn)

	zpn.updatePartitionMetrics()

	for _, measurements := range [][]recordedMeasurement{
		jobsGauge.recorded(),
		instancesGauge.recorded(),
	} {
		if len(measurements) == 0 {
			t.Fatal("expected at least one measurement")
		}
		var found bool
		for _, kv := range measurements[0].attrs {
			if string(kv.Key) == "partition" && kv.Value.AsInt64() == int64(zpn.PartitionId) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("measurement missing partition=%d attribute, got attrs: %v",
				zpn.PartitionId, measurements[0].attrs)
		}
	}
}

func TestUpdatePartitionMetrics_RecordsNotCalledOnQueryError(t *testing.T) {
	zpn, _, _, _, _ := prepareTestSetup(t, false)
	defer zpn.Stop()

	// Force DB queries to fail; Stop() will attempt Close again, which is harmless.
	zpn.DB.Store.Close(true)

	jobsGauge, instancesGauge := injectFakeMetrics(zpn)

	zpn.updatePartitionMetrics()

	if len(jobsGauge.recorded()) != 0 || len(instancesGauge.recorded()) != 0 {
		t.Error("expected no gauge recordings when DB queries fail")
	}
}

func TestUpdatePartitionMetrics_IdempotentOnRepeatedCalls(t *testing.T) {
	zpn, _, _, _, _ := prepareTestSetup(t, false)
	defer zpn.Stop()

	saveWaitingJob(t, zpn, 3001)

	jobsGauge, _ := injectFakeMetrics(zpn)

	const calls = 3
	for range calls {
		zpn.updatePartitionMetrics()
	}

	measurements := jobsGauge.recorded()
	if len(measurements) != calls {
		t.Fatalf("expected %d measurements after %d calls, got %d", calls, calls, len(measurements))
	}
	for i, m := range measurements {
		if m.value != 1 {
			t.Errorf("call %d: want value 1, got %d", i+1, m.value)
		}
	}
}
