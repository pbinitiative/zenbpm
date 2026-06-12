//go:build acceptance

package acceptance_test

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tokenExpectation struct {
	elementID string
	state     string
}

type ProcessCheckpoint struct {
	State        zenclient.ProcessInstanceState
	Variables    map[string]any
	ActiveTokens []tokenExpectation
	ActiveJobs   []string
	JobVariables map[string]map[string]any
	Incidents    bool
}

func assertProcessCheckpoint(t testing.TB, processInstanceKey int64, want ProcessCheckpoint) {
	t.Helper()
	require.NotEmpty(t, want.State,
		"assertProcessCheckpoint on instance %d: State must be set", processInstanceKey)

	assert.Eventually(t, func() bool {
		return getProcessInstance(t, processInstanceKey).State == want.State
	}, 15*time.Second, 200*time.Millisecond,
		"instance %d did not reach state %s", processInstanceKey, want.State)

	require.NotNil(t, want.Variables,
		"assertProcessCheckpoint on instance %d: Variables must be set explicitly; "+
			"use map[string]any{} to assert no variables", processInstanceKey)
	var ok bool
	ok = assert.Eventually(t, func() bool {
		return reflect.DeepEqual(
			getProcessInstance(t, processInstanceKey).Variables,
			want.Variables,
		)
	}, 10*time.Second, 200*time.Millisecond,
		"instance %d variables did not exactly match expected", processInstanceKey)
	if !ok {
		actual := getProcessInstance(t, processInstanceKey).Variables
		t.Logf("actual   variables on instance %d: %#v", processInstanceKey, actual)
		t.Logf("expected variables on instance %d: %#v", processInstanceKey, want.Variables)
	}

	ok = assert.Eventually(t, func() bool {
		instances := getProcessInstance(t, processInstanceKey).ActiveElementInstances
		if len(instances) != len(want.ActiveTokens) {
			return false
		}
		for _, token := range want.ActiveTokens {
			found := false
			for _, el := range instances {
				if el.ElementId == token.elementID && el.State == token.state {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}, 10*time.Second, 200*time.Millisecond,
		"instance %d active token set did not exactly match expected %v",
		processInstanceKey, want.ActiveTokens)
	if !ok {
		actual := getProcessInstance(t, processInstanceKey).ActiveElementInstances
		t.Logf("actual active tokens on instance %d: %s",
			processInstanceKey, formatElementInstances(actual))
	}

	ok = assert.Eventually(t, func() bool {
		active := activeJobElementIDs(t, processInstanceKey)
		if len(active) != len(want.ActiveJobs) {
			return false
		}
		expected := make(map[string]bool, len(want.ActiveJobs))
		for _, id := range want.ActiveJobs {
			expected[id] = true
		}
		for _, id := range active {
			if !expected[id] {
				return false
			}
		}
		return true
	}, 10*time.Second, 200*time.Millisecond,
		"instance %d active job set did not exactly match expected %v",
		processInstanceKey, want.ActiveJobs)
	if !ok {
		t.Logf("actual active jobs on instance %d: %v",
			processInstanceKey, activeJobElementIDs(t, processInstanceKey))
	}

	for elementID, vars := range want.JobVariables {
		var matchedJob *zenclient.Job
		assert.Eventually(t, func() bool {
			jobs := getInstanceJobs(t, processInstanceKey)
			for i := range jobs {
				if jobs[i].ElementId == elementID {
					matchedJob = &jobs[i]
					return true
				}
			}
			return false
		}, 10*time.Second, 200*time.Millisecond,
			"job for element %q on instance %d not found", elementID, processInstanceKey)

		if matchedJob != nil {
			for name, value := range vars {
				assert.Equal(t, value, matchedJob.InputVariables[name],
					"job input variable %q for element %q on instance %d",
					name, elementID, processInstanceKey)
			}
		}
	}

	if want.Incidents {
		assertHasIncidents(t, processInstanceKey)
	} else {
		assertNoIncidents(t, processInstanceKey)
	}
}

func formatElementInstances(instances []zenclient.ElementInstance) string {
	parts := make([]string, len(instances))
	for i, el := range instances {
		parts[i] = fmt.Sprintf("%s(%s)", el.ElementId, el.State)
	}
	return fmt.Sprintf("%v", parts)
}

func activeJobElementIDs(t testing.TB, processInstanceKey int64) []string {
	t.Helper()
	jobs := getInstanceJobs(t, processInstanceKey)
	var ids []string
	for _, j := range jobs {
		if j.State == zenclient.JobStateActive {
			ids = append(ids, j.ElementId)
		}
	}
	return ids
}

// --- Deployment helpers ---

func resourcesDir(t testing.TB) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err, "getting current working directory")
	return filepath.Join(wd, "testdata")
}

func deployBpmn(t testing.TB, relPath string) *zenclient.CreateProcessDefinitionResponse {
	t.Helper()
	loc := filepath.Join(resourcesDir(t), relPath)
	file, err := os.ReadFile(loc)
	require.NoError(t, err, "reading BPMN file %s", loc)

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("resource", filepath.Base(relPath))
	require.NoError(t, err)
	_, err = part.Write(file)
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(
		t.Context(),
		mw.FormDataContentType(),
		&body,
	)
	require.NoError(t, err)
	require.True(t, resp.StatusCode() == http.StatusOK || resp.StatusCode() == http.StatusCreated,
		"deploy BPMN %s returned %d", relPath, resp.StatusCode())
	return resp
}

func deployDmn(t testing.TB, relPath string) *zenclient.CreateDmnResourceDefinitionResponse {
	t.Helper()
	loc := filepath.Join(resourcesDir(t), relPath)
	file, err := os.Open(loc)
	require.NoError(t, err, "opening DMN file %s", loc)
	defer file.Close()

	resp, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
		t.Context(),
		"application/xml",
		file,
	)
	require.NoError(t, err)
	require.True(t, resp.StatusCode() == http.StatusOK || resp.StatusCode() == http.StatusCreated,
		"deploy DMN %s returned %d", relPath, resp.StatusCode())
	return resp
}

// --- Process instance helpers ---

func startProcess(t testing.TB, bpmnProcessId string, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()
	resp, err := app.restClient.CreateProcessInstanceWithResponse(t.Context(),
		zenclient.CreateProcessInstanceJSONRequestBody{
			BpmnProcessId: &bpmnProcessId,
			Variables:     &variables,
		})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode(),
		"start process %s returned %d", bpmnProcessId, resp.StatusCode())
	require.NotNil(t, resp.JSON201)
	return *resp.JSON201
}

func getProcessInstance(t testing.TB, key int64) zenclient.ProcessInstance {
	t.Helper()
	resp, err := app.restClient.GetProcessInstanceWithResponse(t.Context(), key)
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	return *resp.JSON200
}

func waitForState(t testing.TB, key int64, expected zenclient.ProcessInstanceState) {
	t.Helper()
	assert.Eventually(t, func() bool {
		return getProcessInstance(t, key).State == expected
	}, 15*time.Second, 200*time.Millisecond,
		"process instance %d did not reach state %s", key, expected)
}

func cancelProcess(t testing.TB, key int64) {
	t.Helper()
	resp, err := app.restClient.CancelProcessInstanceWithResponse(context.Background(), key)
	assert.NoError(t, err)
	sc := resp.StatusCode()
	assert.True(t, sc == http.StatusNoContent || sc == http.StatusConflict,
		"cancel process %d returned %d", key, sc)
}

func getChildInstances(t testing.TB, parentKey int64) []zenclient.ProcessInstancesSimple {
	t.Helper()
	raw, err := app.restClient.GetProcessInstancesWithResponse(t.Context(),
		&zenclient.GetProcessInstancesParams{
			ParentProcessInstanceKey: &parentKey,
		})
	require.NoError(t, err)
	require.NotNil(t, raw.JSON200)
	if len(raw.JSON200.Partitions) == 0 {
		return nil
	}
	return raw.JSON200.Partitions[0].Items
}

func findProcessDefinition(t testing.TB, bpmnProcessId string) zenclient.ProcessDefinitionSimple {
	t.Helper()
	resp, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(),
		&zenclient.GetProcessDefinitionsParams{})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	for _, d := range resp.JSON200.Items {
		if d.BpmnProcessId == bpmnProcessId {
			return d
		}
	}
	t.Fatalf("process definition %q not found", bpmnProcessId)
	return zenclient.ProcessDefinitionSimple{}
}

// --- Job helpers ---

func waitForJob(t testing.TB, processInstanceKey int64, elementId string) zenclient.Job {
	t.Helper()
	var found zenclient.Job
	require.Eventually(t, func() bool {
		jobs := getInstanceJobs(t, processInstanceKey)
		for _, j := range jobs {
			if j.ElementId == elementId && j.State == zenclient.JobStateActive {
				found = j
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond,
		"job for element %s on instance %d not found", elementId, processInstanceKey)
	return found
}

func getInstanceJobs(t testing.TB, key int64) []zenclient.Job {
	t.Helper()
	raw, err := app.restClient.GetProcessInstanceJobsWithResponse(t.Context(), key, &zenclient.GetProcessInstanceJobsParams{})
	require.NoError(t, err)
	require.NotNil(t, raw.JSON200)
	return raw.JSON200.Items
}

func completeJob(t testing.TB, jobKey int64, variables map[string]any) {
	t.Helper()
	resp, err := app.restClient.CompleteJobWithResponse(t.Context(), jobKey,
		zenclient.CompleteJobJSONRequestBody{Variables: &variables})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode(),
		"complete job %d returned %d", jobKey, resp.StatusCode())
}

// --- History helpers ---

func historyElementIds(t testing.TB, processInstanceKey int64) []string {
	t.Helper()
	resp, err := app.restClient.GetHistoryWithResponse(t.Context(), processInstanceKey, &zenclient.GetHistoryParams{
		Page: ptr.To(int32(1)),
		Size: ptr.To(int32(-1)),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	if resp.JSON200.Items == nil {
		return nil
	}
	ids := make([]string, 0, len(*resp.JSON200.Items))
	for _, h := range *resp.JSON200.Items {
		ids = append(ids, h.ElementId)
	}
	return ids
}

func assertVisited(t testing.TB, processInstanceKey int64, want ...string) {
	t.Helper()
	ids := historyElementIds(t, processInstanceKey)
	t.Logf("history : %v", ids)
	for _, id := range want {
		assert.Contains(t, ids, id, "element %q should have been visited", id)
	}
}

func assertNotVisited(t testing.TB, processInstanceKey int64, notWant ...string) {
	t.Helper()
	ids := historyElementIds(t, processInstanceKey)
	for _, id := range notWant {
		assert.NotContains(t, ids, id, "element %q should NOT have been visited", id)
	}
}

// --- Variable helpers ---

func assertVariable(t testing.TB, key int64, name string, expected any) {
	t.Helper()
	inst := getProcessInstance(t, key)
	assert.Equal(t, expected, inst.Variables[name], "variable %q on instance %d", name, key)
}

func assertVariableContains(t testing.TB, key int64, name string, substr string) {
	t.Helper()
	inst := getProcessInstance(t, key)
	val, _ := inst.Variables[name].(string)
	assert.True(t, strings.Contains(val, substr),
		"variable %q = %q should contain %q", name, val, substr)
}

// --- Incident helpers ---

func getInstanceIncidents(t testing.TB, key int64) []zenclient.Incident {
	t.Helper()
	resp, err := app.restClient.GetIncidentsWithResponse(t.Context(), key,
		&zenclient.GetIncidentsParams{})
	require.NoError(t, err)
	require.NotNil(t, resp.JSON200)
	return resp.JSON200.Items
}

func assertNoIncidents(t testing.TB, key int64) {
	t.Helper()
	incidents := getInstanceIncidents(t, key)
	assert.Empty(t, incidents, "expected no incidents on process instance %d", key)
}

func assertHasIncidents(t testing.TB, key int64) {
	t.Helper()
	assert.Eventually(t, func() bool {
		return len(getInstanceIncidents(t, key)) > 0
	}, 10*time.Second, 200*time.Millisecond,
		"expected at least one incident on process instance %d", key)
}
