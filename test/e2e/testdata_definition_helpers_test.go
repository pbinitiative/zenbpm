package e2e

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func deployTestDataProcessDefinitionKey(t *testing.T, filePath string) int64 {
	t.Helper()

	definition := deployAndGetUniqueProcessDefinition(t, filePath)
	require.NotZero(t, definition.Key)
	return definition.Key
}

func deployTestDataDefinitionWithJobType(t testing.TB, filename string, processId string, jobTypeMap map[string]string) (public.CreateProcessDefinition201JSONResponse, error) {
	t.Helper()

	return deployTestDataDefinition(t, filename, processId, jobTypeMap, nil)
}

func deployTestDataDefinition(t testing.TB, filename string, processId string, jobTypeMap map[string]string, calledElementProcessIdMap map[string]string) (public.CreateProcessDefinition201JSONResponse, error) {
	t.Helper()

	result := public.CreateProcessDefinition201JSONResponse{}
	file, err := readE2ETestDataBPMN(filename)
	if err != nil {
		return result, err
	}

	re, err := regexp.Compile(`bpmn:process id="[^"]+"`)
	if err != nil {
		return result, err
	}
	file = re.ReplaceAll(file, fmt.Appendf([]byte{}, `bpmn:process id="%s"`, processId))

	for k, v := range jobTypeMap {
		template := `taskDefinition type="%s"`
		oldBytes := &bytes.Buffer{}
		_, err := fmt.Fprintf(oldBytes, template, k)
		require.NoError(t, err)
		newBytes := &bytes.Buffer{}
		_, err = fmt.Fprintf(newBytes, template, v)
		require.NoError(t, err)
		file = bytes.ReplaceAll(file, oldBytes.Bytes(), newBytes.Bytes())
	}

	for oldProcessId, newProcessId := range calledElementProcessIdMap {
		oldBytes := []byte(fmt.Sprintf(`calledElement processId="%s"`, oldProcessId))
		if !bytes.Contains(file, oldBytes) {
			return result, fmt.Errorf("failed to find calledElement processId %q in %s", oldProcessId, filename)
		}
		newBytes := []byte(fmt.Sprintf(`calledElement processId="%s"`, newProcessId))
		file = bytes.ReplaceAll(file, oldBytes, newBytes)
	}

	resp, err := deployDefinitionFromBytes(t, file)
	if err != nil {
		return result, err
	}
	switch {
	case resp.JSON201 != nil:
		result.ProcessDefinitionKey = resp.JSON201.ProcessDefinitionKey
	case resp.JSON200 != nil:
		result.ProcessDefinitionKey = resp.JSON200.ProcessDefinitionKey
	default:
		return result, fmt.Errorf("failed to deploy process definition %s: unexpected response %s", filename, resp.Status())
	}
	return result, nil
}

const callActivityChildProcessID = "simple_task_for_call_activity_error_boundary_event"

func deployCallActivityTestDataProcessDefinitions(t testing.TB, parentFilename string) int64 {
	t.Helper()

	uniqueSuffix := time.Now().UnixNano()
	childProcessID := fmt.Sprintf("%s-%d", callActivityChildProcessID, uniqueSuffix)
	childJobType := fmt.Sprintf("call-activity-child-%d", uniqueSuffix)

	_, err := deployTestDataDefinitionWithJobType(
		t,
		"testdata/call_activity/call_activity_with_error_boundary_event_child_process.bpmn",
		childProcessID,
		map[string]string{
			"TestType": childJobType,
		},
	)
	require.NoError(t, err)

	parentProcessID := fmt.Sprintf("call-activity-parent-%d", uniqueSuffix)
	parentDefinition, err := deployTestDataDefinition(
		t,
		parentFilename,
		parentProcessID,
		nil,
		map[string]string{
			callActivityChildProcessID: childProcessID,
		},
	)
	require.NoError(t, err)
	require.NotZero(t, parentDefinition.ProcessDefinitionKey)
	return parentDefinition.ProcessDefinitionKey
}

// createCallActivityInstance deploys the parent fixture together with its called child process,
// starts the parent with the given variables and waits for the called child instance.
func createCallActivityInstance(t testing.TB, filename string, variables map[string]any) (zenclient.ProcessInstance, zenclient.ProcessInstancesSimple) {
	t.Helper()

	parentDefinitionKey := deployCallActivityTestDataProcessDefinitions(t, filename)
	processInstance := createProcessInstanceWithVariables(t, parentDefinitionKey, variables)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	childInstance := waitForChildProcessInstance(t, processInstance.Key, 0)
	return processInstance, childInstance
}

func readE2ETestDataBPMN(filename string) ([]byte, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")

	if strings.HasPrefix(filename, "testdata/") {
		return os.ReadFile(filepath.Join(wd, "test", "e2e", filename))
	}

	return os.ReadFile(filepath.Join(wd, "test", "e2e", "testdata", filename))
}
