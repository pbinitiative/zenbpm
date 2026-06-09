package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
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

	resp, err := app.NewRequest(t).
		WithPath("/v1/process-definitions").
		WithMethod("POST").
		WithMultipartBody(file, filename).
		DoOk()
	if err != nil {
		return result, fmt.Errorf("failed to deploy process definition: %s %w", string(resp), err)
	}
	if err = json.Unmarshal(resp, &result); err != nil {
		return result, fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return result, nil
}

const callActivityErrorBoundaryChildProcessId = "simple_task_for_call_activity_error_boundary_event"

func deployCallActivityErrorBoundaryTestDataProcessDefinitions(t testing.TB, parentFilename string) int64 {
	t.Helper()

	uniqueSuffix := time.Now().UnixNano()
	childProcessId := fmt.Sprintf("%s-%d", callActivityErrorBoundaryChildProcessId, uniqueSuffix)
	childJobType := fmt.Sprintf("call-activity-child-%d", uniqueSuffix)

	_, err := deployTestDataDefinitionWithJobType(
		t,
		"testdata/call_activity/call_activity_with_error_boundary_event_child_process.bpmn",
		childProcessId,
		map[string]string{
			"TestType": childJobType,
		},
	)
	require.NoError(t, err)

	parentProcessId := fmt.Sprintf("call-activity-error-boundary-parent-%d", uniqueSuffix)
	parentDefinition, err := deployTestDataDefinition(
		t,
		parentFilename,
		parentProcessId,
		nil,
		map[string]string{
			callActivityErrorBoundaryChildProcessId: childProcessId,
		},
	)
	require.NoError(t, err)
	require.NotZero(t, parentDefinition.ProcessDefinitionKey)
	return parentDefinition.ProcessDefinitionKey
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
