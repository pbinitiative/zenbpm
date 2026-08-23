package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var lastMessageEventTestSuffix atomic.Int64

func messageEventTestSuffix() int64 {
	for {
		previous := lastMessageEventTestSuffix.Load()
		next := max(time.Now().UnixNano(), previous+1)
		if lastMessageEventTestSuffix.CompareAndSwap(previous, next) {
			return next
		}
	}
}

func readBPMNTestCaseFile(t testing.TB, filepathn string) []byte {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	loc := filepath.Join(wd, filepathn)

	content, err := os.ReadFile(loc)
	require.NoError(t, err)
	return content
}

// replaceInBPMNFixture rewrites a fixture and fails the test if the key is missing.
//
// These tests rewrite fixtures to get per-run unique process ids, message names and correlation keys.
// A plain strings.Replace silently does nothing once a fixture is reformatted, its attribute order
// changes, or an editor re-escapes the XML — leaving the test correlating against a name that is not
// in the deployed model, or asserting against a different process than it thinks. Requiring the key
// to be present turns those silent false positives into a loud failure at the point of the change.
func replaceInBPMNFixture(t testing.TB, filename string, content string, oldValue string, newValue string) string {
	t.Helper()

	require.Contains(t, content, oldValue,
		"fixture %s must contain %q so it can be rewritten for this test", filename, oldValue)
	return strings.ReplaceAll(content, oldValue, newValue)
}

// requireInterruptingBoundary flips the fixture's single non-interrupting boundary event to an
// interrupting one. The exact count is asserted because silently flipping zero of them would leave the
// test asserting non-interrupting behaviour under an "interrupting" name, and flipping one of several
// would pick an arbitrary boundary event.
func requireInterruptingBoundary(t testing.TB, filename string, content string) string {
	t.Helper()

	const nonInterrupting = `cancelActivity="false"`
	require.Equal(t, 1, strings.Count(content, nonInterrupting),
		"fixture %s must contain exactly one %s to be deployed as an interrupting variant", filename, nonInterrupting)
	return strings.Replace(content, nonInterrupting, `cancelActivity="true"`, 1)
}

func deployMessageBoundaryDefinition(t testing.TB, filename string, baseProcessID string) (int64, string, string) {
	return deployMessageBoundaryDefinitionWithInterrupting(t, filename, baseProcessID, false)
}

func deployInterruptingMessageBoundaryDefinition(t testing.TB, filename string, baseProcessID string) (int64, string, string) {
	return deployMessageBoundaryDefinitionWithInterrupting(t, filename, baseProcessID, true)
}

func deployMessageBoundaryDefinitionWithInterrupting(t testing.TB, filename string, baseProcessID string, interrupting bool) (int64, string, string) {
	t.Helper()

	suffix := messageEventTestSuffix()
	processID := fmt.Sprintf("%s-%d", baseProcessID, suffix)
	messageName := fmt.Sprintf("%s-ref-%d", baseProcessID, suffix)
	correlationKey := fmt.Sprintf("%s-key-%d", baseProcessID, suffix)
	content := string(readBPMNTestCaseFile(t, filename))
	if interrupting {
		content = requireInterruptingBoundary(t, filename, content)
	}
	content = replaceInBPMNFixture(t, filename, content,
		fmt.Sprintf(`bpmn:process id="%s"`, baseProcessID), fmt.Sprintf(`bpmn:process id="%s"`, processID))
	content = replaceInBPMNFixture(t, filename, content,
		`name="message"`, fmt.Sprintf(`name="%s"`, messageName))
	content = replaceInBPMNFixture(t, filename, content,
		`correlationKey="=correlationKey"`, fmt.Sprintf(`correlationKey="=&#34;%s&#34;"`, correlationKey))

	return deployBPMNTestCaseContent(t, filename, []byte(content)), messageName, correlationKey
}

func deployTwoMessageBoundaryDefinition(t testing.TB, filename string, baseProcessID string) (int64, string, string, string, string) {
	t.Helper()

	suffix := messageEventTestSuffix()
	processID := fmt.Sprintf("%s-%d", baseProcessID, suffix)
	messageAName := fmt.Sprintf("%s-a-ref-%d", baseProcessID, suffix)
	messageBName := fmt.Sprintf("%s-b-ref-%d", baseProcessID, suffix)
	correlationKeyA := fmt.Sprintf("%s-a-key-%d", baseProcessID, suffix)
	correlationKeyB := fmt.Sprintf("%s-b-key-%d", baseProcessID, suffix)
	content := string(readBPMNTestCaseFile(t, filename))
	content = replaceInBPMNFixture(t, filename, content,
		fmt.Sprintf(`bpmn:process id="%s"`, baseProcessID), fmt.Sprintf(`bpmn:process id="%s"`, processID))
	content = replaceInBPMNFixture(t, filename, content,
		`name="messageA"`, fmt.Sprintf(`name="%s"`, messageAName))
	content = replaceInBPMNFixture(t, filename, content,
		`name="messageB"`, fmt.Sprintf(`name="%s"`, messageBName))
	content = replaceInBPMNFixture(t, filename, content,
		`correlationKey="=correlationKeyA"`, fmt.Sprintf(`correlationKey="=&#34;%s&#34;"`, correlationKeyA))
	content = replaceInBPMNFixture(t, filename, content,
		`correlationKey="=correlationKeyB"`, fmt.Sprintf(`correlationKey="=&#34;%s&#34;"`, correlationKeyB))

	definitionKey := deployBPMNTestCaseContent(t, filename, []byte(content))
	return definitionKey, messageAName, correlationKeyA, messageBName, correlationKeyB
}

func createMessageBoundaryInstance(t testing.TB, definitionKey int64) zenclient.ProcessInstance {
	t.Helper()

	processInstance, err := createProcessInstance(t, &definitionKey, map[string]any{})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})
	return processInstance
}

func deployBPMNTestCaseContent(t testing.TB, filename string, content []byte) int64 {
	t.Helper()

	response, err := deployDefinitionFromBytes(t, content, filename)
	require.NoError(t, err)

	if response.JSON201 != nil {
		require.NotZero(t, response.JSON201.ProcessDefinitionKey)
		return response.JSON201.ProcessDefinitionKey
	}
	require.NotNil(t, response.JSON200)
	require.NotZero(t, response.JSON200.ProcessDefinitionKey)
	return response.JSON200.ProcessDefinitionKey
}

func waitForProcessInstancesByBPMNProcessID(t testing.TB, bpmnProcessID string, expectedCount int) []zenclient.ProcessInstancesSimple {
	t.Helper()

	var instances []zenclient.ProcessInstancesSimple
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		instances = processInstancesByBPMNProcessID(t, bpmnProcessID)
		assert.Len(collect, instances, expectedCount)
	}, 10*time.Second, 100*time.Millisecond, "expected %d process instances for BPMN process id %s", expectedCount, bpmnProcessID)

	return instances
}

func processInstancesByBPMNProcessID(t testing.TB, bpmnProcessID string) []zenclient.ProcessInstancesSimple {
	t.Helper()

	size := int32(100)
	response, err := app.restClient.GetProcessInstancesWithResponse(t.Context(), &zenclient.GetProcessInstancesParams{
		BpmnProcessId: &bpmnProcessID,
		Size:          &size,
	})
	require.NoError(t, err)
	require.NotNil(t, response.JSON200)

	instances := make([]zenclient.ProcessInstancesSimple, 0, response.JSON200.TotalCount)
	for _, partition := range response.JSON200.Partitions {
		instances = append(instances, partition.Items...)
	}
	return instances
}
