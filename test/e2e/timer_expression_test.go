package e2e

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	timerExpressionCatchEventBpmn = "testdata/timer/timer-expression-catch-event.bpmn"
	// the fixtures of the engine's own tests of timer expressions
	timerExpressionBoundaryCycleBpmn        = "../../pkg/bpmn/test-cases/timer_expression/timer-expression-boundary-cycle.bpmn"
	timerExpressionEventSubprocessCycleBpmn = "../../pkg/bpmn/test-cases/timer_expression/timer-expression-event-subprocess-cycle.bpmn"
	timerExpressionProcessStartBpmn         = "../../pkg/bpmn/test-cases/timer_expression/timer-expression-process-start.bpmn"
	// the timer value of the fixture, which a test replaces to vary it
	timerExpressionOfFixture = `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=dueAt</bpmn:timeDate>`
)

func TestTimerExpression(t *testing.T) {
	t.Run("the timer of an instance is due at what its timeDate expression evaluates to", func(t *testing.T) {
		_, content := uniqueTimerExpressionDefinition(t)
		definitionKey := deployTimerExpressionDefinition(t, content)

		instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"dueAt": "2099-11-30T14:30:00+01:00"})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

		assertTimerSubscriptionDueDate(t, instance.Key, "timer-catch-event", time.Date(2099, 11, 30, 13, 30, 0, 0, time.UTC))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
	})

	t.Run("the timer is due at what a computed FEEL date and time evaluates to", func(t *testing.T) {
		_, content := uniqueTimerExpressionDefinition(t)
		content = bytes.ReplaceAll(content, []byte(timerExpressionOfFixture),
			[]byte(`<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=date and time(sentAt) + duration("P3D")</bpmn:timeDate>`))
		definitionKey := deployTimerExpressionDefinition(t, content)

		instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"sentAt": "2099-11-30T14:30:00+01:00"})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

		assertTimerSubscriptionDueDate(t, instance.Key, "timer-catch-event", time.Date(2099, 12, 3, 13, 30, 0, 0, time.UTC))
	})

	t.Run("an expression which does not evaluate is an incident, resolved once the variable is set", func(t *testing.T) {
		_, content := uniqueTimerExpressionDefinition(t)
		definitionKey := deployTimerExpressionDefinition(t, content)

		instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"unrelated": true})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)
		incidents, err := getProcessInstanceIncidents(t, instance.Key)
		require.NoError(t, err)
		require.Len(t, incidents, 1)
		assert.Equal(t, "timer-catch-event", incidents[0].ElementId)
		assert.Contains(t, incidents[0].Message, `timeDate expression "=dueAt" of element timer-catch-event evaluated to null`)
		assertProcessInstanceHasNoTimerSubscriptions(t, instance.Key)

		require.NoError(t, updateProcessInstanceVariables(t, instance.Key, map[string]any{"dueAt": "2099-11-30T14:30:00+01:00"}))
		resolveIncident(t, incidents[0].Key)

		assertTimerSubscriptionDueDate(t, instance.Key, "timer-catch-event", time.Date(2099, 11, 30, 13, 30, 0, 0, time.UTC))
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
	})

	t.Run("a duration which is negative makes the timer due at once", func(t *testing.T) {
		_, content := uniqueTimerExpressionDefinition(t)
		content = bytes.ReplaceAll(content, []byte(timerExpressionOfFixture),
			[]byte(`<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=date and time(deadline) - now()</bpmn:timeDuration>`))
		definitionKey := deployTimerExpressionDefinition(t, content)

		instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"deadline": "2020-01-01T00:00:00Z"})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

		// the deadline has passed: the timer fires and the instance runs to its end
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateCompleted)
		incidents, err := getProcessInstanceIncidents(t, instance.Key)
		require.NoError(t, err)
		assert.Empty(t, incidents)
	})

	t.Run("a duration whose units have different signs is what it amounts to", func(t *testing.T) {
		_, content := uniqueTimerExpressionDefinition(t)
		content = bytes.ReplaceAll(content, []byte(timerExpressionOfFixture),
			[]byte(`<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=duration(limit) - duration("PT30M")</bpmn:timeDuration>`))
		definitionKey := deployTimerExpressionDefinition(t, content)

		instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"limit": "PT1H"})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			subscription, found, err := findTimerSubscription(t, instance.Key, "timer-catch-event")
			if !assert.NoError(collect, err) || !assert.True(collect, found) || !assert.NotNil(collect, subscription.DueDate) {
				return
			}
			assert.Equal(collect, 30*time.Minute, subscription.DueDate.Sub(subscription.CreatedAt).Round(time.Second))
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("a cycle a variable holds which no timer can follow is an incident, and the instance answers", func(t *testing.T) {
		_, content := uniqueTimerExpressionDefinition(t)
		content = bytes.ReplaceAll(content, []byte(timerExpressionOfFixture),
			[]byte(`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">=cycle</bpmn:timeCycle>`))
		definitionKey := deployTimerExpressionDefinition(t, content)

		for cycle, message := range map[string]string{
			// a period which wraps around to no time at all: stepping by it would never end
			"R/PT2251799813685248H": "longer than a cycle can step by",
			// a first timer in the year 11000, which the API could not hand out
			"R3/P9000Y": `evaluated to "R3/P9000Y", which puts the timer into the year`,
			// nothing left to wait for, on an element whose token waits
			"R1/2020-01-01T00:00:00Z/PT1H": "a cycle which has no occurrence left",
		} {
			started := time.Now()
			instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"cycle": cycle})
			t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })
			assert.Less(t, time.Since(started), 10*time.Second, cycle)

			waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateFailed)
			incidents, err := getProcessInstanceIncidents(t, instance.Key)
			require.NoError(t, err, cycle)
			require.Len(t, incidents, 1, cycle)
			assert.Contains(t, incidents[0].Message, message, cycle)
			assertProcessInstanceHasNoTimerSubscriptions(t, instance.Key)

			require.NoError(t, updateProcessInstanceVariables(t, instance.Key, map[string]any{"cycle": "R1/PT1H"}))
			resolveIncident(t, incidents[0].Key)
			assertTimerSubscriptionState(t, instance.Key, "timer-catch-event", zenclient.EventSubscriptionStateActive)
		}
	})

	t.Run("the timers of an instance are listed whatever moment of the year 9999 its variable names", func(t *testing.T) {
		_, content := uniqueTimerExpressionDefinition(t)
		definitionKey := deployTimerExpressionDefinition(t, content)

		// In the last hours of 9999 in UTC it is the year 10000 east of it, and
		// the API writes a moment in the zone of the engine. Wherever the engine
		// runs, the instance either has a timer the API can write or an incident.
		for _, dueAt := range []string{"9999-12-31T11:00:00Z", "9999-12-31T23:59:59Z", "9999-12-31T23:59:59-12:00"} {
			instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"dueAt": dueAt})
			t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				subscriptions, err := getProcessInstanceTimerSubscriptions(t, instance.Key)
				if !assert.NoError(collect, err, "the timers of the instance are listed") {
					return
				}
				incidents, err := getProcessInstanceIncidents(t, instance.Key)
				if !assert.NoError(collect, err) {
					return
				}
				assert.Equal(collect, 1, len(subscriptions)+len(incidents), "a timer or an incident")
				for _, incident := range incidents {
					assert.Contains(collect, incident.Message, "a timer is due in the years 1 to 9999")
				}
			}, 5*time.Second, 100*time.Millisecond, dueAt)
		}
	})

	t.Run("CreateProcessDefinition - 400 for a timer expression which does not parse", func(t *testing.T) {
		processID, content := uniqueTimerExpressionDefinition(t)
		content = bytes.ReplaceAll(content, []byte(timerExpressionOfFixture),
			[]byte(`<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=dueAt +</bpmn:timeDate>`))

		resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(content))

		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400, "unexpected response body: %s", string(resp.Body))
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
		assert.Contains(t, resp.JSON400.Message, `invalid timer definition of element id="timer-catch-event"`)
		assert.Contains(t, resp.JSON400.Message, `timeDate expression "=dueAt +" of element timer-catch-event is not a valid FEEL expression`)
		assert.Empty(t, getProcessDefinitionVersions(t, processID, false), "a refused definition must not be stored")
	})

	t.Run("CreateProcessDefinition - 400 for the start event of a process whose timer needs a variable", func(t *testing.T) {
		processID, content := uniqueTimerExpressionDefinition(t)
		const startEventEnd = "</bpmn:startEvent>"
		require.Contains(t, string(content), startEventEnd)
		content = bytes.Replace(content, []byte(startEventEnd), []byte(
			`<bpmn:timerEventDefinition id="StartTimer"><bpmn:timeDate xsi:type="bpmn:tFormalExpression">=startAt</bpmn:timeDate></bpmn:timerEventDefinition>`+startEventEnd), 1)

		resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(content))

		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400, "unexpected response body: %s", string(resp.Body))
		assert.Contains(t, resp.JSON400.Message, `timeDate expression "=startAt" of element StartEvent_1 evaluated to null`)
		assert.Contains(t, resp.JSON400.Message, "evaluated without variables")
		assert.Empty(t, getProcessDefinitionVersions(t, processID, false), "a refused definition must not be stored")
	})
}

// TestTimeCycleExpression runs cycles whose expression is evaluated once on the
// storage of a partition: every timer after the first takes the cycle from the
// row of the timer before it, not from the variables.
func TestTimeCycleExpression(t *testing.T) {
	t.Run("a boundary cycle goes on as it began after its variable changes", func(t *testing.T) {
		_, content := uniqueDefinitionOf(t, timerExpressionBoundaryCycleBpmn, "timer-expression-boundary-cycle")
		definitionKey := deployTimerExpressionDefinition(t, content)

		instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"cycle": "R3/PT1S"})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })
		assertTimerSubscriptionState(t, instance.Key, "timer-boundary-event", zenclient.EventSubscriptionStateActive)
		// what the variable holds from now on is no cycle at all
		require.NoError(t, updateProcessInstanceVariables(t, instance.Key, map[string]any{"cycle": "not a cycle"}))

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			subscriptions, err := getProcessInstanceTimerSubscriptions(t, instance.Key)
			if !assert.NoError(collect, err) {
				return
			}
			fired, waiting := 0, 0
			for _, subscription := range subscriptions {
				if subscription.ElementId != "timer-boundary-event" {
					continue
				}
				switch subscription.State {
				case zenclient.EventSubscriptionStateCompleted:
					fired++
				case zenclient.EventSubscriptionStateActive:
					waiting++
				}
			}
			assert.Equal(collect, 3, fired, "R3 fires three times")
			assert.Zero(collect, waiting, "and no fourth timer waits")
		}, 20*time.Second, 200*time.Millisecond)
		incidents, err := getProcessInstanceIncidents(t, instance.Key)
		require.NoError(t, err)
		assert.Empty(t, incidents)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
	})

	t.Run("the cycle of a process start event is evaluated with the deployment and creates its instances", func(t *testing.T) {
		processID, content := uniqueDefinitionOf(t, timerExpressionProcessStartBpmn, "timer-expression-process-start")
		const fixtureTimer = `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=now() + duration("PT1H")</bpmn:timeDate>`
		require.Contains(t, string(content), fixtureTimer)
		content = bytes.ReplaceAll(content, []byte(fixtureTimer),
			[]byte(`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">="R2/PT" + string(1) + "S"</bpmn:timeCycle>`))
		definitionKey := deployTimerExpressionDefinition(t, content)

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			assert.Len(collect, listParentInstances(t, processID), 2, "R2 creates two instances")
		}, 20*time.Second, 200*time.Millisecond)
		instances := listParentInstances(t, processID)
		for _, instance := range instances {
			t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })
		}

		// Both timers carry the cycle the expression evaluated to, read back from
		// the storage of the partition, and the cycle is at its end: no third
		// timer waits to create a third instance.
		store := mustGetPartitionStore(t, instances[0].Key)
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			fired, err := store.FindProcessDefinitionTimers(t.Context(), definitionKey, bpmnruntime.TimerStateTriggered)
			if !assert.NoError(collect, err) || !assert.Len(collect, fired, 2) {
				return
			}
			for _, timer := range fired {
				assert.Equal(collect, "R2/PT1S", timer.ResolvedCycle)
			}
			waiting, err := store.FindProcessDefinitionTimers(t.Context(), definitionKey, bpmnruntime.TimerStateCreated)
			if assert.NoError(collect, err) {
				assert.Empty(collect, waiting)
			}
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("an event subprocess timer which still does not evaluate refuses to be resolved until its variable is set", func(t *testing.T) {
		_, content := uniqueDefinitionOf(t, timerExpressionEventSubprocessCycleBpmn, "timer-expression-event-subprocess-cycle")
		definitionKey := deployTimerExpressionDefinition(t, content)

		instance := createProcessInstanceWithVariables(t, definitionKey, map[string]any{"unrelated": true})
		t.Cleanup(func() { cleanupOwnedProcessInstance(t, instance.Key) })

		// the instance runs although the timer of its event subprocess could not be created
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		var incidentKey int64
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			incidents, err := getProcessInstanceIncidents(t, instance.Key)
			if !assert.NoError(collect, err) || !assert.Len(collect, incidents, 1) {
				return
			}
			assert.Equal(collect, "eventSubprocessCycleStart", incidents[0].ElementId)
			assert.Contains(collect, incidents[0].Message, `timeCycle expression "=cycle" of element eventSubprocessCycleStart evaluated to null`)
			assert.NotContains(collect, incidents[0].Message, "recoverable event subprocess subscription")
			incidentKey = incidents[0].Key
		}, 5*time.Second, 100*time.Millisecond)

		response, err := app.restClient.ResolveIncidentWithResponse(t.Context(), incidentKey, zenclient.ResolveIncidentJSONRequestBody{})
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, response.StatusCode(), "unexpected response body: %s", string(response.Body))
		assert.Contains(t, string(response.Body), "evaluated to null")
		assertProcessInstanceHasNoTimerSubscriptions(t, instance.Key)

		require.NoError(t, updateProcessInstanceVariables(t, instance.Key, map[string]any{"cycle": "R2/PT1H"}))
		resolveIncident(t, incidentKey)
		assertTimerSubscriptionState(t, instance.Key, "eventSubprocessCycleStart", zenclient.EventSubscriptionStateActive)
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
	})
}

func TestTimerExpressionOfProcessStartEventDeployment(t *testing.T) {
	t.Run("CreateProcessDefinition - 400 for a timeDuration expression on the start event of a process", func(t *testing.T) {
		processID, content := uniqueDefinitionOf(t, timerExpressionProcessStartBpmn, "timer-expression-process-start")
		const fixtureTimer = `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=now() + duration("PT1H")</bpmn:timeDate>`
		require.Contains(t, string(content), fixtureTimer)
		content = bytes.ReplaceAll(content, []byte(fixtureTimer),
			[]byte(`<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=duration("PT1H")</bpmn:timeDuration>`))

		resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(content))

		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400, "unexpected response body: %s", string(resp.Body))
		assert.Contains(t, resp.JSON400.Message, "is not supported on the start event of a process, which takes a timeDate or a timeCycle")
		assert.Empty(t, getProcessDefinitionVersions(t, processID, false), "a refused definition must not be stored")
	})

	t.Run("CreateProcessDefinition - 400 for a start event cycle no timer can follow", func(t *testing.T) {
		const fixtureTimer = `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=now() + duration("PT1H")</bpmn:timeDate>`
		for cycle, message := range map[string]string{
			"R3/P9000Y":             "which puts the timer into the year",
			"R/PT2251799813685248H": "longer than a cycle can step by",
		} {
			processID, content := uniqueDefinitionOf(t, timerExpressionProcessStartBpmn, "timer-expression-process-start")
			require.Contains(t, string(content), fixtureTimer)
			content = bytes.ReplaceAll(content, []byte(fixtureTimer),
				[]byte(`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">="`+cycle+`"</bpmn:timeCycle>`))

			resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(content))

			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode(), cycle)
			require.NotNil(t, resp.JSON400, "unexpected response body: %s", string(resp.Body))
			assert.Contains(t, resp.JSON400.Message, message)
			assert.Empty(t, getProcessDefinitionVersions(t, processID, false), "a refused definition must not be stored")
		}
	})

	t.Run("CreateProcessDefinition - a start event cycle counted back over many repetitions is deployed at once", func(t *testing.T) {
		_, content := uniqueDefinitionOf(t, timerExpressionProcessStartBpmn, "timer-expression-process-start")
		const fixtureTimer = `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=now() + duration("PT1H")</bpmn:timeDate>`
		require.Contains(t, string(content), fixtureTimer)
		content = bytes.ReplaceAll(content, []byte(fixtureTimer),
			[]byte(`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">="R1000000000/PT1S/2099-12-03T09:00:00Z"</bpmn:timeCycle>`))

		started := time.Now()
		resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(content))

		require.NoError(t, err)
		require.NotNil(t, resp.JSON201, "unexpected response body: %s", string(resp.Body))
		assert.Less(t, time.Since(started), 10*time.Second, "the repetitions are not walked through")
	})

	t.Run("CreateProcessDefinition - 400 for a timer expression longer than one may be", func(t *testing.T) {
		processID, content := uniqueTimerExpressionDefinition(t)
		content = bytes.ReplaceAll(content, []byte(timerExpressionOfFixture),
			[]byte(`<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=dueAt`+strings.Repeat(` + duration(grace)`, 250)+`</bpmn:timeDate>`))

		resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(content))

		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400, "unexpected response body: %s", string(resp.Body))
		assert.Contains(t, resp.JSON400.Message, "a timer expression has at most 4000")
		assert.Empty(t, getProcessDefinitionVersions(t, processID, false), "a refused definition must not be stored")
	})

	t.Run("CreateProcessDefinition - 400 for a start event expression which runs on and on", func(t *testing.T) {
		processID, content := uniqueDefinitionOf(t, timerExpressionProcessStartBpmn, "timer-expression-process-start")
		const fixtureTimer = `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=now() + duration("PT1H")</bpmn:timeDate>`
		require.Contains(t, string(content), fixtureTimer)
		content = bytes.ReplaceAll(content, []byte(fixtureTimer),
			[]byte(`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">="R/PT" + string(count(for i in 1..1000000000 return i)) + "H"</bpmn:timeCycle>`))

		started := time.Now()
		resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), "application/octet-stream", bytes.NewReader(content))

		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400, "unexpected response body: %s", string(resp.Body))
		assert.Contains(t, resp.JSON400.Message, "took too long to evaluate")
		assert.Less(t, time.Since(started), 30*time.Second, "the deployment is refused instead of waiting for the expression")
		assert.Empty(t, getProcessDefinitionVersions(t, processID, false), "a refused definition must not be stored")
	})
}

func uniqueTimerExpressionDefinition(t *testing.T) (string, []byte) {
	t.Helper()

	return uniqueDefinitionOf(t, timerExpressionCatchEventBpmn, "timer-expression-catch-event")
}

// uniqueDefinitionOf reads a fixture and gives its process an id of this run,
// so that a run deploys a definition of its own.
func uniqueDefinitionOf(t *testing.T, path string, fixtureProcessID string) (string, []byte) {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	processID := fmt.Sprintf("%s-%d", fixtureProcessID, time.Now().UnixNano())
	require.Contains(t, string(content), `bpmn:process id="`+fixtureProcessID+`"`)
	content = bytes.ReplaceAll(content, []byte(`bpmn:process id="`+fixtureProcessID+`"`), []byte(`bpmn:process id="`+processID+`"`))
	content = bytes.ReplaceAll(content, []byte(`bpmnElement="`+fixtureProcessID+`"`), []byte(`bpmnElement="`+processID+`"`))
	return processID, content
}

func deployTimerExpressionDefinition(t *testing.T, content []byte) int64 {
	t.Helper()

	response := deployProcessDefinitionContent(t, content)
	require.NotNil(t, response.JSON201, "unexpected response body: %s", string(response.Body))
	return response.JSON201.ProcessDefinitionKey
}

func assertTimerSubscriptionDueDate(t testing.TB, processInstanceKey int64, elementID string, expected time.Time) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		subscription, found, err := findTimerSubscription(t, processInstanceKey, elementID)
		if !assert.NoError(collect, err) || !assert.Truef(collect, found, "timer subscription %s should exist", elementID) {
			return
		}
		assert.Equal(collect, zenclient.EventSubscriptionStateActive, subscription.State)
		if assert.NotNil(collect, subscription.DueDate) {
			assert.Truef(collect, expected.Equal(*subscription.DueDate), "due date %s, expected %s", subscription.DueDate, expected)
		}
	}, 5*time.Second, 100*time.Millisecond, "timer subscription %s should be due at %s", elementID, expected)
}
