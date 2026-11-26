package bpmn20

import (
	"encoding/xml"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeInfoText(t *testing.T) {
	for name, tc := range map[string]struct {
		xmlText      string
		text         string
		isExpression bool
	}{
		"a literal":                        {"PT1S", "PT1S", false},
		"a literal with space around":      {"\n      R3/PT10S\n    ", "R3/PT10S", false},
		"a cron literal":                   {"0 0 9 * * *", "0 0 9 * * *", false},
		"an expression":                    {"=dueAt", "=dueAt", true},
		"an expression with space around":  {"\n  = dueAt \n", "= dueAt", true},
		"an expression with entities":      {"=if a &lt; b &amp;&amp; c &gt; d then &quot;PT1S&quot; else x", `=if a < b && c > d then "PT1S" else x`, true},
		"an expression in a CDATA section": {`<![CDATA[=if a < b then "PT1S" else x]]>`, `=if a < b then "PT1S" else x`, true},
		"an expression with a comment":     {"=dueAt<!-- when it is due -->", "=dueAt", true},
		// a value set in code rather than read from a file may not be XML
		"an expression which is not XML": {"=if a < b then x else y", "=if a < b then x else y", true},
		"nothing":                        {"", "", false},
		"an equals sign after the start": {"R3/PT10S=", "R3/PT10S=", false},
	} {
		t.Run(name, func(t *testing.T) {
			info := &TTimeInfo{XMLText: tc.xmlText}
			assert.Equal(t, tc.text, info.Text())
			assert.Equal(t, tc.isExpression, info.IsExpression())
		})
	}

	var none *TTimeInfo
	assert.Empty(t, none.Text())
	assert.False(t, none.IsExpression())
}

func TestTimeInfoTextOfParsedDefinition(t *testing.T) {
	var definition struct {
		Timer TTimerEventDefinition `xml:"timerEventDefinition"`
	}
	require.NoError(t, xml.Unmarshal([]byte(`<event xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
		<timerEventDefinition id="timer">
			<timeDate xsi:type="tFormalExpression">=if priority &lt; 5 then early else late</timeDate>
			<timeDuration><![CDATA[=if a < b then "PT1S" else "PT2S"]]></timeDuration>
			<timeCycle>R3/PT10S</timeCycle>
		</timerEventDefinition>
	</event>`), &definition))

	assert.Equal(t, "=if priority < 5 then early else late", definition.Timer.TimeDate.Text())
	assert.Equal(t, `=if a < b then "PT1S" else "PT2S"`, definition.Timer.TimeDuration.Text())
	assert.True(t, definition.Timer.TimeDuration.IsExpression())
	assert.Equal(t, "R3/PT10S", definition.Timer.TimeCycle.Text())
	assert.False(t, definition.Timer.TimeCycle.IsExpression())
	assert.Equal(t, "R3/PT10S", definition.Timer.TimeCycle.XMLText, "a literal is still read as it stands in the file")
}

func TestTimerDefinitionsWalksEveryElementWhichCarriesATimer(t *testing.T) {
	for fixture, want := range map[string][]struct {
		elementID         string
		processLevelStart bool
	}{
		"../../test-cases/timer_expression/timer-expression-catch-event.bpmn":            {{"timer-catch-event", false}},
		"../../test-cases/timer_expression/timer-expression-boundary-cycle.bpmn":         {{"timer-boundary-event", false}},
		"../../test-cases/timer_expression/timer-expression-event-based-gateway.bpmn":    {{"timer1", false}},
		"../../test-cases/timer_expression/timer-expression-event-subprocess-cycle.bpmn": {{"eventSubprocessCycleStart", false}},
		"../../test-cases/timer_expression/timer-expression-process-start.bpmn":          {{"timerStartEvent_1234", true}},
		// event subprocesses nested three deep, in the order of their ids
		"../../test-cases/timer_event_subprocess/timer-event-subprocess-nested-interrupting.bpmn": {
			{"eventSubProcessL2TimerEvent_075kpin", false},
			{"eventSubProcessL3TimerEvent_0zi70w1", false},
			{"eventSubprocessL1TimerEvent_12i3m6f", false},
		},
	} {
		t.Run(fixture, func(t *testing.T) {
			xmlData, err := os.ReadFile(fixture)
			require.NoError(t, err)
			var definitions TDefinitions
			require.NoError(t, xml.Unmarshal(xmlData, &definitions))

			refs := definitions.TimerDefinitions()
			require.Len(t, refs, len(want))
			for i, ref := range refs {
				assert.Equal(t, want[i].elementID, ref.ElementID)
				assert.Equal(t, want[i].processLevelStart, ref.ProcessLevelStart, ref.ElementID)
			}
		})
	}
}
