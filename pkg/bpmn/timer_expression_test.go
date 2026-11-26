package bpmn

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/script/feel"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeDateOfResult(t *testing.T) {
	want := time.Date(2099, 12, 3, 8, 0, 0, 0, time.UTC)

	for name, expression := range map[string]string{
		"a string with an offset":            `"2099-12-03T09:00:00+01:00"`,
		"a FEEL date and time":               `date and time("2099-12-03T09:00:00+01:00")`,
		"a FEEL date and time in UTC":        `date and time("2099-12-03T08:00:00Z")`,
		"a FEEL date and time computed":      `date and time("2099-11-30T08:00:00Z") + duration("P3D")`,
		"a string a variable holds":          `sentAt`,
		"a FEEL date and time of a variable": `date and time(sentAt)`,
	} {
		t.Run(name, func(t *testing.T) {
			dueAt, err := timeDateOfResult(feelResult(t, expression, map[string]any{"sentAt": "2099-12-03T09:00:00+01:00"}))
			require.NoError(t, err)
			assert.True(t, want.Equal(dueAt), "got %s", dueAt)
		})
	}

	t.Run("a value without a zone is read in the local zone, whether a string or a FEEL date and time", func(t *testing.T) {
		local := time.Date(2099, 12, 3, 9, 0, 0, 0, time.Local)
		for _, expression := range []string{`"2099-12-03T09:00:00"`, `date and time("2099-12-03T09:00:00")`} {
			dueAt, err := timeDateOfResult(feelResult(t, expression, nil))
			require.NoError(t, err, expression)
			assert.True(t, local.Equal(dueAt), "%s: got %s", expression, dueAt)
		}
	})

	t.Run("a FEEL date is its midnight in UTC", func(t *testing.T) {
		dueAt, err := timeDateOfResult(feelResult(t, `date("2099-12-03")`, nil))
		require.NoError(t, err)
		assert.True(t, time.Date(2099, 12, 3, 0, 0, 0, 0, time.UTC).Equal(dueAt), "got %s", dueAt)
	})

	for name, refused := range map[string]struct {
		expression string
		message    string
	}{
		"a FEEL time":                {`time("09:00:00")`, "which has no date"},
		"a moment of 1900-01-01":     {`date and time("1900-01-01T09:00:00Z")`, "or is a moment of 1900-01-01"},
		"a number":                   {`42`, "which is neither an ISO 8601 date-time string nor a FEEL date and time"},
		"a FEEL duration":            {`duration("P1D")`, "which is neither an ISO 8601 date-time string nor a FEEL date and time"},
		"a string which is no date":  {`"next week"`, "which is not an ISO 8601 date-time"},
		"a list":                     {`["2099-12-03T09:00:00Z"]`, "which is neither"},
		"a context made to look so":  {`{isLuxonDateTime: true}`, "which carries its moment as"},
		"a context with a bad stamp": {`{isLuxonDateTime: true, ts: "soon"}`, "which carries its moment as"},
	} {
		t.Run(name+" is refused", func(t *testing.T) {
			_, err := timeDateOfResult(feelResult(t, refused.expression, nil))
			assert.ErrorContains(t, err, refused.message)
		})
	}

	t.Run("a context made to look like a date is not quoted", func(t *testing.T) {
		secret := map[string]any{"iban": "CZ6508000000192000145399"}
		for name, tc := range map[string]struct {
			result  map[string]any
			message string
		}{
			"as what is invalid":  {map[string]any{"isLuxonDateTime": true, "invalid": secret}, "which is invalid: a context"},
			"in place of a stamp": {map[string]any{"isLuxonDateTime": true, "ts": []any{secret}}, "which carries its moment as a list instead"},
		} {
			_, err := timeDateOfResult(tc.result)
			require.ErrorContains(t, err, tc.message, name)
			assert.NotContains(t, err.Error(), "CZ65", name)
		}
	})
}

func TestTimeDurationOfResult(t *testing.T) {
	base := time.Date(2099, 1, 31, 12, 0, 0, 0, time.UTC)

	for name, expected := range map[string]struct {
		expression string
		want       time.Time
	}{
		"a string":                       {`"PT1H"`, base.Add(time.Hour)},
		"a string of days":               {`"P3D"`, base.AddDate(0, 0, 3)},
		"a FEEL duration":                {`duration("PT90M")`, base.Add(90 * time.Minute)},
		"a FEEL duration of days":        {`duration("P3D")`, base.AddDate(0, 0, 3)},
		"a FEEL duration of a month":     {`duration("P1M")`, base.AddDate(0, 1, 0)},
		"a FEEL duration of years":       {`duration("P1Y2M")`, base.AddDate(1, 2, 0)},
		"a FEEL duration of days, hours": {`duration("P1DT2H")`, base.AddDate(0, 0, 1).Add(2 * time.Hour)},
		"a fraction of a second":         {`duration("PT1.5S")`, base.Add(1500 * time.Millisecond)},
		"no time at all":                 {`duration("PT0S")`, base},
		"the time between two moments":   {`date and time("2099-12-03T09:00:00Z") - date and time("2099-12-03T08:59:30Z")`, base.Add(30 * time.Second)},
		"a duration a variable holds":    {`waitFor`, base.Add(15 * time.Minute)},
		// the FEEL runtime keeps the units of a difference apart, each with its sign
		"an hour less half an hour":        {`duration("PT1H") - duration("PT30M")`, base.Add(30 * time.Minute)},
		"a day less two hours":             {`duration("P1D") - duration("PT2H")`, base.AddDate(0, 0, 1).Add(-2 * time.Hour)},
		"a day and a negative hour":        {`duration("P1D") + duration("-PT1H")`, base.AddDate(0, 0, 1).Add(-time.Hour)},
		"a month less a day":               {`duration("P1M") - duration("P1D")`, base.AddDate(0, 1, -1)},
		"a negative duration":              {`duration("-PT1H")`, base.Add(-time.Hour)},
		"the time since a moment gone by":  {`date and time("2099-12-03T08:00:00Z") - date and time("2099-12-03T09:00:00Z")`, base.Add(-time.Hour)},
		"a string of weeks, days and time": {`"P1W2DT3H4M5S"`, base.AddDate(0, 0, 9).Add(3*time.Hour + 4*time.Minute + 5*time.Second)},
		// a duration string as FEEL writes it: a sign on the whole or on a unit, fractions of a second
		"a negative string":                {`"-PT1H"`, base.Add(-time.Hour)},
		"a string with a fraction":         {`"PT1.5S"`, base.Add(1500 * time.Millisecond)},
		"a string with negative units":     {`"PT-2H-0.25S"`, base.Add(-2*time.Hour - 250*time.Millisecond)},
		"a string of days and a fraction":  {`"P1DT2H0.25S"`, base.AddDate(0, 0, 1).Add(2*time.Hour + 250*time.Millisecond)},
		"a negative string, negative unit": {`"-P1DT-2H"`, base.AddDate(0, 0, -1).Add(2 * time.Hour)},
		"a duration made a string":         {`string(duration("PT1H") - duration("PT2H"))`, base.Add(-time.Hour)},
		"the time left, made a string": {`string(date and time("2099-12-03T08:00:00Z") - date and time("2099-12-03T09:00:00.250Z"))`,
			base.Add(-time.Hour - 250*time.Millisecond)},
	} {
		t.Run(name, func(t *testing.T) {
			offset, err := timeDurationOfResult(feelResult(t, expected.expression, map[string]any{"waitFor": "PT15M"}))
			require.NoError(t, err)
			assert.True(t, expected.want.Equal(offset.Shift(base)), "got %s", offset.Shift(base))
		})
	}

	t.Run("a duration moves a timer as the same literal does", func(t *testing.T) {
		// the literal is read as createDurationTimer reads it, from the model
		literalDueAt := func(t *testing.T, literal string, from time.Time) time.Time {
			t.Helper()
			value, err := findDurationValue(bpmn20.TTimerEventDefinition{TimeDuration: &bpmn20.TTimeInfo{XMLText: literal}})
			require.NoError(t, err)
			return value.Shift(from)
		}
		newYork, err := time.LoadLocation("America/New_York")
		if err != nil {
			t.Skipf("no time zone database: %v", err)
		}
		// the second 01:15 of the night daylight saving time ended in New York
		inRepeatedHour := time.Date(2011, 11, 6, 6, 15, 0, 0, time.UTC).In(newYork)
		for _, tc := range []struct{ literal, expression string }{
			{"P1M", `duration("P1M")`},
			{"P1DT2H", `duration("P1DT2H")`},
			{"PT15M", `duration("PT15M")`},
		} {
			for _, from := range []time.Time{base, inRepeatedHour} {
				offset, err := timeDurationOfResult(feelResult(t, tc.expression, nil))
				require.NoError(t, err, tc.expression)
				want := literalDueAt(t, tc.literal, from)
				assert.True(t, want.Equal(offset.Shift(from)), "%s from %s: got %s, the literal %s", tc.expression, from, offset.Shift(from), want)
			}
		}
		offset, err := timeDurationOfResult(feelResult(t, `duration("PT15M")`, nil))
		require.NoError(t, err)
		assert.True(t, inRepeatedHour.Add(15*time.Minute).Equal(offset.Shift(inRepeatedHour)),
			"a duration of the clock keeps the instant it starts from, in the repeated hour as well")
	})

	t.Run("the units of a duration are bounded before they are added up", func(t *testing.T) {
		// added in some orders, the second would be lost between the other two
		for range 20 {
			_, err := timeDurationOfResult("PT1000000000000000H-60000000000000000M1S")
			require.ErrorContains(t, err, "which is more than a timer can wait")
		}
	})

	for name, refused := range map[string]struct {
		result  any
		message string
	}{
		"a number":                                    {feelResult(t, `5`, nil), "which is neither an ISO 8601 duration string nor a FEEL duration"},
		"a string naming no unit":                     {"P", "which is not an ISO 8601 duration"},
		"a string naming no time":                     {"PT", "which is not an ISO 8601 duration"},
		"a string without its P":                      {"1H", "which is not an ISO 8601 duration"},
		"a string of half a day":                      {"P1.5D", "years, months, weeks and days must be whole numbers"},
		"a string of more digits than a number holds": {"P" + strings.Repeat("9", 400) + "D", "which has more days than a timer can wait"},
		"a FEEL date and time":                        {feelResult(t, `date and time("2099-12-03T09:00:00Z")`, nil), "which is neither"},
		"a string which is none":                      {"soon", "which is not an ISO 8601 duration"},
		"half a day": {map[string]any{"isLuxonDuration": true, "values": map[string]any{"days": 1.5}},
			"years, months, weeks and days must be whole numbers"},
		"a duration naming no unit": {map[string]any{"isLuxonDuration": true, "values": map[string]any{}},
			"FEEL duration names no amount of any unit"},
		"hours beyond what a timer waits": {map[string]any{"isLuxonDuration": true, "values": map[string]any{"hours": 1e15}},
			"which is more than a timer can wait"},
		"a string of hours beyond what a timer waits": {"PT99999999999H", "which is more than a timer can wait"},
		"years beyond what a timer waits": {map[string]any{"isLuxonDuration": true, "values": map[string]any{"years": 2e9}},
			"more than a timer can wait"},
	} {
		t.Run(name+" is refused", func(t *testing.T) {
			_, err := timeDurationOfResult(refused.result)
			assert.ErrorContains(t, err, refused.message)
		})
	}

	t.Run("a context made to look like a duration is not quoted", func(t *testing.T) {
		secret := map[string]any{"iban": "CZ6508000000192000145399"}
		for name, tc := range map[string]struct {
			result  map[string]any
			message string
		}{
			"in place of an amount": {map[string]any{"isLuxonDuration": true, "values": map[string]any{"hours": secret}},
				"FEEL duration has a context hours, which is not a number"},
			"in place of the units": {map[string]any{"isLuxonDuration": true, "values": []any{secret}},
				"FEEL duration carries its units as a list instead"},
			"as what is invalid": {map[string]any{"isLuxonDuration": true, "invalid": secret, "values": map[string]any{"hours": 1}},
				"FEEL duration is invalid: a context"},
		} {
			_, err := timeDurationOfResult(tc.result)
			require.ErrorContains(t, err, tc.message, name)
			assert.NotContains(t, err.Error(), "CZ65", name)
		}

		_, err := timeDurationOfResult(map[string]any{"isLuxonDuration": true, "invalid": map[string]any{"reason": "unparsable", "explanation": "…"}, "values": map[string]any{}})
		assert.ErrorContains(t, err, "FEEL duration is invalid: unparsable", "the reason of a duration Luxon marked invalid is quoted")
	})
}

func TestTimeCycleOfResult(t *testing.T) {
	text, spec, err := timeCycleOfResult(" R3/PT10S ")
	require.NoError(t, err)
	assert.Equal(t, "R3/PT10S", text, "the cycle is kept without the space around it")
	assert.Equal(t, 3, spec.repetitions)

	text, spec, err = timeCycleOfResult("0 0 9 * * *")
	require.NoError(t, err)
	assert.Equal(t, "0 0 9 * * *", text)
	assert.Equal(t, cycleKindCron, spec.kind)

	_, _, err = timeCycleOfResult(int64(3))
	assert.ErrorContains(t, err, "which is not a string holding an ISO 8601 repeating interval or a cron expression")
	_, _, err = timeCycleOfResult(feelResult(t, `duration("PT10S")`, nil))
	assert.ErrorContains(t, err, "which is not a string")
	_, _, err = timeCycleOfResult("R3/soon")
	assert.ErrorContains(t, err, "which is neither an ISO 8601 repeating interval nor a cron expression")
	_, _, err = timeCycleOfResult("R/PT2251799813685248H")
	assert.ErrorContains(t, err, "longer than a cycle can step by", "a period which wraps around to no time at all")
}

// The API writes a moment in the time zone of the engine, where the last hours
// of the year 9999 in UTC are in the year 10000 already: the year a timer is
// due in is held to its bounds in both.
func TestTimerMomentIsCheckedInUTCAndInTheZoneOfTheEngine(t *testing.T) {
	original := time.Local
	time.Local = time.FixedZone("east of UTC", 60*60)
	t.Cleanup(func() { time.Local = original })

	_, err := timeDateOfResult("9999-12-31T22:59:59Z")
	assert.NoError(t, err)
	_, err = timeDateOfResult("9999-12-31T23:30:00Z")
	assert.ErrorContains(t, err, "which puts the timer into the year 10000: a timer is due in the years 1 to 9999")
	assert.Error(t, checkTimerMoment(time.Date(10000, 1, 1, 0, 30, 0, 0, time.UTC)))

	time.Local = time.FixedZone("west of UTC", -8*60*60)
	assert.NoError(t, checkTimerMoment(time.Date(1, 1, 1, 8, 0, 0, 0, time.UTC)))
	assert.ErrorContains(t, checkTimerMoment(time.Date(1, 1, 1, 7, 59, 59, 0, time.UTC)), "into the year 0")
}

func TestTimerExpressionOfCatchEventIsEvaluatedAgainstTheInstanceVariables(t *testing.T) {
	for name, tc := range map[string]struct {
		timer     string
		variables map[string]any
		dueAt     func(createdAt time.Time) time.Time
	}{
		"timeDate naming a variable": {
			timer:     `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=dueAt</bpmn:timeDate>`,
			variables: map[string]any{"dueAt": "2099-11-30T14:30:00+01:00"},
			dueAt:     func(time.Time) time.Time { return time.Date(2099, 11, 30, 13, 30, 0, 0, time.UTC) },
		},
		"timeDate computed from a variable": {
			timer:     `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=date and time(sentAt) + duration("P3D")</bpmn:timeDate>`,
			variables: map[string]any{"sentAt": "2099-11-30T14:30:00+01:00"},
			dueAt:     func(time.Time) time.Time { return time.Date(2099, 12, 3, 13, 30, 0, 0, time.UTC) },
		},
		"timeDate comparing variables, written with entities": {
			timer:     `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=if priority &lt; 5 then early else late</bpmn:timeDate>`,
			variables: map[string]any{"priority": 1, "early": "2099-01-01T00:00:00Z", "late": "2099-06-01T00:00:00Z"},
			dueAt:     func(time.Time) time.Time { return time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC) },
		},
		"timeDate comparing variables, written as CDATA": {
			timer:     `<bpmn:timeDate xsi:type="bpmn:tFormalExpression"><![CDATA[=if priority < 5 then early else late]]></bpmn:timeDate>`,
			variables: map[string]any{"priority": 9, "early": "2099-01-01T00:00:00Z", "late": "2099-06-01T00:00:00Z"},
			dueAt:     func(time.Time) time.Time { return time.Date(2099, 6, 1, 0, 0, 0, 0, time.UTC) },
		},
		"timeDuration naming a variable": {
			timer:     `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=waitFor</bpmn:timeDuration>`,
			variables: map[string]any{"waitFor": "PT2H"},
			dueAt:     func(createdAt time.Time) time.Time { return createdAt.Add(2 * time.Hour) },
		},
		"timeDuration which is a FEEL duration": {
			timer:     `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=duration(waitFor) + duration("PT30M")</bpmn:timeDuration>`,
			variables: map[string]any{"waitFor": "PT2H"},
			dueAt:     func(createdAt time.Time) time.Time { return createdAt.Add(150 * time.Minute) },
		},
		"timeCycle naming a variable": {
			timer:     `<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">=cycle</bpmn:timeCycle>`,
			variables: map[string]any{"cycle": "R3/PT4H"},
			dueAt:     func(createdAt time.Time) time.Time { return createdAt.Add(4 * time.Hour) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, store := newTimerExpressionEngine(t)
			definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer, tc.timer)

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, tc.variables)
			require.NoError(t, err)

			timers, err := store.FindProcessInstanceTimers(t.Context(), instance.ProcessInstance().Key, runtime.TimerStateCreated)
			require.NoError(t, err)
			require.Len(t, timers, 1)
			assert.Equal(t, "timer-catch-event", timers[0].ElementId)
			assert.True(t, tc.dueAt(timers[0].CreatedAt).Equal(timers[0].DueAt), "due at %s, created at %s", timers[0].DueAt, timers[0].CreatedAt)
			assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)
		})
	}
}

// A variable may hold a FEEL date and time or a FEEL duration itself, as one
// which an output mapping filled with the result of an expression does. Named
// by a timer expression it is the value it was when it was stored.
func TestTimerExpressionReadsAFeelValueWhichAVariableHolds(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)

	date := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn")
	instance, err := engine.CreateInstanceByKey(t.Context(), date.Key,
		map[string]any{"dueAt": feelResult(t, `date and time("2099-11-30T14:30:00+01:00")`, nil)})
	require.NoError(t, err)
	timer := requireCreatedTimer(t, store, instance.ProcessInstance().Key, "timer-catch-event")
	assert.True(t, time.Date(2099, 11, 30, 13, 30, 0, 0, time.UTC).Equal(timer.DueAt), "due at %s", timer.DueAt)

	duration := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer,
		`<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=dueAt</bpmn:timeDuration>`)
	instance, err = engine.CreateInstanceByKey(t.Context(), duration.Key,
		map[string]any{"dueAt": feelResult(t, `duration("P1D") - duration("PT2H")`, nil)})
	require.NoError(t, err)
	timer = requireCreatedTimer(t, store, instance.ProcessInstance().Key, "timer-catch-event")
	assert.True(t, timer.CreatedAt.AddDate(0, 0, 1).Add(-2*time.Hour).Equal(timer.DueAt), "due at %s, created at %s", timer.DueAt, timer.CreatedAt)
}

func TestTimerLiteralOfCatchEventIsReadAsBefore(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn",
		catchEventTimer, `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">2099-11-30T14:30:00+01:00</bpmn:timeDate>`)

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)

	timers, err := store.FindProcessInstanceTimers(t.Context(), instance.ProcessInstance().Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	require.Len(t, timers, 1)
	assert.True(t, time.Date(2099, 11, 30, 13, 30, 0, 0, time.UTC).Equal(timers[0].DueAt))
	assert.Empty(t, timers[0].ResolvedCycle)
}

func TestTimerExpressionWhichDoesNotEvaluateIsAnIncidentToResolve(t *testing.T) {
	for name, tc := range map[string]struct {
		variables map[string]any
		message   string
	}{
		"the variable is missing": {
			variables: nil,
			message:   `timeDate expression "=dueAt" of element timer-catch-event evaluated to null`,
		},
		"the variable holds no date": {
			variables: map[string]any{"dueAt": 42},
			message:   `timeDate expression "=dueAt" of element timer-catch-event evaluated to 42, which is neither an ISO 8601 date-time string nor a FEEL date and time`,
		},
		"the variable holds a string which is no date": {
			variables: map[string]any{"dueAt": "next week"},
			message:   `timeDate expression "=dueAt" of element timer-catch-event evaluated to "next week", which is not an ISO 8601 date-time`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, store := newTimerExpressionEngine(t)
			definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn")

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, tc.variables)
			require.ErrorContains(t, err, tc.message)
			assert.NotContains(t, err.Error(), "<nil>")
			processInstanceKey := instance.ProcessInstance().Key

			incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
			require.NoError(t, err)
			require.Len(t, incidents, 1)
			assert.Contains(t, incidents[0].Message, tc.message)
			assert.Equal(t, "timer-catch-event", incidents[0].ElementId)
			assert.Nil(t, incidents[0].ResolvedAt)
			timers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
			require.NoError(t, err)
			assert.Empty(t, timers)

			store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("dueAt", "2099-11-30T14:30:00+01:00")
			require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))

			require.Eventually(t, func() bool {
				timers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
				return err == nil && len(timers) == 1 && time.Date(2099, 11, 30, 13, 30, 0, 0, time.UTC).Equal(timers[0].DueAt)
			}, 5*time.Second, 20*time.Millisecond, "resolving the incident evaluates the expression again and creates the timer")
			waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateActive)
			incident, err := store.FindIncidentByKey(t.Context(), incidents[0].Key)
			require.NoError(t, err)
			assert.NotNil(t, incident.ResolvedAt)
		})
	}
}

// A duration which amounts to nothing or less, as the time left until a
// deadline which has passed does, makes the timer due at once, like a timeDate
// in the past: the instance goes on instead of stopping at an incident which
// only a change of its variables could resolve.
func TestTimeDurationExpressionWhichIsNegativeMakesTheTimerDueAtOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		timer     string
		variables map[string]any
	}{
		"the time left until a deadline gone by": {
			timer:     `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=date and time(deadline) - now()</bpmn:timeDuration>`,
			variables: map[string]any{"deadline": "2020-01-01T00:00:00Z"},
		},
		"a negative FEEL duration": {
			timer:     `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=duration("-PT1H")</bpmn:timeDuration>`,
			variables: nil,
		},
		"no time at all": {
			timer:     `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=duration("PT0S")</bpmn:timeDuration>`,
			variables: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, store := newTimerExpressionEngine(t)
			definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer, tc.timer)

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, tc.variables)
			require.NoError(t, err)
			processInstanceKey := instance.ProcessInstance().Key

			timer := requireCreatedTimer(t, store, processInstanceKey, "timer-catch-event")
			assert.False(t, timer.DueAt.After(timer.CreatedAt), "due at %s, created at %s", timer.DueAt, timer.CreatedAt)
			incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
			require.NoError(t, err)
			assert.Empty(t, incidents)

			fireTimer(t, engine, timer)
			waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateCompleted)
		})
	}
}

// An expression which measures the time left reads the clock while it is
// evaluated. The timer counts from a moment after that, so it is due at the
// deadline or a moment later, never ahead of it.
func TestTimeDurationExpressionOfTheTimeLeftIsNotDueAheadOfItsDeadline(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer,
		`<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=date and time(deadline) - now()</bpmn:timeDuration>`)

	for range 20 {
		deadline := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"deadline": deadline.UTC().Format("2006-01-02T15:04:05.000Z")})
		require.NoError(t, err)

		timer := requireCreatedTimer(t, store, instance.ProcessInstance().Key, "timer-catch-event")
		assert.False(t, timer.DueAt.Before(deadline), "due at %s, ahead of the deadline %s", timer.DueAt, deadline)
		assert.Less(t, timer.DueAt.Sub(deadline), 10*time.Second, "and no later than the evaluation took")
		assert.Equal(t, timer.DueAt.Sub(timer.CreatedAt), timer.Duration)
	}
}

func TestTimeDurationExpressionWithUnitsOfDifferentSignsCreatesItsTimer(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer,
		`<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=duration(limit) - duration("PT30M")</bpmn:timeDuration>`)

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"limit": "PT1H"})
	require.NoError(t, err)

	timer := requireCreatedTimer(t, store, instance.ProcessInstance().Key, "timer-catch-event")
	assert.Equal(t, 30*time.Minute, timer.DueAt.Sub(timer.CreatedAt), "an hour less half an hour is half an hour")
}

// A moment computed from variables can lie where the engine and its API carry
// no moment. It is refused when the timer is created, instead of a timer which
// fires at a moment that wrapped around or which the API cannot hand out.
func TestTimerExpressionWhichPutsTheTimerBeyondTheYearsATimerIsDueInIsAnIncident(t *testing.T) {
	for name, tc := range map[string]struct {
		timer     string
		variables map[string]any
		message   string
	}{
		"a duration of thousands of years": {
			timer:     `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=wait</bpmn:timeDuration>`,
			variables: map[string]any{"wait": "P9000Y"},
			message:   `timeDuration expression "=wait" of element timer-catch-event evaluated to "P9000Y", which puts the timer into the year`,
		},
		"a FEEL duration of thousands of years": {
			timer:     `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=duration(wait)</bpmn:timeDuration>`,
			variables: map[string]any{"wait": "P9000Y"},
			message:   `evaluated to a FEEL duration, which puts the timer into the year`,
		},
		"hours which do not fit": {
			timer:     `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=wait</bpmn:timeDuration>`,
			variables: map[string]any{"wait": "PT99999999999H"},
			message:   `evaluated to "PT99999999999H", which is more than a timer can wait`,
		},
		"a date thousands of years on": {
			timer:     `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=now() + duration("P9000Y")</bpmn:timeDate>`,
			variables: nil,
			message:   `a timer is due in the years 1 to 9999`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, store := newTimerExpressionEngine(t)
			definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer, tc.timer)

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, tc.variables)
			require.ErrorContains(t, err, tc.message)

			timers, err := store.FindProcessInstanceTimers(t.Context(), instance.ProcessInstance().Key, runtime.TimerStateCreated)
			require.NoError(t, err)
			assert.Empty(t, timers)
			incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
			require.NoError(t, err)
			require.Len(t, incidents, 1)
		})
	}
}

// A cycle a variable holds is held to what a timer can be: a period the clock
// can step by, and a first timer in the years a timer is due in. Anything else
// is an incident, where a timer of such a cycle would keep the engine busy
// without end or lie where the API hands out no moment.
func TestTimeCycleExpressionWhichNoTimerCanFollowIsAnIncident(t *testing.T) {
	for name, tc := range map[string]struct {
		cycle   string
		message string
	}{
		"a period of thousands of years": {
			cycle:   "R3/P9000Y",
			message: `timeCycle expression "=cycle" of element timer-catch-event evaluated to "R3/P9000Y", which puts the timer into the year`,
		},
		"a period which wraps around to no time at all": {
			cycle:   "R/PT2251799813685248H",
			message: `evaluated to "R/PT2251799813685248H", which is neither an ISO 8601 repeating interval nor a cron expression: invalid period "PT2251799813685248H" in timeCycle "R/PT2251799813685248H": longer than a cycle can step by`,
		},
		"a period which wraps around to less than none": {
			cycle:   "R2/PT99999999999H",
			message: `longer than a cycle can step by`,
		},
		"a cycle which is over": {
			cycle:   "R1/2020-01-01T00:00:00Z/PT1H",
			message: `timeCycle expression "=cycle" of element timer-catch-event evaluated to "R1/2020-01-01T00:00:00Z/PT1H", a cycle which has no occurrence left`,
		},
		"a cycle which ended": {
			cycle:   "R3/PT1H/2020-01-01T00:00:00Z",
			message: `a cycle which has no occurrence left`,
		},
		"a cron expression of a year gone by": {
			cycle:   "0 0 1 1 * 2020",
			message: `evaluated to "0 0 1 1 * 2020", a cycle which has no occurrence left`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, store := newTimerExpressionEngine(t)
			definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer,
				`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">=cycle</bpmn:timeCycle>`)

			started := time.Now()
			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"cycle": tc.cycle})
			require.ErrorContains(t, err, tc.message)
			assert.Less(t, time.Since(started), 5*time.Second)
			processInstanceKey := instance.ProcessInstance().Key

			timers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
			require.NoError(t, err)
			assert.Empty(t, timers)
			incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
			require.NoError(t, err)
			require.Len(t, incidents, 1)
			assert.Contains(t, incidents[0].Message, tc.message)

			// the incident is resolved the way every other one of a timer expression is
			store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("cycle", "R1/PT1H")
			require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))
			timer := requireCreatedTimer(t, store, processInstanceKey, "timer-catch-event")
			assert.Equal(t, "R1/PT1H", timer.ResolvedCycle)
		})
	}
}

// A cycle which began long ago, or counts back from its end over many
// repetitions, is a cycle like any other: its next timer is found without
// walking through what has passed.
func TestTimeCycleExpressionWithManyPassedOccurrencesCreatesItsTimer(t *testing.T) {
	for cycle, isDue := range map[string]func(timer runtime.Timer) bool{
		// every second since 1970: the next second
		"R/1970-01-01T00:00:00Z/PT1S": func(timer runtime.Timer) bool {
			return timer.DueAt.After(timer.CreatedAt) && timer.DueAt.Sub(timer.CreatedAt) <= time.Second
		},
		// a thousand million seconds which end in 2099: the first of them
		"R1000000000/PT1S/2099-12-03T09:00:00Z": func(timer runtime.Timer) bool {
			return time.Date(2099, 12, 3, 9, 0, 0, 0, time.UTC).Add(-999_999_999 * time.Second).Equal(timer.DueAt)
		},
		// days which would begin before the year 1: the next 09:00 UTC
		"R1000000000/P1D/2099-12-03T09:00:00Z": func(timer runtime.Timer) bool {
			next := time.Date(timer.CreatedAt.UTC().Year(), timer.CreatedAt.UTC().Month(), timer.CreatedAt.UTC().Day(), 9, 0, 0, 0, time.UTC)
			if !next.After(timer.CreatedAt) {
				next = next.AddDate(0, 0, 1)
			}
			return next.Equal(timer.DueAt)
		},
	} {
		engine, store := newTimerExpressionEngine(t)
		definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer,
			`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">=cycle</bpmn:timeCycle>`)

		started := time.Now()
		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"cycle": cycle})
		require.NoError(t, err, cycle)
		assert.Less(t, time.Since(started), 2*time.Second, cycle)

		timer := requireCreatedTimer(t, store, instance.ProcessInstance().Key, "timer-catch-event")
		assert.Equal(t, cycle, timer.ResolvedCycle)
		assert.True(t, isDue(timer), "%s: due at %s, created at %s", cycle, timer.DueAt, timer.CreatedAt)
	}
}

// The token of an event-based gateway waits for the timer as that of a catch
// event does, so a cycle which is over is an incident there as well.
func TestTimeCycleExpressionOfEventBasedGatewayWhichIsOverIsAnIncident(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-event-based-gateway.bpmn",
		`<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=waitFor</bpmn:timeDuration>`,
		`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">=waitFor</bpmn:timeCycle>`)

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"waitFor": "R1/2020-01-01T00:00:00Z/PT1H"})
	require.ErrorContains(t, err, `timeCycle expression "=waitFor" of element timer1 evaluated to "R1/2020-01-01T00:00:00Z/PT1H", a cycle which has no occurrence left`)
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
}

// The token of an activity does not wait for the timer of its boundary event.
// A cycle which is over has no reminder left to give, and that is all: the
// activity goes on as it does for a literal.
func TestTimeCycleExpressionOfBoundaryEventWhichIsOverLeavesItsActivityAlone(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-boundary-cycle.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"cycle": "R1/2020-01-01T00:00:00Z/PT1H"})
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key

	timers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, timers)
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	assert.Empty(t, incidents)
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)
}

// The first timer of a cycle is refused when it is due beyond the years a
// timer is due in. A later one has no expression to fail and no token to raise
// an incident on: the cycle is over instead of a timer the API cannot hand out.
func TestTimeCycleExpressionEndsWhereItsNextTimerIsBeyondTheYearsATimerIsDueIn(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-boundary-cycle.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"cycle": "R3/P5000Y"})
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key

	first := requireCreatedTimer(t, store, processInstanceKey, "timer-boundary-event")
	assert.Equal(t, time.Now().Year()+5000, first.DueAt.Year())

	fireTimer(t, engine, first)
	created, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, created, "the second timer would be due after the year 9999")
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	assert.Empty(t, incidents)
}

// What a parser says of a value quotes the value. The message of the incident
// cuts that as it cuts the value itself, however long the variable is.
func TestTimerExpressionErrorCutsWhatAParserQuotes(t *testing.T) {
	for name, tc := range map[string]struct {
		timer string
		value string
	}{
		"a cycle":    {`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">=dueAt</bpmn:timeCycle>`, "R3/" + strings.Repeat("x", 200_000)},
		"a cron":     {`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">=dueAt</bpmn:timeCycle>`, strings.Repeat("x ", 100_000)},
		"a date":     {catchEventTimer, strings.Repeat("x", 200_000)},
		"a duration": {`<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=dueAt</bpmn:timeDuration>`, "P" + strings.Repeat("x", 200_000)},
	} {
		t.Run(name, func(t *testing.T) {
			engine, store := newTimerExpressionEngine(t)
			definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn", catchEventTimer, tc.timer)

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"dueAt": tc.value})
			require.Error(t, err)
			incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
			require.NoError(t, err)
			require.Len(t, incidents, 1)
			assert.Less(t, len(incidents[0].Message), 1000, "the message is of a length to read")
		})
	}
}

// The message of the incident quotes a result the modeller can correct, a
// string, a number or a boolean. A context or a list may hold anything the
// instance knows, so only its kind is named.
func TestTimerExpressionErrorDoesNotQuoteAContextOrAList(t *testing.T) {
	for name, tc := range map[string]struct {
		variables map[string]any
		kind      string
	}{
		"a context": {map[string]any{"dueAt": map[string]any{"iban": "CZ6508000000192000145399", "name": "Jane Doe"}}, "a context"},
		"a list":    {map[string]any{"dueAt": []any{"CZ6508000000192000145399", "Jane Doe"}}, "a list"},
	} {
		t.Run(name, func(t *testing.T) {
			engine, store := newTimerExpressionEngine(t)
			definition := deployTimerExpressionProcess(t, engine, "timer-expression-catch-event.bpmn")

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, tc.variables)
			require.ErrorContains(t, err, `timeDate expression "=dueAt" of element timer-catch-event evaluated to `+tc.kind+`, which is neither an ISO 8601 date-time string nor a FEEL date and time`)

			incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
			require.NoError(t, err)
			require.Len(t, incidents, 1)
			assert.NotContains(t, incidents[0].Message, "CZ6508000000192000145399")
			assert.NotContains(t, incidents[0].Message, "Jane Doe")
			assert.NotContains(t, incidents[0].Message, "interface")
		})
	}

	assert.Equal(t, `"`+strings.Repeat("x", 80)+`..."`, describeTimerResult(strings.Repeat("x", 200)), "a long string is cut short")
	assert.Equal(t, "true", describeTimerResult(true))
	assert.Equal(t, "1.5", describeTimerResult(1.5))
}

func TestTimerExpressionOfEventBasedGateway(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-event-based-gateway.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"waitFor": "PT45M"})
	require.NoError(t, err)

	timers, err := store.FindProcessInstanceTimers(t.Context(), instance.ProcessInstance().Key, runtime.TimerStateCreated)
	require.NoError(t, err)
	require.Len(t, timers, 1)
	assert.Equal(t, "timer1", timers[0].ElementId)
	assert.Equal(t, 45*time.Minute, timers[0].DueAt.Sub(timers[0].CreatedAt))

	failed, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.ErrorContains(t, err, `timeDuration expression "=waitFor" of element timer1 evaluated to null`)
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), failed.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
}

// The branches of an event-based gateway are subscribed to one after the
// other. A timer branch whose expression fails takes what the branches before
// it subscribed to with it, so that resolving the incident does not subscribe
// to them a second time.
func TestTimerExpressionOfEventBasedGatewayWhichFailsLeavesNoSubscriptionOfAnotherBranch(t *testing.T) {
	const timerFirst = `      <bpmn:outgoing>Flow_timer</bpmn:outgoing>
      <bpmn:outgoing>Flow_message</bpmn:outgoing>`
	const messageFirst = `      <bpmn:outgoing>Flow_message</bpmn:outgoing>
      <bpmn:outgoing>Flow_timer</bpmn:outgoing>`

	for name, order := range map[string]string{"the timer branch first": timerFirst, "the message branch first": messageFirst} {
		t.Run(name, func(t *testing.T) {
			engine, store := newTimerExpressionEngine(t)
			definition := deployTimerExpressionProcess(t, engine, "timer-expression-event-based-gateway.bpmn", timerFirst, order)

			instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
			require.ErrorContains(t, err, `timeDuration expression "=waitFor" of element timer1 evaluated to null`)
			processInstanceKey := instance.ProcessInstance().Key
			incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
			require.NoError(t, err)
			require.Len(t, incidents, 1)
			subscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), processInstanceKey, runtime.ActivityStateActive)
			require.NoError(t, err)
			assert.Empty(t, subscriptions, "the failed gateway subscribed to nothing")

			store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("waitFor", "PT45M")
			require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))
			requireCreatedTimer(t, store, processInstanceKey, "timer1")
			subscriptions, err = store.FindProcessInstanceMessageSubscriptions(t.Context(), processInstanceKey, runtime.ActivityStateActive)
			require.NoError(t, err)
			assert.Len(t, subscriptions, 1, "the message branch is subscribed to once")
		})
	}
}

// The same for the boundary events of an activity: a message boundary event
// before a timer boundary event whose expression fails is subscribed to once
// the incident is resolved, and only once.
func TestTimerExpressionOfBoundaryEventWhichFailsLeavesNoSubscriptionOfAnotherBoundaryEvent(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-boundary-after-message.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.ErrorContains(t, err, `timeDuration expression "=waitFor" of element timer-boundary-event evaluated to null`)
	processInstanceKey := instance.ProcessInstance().Key
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	subscriptions, err := store.FindProcessInstanceMessageSubscriptions(t.Context(), processInstanceKey, runtime.ActivityStateActive)
	require.NoError(t, err)
	assert.Empty(t, subscriptions, "the failed activity subscribed to nothing")

	store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("waitFor", "PT45M")
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))
	requireCreatedTimer(t, store, processInstanceKey, "timer-boundary-event")
	subscriptions, err = store.FindProcessInstanceMessageSubscriptions(t.Context(), processInstanceKey, runtime.ActivityStateActive)
	require.NoError(t, err)
	assert.Len(t, subscriptions, 1, "the message boundary event is subscribed to once")
}

// A boundary timer whose cycle is an expression evaluates it when its activity
// is entered. Every later timer of the cycle repeats what it evaluated to then,
// whatever the variable holds by the time the timer fires.
func TestTimeCycleExpressionOfBoundaryEventIsEvaluatedOnce(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-boundary-cycle.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"cycle": "R3/PT1H"})
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key

	first := requireCreatedTimer(t, store, processInstanceKey, "timer-boundary-event")
	assert.Equal(t, "R3/PT1H", first.ResolvedCycle)
	assert.Equal(t, time.Hour, first.DueAt.Sub(first.CreatedAt))

	// the variable changes after the cycle began: to another cycle, then to nothing a cycle could be
	store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("cycle", "R9/PT5M")
	fireTimer(t, engine, first)
	second := requireCreatedTimer(t, store, processInstanceKey, "timer-boundary-event")
	assert.NotEqual(t, first.Key, second.Key)
	assert.Equal(t, "R3/PT1H", second.ResolvedCycle)
	assert.True(t, first.DueAt.Add(time.Hour).Equal(second.DueAt), "the second timer is due an hour after the first, got %s after %s", second.DueAt, first.DueAt)

	store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("cycle", nil)
	fireTimer(t, engine, second)
	third := requireCreatedTimer(t, store, processInstanceKey, "timer-boundary-event")
	assert.Equal(t, "R3/PT1H", third.ResolvedCycle)
	assert.True(t, first.DueAt.Add(2*time.Hour).Equal(third.DueAt))

	fireTimer(t, engine, third)
	created, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, created, "R3 fires three times")
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	assert.Empty(t, incidents)
}

func TestTimerExpressionOfBoundaryEventWhichDoesNotEvaluateIsAnIncidentOfItsActivity(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-boundary-cycle.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"cycle": "whenever"})
	require.ErrorContains(t, err, `timeCycle expression "=cycle" of element timer-boundary-event evaluated to "whenever", which is neither an ISO 8601 repeating interval nor a cron expression`)
	processInstanceKey := instance.ProcessInstance().Key

	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, "service-task-id", incidents[0].ElementId, "the incident is on the activity the boundary event is attached to")
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), processInstanceKey)
	require.NoError(t, err)
	assert.Empty(t, jobs, "the activity did not start")

	store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("cycle", "R2/PT1H")
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))

	require.Eventually(t, func() bool {
		timers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
		if err != nil || len(timers) != 1 {
			return false
		}
		jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), processInstanceKey)
		return err == nil && len(jobs) == 1 && timers[0].ResolvedCycle == "R2/PT1H"
	}, 5*time.Second, 20*time.Millisecond, "resolving the incident starts the activity with its boundary timer")
}

// The boundary events of an activity with a start listener are subscribed once
// the listener is done. An expression which does not evaluate then is resolved
// like any other, without running the listener a second time.
func TestTimerExpressionOfBoundaryEventIsResolvedOnAnActivityWithStartListener(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-boundary-cycle.bpmn",
		`<zeebe:taskDefinition type="simple-job" />`,
		`<zeebe:taskDefinition type="simple-job" /><zeebe:executionListeners><zeebe:executionListener eventType="start" type="boundary-start-listener" /></zeebe:executionListeners>`)

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "boundary-start-listener", jobs[0].Type)

	// completing the listener lets the activity begin its own work, which
	// subscribes the boundary event
	require.NoError(t, engine.JobCompleteByKey(t.Context(), jobs[0].Key, nil))
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, "service-task-id", incidents[0].ElementId)
	assert.Contains(t, incidents[0].Message, `timeCycle expression "=cycle" of element timer-boundary-event evaluated to null`)

	store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("cycle", "R2/PT1H")
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))

	timer := requireCreatedTimer(t, store, processInstanceKey, "timer-boundary-event")
	assert.Equal(t, "R2/PT1H", timer.ResolvedCycle)
	require.Eventually(t, func() bool {
		jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), processInstanceKey)
		return err == nil && len(jobs) == 1 && jobs[0].Type == "simple-job"
	}, 5*time.Second, 20*time.Millisecond, "the activity started its own work without running its start listener again")
}

// The timer of an event subprocess start event is created with the instance.
// An expression which does not evaluate must not keep the instance from
// running: it is an incident of the start event, resolved by correcting the
// variables.
func TestTimerExpressionOfEventSubprocessWhichDoesNotEvaluateIsAnIncidentToResolve(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-event-subprocess-cycle.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
	require.NoError(t, err, "the instance is created although the timer of its event subprocess is not")
	processInstanceKey := instance.ProcessInstance().Key
	waitForProcessInstanceState(t, store, processInstanceKey, runtime.ActivityStateActive)

	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, "eventSubprocessCycleStart", incidents[0].ElementId)
	assert.Contains(t, incidents[0].Message, `timeCycle expression "=cycle" of element eventSubprocessCycleStart evaluated to null`)
	assert.True(t, strings.HasPrefix(incidents[0].Message, "failed to create timers for timer start event eventSubprocessCycleStart"),
		"the message says what failed, not how the engine sorts the failure: %s", incidents[0].Message)
	assert.Equal(t, 1, strings.Count(incidents[0].Message, "timer start event eventSubprocessCycleStart"),
		"the message names the start event once: %s", incidents[0].Message)
	timers, err := store.FindProcessInstanceTimers(t.Context(), processInstanceKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Empty(t, timers)

	// resolving before the variables are corrected changes nothing, and says so
	err = engine.ResolveIncident(t.Context(), incidents[0].Key)
	require.ErrorIs(t, err, ErrIncidentNotResolvable)
	require.ErrorContains(t, err, `timeCycle expression "=cycle" of element eventSubprocessCycleStart evaluated to null`)
	require.ErrorContains(t, err, fmt.Sprintf("correct the variables of process instance %d which the expression reads", processInstanceKey),
		"the instance whose variables to correct is named")
	stillOpen, err := store.FindIncidentByKey(t.Context(), incidents[0].Key)
	require.NoError(t, err)
	assert.Nil(t, stillOpen.ResolvedAt)
	incidents, err = store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	require.Len(t, incidents, 1, "no second incident")

	store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("cycle", "R2/PT1H")
	require.NoError(t, engine.ResolveIncident(t.Context(), incidents[0].Key))

	timer := requireCreatedTimer(t, store, processInstanceKey, "eventSubprocessCycleStart")
	assert.Equal(t, "R2/PT1H", timer.ResolvedCycle)
	assert.Nil(t, timer.Token)
	incident, err := store.FindIncidentByKey(t.Context(), incidents[0].Key)
	require.NoError(t, err)
	assert.NotNil(t, incident.ResolvedAt)
}

func TestTimeCycleExpressionOfEventSubprocessIsEvaluatedOnce(t *testing.T) {
	engine, store := newTimerExpressionEngine(t)
	definition := deployTimerExpressionProcess(t, engine, "timer-expression-event-subprocess-cycle.bpmn")

	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"cycle": "R2/PT1H"})
	require.NoError(t, err)
	processInstanceKey := instance.ProcessInstance().Key

	first := requireCreatedTimer(t, store, processInstanceKey, "eventSubprocessCycleStart")
	assert.Equal(t, "R2/PT1H", first.ResolvedCycle)

	store.ProcessInstances[processInstanceKey].ProcessInstance().VariableHolder.SetLocalVariable("cycle", "not a cycle")
	_, _, err = engine.TriggerTimer(t.Context(), first)
	require.NoError(t, err)

	var second runtime.Timer
	require.Eventually(t, func() bool {
		timers, err := store.FindProcessInstanceTimersByElement(t.Context(), processInstanceKey, "eventSubprocessCycleStart", runtime.TimerStateCreated)
		if err != nil || len(timers) != 1 || timers[0].Key == first.Key {
			return false
		}
		second = timers[0]
		return true
	}, 5*time.Second, 20*time.Millisecond, "the cycle is renewed after its first timer fired")
	assert.Equal(t, "R2/PT1H", second.ResolvedCycle)
	assert.True(t, first.DueAt.Add(time.Hour).Equal(second.DueAt))
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), processInstanceKey)
	require.NoError(t, err)
	assert.Empty(t, incidents)
}

// The timer of an event subprocess start event inside an embedded subprocess
// is created when the embedded subprocess is entered, and its expression reads
// the variables of the process.
func TestTimerExpressionOfEventSubprocessInsideEmbeddedSubprocessReadsProcessVariables(t *testing.T) {
	read := func(t *testing.T) []byte {
		data, err := os.ReadFile("./test-cases/timer_event_subprocess/timer-event-subprocess-non-interrupting-nested.bpmn")
		require.NoError(t, err)
		const literal = `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">PT1S</bpmn:timeDuration>`
		require.Equal(t, 1, strings.Count(string(data), literal), "the fixture has the one timer to replace")
		return []byte(strings.Replace(string(data), literal, `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=waitFor</bpmn:timeDuration>`, 1))
	}

	t.Run("the timer is due after the duration the variable holds", func(t *testing.T) {
		engine, store := newTimerExpressionEngine(t)
		definition, err := engine.LoadFromBytes(t.Context(), read(t), engine.generateKey())
		require.NoError(t, err)

		_, err = engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"waitFor": "PT3H"})
		require.NoError(t, err)

		var timer runtime.Timer
		require.Eventually(t, func() bool {
			for _, candidate := range store.Timers {
				if candidate.ElementId == "eventSubprocessTimerEvent_010eof4" && candidate.TimerState == runtime.TimerStateCreated {
					timer = candidate
					return true
				}
			}
			return false
		}, 5*time.Second, 20*time.Millisecond, "the event subprocess of the embedded subprocess has its timer")
		assert.Equal(t, 3*time.Hour, timer.DueAt.Sub(timer.CreatedAt))
	})

	t.Run("an expression which does not evaluate is an incident and the subprocess runs on", func(t *testing.T) {
		engine, store := newTimerExpressionEngine(t)
		definition, err := engine.LoadFromBytes(t.Context(), read(t), engine.generateKey())
		require.NoError(t, err)

		_, err = engine.CreateInstanceByKey(t.Context(), definition.Key, nil)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for _, incident := range store.Incidents {
				if incident.ElementId == "eventSubprocessTimerEvent_010eof4" &&
					strings.Contains(incident.Message, `timeDuration expression "=waitFor" of element eventSubprocessTimerEvent_010eof4 evaluated to null`) {
					return true
				}
			}
			return false
		}, 5*time.Second, 20*time.Millisecond, "the timer which was not created is an incident of its start event")
		require.Eventually(t, func() bool {
			jobs, err := store.FindActiveJobsByType(t.Context(), "subProcessJobType")
			return err == nil && len(jobs) == 1
		}, 5*time.Second, 20*time.Millisecond, "the embedded subprocess reached its task")
	})
}

// The timer of the start event of a process is created when the definition is
// deployed: there is no instance, so its expression has no variables.
func TestTimerExpressionOfProcessStartEventIsEvaluatedWithoutVariables(t *testing.T) {
	t.Run("an expression which needs none is evaluated when the definition is registered", func(t *testing.T) {
		engine, store := newTimerExpressionEngine(t)
		definition := deployTimerExpressionProcess(t, engine, "timer-expression-process-start.bpmn")
		require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), definition.Key))

		timers, err := store.FindProcessDefinitionTimers(t.Context(), definition.Key, runtime.TimerStateCreated)
		require.NoError(t, err)
		require.Len(t, timers, 1)
		assert.InDelta(t, time.Hour, timers[0].DueAt.Sub(timers[0].CreatedAt), float64(5*time.Second))
	})

	t.Run("a cycle is evaluated once and repeated by every renewal", func(t *testing.T) {
		engine, store := newTimerExpressionEngine(t)
		definition := deployTimerExpressionProcess(t, engine, "timer-expression-process-start.bpmn",
			processStartTimer, `<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">="R2/PT" + string(2) + "H"</bpmn:timeCycle>`)
		require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), definition.Key))

		timers, err := store.FindProcessDefinitionTimers(t.Context(), definition.Key, runtime.TimerStateCreated)
		require.NoError(t, err)
		require.Len(t, timers, 1)
		first := timers[0]
		assert.Equal(t, "R2/PT2H", first.ResolvedCycle)

		_, _, err = engine.TriggerTimer(t.Context(), first)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			timers, err := store.FindProcessDefinitionTimers(t.Context(), definition.Key, runtime.TimerStateCreated)
			return err == nil && len(timers) == 1 && timers[0].Key != first.Key &&
				timers[0].ResolvedCycle == "R2/PT2H" && first.DueAt.Add(2*time.Hour).Equal(timers[0].DueAt)
		}, 5*time.Second, 20*time.Millisecond, "the renewal repeats the cycle the first timer carries")
	})
}

// A timer expression is checked when its definition is deployed: by
// LoadFromBytes, and by the check the cluster makes before it allocates a key
// and version. A partition stores what the cluster allocated without checking
// it again, which could come out otherwise there, and what is stored already
// keeps loading.
func TestTimerExpressionIsADeploymentCheck(t *testing.T) {
	for name, tc := range map[string]struct {
		fixture     string
		placeholder string
		timer       string
		message     string
	}{
		"an expression which does not parse": {
			fixture:     "timer-expression-catch-event.bpmn",
			placeholder: catchEventTimer,
			timer:       `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=dueAt +</bpmn:timeDate>`,
			message:     `invalid timer definition of element id="timer-catch-event": timeDate expression "=dueAt +" of element timer-catch-event is not a valid FEEL expression`,
		},
		"an expression which is empty": {
			fixture:     "timer-expression-catch-event.bpmn",
			placeholder: catchEventTimer,
			timer:       `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">= </bpmn:timeDuration>`,
			message:     `timeDuration expression "=" of element timer-catch-event is empty`,
		},
		"an expression marked twice, which the FEEL parser takes and which evaluates to null": {
			fixture:     "timer-expression-catch-event.bpmn",
			placeholder: catchEventTimer,
			timer:       `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">==dueAt</bpmn:timeDate>`,
			message:     `timeDate expression "==dueAt" of element timer-catch-event starts with more than one '='`,
		},
		"an expression of a boundary event which does not parse": {
			fixture:     "timer-expression-boundary-cycle.bpmn",
			placeholder: `>=cycle</bpmn:timeCycle>`,
			timer:       `>=cycle(</bpmn:timeCycle>`,
			message:     `invalid timer definition of element id="timer-boundary-event": timeCycle expression "=cycle(" of element timer-boundary-event is not a valid FEEL expression`,
		},
		"an expression of an event subprocess start event which does not parse": {
			fixture:     "timer-expression-event-subprocess-cycle.bpmn",
			placeholder: `>=cycle</bpmn:timeCycle>`,
			timer:       `>=1 2</bpmn:timeCycle>`,
			message:     `invalid timer definition of element id="eventSubprocessCycleStart"`,
		},
		"a start event of the process which needs a variable": {
			fixture:     "timer-expression-process-start.bpmn",
			placeholder: processStartTimer,
			timer:       `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=startAt</bpmn:timeDate>`,
			message:     `timeDate expression "=startAt" of element timerStartEvent_1234 evaluated to null (the timer of the start event of a process is evaluated without variables)`,
		},
		"a start event of the process with a duration, which nothing could be measured from": {
			fixture:     "timer-expression-process-start.bpmn",
			placeholder: processStartTimer,
			timer:       `<bpmn:timeDuration xsi:type="bpmn:tFormalExpression">=duration("PT1H")</bpmn:timeDuration>`,
			message:     `timeDuration expression "=duration(\"PT1H\")" of element timerStartEvent_1234 is not supported on the start event of a process, which takes a timeDate or a timeCycle`,
		},
		"a start event of the process whose date lies beyond the years a timer is due in": {
			fixture:     "timer-expression-process-start.bpmn",
			placeholder: processStartTimer,
			timer:       `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=now() + duration("P9000Y")</bpmn:timeDate>`,
			message:     `evaluated to a FEEL date or time, which puts the timer into the year`,
		},
		"a start event of the process whose cycle steps beyond the years a timer is due in": {
			fixture:     "timer-expression-process-start.bpmn",
			placeholder: processStartTimer,
			timer:       `<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">="R3/P9000Y"</bpmn:timeCycle>`,
			message:     `timeCycle expression "=\"R3/P9000Y\"" of element timerStartEvent_1234 evaluated to "R3/P9000Y", which puts the timer into the year`,
		},
		"a start event of the process whose cycle has a period no clock steps by": {
			fixture:     "timer-expression-process-start.bpmn",
			placeholder: processStartTimer,
			timer:       `<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">="R/PT2251799813685248H"</bpmn:timeCycle>`,
			message:     `longer than a cycle can step by`,
		},
		"an expression longer than a timer expression may be": {
			fixture:     "timer-expression-catch-event.bpmn",
			placeholder: catchEventTimer,
			timer:       `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=dueAt` + strings.Repeat(" + duration(grace)", 250) + `</bpmn:timeDate>`,
			message:     `invalid timer definition of element id="timer-catch-event": timeDate expression "=..." of element timer-catch-event has 4505 characters, and a timer expression has at most 4000`,
		},
		"a start event of the process whose cycle is no cycle": {
			fixture:     "timer-expression-process-start.bpmn",
			placeholder: processStartTimer,
			timer:       `<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">=now()</bpmn:timeCycle>`,
			message:     `timeCycle expression "=now()" of element timerStartEvent_1234 evaluated to a FEEL date or time, which is not a string`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, _ := newTimerExpressionEngine(t)
			xml := timerExpressionProcess(t, tc.fixture, tc.placeholder, tc.timer)

			for way, deploy := range map[string]func() error{
				"LoadFromBytes": func() error {
					_, err := engine.LoadFromBytes(t.Context(), xml, engine.generateKey())
					return err
				},
				"ParseProcessDefinitionIdentity": func() error {
					_, err := ParseProcessDefinitionIdentity(xml)
					return err
				},
			} {
				assert.ErrorContains(t, deploy(), tc.message, "%s: the deployment is refused", way)
			}

			_, err := engine.DeployProcessDefinition(t.Context(), xml, engine.generateKey(), 1)
			assert.NoError(t, err, "a partition stores what the cluster checked before it allocated the version")
			_, err = engine.ImportProcessDefinition(t.Context(), xml, engine.generateKey(), 2, false)
			assert.NoError(t, err, "what is stored keeps loading")
		})
	}

	t.Run("an expression whose check takes too long is refused as one which could not be checked", func(t *testing.T) {
		// the check of an expression nested this deeply would take minutes
		xml := timerExpressionProcess(t, "timer-expression-catch-event.bpmn", catchEventTimer,
			`<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=`+strings.Repeat(`{a:`, 1100)+`</bpmn:timeDate>`)
		started := time.Now()
		_, err := ParseProcessDefinitionIdentity(xml)
		require.ErrorIs(t, err, feel.ErrEvaluationTookTooLong)
		assert.ErrorContains(t, err, `of element timer-catch-event could not be checked; write it shorter or nested less deeply: FEEL expression took too long to check`)
		assert.Less(t, time.Since(started), 2*feel.StandaloneEvaluationLimit+5*time.Second, "the check is stopped at its limit")
	})

	t.Run("an expression of an instance may need variables and name them with spaces", func(t *testing.T) {
		engine, _ := newTimerExpressionEngine(t)
		xml := timerExpressionProcess(t, "timer-expression-catch-event.bpmn",
			catchEventTimer, `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=date and time(sent at) + duration(grace period)</bpmn:timeDate>`)
		_, err := engine.LoadFromBytes(t.Context(), xml, engine.generateKey())
		assert.NoError(t, err)
	})

	t.Run("a cycle of a start event which counts back over many repetitions is read without walking through them", func(t *testing.T) {
		xml := timerExpressionProcess(t, "timer-expression-process-start.bpmn", processStartTimer,
			`<bpmn:timeCycle xsi:type="bpmn:tFormalExpression">="R1000000000/PT1S/2099-12-03T09:00:00Z"</bpmn:timeCycle>`)
		started := time.Now()
		_, err := ParseProcessDefinitionIdentity(xml)
		assert.NoError(t, err)
		assert.Less(t, time.Since(started), 2*time.Second)
	})

	t.Run("a literal is not looked at, as before", func(t *testing.T) {
		engine, _ := newTimerExpressionEngine(t)
		xml := timerExpressionProcess(t, "timer-expression-catch-event.bpmn",
			catchEventTimer, `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">no date at all</bpmn:timeDate>`)
		_, err := engine.LoadFromBytes(t.Context(), xml, engine.generateKey())
		assert.NoError(t, err)
	})
}

func TestCycleSpecOfFiredTimer(t *testing.T) {
	expression := bpmn20.TTimerEventDefinition{TimeCycle: &bpmn20.TTimeInfo{XMLText: "=cycle"}}
	literal := bpmn20.TTimerEventDefinition{TimeCycle: &bpmn20.TTimeInfo{XMLText: "R4/PT1S"}}

	spec, err := cycleSpecOfFiredTimer(expression, runtime.Timer{ResolvedCycle: "R7/PT1M"})
	require.NoError(t, err)
	assert.Equal(t, 7, spec.repetitions, "the timer carries the cycle its expression evaluated to")

	spec, err = cycleSpecOfFiredTimer(literal, runtime.Timer{})
	require.NoError(t, err)
	assert.Equal(t, 4, spec.repetitions, "a literal is read from the model")

	_, err = cycleSpecOfFiredTimer(expression, runtime.Timer{Key: 12})
	assert.ErrorContains(t, err, `timer 12 does not carry the cycle the expression "=cycle" evaluated to`)
}

const (
	// the timer values of the fixtures, which a test replaces to vary them
	catchEventTimer   = `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=dueAt</bpmn:timeDate>`
	processStartTimer = `<bpmn:timeDate xsi:type="bpmn:tFormalExpression">=now() + duration("PT1H")</bpmn:timeDate>`
)

// newTimerExpressionEngine creates an engine which is not started: no timer
// fires on its own, the tests fire the timers they mean to.
func newTimerExpressionEngine(t *testing.T) (*Engine, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	return &engine, store
}

// timerExpressionProcess reads a fixture of test-cases/timer_expression,
// replacing each old text by the new one which follows it.
func timerExpressionProcess(t *testing.T, fixture string, oldnew ...string) []byte {
	t.Helper()
	data, err := os.ReadFile("./test-cases/timer_expression/" + fixture)
	require.NoError(t, err)
	for i := 0; i < len(oldnew); i += 2 {
		require.Contains(t, string(data), oldnew[i], "the fixture holds the text to replace")
	}
	return []byte(strings.NewReplacer(oldnew...).Replace(string(data)))
}

func deployTimerExpressionProcess(t *testing.T, engine *Engine, fixture string, oldnew ...string) *runtime.ProcessDefinition {
	t.Helper()
	definition, err := engine.LoadFromBytes(t.Context(), timerExpressionProcess(t, fixture, oldnew...), engine.generateKey())
	require.NoError(t, err)
	return definition
}

// feelResult evaluates a FEEL expression to what the FEEL runtime hands over.
func feelResult(t *testing.T, expression string, variables map[string]any) any {
	t.Helper()
	if variables == nil {
		variables = map[string]any{}
	}
	result, err := feel.Evaluate(expression, variables)
	require.NoError(t, err)
	require.NotNil(t, result, "%s evaluates to a value", expression)
	return result
}

// requireCreatedTimer returns the one timer of the element which waits to
// fire.
func requireCreatedTimer(t *testing.T, store *inmemory.Storage, processInstanceKey int64, elementId string) runtime.Timer {
	t.Helper()
	var timer runtime.Timer
	require.Eventually(t, func() bool {
		timers, err := store.FindProcessInstanceTimersByElement(t.Context(), processInstanceKey, elementId, runtime.TimerStateCreated)
		if err != nil || len(timers) != 1 {
			return false
		}
		timer = timers[0]
		return true
	}, 5*time.Second, 20*time.Millisecond, "element %s has one timer waiting", elementId)
	return timer
}

// fireTimer fires a timer of a token and runs the instance on.
func fireTimer(t *testing.T, engine *Engine, timer runtime.Timer) {
	t.Helper()
	instance, tokens, err := engine.TriggerTimer(t.Context(), timer)
	require.NoError(t, err)
	require.NotNil(t, instance)
	require.NoError(t, engine.RunProcessInstance(t.Context(), *instance, tokens))
}
