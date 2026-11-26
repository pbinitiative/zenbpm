package bpmn

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/navsmb/datetime"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/script/feel"
)

// The kinds of value a timer event definition carries, named as their BPMN
// elements are.
const (
	timerKindDate     = "timeDate"
	timerKindDuration = "timeDuration"
	timerKindCycle    = "timeCycle"
)

// feelEvaluator evaluates a FEEL expression, written without the '=' which
// marks it in a BPMN attribute, against variables.
type feelEvaluator func(expression string, variables map[string]any) (any, error)

// timerOffset is how far a timeDuration expression puts a timer from the
// moment it is created: years, months, weeks and days, which move it by the
// calendar the way a literal duration does, and the rest, which moves it by the
// clock. A FEEL duration can name fractions of a second, which the ISO 8601
// duration of a literal cannot hold, and its units can have different signs,
// as those of duration("P1D") - duration("PT2H") have.
type timerOffset struct {
	years, months, days int
	clock               time.Duration
}

// Shift returns t moved by the offset.
func (offset timerOffset) Shift(t time.Time) time.Time {
	return t.AddDate(offset.years, offset.months, offset.days).Add(offset.clock)
}

// maxTimerCalendarUnits bounds the years, months, weeks or days of a timer
// duration: far beyond any moment a timer may be due at (see
// checkTimerMoment), yet small enough for the calendar to add up without
// wrapping around.
const maxTimerCalendarUnits = 100_000_000

// timerOffsetOf builds the offset of a duration from the amount of each of its
// units, which may be negative. Years, months, weeks and days have to be whole
// numbers, as the calendar moves by whole ones; hours and below may carry a
// fraction.
func timerOffsetOf(units map[string]float64) (timerOffset, error) {
	offset := timerOffset{}
	seconds := 0.0
	for unit, count := range units {
		switch unit {
		case "years", "quarters", "months", "weeks", "days":
			if count != math.Trunc(count) {
				return timerOffset{}, fmt.Errorf("which has %v %s: years, months, weeks and days must be whole numbers", count, unit)
			}
			if math.Abs(count) > maxTimerCalendarUnits {
				return timerOffset{}, fmt.Errorf("which has %v %s: more than a timer can wait", count, unit)
			}
			switch unit {
			case "years":
				offset.years += int(count)
			case "quarters":
				offset.months += 3 * int(count)
			case "months":
				offset.months += int(count)
			case "weeks":
				offset.days += 7 * int(count)
			case "days":
				offset.days += int(count)
			}
		case "hours":
			seconds += count * 60 * 60
		case "minutes":
			seconds += count * 60
		case "seconds":
			seconds += count
		case "milliseconds":
			seconds += count / 1000
		}
	}
	// converting a float beyond the range of int64 is not defined, so the
	// bound is checked before the conversion
	if math.IsNaN(seconds) || math.Abs(seconds) >= float64(math.MaxInt64)/float64(time.Second) {
		return timerOffset{}, errors.New("which is more than a timer can wait")
	}
	offset.clock = time.Duration(seconds * float64(time.Second))
	return offset, nil
}

// checkTimerMoment refuses a moment a timer cannot be due at. The engine and
// its API carry a moment in the years 1 to 9999; a duration or a date computed
// from variables can lie outside of them, where a literal seldom did. The year
// is looked at both in UTC and in the time zone of the engine, which is the one
// the API writes a moment in: around the turn of a year the two differ.
func checkTimerMoment(dueAt time.Time) error {
	for _, year := range []int{dueAt.UTC().Year(), dueAt.Local().Year()} {
		if year < 1 || year > 9999 {
			return fmt.Errorf("which puts the timer into the year %d: a timer is due in the years 1 to 9999", year)
		}
	}
	return nil
}

// timerVariables are the variables a timer expression of the instance is
// evaluated against.
func timerVariables(instance runtime.ProcessInstance) map[string]any {
	return instance.ProcessInstance().VariableHolder.LocalVariables()
}

// evaluateTimerExpression evaluates the FEEL expression of a timer value. A
// null result is refused here for every kind: it is what a reference to a
// variable the instance does not have evaluates to.
func evaluateTimerExpression(evaluate feelEvaluator, kind string, elementId string, info *bpmn20.TTimeInfo, variables map[string]any) (any, error) {
	expression := info.Text()
	source := strings.TrimSpace(strings.TrimPrefix(expression, "="))
	if source == "" {
		return nil, &TimerExpressionError{Kind: kind, ElementId: elementId, Expression: expression, Reason: "is empty"}
	}
	if variables == nil {
		variables = map[string]any{}
	}
	result, err := evaluate(source, variables)
	if err != nil {
		return nil, &TimerExpressionError{Kind: kind, ElementId: elementId, Expression: expression, Reason: "failed to evaluate", Err: err}
	}
	if result == nil {
		return nil, &TimerExpressionError{Kind: kind, ElementId: elementId, Expression: expression, Reason: "evaluated to null"}
	}
	return result, nil
}

// unusableTimerResult is the error of an expression which evaluated, but to
// something its kind of timer cannot take. The cause is cut like the result
// is: a parser quotes what it was given, which may be as long as a variable
// can be.
func unusableTimerResult(kind string, elementId string, info *bpmn20.TTimeInfo, result any, cause error) error {
	const maxCauseLength = 200
	reason := []rune(cause.Error())
	if len(reason) > maxCauseLength {
		reason = append(reason[:maxCauseLength], []rune("...")...)
	}
	return &TimerExpressionError{
		Kind:       kind,
		ElementId:  elementId,
		Expression: info.Text(),
		Reason:     fmt.Sprintf("evaluated to %s, %s", describeTimerResult(result), string(reason)),
	}
}

// resolveTimeDate evaluates a timeDate expression to the moment its timer is
// due.
func resolveTimeDate(evaluate feelEvaluator, elementId string, info *bpmn20.TTimeInfo, variables map[string]any) (time.Time, error) {
	result, err := evaluateTimerExpression(evaluate, timerKindDate, elementId, info, variables)
	if err != nil {
		return time.Time{}, err
	}
	dueAt, err := timeDateOfResult(result)
	if err != nil {
		return time.Time{}, unusableTimerResult(timerKindDate, elementId, info, result, err)
	}
	return dueAt, nil
}

// resolveTimeDuration evaluates a timeDuration expression to the moment its
// timer is created and the moment it is due, which is the duration the
// expression evaluated to after the former. A duration which is zero or
// negative, as the time left until a deadline which has passed is, makes the
// timer due at once, like a timeDate in the past.
//
// The moment of creation is read after the evaluation. An expression which
// measures the time left, such as deadline - now(), reads the clock while it is
// evaluated: counted from a moment before that, the timer would be due ahead
// of the deadline by the time the evaluation took.
func resolveTimeDuration(evaluate feelEvaluator, elementId string, info *bpmn20.TTimeInfo, variables map[string]any) (createdAt time.Time, dueAt time.Time, err error) {
	result, err := evaluateTimerExpression(evaluate, timerKindDuration, elementId, info, variables)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	offset, err := timeDurationOfResult(result)
	if err != nil {
		return time.Time{}, time.Time{}, unusableTimerResult(timerKindDuration, elementId, info, result, err)
	}
	createdAt = time.Now()
	dueAt = offset.Shift(createdAt)
	if err := checkTimerMoment(dueAt); err != nil {
		return time.Time{}, time.Time{}, unusableTimerResult(timerKindDuration, elementId, info, result, err)
	}
	return createdAt, dueAt, nil
}

// resolveTimeCycle evaluates a timeCycle expression to the cycle it names: the
// text, which the timers of the cycle keep (see runtime.Timer.ResolvedCycle),
// and what it parses to.
func resolveTimeCycle(evaluate feelEvaluator, elementId string, info *bpmn20.TTimeInfo, variables map[string]any) (string, cycleSpec, error) {
	result, err := evaluateTimerExpression(evaluate, timerKindCycle, elementId, info, variables)
	if err != nil {
		return "", cycleSpec{}, err
	}
	text, spec, err := timeCycleOfResult(result)
	if err != nil {
		return "", cycleSpec{}, unusableTimerResult(timerKindCycle, elementId, info, result, err)
	}
	// the first timer of the cycle is held to the years every timer is
	if first, ok := spec.nextDueAt(time.Now(), 0, time.Time{}); ok {
		if err := checkTimerMoment(first); err != nil {
			return "", cycleSpec{}, unusableTimerResult(timerKindCycle, elementId, info, result, err)
		}
	}
	return text, spec, nil
}

// cycleWithoutOccurrence is the error of a timeCycle expression which
// evaluated to a cycle that is over, on an element whose token waits for the
// timer: no timer would ever let it go on.
func cycleWithoutOccurrence(elementId string, info *bpmn20.TTimeInfo, cycle string) error {
	return &TimerExpressionError{
		Kind:       timerKindCycle,
		ElementId:  elementId,
		Expression: info.Text(),
		Reason:     fmt.Sprintf("evaluated to %s, a cycle which has no occurrence left", describeTimerResult(cycle)),
	}
}

// timeDateOfResult reads what a timeDate expression produced: a string a
// literal timeDate could hold, or a FEEL date and time. A FEEL date counts as
// its midnight in UTC, which is how the FEEL runtime holds it; a FEEL time
// names no day and is refused.
func timeDateOfResult(result any) (time.Time, error) {
	if text, ok := result.(string); ok {
		dueAt, err := datetime.ParseLocal(strings.TrimSpace(text))
		if err != nil {
			return time.Time{}, fmt.Errorf("which is not an ISO 8601 date-time: %w", err)
		}
		return dueAt, checkTimerMoment(dueAt)
	}
	if !isFeelDateTime(result) {
		return time.Time{}, errors.New("which is neither an ISO 8601 date-time string nor a FEEL date and time")
	}
	fields := result.(map[string]any)
	if invalid := fields["invalid"]; invalid != nil {
		return time.Time{}, fmt.Errorf("which is invalid: %s", shortDescriptionOf(invalid))
	}
	// the FEEL runtime holds a time as that time on the first day of 1900
	if calendar, ok := fields["c"].(map[string]any); ok {
		year, _ := numberOf(calendar["year"])
		month, _ := numberOf(calendar["month"])
		day, _ := numberOf(calendar["day"])
		if year == 1900 && month == 1 && day == 1 {
			return time.Time{}, errors.New("which has no date, or is a moment of 1900-01-01, the day the FEEL runtime holds a time on: give a FEEL date and time of another day, or an ISO 8601 date-time string")
		}
	}
	milliseconds, ok := numberOf(fields["ts"])
	if !ok || math.IsNaN(milliseconds) || math.IsInf(milliseconds, 0) || math.Abs(milliseconds) > maxTimerEpochMilliseconds {
		return time.Time{}, fmt.Errorf("which carries its moment as %s instead of milliseconds since 1970", shortDescriptionOf(fields["ts"]))
	}
	dueAt := time.UnixMilli(int64(milliseconds))
	return dueAt, checkTimerMoment(dueAt)
}

// maxTimerEpochMilliseconds is the last moment a JavaScript date reaches, in
// the year 275760: a bound for reading the moment of a FEEL date and time into
// an int64, before checkTimerMoment looks at the year.
const maxTimerEpochMilliseconds = 8.64e15

// timeDurationOfResult reads what a timeDuration expression produced: an ISO
// 8601 duration string, or a FEEL duration. The units of either are taken with
// their signs and added up, so that the difference of two durations is what it
// amounts to.
func timeDurationOfResult(result any) (timerOffset, error) {
	if text, ok := result.(string); ok {
		units, err := durationUnitsOfText(strings.TrimSpace(text))
		if err != nil {
			return timerOffset{}, err
		}
		return timerOffsetOf(units)
	}
	if !isFeelDuration(result) {
		return timerOffset{}, errors.New("which is neither an ISO 8601 duration string nor a FEEL duration")
	}
	units, err := feelDurationUnits(result.(map[string]any))
	if err != nil {
		return timerOffset{}, fmt.Errorf("which cannot be read: %w", err)
	}
	return timerOffsetOf(units)
}

// durationAmount is the amount of one unit of a duration string.
const durationAmount = `(-?\d+(?:\.\d+)?)`

// durationText is an ISO 8601 duration as an expression may hand it over. It
// is wider than what a literal timeDuration takes: FEEL writes a duration with
// a sign on the whole ("-PT1H") or on its units ("PT-2H-0.25S", what string()
// makes of a difference), and with fractions of a second.
var durationText = regexp.MustCompile(`^([+-])?P` +
	`(?:` + durationAmount + `Y)?(?:` + durationAmount + `M)?(?:` + durationAmount + `W)?(?:` + durationAmount + `D)?` +
	`(?:T(?:` + durationAmount + `H)?(?:` + durationAmount + `M)?(?:` + durationAmount + `S)?)?$`)

// durationUnitsOfText reads a duration string into the amount of each of its
// units, for timerOffsetOf.
func durationUnitsOfText(text string) (map[string]float64, error) {
	notADuration := errors.New("which is not an ISO 8601 duration")
	match := durationText.FindStringSubmatch(text)
	if match == nil {
		return nil, notADuration
	}
	sign := 1.0
	if match[1] == "-" {
		sign = -1
	}
	units := map[string]float64{}
	for i, unit := range []string{"years", "months", "weeks", "days", "hours", "minutes", "seconds"} {
		amount := match[i+2]
		if amount == "" {
			continue
		}
		count, err := strconv.ParseFloat(amount, 64)
		if err != nil {
			// the digits are those of a number, so it is one beyond what a float holds
			return nil, fmt.Errorf("which has more %s than a timer can wait", unit)
		}
		units[unit] = sign * count
	}
	if len(units) == 0 {
		// "P" and "PT" name no unit
		return nil, notADuration
	}
	return units, nil
}

// timeCycleOfResult reads what a timeCycle expression produced: a string a
// literal timeCycle could hold, an ISO 8601 repeating interval or a cron
// expression.
func timeCycleOfResult(result any) (string, cycleSpec, error) {
	text, ok := result.(string)
	if !ok {
		return "", cycleSpec{}, errors.New("which is not a string holding an ISO 8601 repeating interval or a cron expression")
	}
	text = strings.TrimSpace(text)
	spec, err := parseCycle(text)
	if err != nil {
		return "", cycleSpec{}, fmt.Errorf("which is neither an ISO 8601 repeating interval nor a cron expression: %w", err)
	}
	return text, spec, nil
}

// cycleSpecOfFiredTimer is the cycle the next timer after a fired one follows:
// the cycle its expression evaluated to when the cycle began, or the literal
// of the model.
func cycleSpecOfFiredTimer(timerDef bpmn20.TTimerEventDefinition, firedTimer runtime.Timer) (cycleSpec, error) {
	if firedTimer.ResolvedCycle != "" {
		return parseCycle(firedTimer.ResolvedCycle)
	}
	if timerDef.TimeCycle.IsExpression() {
		// without this the expression itself would be read as a cron expression
		return cycleSpec{}, fmt.Errorf("timer %d does not carry the cycle the expression %q evaluated to", firedTimer.Key, timerDef.TimeCycle.Text())
	}
	return findCycleValue(timerDef)
}

// isFeelDateTime reports whether an expression result is a FEEL date, time or
// date and time: the FEEL runtime hands each over as the fields of the Luxon
// DateTime which holds it.
func isFeelDateTime(result any) bool {
	fields, ok := result.(map[string]any)
	return ok && fields["isLuxonDateTime"] == true
}

// numberOf reads a number the FEEL runtime handed over, which is an int64 or a
// float64 depending on its value.
func numberOf(value any) (float64, bool) {
	switch number := value.(type) {
	case int64:
		return float64(number), true
	case float64:
		return number, true
	case int:
		return float64(number), true
	}
	return 0, false
}

// describeTimerResult names an expression result in an error message, which
// becomes the message of an incident. A string, number or boolean is quoted,
// as it is what the modeller has to correct; a context or a list is named by
// its kind only, for it may hold anything the instance knows, and a FEEL date
// or duration by what it is, for its fields would say little.
func describeTimerResult(result any) string {
	const maxLength = 80
	switch value := result.(type) {
	case string:
		text := []rune(value)
		if len(text) > maxLength {
			text = append(text[:maxLength], []rune("...")...)
		}
		return fmt.Sprintf("%q", string(text))
	case bool, int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", value)
	case []any:
		return "a list"
	case map[string]any:
		switch {
		case isFeelDateTime(result):
			return "a FEEL date or time"
		case isFeelDuration(result):
			return "a FEEL duration"
		}
		return "a context"
	}
	return fmt.Sprintf("a value of an unexpected kind (%T)", result)
}

// validateTimerExpressions is the check a deployment makes of the timer values
// which are FEEL expressions: each has to parse. A literal is not looked at,
// it is read when its timer is created.
//
// The timer of the start event of the process itself is created with the
// deployment, where there are no variables, so its expression is evaluated
// here as well, against no variables: one which needs a variable evaluates to
// null and the deployment is refused, instead of storing a definition whose
// start event can never be armed.
//
// The checks run where no engine exists (see ParseProcessDefinitionIdentity),
// hence the FEEL runner of the feel package.
func validateTimerExpressions(definitions *bpmn20.TDefinitions) error {
	for _, ref := range definitions.TimerDefinitions() {
		values := []struct {
			kind string
			info *bpmn20.TTimeInfo
		}{
			{timerKindDate, ref.Definition.TimeDate},
			{timerKindDuration, ref.Definition.TimeDuration},
			{timerKindCycle, ref.Definition.TimeCycle},
		}
		for _, value := range values {
			if !value.info.IsExpression() {
				continue
			}
			if err := validateTimerExpression(ref, value.kind, value.info); err != nil {
				return fmt.Errorf("invalid timer definition of element id=%q: %w", ref.ElementID, err)
			}
		}
	}
	return nil
}

// maxTimerExpressionLength bounds a timer expression. The check of its syntax
// takes time which grows with its length and, steeply, with its nesting: an
// ordinary expression of a few thousand characters takes tenths of a second,
// one nested a thousand levels deep seconds. The check holds the FEEL runner
// every deployment checks its expressions on, so it is also stopped after
// feel.StandaloneEvaluationLimit; the length only keeps the time of the
// ordinary ones short.
const maxTimerExpressionLength = 4000

func validateTimerExpression(ref bpmn20.TimerDefinitionRef, kind string, info *bpmn20.TTimeInfo) error {
	expression := info.Text()
	source := strings.TrimSpace(strings.TrimPrefix(expression, "="))
	if source == "" {
		return &TimerExpressionError{Kind: kind, ElementId: ref.ElementID, Expression: expression, Reason: "is empty"}
	}
	if strings.HasPrefix(source, "=") {
		// The FEEL parser takes "=x" as well, and it evaluates to null at runtime.
		return &TimerExpressionError{Kind: kind, ElementId: ref.ElementID, Expression: expression, Reason: "starts with more than one '=': one marks an expression"}
	}
	if length := utf8.RuneCountInString(source); length > maxTimerExpressionLength {
		// not quoted: it is too long to read in a message
		return &TimerExpressionError{Kind: kind, ElementId: ref.ElementID, Expression: "=...",
			Reason: fmt.Sprintf("has %d characters, and a timer expression has at most %d", length, maxTimerExpressionLength)}
	}
	if err := feel.ValidateExpression(source); err != nil {
		reason := "is not a valid FEEL expression"
		if errors.Is(err, feel.ErrEvaluationTookTooLong) || errors.Is(err, feel.ErrEarlierExpressionStillRunning) {
			reason = "could not be checked; write it shorter or nested less deeply"
		}
		return &TimerExpressionError{Kind: kind, ElementId: ref.ElementID, Expression: expression, Reason: reason, Err: err}
	}
	if !ref.ProcessLevelStart {
		return nil
	}
	var err error
	switch kind {
	case timerKindDate:
		_, err = resolveTimeDate(feel.Evaluate, ref.ElementID, info, nil)
	case timerKindCycle:
		_, _, err = resolveTimeCycle(feel.Evaluate, ref.ElementID, info, nil)
	default:
		// There is nothing a duration could be measured from. A literal
		// timeDuration is refused only when its timer is created, after the
		// definition is stored, which is what happened to it before.
		return &TimerExpressionError{Kind: kind, ElementId: ref.ElementID, Expression: expression,
			Reason: "is not supported on the start event of a process, which takes a timeDate or a timeCycle"}
	}
	if err != nil {
		return fmt.Errorf("%w (the timer of the start event of a process is evaluated without variables)", err)
	}
	return nil
}
