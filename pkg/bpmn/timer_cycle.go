package bpmn

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/adhocore/gronx"
	"github.com/navsmb/datetime"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/senseyeio/duration"
)

type cycleKind int

const (
	cycleKindISO cycleKind = iota
	cycleKindCron
)

// cycleSpec is the parsed representation of a BPMN timeCycle expression.
// It supports ISO 8601 repeating intervals and Camunda-compatible cron expressions.
type cycleSpec struct {
	kind cycleKind
	// repetitions is the maximum number of fires an ISO cycle should produce.
	// A value of -1 means infinite repetitions. Cron cycles are always infinite.
	repetitions int
	// hasStart reports whether the ISO cycle expression includes/derives an explicit first occurrence.
	hasStart bool
	// start is the first occurrence time when hasStart is true.
	start time.Time
	// period is the ISO 8601 duration between occurrences.
	period duration.Duration
	// fixedPeriod is used for ISO start/end intervals, whose period is an exact wall-clock duration.
	fixedPeriod    time.Duration
	hasFixedPeriod bool
	// cronExpr is the normalized cron expression evaluated via gronx for each tick.
	// gronx natively accepts 5-, 6-, and 7-field expressions (sec? min hour dom mon dow year?)
	// and treats `?` as a synonym for `*`, so no extra preprocessing is required.
	cronExpr string
}

// parseCycle parses a BPMN timeCycle expression.
//
// Supported ISO 8601 repeating interval forms:
//
//	R[n]/<duration>
//	R[n]/<start>/<duration>
//	R[n]/<duration>/<end>
//	R[n]/<start>/<end>
//
// Supported cron expressions are delegated to gronx, which accepts common
// Camunda/Quartz-like 5-, 6-, and 7-field expressions (with `?` treated as `*`),
// including optional seconds/year fields.
func parseCycle(expr string) (cycleSpec, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return cycleSpec{}, fmt.Errorf("timeCycle expression is empty")
	}
	if strings.HasPrefix(trimmed, "R") {
		return parseISORepeatingInterval(trimmed)
	}
	return parseCronCycle(trimmed)
}

func parseISORepeatingInterval(expr string) (cycleSpec, error) {
	parts := strings.Split(expr, "/")
	if len(parts) < 2 || len(parts) > 3 {
		return cycleSpec{}, fmt.Errorf("invalid timeCycle %q: expected ISO 8601 repeating interval", expr)
	}
	if parts[0] == "" || !strings.HasPrefix(parts[0], "R") {
		return cycleSpec{}, fmt.Errorf("invalid timeCycle %q: expected ISO 8601 repeating interval starting with 'R'", expr)
	}
	repStr := strings.TrimPrefix(parts[0], "R")
	reps := -1
	if repStr != "" {
		n, err := strconv.Atoi(repStr)
		if err != nil {
			return cycleSpec{}, fmt.Errorf("invalid repetitions %q in timeCycle %q: %w", parts[0], expr, err)
		}
		if n <= 0 {
			return cycleSpec{}, fmt.Errorf("invalid repetitions %q in timeCycle %q: must be a positive integer", parts[0], expr)
		}
		reps = n
	}
	spec := cycleSpec{kind: cycleKindISO, repetitions: reps}
	interval := parts[1:]
	if len(interval) == 1 {
		d, err := parseNonZeroDuration(interval[0], expr)
		if err != nil {
			return cycleSpec{}, err
		}
		spec.period = d
		return spec, nil
	}
	first, second := interval[0], interval[1]
	firstDuration, firstDurationOK := tryParseDuration(first)
	firstTime, firstTimeOK := tryParseTime(first)
	secondDuration, secondDurationOK := tryParseDuration(second)
	secondTime, secondTimeOK := tryParseTime(second)
	switch {
	case firstTimeOK && secondDurationOK:
		if isZeroDuration(secondDuration) {
			return cycleSpec{}, fmt.Errorf("invalid period %q in timeCycle %q: must not be zero", second, expr)
		}
		spec.hasStart = true
		spec.start = firstTime
		spec.period = secondDuration
		return spec, nil
	case firstDurationOK && secondTimeOK:
		if isZeroDuration(firstDuration) {
			return cycleSpec{}, fmt.Errorf("invalid period %q in timeCycle %q: must not be zero", first, expr)
		}
		spec.hasStart = true
		shifts := 1
		if reps > 0 {
			shifts = reps - 1
		}
		spec.start = secondTime
		for i := 0; i < shifts; i++ {
			spec.start = shiftBack(spec.start, firstDuration)
		}
		spec.period = firstDuration
		return spec, nil
	case firstTimeOK && secondTimeOK:
		period := secondTime.Sub(firstTime)
		if period <= 0 {
			return cycleSpec{}, fmt.Errorf("invalid interval %q/%q in timeCycle %q: end must be after start", first, second, expr)
		}
		spec.hasStart = true
		spec.start = firstTime
		spec.fixedPeriod = period
		spec.hasFixedPeriod = true
		return spec, nil
	default:
		return cycleSpec{}, fmt.Errorf("invalid ISO 8601 repeating interval %q: expected duration, start/duration, duration/end, or start/end", expr)
	}
}

func parseCronCycle(expr string) (cycleSpec, error) {
	normalized := strings.Join(strings.Fields(expr), " ")
	if !gronx.IsValid(normalized) {
		return cycleSpec{}, fmt.Errorf("invalid timeCycle cron expression %q", expr)
	}
	return cycleSpec{kind: cycleKindCron, repetitions: -1, cronExpr: normalized}, nil
}

func parseNonZeroDuration(value string, expr string) (duration.Duration, error) {
	d, err := duration.ParseISO8601(value)
	if err != nil {
		return duration.Duration{}, fmt.Errorf("invalid period %q in timeCycle %q: %w", value, expr, err)
	}
	if isZeroDuration(d) {
		return duration.Duration{}, fmt.Errorf("invalid period %q in timeCycle %q: must not be zero", value, expr)
	}
	return d, nil
}

func tryParseDuration(value string) (duration.Duration, bool) {
	d, err := duration.ParseISO8601(value)
	return d, err == nil
}

func tryParseTime(value string) (time.Time, bool) {
	t, err := datetime.ParseLocal(value)
	return t, err == nil
}

// shiftBack returns the inverse of duration.Duration.Shift for a positive ISO duration.
func shiftBack(t time.Time, d duration.Duration) time.Time {
	t = t.Add(-durationTimePart(d))
	return t.AddDate(-d.Y, -d.M, -(d.W*7 + d.D))
}

func durationTimePart(d duration.Duration) time.Duration {
	return time.Duration(d.TH)*time.Hour + time.Duration(d.TM)*time.Minute + time.Duration(d.TS)*time.Second
}

func (c cycleSpec) shift(t time.Time) time.Time {
	if c.hasFixedPeriod {
		return t.Add(c.fixedPeriod)
	}
	return c.period.Shift(t)
}

// findCycleValue extracts and parses the timeCycle expression from a TimerEventDefinition.
func findCycleValue(timerDef bpmn20.TTimerEventDefinition) (cycleSpec, error) {
	if timerDef.TimeCycle == nil {
		return cycleSpec{}, fmt.Errorf("timerDef.TimeCycle is nil")
	}
	cycleStr := timerDef.TimeCycle.XMLText
	if len(strings.TrimSpace(cycleStr)) == 0 {
		id := "<unknown>"
		if timerDef.Id != nil {
			id = *timerDef.Id
		}
		return cycleSpec{}, newEngineErrorf("Can't find 'timeCycle' value for element with id=%s", id)
	}
	return parseCycle(cycleStr)
}

// firstCycleDueAt returns the first future occurrence for tests and timer creation without persisted history.
func (c cycleSpec) firstCycleDueAt(now time.Time) time.Time {
	due, ok := c.nextDueAt(now, 0, time.Time{})
	if !ok {
		return time.Time{}
	}
	return due
}

// nextCycleDueAt returns the next future occurrence for tests.
func (c cycleSpec) nextCycleDueAt(now time.Time) time.Time {
	due, ok := c.nextDueAt(now, 0, time.Time{})
	if !ok {
		return time.Time{}
	}
	return due
}

// nextDueAt returns the next due date strictly after now. consumedOccurrences is the number of
// occurrences already consumed or intentionally skipped. firstDueFallback is used for R[n]/duration
// cycles whose first due date is derived when the first timer is created.
func (c cycleSpec) nextDueAt(now time.Time, consumedOccurrences int, firstDueFallback time.Time) (time.Time, bool) {
	if c.kind == cycleKindCron {
		next, err := gronx.NextTickAfter(c.cronExpr, now, false)
		if err != nil || next.IsZero() {
			return time.Time{}, false
		}
		return next, true
	}
	if c.repetitions != -1 && consumedOccurrences >= c.repetitions {
		return time.Time{}, false
	}
	var due time.Time
	occurrenceIndex := consumedOccurrences
	if c.hasStart {
		due = c.start
		for i := 0; i < occurrenceIndex; i++ {
			due = c.shift(due)
		}
	} else if !firstDueFallback.IsZero() {
		due = firstDueFallback
		for i := 0; i < occurrenceIndex; i++ {
			due = c.shift(due)
		}
	} else {
		// R[n]/duration without persisted history starts relative to subscription creation.
		due = c.shift(now)
	}
	for !due.After(now) {
		occurrenceIndex++
		if c.repetitions != -1 && occurrenceIndex >= c.repetitions {
			return time.Time{}, false
		}
		due = c.shift(due)
	}
	return due, true
}

// extractTimerEventDefinition returns the sole TTimerEventDefinition from the list, if any.
//
// BPMN 2.0 technically permits multiple event definitions on a single catch event, but our
// runtime Timer model only persists ElementId — not the specific event definition that fired —
// so re-arming and renewal can't pick the right one back. To avoid silently selecting the wrong
// definition we reject ambiguous configurations up front instead of returning the first match.
func extractTimerEventDefinition(defs []bpmn20.EventDefinition) (*bpmn20.TTimerEventDefinition, error) {
	var found *bpmn20.TTimerEventDefinition
	for i := range defs {
		td, ok := defs[i].(bpmn20.TTimerEventDefinition)
		if !ok {
			continue
		}
		if found != nil {
			id := "<unknown>"
			if td.Id != nil {
				id = *td.Id
			}
			return nil, fmt.Errorf("element has multiple timer event definitions (e.g. %s); only one is supported", id)
		}
		tdCopy := td
		found = &tdCopy
	}
	return found, nil
}

func isZeroDuration(d duration.Duration) bool {
	return d.Y == 0 && d.M == 0 && d.W == 0 && d.D == 0 && d.TH == 0 && d.TM == 0 && d.TS == 0
}

// findTimerEventDefinition locates the TTimerEventDefinition for the given elementId in the
// provided process definition. It looks at top-level start events, intermediate catch events,
// boundary events, and start events of (nested) event subprocesses. Returns an error if the
// matching element carries multiple timer event definitions (see extractTimerEventDefinition).
func findTimerEventDefinition(definition *runtime.ProcessDefinition, elementId string) (*bpmn20.TTimerEventDefinition, error) {
	if definition == nil {
		return nil, nil
	}
	process := &definition.Definitions.Process
	for i := range process.StartEvents {
		se := process.StartEvents[i]
		if se.GetId() == elementId {
			td, err := extractTimerEventDefinition(se.EventDefinitions)
			if err != nil {
				return nil, fmt.Errorf("element %s: %w", elementId, err)
			}
			if td != nil {
				return td, nil
			}
		}
	}
	for i := range process.IntermediateCatchEvent {
		ice := process.IntermediateCatchEvent[i]
		if ice.GetId() == elementId {
			if td, ok := ice.EventDefinition.(bpmn20.TTimerEventDefinition); ok {
				return &td, nil
			}
		}
	}
	for i := range process.BoundaryEvent {
		be := process.BoundaryEvent[i]
		if be.GetId() == elementId {
			if td, ok := be.EventDefinition.(bpmn20.TTimerEventDefinition); ok {
				return &td, nil
			}
		}
	}
	if _, se := process.GetSubprocessAndStartEventById(elementId); se != nil {
		td, err := extractTimerEventDefinition(se.EventDefinitions)
		if err != nil {
			return nil, fmt.Errorf("element %s: %w", elementId, err)
		}
		if td != nil {
			return td, nil
		}
	}
	return nil, nil
}

// isInterruptingTimerElement reports whether the given timer should NOT be re-armed
// after firing because the element it belongs to is one-shot / interrupting.
func isInterruptingTimerElement(definition *runtime.ProcessDefinition, timer runtime.Timer) bool {
	if definition == nil {
		return false
	}
	if timer.ProcessInstanceKey == nil {
		return false
	}
	process := &definition.Definitions.Process
	if _, se := process.GetSubprocessAndStartEventById(timer.ElementId); se != nil {
		return se.IsInterrupting
	}
	for _, be := range process.BoundaryEvent {
		if be.GetId() == timer.ElementId {
			return be.CancellActivity
		}
	}
	for _, ice := range process.IntermediateCatchEvent {
		if ice.GetId() == timer.ElementId {
			return true
		}
	}
	for _, se := range process.StartEvents {
		if se.GetId() == timer.ElementId {
			return false
		}
	}
	// Safe default: an element we cannot classify is treated as one-shot so we never
	// accidentally re-arm a timer for an unknown / unsupported element.
	return true
}

type cycleTimerStats struct {
	triggeredCount int
	firstDue       time.Time
	createdExists  bool
}

// cycleTimerStats computes the persisted view of a cycle timer's life-cycle for the given element.
//
// inFlightTriggeredTimerKey, if non-nil, identifies a timer that has been transitioned to
// TimerStateTriggered on an open (not yet flushed) batch and is therefore still reported as
// Created by the persistence layer. The stats compensate for that by counting it as Triggered
// and not as Created, so renewal logic that runs in the same batch sees a consistent view.
func (engine *Engine) cycleTimerStats(ctx context.Context, processDefinitionKey int64, processInstanceKey *int64, elementId string, inFlightTriggeredTimerKey *int64) (cycleTimerStats, error) {
	stats := cycleTimerStats{}
	accountForTimerAsTriggered := func(t runtime.Timer) {
		stats.triggeredCount++
		if stats.firstDue.IsZero() || t.DueAt.Before(stats.firstDue) {
			stats.firstDue = t.DueAt
		}
	}
	triggered, err := engine.findCycleTimersByState(ctx, processDefinitionKey, processInstanceKey, runtime.TimerStateTriggered)
	if err != nil {
		return stats, fmt.Errorf("failed to load triggered timers for element %s: %w", elementId, err)
	}
	for _, t := range triggered {
		if t.ElementId != elementId {
			continue
		}
		accountForTimerAsTriggered(t)
	}
	created, err := engine.findCycleTimersByState(ctx, processDefinitionKey, processInstanceKey, runtime.TimerStateCreated)
	if err != nil {
		return stats, fmt.Errorf("failed to load created timers for element %s: %w", elementId, err)
	}
	for _, t := range created {
		if t.ElementId != elementId {
			continue
		}
		// The just-consumed timer is still persisted as Created (its Triggered update lives in the
		// open batch). Treat it as Triggered here so renewal sees the same view it will after flush.
		if inFlightTriggeredTimerKey != nil && t.Key == *inFlightTriggeredTimerKey {
			accountForTimerAsTriggered(t)
			continue
		}
		stats.createdExists = true
		if stats.firstDue.IsZero() || t.DueAt.Before(stats.firstDue) {
			stats.firstDue = t.DueAt
		}
	}
	return stats, nil
}

func (engine *Engine) findCycleTimersByState(ctx context.Context, processDefinitionKey int64, processInstanceKey *int64, state runtime.TimerState) ([]runtime.Timer, error) {
	if processInstanceKey != nil {
		return engine.persistence.FindProcessInstanceTimers(ctx, *processInstanceKey, state)
	}
	timers, err := engine.persistence.FindProcessDefinitionTimers(ctx, processDefinitionKey, state)
	if err != nil {
		return nil, err
	}
	// Allocate a fresh slice: the storage.Storage contract does not guarantee that the returned slice
	// is exclusively owned by the caller, so we must not truncate/append into its backing array.
	filtered := make([]runtime.Timer, 0, len(timers))
	for _, t := range timers {
		if t.ProcessInstanceKey == nil {
			filtered = append(filtered, t)
		}
	}
	return filtered, nil
}

// createCycleStartTimer builds the next runtime.Timer for a timer start event whose
// timer event definition uses timeCycle. It returns nil if a Created timer already
// exists for this element (don't double-arm) or if all repetitions of a finite cycle
// have already been consumed.
//
// inFlightTriggeredTimerKey may be set during renewal to identify a timer whose Triggered
// transition lives in the open batch and is therefore still persisted as Created — see
// cycleTimerStats for details.
func (engine *Engine) createCycleStartTimer(
	ctx context.Context,
	processDefinitionKey int64,
	processInstanceKey *int64,
	elementId string,
	timerDef bpmn20.TTimerEventDefinition,
	inFlightTriggeredTimerKey *int64,
) (*runtime.Timer, error) {
	spec, err := findCycleValue(timerDef)
	if err != nil {
		return nil, err
	}
	stats, err := engine.cycleTimerStats(ctx, processDefinitionKey, processInstanceKey, elementId, inFlightTriggeredTimerKey)
	if err != nil {
		return nil, err
	}
	if stats.createdExists {
		return nil, nil
	}
	now := time.Now()
	dueAt, ok := spec.nextDueAt(now, stats.triggeredCount, stats.firstDue)
	if !ok {
		return nil, nil
	}
	return &runtime.Timer{
		ElementId:            elementId,
		Key:                  engine.generateKey(),
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: processDefinitionKey,
		ProcessInstanceKey:   processInstanceKey,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            now,
		DueAt:                dueAt,
		Duration:             dueAt.Sub(now),
		Token:                nil,
	}, nil
}

// scheduleNextCycleTimer schedules the next iteration for a fired timeCycle timer on the
// open EngineBatch. It is a no-op when the timer's element is interrupting or the cycle
// has been exhausted.
func (engine *Engine) scheduleNextCycleTimer(
	ctx context.Context,
	batch *EngineBatch,
	definition *runtime.ProcessDefinition,
	triggeredTimer runtime.Timer,
) error {
	nextTimer, err := engine.buildNextCycleTimer(ctx, definition, triggeredTimer)
	if err != nil || nextTimer == nil {
		return err
	}
	return engine.persistNextCycleTimer(ctx, batch, triggeredTimer.ElementId, *nextTimer)
}

// scheduleNextBoundaryCycleTimer schedules the next iteration for a fired timeCycle boundary
// timer on the open EngineBatch. The boundary listener (and its timer definition / interrupting
// flag) is resolved by the caller, avoiding a redundant process-tree lookup by ElementId.
// It is a no-op when the boundary event is interrupting or all repetitions have been consumed.
func (engine *Engine) scheduleNextBoundaryCycleTimer(
	ctx context.Context,
	batch *EngineBatch,
	listener *bpmn20.TBoundaryEvent,
	timerDef bpmn20.TTimerEventDefinition,
	triggeredTimer runtime.Timer,
) error {
	if listener.CancellActivity || timerDef.TimeCycle == nil {
		return nil
	}
	nextTimer, err := engine.buildNextCycleTimerFromSpec(ctx, timerDef, triggeredTimer)
	if err != nil || nextTimer == nil {
		return err
	}
	return engine.persistNextCycleTimer(ctx, batch, triggeredTimer.ElementId, *nextTimer)
}

// persistNextCycleTimer saves the next cycle timer to the batch and registers it for firing.
func (engine *Engine) persistNextCycleTimer(ctx context.Context, batch *EngineBatch, elementId string, timer runtime.Timer) error {
	if err := batch.SaveTimer(ctx, timer); err != nil {
		return fmt.Errorf("failed to save next cycle timer for element %s: %w", elementId, err)
	}
	batch.AddPostFlushAction(ctx, func() {
		engine.timerManager.registerTimer(timer)
	})
	return nil
}

// buildNextCycleTimer computes the next runtime.Timer for a fired timeCycle timer.
// Returns (nil, nil) when the timer should not be re-armed (interrupting element,
// exhausted repetitions, or not a cycle timer).
//
// This variant infers the timer event definition and interrupting-ness from the process
// definition using the persisted timer's ElementId, which works for start, intermediate-catch,
// event-subprocess and (now that boundary timers persist the boundary event id) boundary timer
// events. Boundary timers prefer scheduleNextBoundaryCycleTimer because the caller already has
// the resolved listener in scope.
func (engine *Engine) buildNextCycleTimer(
	ctx context.Context,
	definition *runtime.ProcessDefinition,
	triggeredTimer runtime.Timer,
) (*runtime.Timer, error) {
	timerDef, err := findTimerEventDefinition(definition, triggeredTimer.ElementId)
	if err != nil {
		return nil, err
	}
	if timerDef == nil || timerDef.TimeCycle == nil {
		return nil, nil
	}
	if isInterruptingTimerElement(definition, triggeredTimer) {
		return nil, nil
	}
	return engine.buildNextCycleTimerFromSpec(ctx, *timerDef, triggeredTimer)
}

// buildNextCycleTimerFromSpec is the common core of cycle timer re-arming: it consults
// the persisted cycle history (via cycleTimerStats), computes the next due date according
// to the given timer event definition, and returns the next runtime.Timer to schedule.
// Returns (nil, nil) when the cycle has been exhausted.
func (engine *Engine) buildNextCycleTimerFromSpec(
	ctx context.Context,
	timerDef bpmn20.TTimerEventDefinition,
	triggeredTimer runtime.Timer,
) (*runtime.Timer, error) {
	spec, err := findCycleValue(timerDef)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timeCycle for element %s: %w", triggeredTimer.ElementId, err)
	}
	stats, err := engine.cycleTimerStats(ctx, triggeredTimer.ProcessDefinitionKey, triggeredTimer.ProcessInstanceKey, triggeredTimer.ElementId, nil)
	if err != nil {
		return nil, err
	}
	consumed := stats.triggeredCount + 1 // current timer is saved on the same batch and is not visible to the stats query yet.
	firstDue := stats.firstDue
	if firstDue.IsZero() {
		firstDue = triggeredTimer.DueAt
	}
	now := time.Now()
	dueAt, ok := spec.nextDueAt(now, consumed, firstDue)
	if !ok {
		return nil, nil
	}
	return &runtime.Timer{
		ElementId:            triggeredTimer.ElementId,
		Key:                  engine.generateKey(),
		ElementInstanceKey:   triggeredTimer.ElementInstanceKey,
		ProcessDefinitionKey: triggeredTimer.ProcessDefinitionKey,
		ProcessInstanceKey:   triggeredTimer.ProcessInstanceKey,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            now,
		DueAt:                dueAt,
		Duration:             dueAt.Sub(now),
		Token:                triggeredTimer.Token,
	}, nil
}
