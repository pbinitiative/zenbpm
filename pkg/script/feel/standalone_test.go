package feel

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateExpressionChecksSyntaxOnly(t *testing.T) {
	for _, expression := range []string{
		`dueAt`,
		`date and time(sentAt) + duration("P3D")`,
		`if priority < 5 then early else late`,
		// a name of several words parses as one name without its variable
		`date and time(sent at) + duration(grace period)`,
		`now() + duration("PT1H")`,
	} {
		assert.NoError(t, ValidateExpression(expression), expression)
	}
	for _, expression := range []string{`dueAt +`, `date and time(`, `1 2`} {
		assert.Error(t, ValidateExpression(expression), expression)
	}
}

// Which words are a name depends on the variables in FEEL, and a check of
// syntax has none. A name which holds a FEEL keyword therefore evaluates where
// its variable exists but does not parse on its own. This pins the limit, so
// that a change of the FEEL runtime which lifts it is noticed.
func TestValidateExpressionDoesNotParseNamesWhichHoldKeywords(t *testing.T) {
	for _, name := range []string{`valid for`, `a then b`, `return date`} {
		assert.Error(t, ValidateExpression(name), name)

		result, err := Evaluate(name, map[string]any{name: "PT1H"})
		require.NoError(t, err, name)
		assert.Equal(t, "PT1H", result, name)
	}
}

// An expression which runs on and on is stopped, so that it does not hold the
// runner and whatever waits for it, and the runner serves the next expression.
func TestEvaluateStopsAnExpressionWhichTakesTooLong(t *testing.T) {
	standaloneEvaluationLimit = 200 * time.Millisecond
	t.Cleanup(func() { standaloneEvaluationLimit = StandaloneEvaluationLimit })

	started := time.Now()
	_, err := Evaluate(`count(for i in 1..1000000000 return i)`, map[string]any{})
	require.ErrorIs(t, err, ErrEvaluationTookTooLong)
	assert.Less(t, time.Since(started), 30*time.Second, "the evaluation was interrupted rather than left to finish")

	result, err := Evaluate(`1 + 1`, map[string]any{})
	require.NoError(t, err, "the next expression is evaluated")
	assert.EqualValues(t, 2, result)

	// a check of syntax has a runner of its own and does not wait for an evaluation
	evaluating := make(chan struct{})
	evaluated := make(chan error, 1)
	standaloneEvaluator.mu.Lock()
	go func() {
		close(evaluating)
		_, err := Evaluate(`1 + 1`, map[string]any{})
		evaluated <- err
	}()
	<-evaluating
	assert.NoError(t, ValidateExpression(`dueAt`), "validation goes on while the evaluator is taken")
	standaloneEvaluator.mu.Unlock()
	require.NoError(t, <-evaluated)
}

// An interrupt ends a FEEL loop but not the match of a regular expression,
// which runs outside of the interpreter. Evaluate returns at the limit all the
// same, so that the caller does not wait for a pattern which backtracks, and
// once the match has ended the runner serves expressions again. That the calls
// in between are refused is pinned by
// TestStandaloneRunnerWaitsAtMostTheLimitForWorkItGaveUp, whose work ends when
// the test says so: when a real match ends is up to the machine.
func TestEvaluateReturnsAtTheLimitWhenTheExpressionCannotBeInterrupted(t *testing.T) {
	standaloneEvaluationLimit = 20 * time.Millisecond
	t.Cleanup(func() { standaloneEvaluationLimit = StandaloneEvaluationLimit })

	// The time of this match doubles with every "a": a quarter of a second at
	// this length, ten times the limit, and seconds under the race detector.
	backtracking := `matches("` + strings.Repeat("a", 22) + `!", "^(?=a)(a+)+$")`

	started := time.Now()
	_, err := Evaluate(backtracking, map[string]any{})
	require.ErrorIs(t, err, ErrEvaluationTookTooLong)
	assert.Less(t, time.Since(started), 2*time.Second, "the evaluation was given up at the limit rather than waited for")

	require.Eventually(t, func() bool {
		result, err := Evaluate(`1 + 1`, map[string]any{})
		return err == nil && result == int64(2)
	}, time.Minute, 50*time.Millisecond, "once the match has ended, expressions are evaluated again")
}

// The check of syntax is held to the same limit: one nested deeply takes
// minutes to parse.
func TestValidateExpressionStopsACheckWhichTakesTooLong(t *testing.T) {
	standaloneEvaluationLimit = 100 * time.Millisecond
	t.Cleanup(func() { standaloneEvaluationLimit = StandaloneEvaluationLimit })

	started := time.Now()
	err := ValidateExpression(strings.Repeat(`{a:`, 1100))
	require.ErrorIs(t, err, ErrEvaluationTookTooLong)
	assert.ErrorContains(t, err, "took too long to check")
	assert.Less(t, time.Since(started), 10*time.Second, "the check was stopped at the limit")

	// a parser is interrupted, so the next check does not wait for it
	assert.NoError(t, ValidateExpression(`dueAt`))
}

// Work given up at the limit which ends while the next call waits for it does
// not keep that call from running. Work which does not end within the limit
// refuses that call, and the ones after it without waiting again, until it
// ends.
func TestStandaloneRunnerWaitsAtMostTheLimitForWorkItGaveUp(t *testing.T) {
	standaloneEvaluationLimit = 100 * time.Millisecond
	t.Cleanup(func() { standaloneEvaluationLimit = StandaloneEvaluationLimit })

	var runner standaloneRunner
	two := func(*FeelinRunner) (any, error) { return 2, nil }

	stuck := newUninterruptibleWork(t)
	_, err := runner.run("evaluate", stuck.work)
	require.ErrorIs(t, err, ErrEvaluationTookTooLong)

	// The next call is given a limit it cannot run out of, and the work is
	// released once that call holds the runner, which it does while it waits.
	standaloneEvaluationLimit = time.Minute
	called := make(chan error, 1)
	var result any
	go func() {
		var err error
		result, err = runner.run("evaluate", two)
		called <- err
	}()
	require.Eventually(t, func() bool {
		if runner.mu.TryLock() {
			runner.mu.Unlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond, "the next call takes the runner")
	stuck.end()
	select {
	case err := <-called:
		require.NoError(t, err, "the work given up ended while this call waited")
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the call did not go on once the work given up had ended")
	}
	assert.Equal(t, 2, result)

	standaloneEvaluationLimit = 100 * time.Millisecond
	stuck = newUninterruptibleWork(t)
	_, err = runner.run("evaluate", stuck.work)
	require.ErrorIs(t, err, ErrEvaluationTookTooLong)
	_, err = runner.run("evaluate", two)
	require.ErrorIs(t, err, ErrEarlierExpressionStillRunning)
	assert.True(t, runner.givenUpOverdue, "the call waited the limit in vain")
	_, err = runner.run("evaluate", two)
	require.ErrorIs(t, err, ErrEarlierExpressionStillRunning, "refused as long as the work given up runs")

	stuck.end()
	require.Eventually(t, func() bool {
		result, err := runner.run("evaluate", two)
		return err == nil && result == 2
	}, 5*time.Second, 10*time.Millisecond)
	assert.False(t, runner.givenUpOverdue)
}

// A panic in the work, which goja raises for an error of the Go runtime, is
// the error of the call rather than the end of the node, and the runner, whose
// state is not known after it, is replaced.
func TestStandaloneRunnerRecoversAPanic(t *testing.T) {
	var runner standaloneRunner
	_, err := runner.run("evaluate", func(*FeelinRunner) (any, error) { panic("broken runner") })
	require.Error(t, err)
	assert.ErrorContains(t, err, "broken runner")
	assert.Nil(t, runner.runner, "the runner is given up")

	result, err := runner.run("evaluate", func(*FeelinRunner) (any, error) { return 2, nil })
	require.NoError(t, err)
	assert.Equal(t, 2, result)
}

func TestEvaluateOnTheStandaloneRunner(t *testing.T) {
	result, err := Evaluate(`greeting + " " + name`, map[string]any{"greeting": "hello", "name": "timer"})
	require.NoError(t, err)
	assert.Equal(t, "hello timer", result)

	result, err = Evaluate(`missing`, map[string]any{})
	require.NoError(t, err)
	assert.Nil(t, result, "a variable which is not there evaluates to null")

	_, err = Evaluate(`1 +`, map[string]any{})
	assert.Error(t, err)

	result, err = Evaluate(`now`, map[string]any{})
	require.NoError(t, err)
	assert.Nil(t, result, "a function is no value")
}

// A FEEL date and time written without a zone is a moment in the zone of the
// server, as a literal timeDate without a zone is. The FEEL runtime takes the
// offset from the Go runtime; this pins it, as timers written either way have
// to fire at the same moment.
func TestDateAndTimeWithoutZoneIsInTheLocalZone(t *testing.T) {
	prague, err := time.LoadLocation("Europe/Prague")
	if err != nil {
		t.Skipf("no time zone database: %v", err)
	}
	original := time.Local
	time.Local = prague
	t.Cleanup(func() { time.Local = original })

	for expression, want := range map[string]time.Time{
		`date and time("2099-12-03T09:00:00")`:       time.Date(2099, 12, 3, 9, 0, 0, 0, prague),
		`date and time("2099-07-03T09:00:00")`:       time.Date(2099, 7, 3, 9, 0, 0, 0, prague),
		`date and time("2099-12-03T09:00:00+05:00")`: time.Date(2099, 12, 3, 4, 0, 0, 0, time.UTC),
		`date and time("2099-12-03T09:00:00Z")`:      time.Date(2099, 12, 3, 9, 0, 0, 0, time.UTC),
		`date("2099-12-03")`:                         time.Date(2099, 12, 3, 0, 0, 0, 0, time.UTC),
	} {
		result, err := Evaluate(expression, map[string]any{})
		require.NoError(t, err, expression)
		fields, ok := result.(map[string]any)
		require.True(t, ok, "%s evaluates to the fields of a date and time, got %T", expression, result)
		require.Equal(t, true, fields["isLuxonDateTime"], expression)
		milliseconds, ok := fields["ts"].(int64)
		require.True(t, ok, "%s carries its moment in milliseconds, got %T", expression, fields["ts"])
		assert.True(t, want.Equal(time.UnixMilli(milliseconds)), "%s: got %s, want %s", expression, time.UnixMilli(milliseconds).UTC(), want.UTC())
	}
}

// uninterruptibleWork is work for a standalone runner which pays no heed to an
// interrupt, as the match of a regular expression does, and which ends when
// the test ends it, or with the test, so that no work outlives a failed
// assertion.
type uninterruptibleWork struct {
	release chan struct{}
	once    sync.Once
}

func newUninterruptibleWork(t *testing.T) *uninterruptibleWork {
	t.Helper()
	work := &uninterruptibleWork{release: make(chan struct{})}
	t.Cleanup(work.end)
	return work
}

func (w *uninterruptibleWork) work(*FeelinRunner) (any, error) {
	<-w.release
	return nil, nil
}

func (w *uninterruptibleWork) end() {
	w.once.Do(func() { close(w.release) })
}
