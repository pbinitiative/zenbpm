package feel

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/internal/safego"
)

// standaloneRunner is a FEEL runner behind a mutex rather than a pooled
// runtime: a pool keeps a goroutine which has to be stopped, and nothing owns
// a package-level runtime to stop it. The runner is created on first use.
type standaloneRunner struct {
	mu     sync.Mutex
	runner *FeelinRunner
	// givenUp is closed once the work of the runner given up last at the
	// limit has ended; nil when that work has ended, or no runner was given
	// up. givenUpOverdue tells that a caller waited the limit for it in vain.
	givenUp        chan struct{}
	givenUpOverdue bool
}

// get returns the runner, creating it when there is none. The caller holds mu.
func (s *standaloneRunner) get() *FeelinRunner {
	if s.runner == nil {
		s.runner = newFeelRunner()
	}
	return s.runner
}

// The runners behind ValidateExpression and Evaluate. They are apart so that a
// check of syntax, which every deployment with a timer expression makes, never
// waits for an evaluation.
var (
	standaloneValidator standaloneRunner
	standaloneEvaluator standaloneRunner
)

// StandaloneEvaluationLimit is how long ValidateExpression and Evaluate let one
// expression take. An expression such as count(for i in 1..1000000000 return
// i) would otherwise hold the runner, and everything waiting for it, until it
// is done; so would the check of one nested a few thousand levels deep, which
// takes minutes.
const StandaloneEvaluationLimit = 2 * time.Second

// standaloneEvaluationLimit is StandaloneEvaluationLimit, a variable for the
// tests to shorten.
var standaloneEvaluationLimit = StandaloneEvaluationLimit

// ErrEvaluationTookTooLong tells that ValidateExpression or Evaluate gave up
// on an expression which took longer than StandaloneEvaluationLimit.
var ErrEvaluationTookTooLong = errors.New("FEEL expression took too long")

// ErrEarlierExpressionStillRunning tells that ValidateExpression or Evaluate
// refused an expression without looking at it, because the work of one given
// up before it has not ended yet (see Evaluate).
var ErrEarlierExpressionStillRunning = errors.New("a FEEL expression given up earlier for taking too long is still running")

// ValidateExpression checks the syntax of a FEEL expression without evaluating
// it. It serves the checks of a deployment, which run where no engine and so
// no FEEL runtime of an engine exists. The expression comes without the '='
// which marks it in a BPMN attribute.
//
// The expression is parsed without variables. Which words are a name depends
// on the variables in FEEL, so a variable whose name holds a FEEL keyword,
// such as "valid for", does not parse here although it evaluates in an
// instance which has it.
//
// A check which takes longer than StandaloneEvaluationLimit fails with
// ErrEvaluationTookTooLong, as an evaluation does in Evaluate.
func ValidateExpression(expression string) error {
	_, err := standaloneValidator.run("check", func(runner *FeelinRunner) (any, error) {
		return nil, (*runner.validateExpression)(expression)
	})
	return err
}

// Evaluate evaluates a FEEL expression the way FeelinRuntime.Evaluate does, on
// a runner of its own. See ValidateExpression for when to use it instead of
// the FEEL runtime of an engine. An expression which runs longer than
// StandaloneEvaluationLimit fails with ErrEvaluationTookTooLong.
//
// Such an expression is interrupted, which ends a FEEL loop. It does not end
// the match of a regular expression, which runs outside of the interpreter and
// can take very long for a pattern that backtracks: Evaluate then returns all
// the same, and leaves that evaluation to end on a runner nothing else uses
// any more. Until it does it keeps a processor busy. So that evaluations given
// up this way cannot add up, there is at most one at a time: the next call
// waits up to the limit for it to end and then fails with
// ErrEarlierExpressionStillRunning, as every call does until it has ended.
func Evaluate(expression string, variableContext map[string]any) (any, error) {
	result, err := standaloneEvaluator.run("evaluate", func(runner *FeelinRunner) (any, error) {
		return (*runner.evalFunc)(expression, variableContext)
	})
	if err != nil {
		return nil, err
	}
	return valueOf(result), nil
}

// outcome is what the work handed to run came to.
type outcome struct {
	result   any
	err      error
	panicked bool
}

// run runs work on the runner, for at most standaloneEvaluationLimit. Work
// which takes longer is interrupted and its runner given up, so that the next
// call meets neither; task names the work in the error ("evaluate", "check").
// A panic of the work is recovered into its error, and the runner, whose state
// is then unknown, is given up as well.
func (s *standaloneRunner) run(task string, work func(runner *FeelinRunner) (any, error)) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := standaloneEvaluationLimit
	if !s.awaitGivenUp(limit) {
		return nil, fmt.Errorf("%w, so this one was refused; the other one ends on its own, or with a restart of the node", ErrEarlierExpressionStillRunning)
	}
	runner := s.get()

	done := make(chan outcome, 1)
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		var result any
		panicked := true
		err := safego.Run("feel-standalone-"+task, safego.DefaultLogger, func() error {
			var err error
			result, err = work(runner)
			panicked = false
			return err
		})
		done <- outcome{result: result, err: err, panicked: panicked}
	}()
	timeout := time.NewTimer(limit)
	defer timeout.Stop()

	select {
	case finished := <-done:
		if finished.panicked {
			s.runner = nil
		}
		return finished.result, finished.err
	case <-timeout.C:
		// The runner is given up: it is still at work, or about to be
		// interrupted, and the next call must meet neither.
		s.runner = nil
		s.givenUp, s.givenUpOverdue = ended, false
		runner.vm.Interrupt(ErrEvaluationTookTooLong)
		return nil, fmt.Errorf("%w to %s: it was stopped after %s", ErrEvaluationTookTooLong, task, limit)
	}
}

// awaitGivenUp reports whether the work of the runner given up last has
// ended, waiting up to limit for it. An interrupted FEEL loop ends at once; a
// match of a regular expression may not end for hours, and once one caller
// waited the limit for it in vain, the next ones do not wait again. The caller
// holds mu.
func (s *standaloneRunner) awaitGivenUp(limit time.Duration) bool {
	if s.givenUp == nil {
		return true
	}
	select {
	case <-s.givenUp:
		s.givenUp, s.givenUpOverdue = nil, false
		return true
	default:
	}
	if s.givenUpOverdue {
		return false
	}
	wait := time.NewTimer(limit)
	defer wait.Stop()
	select {
	case <-s.givenUp:
		s.givenUp = nil
		return true
	case <-wait.C:
		s.givenUpOverdue = true
		return false
	}
}
