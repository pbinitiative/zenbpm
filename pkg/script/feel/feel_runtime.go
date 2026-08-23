package feel

import (
	_ "embed"

	"github.com/dop251/goja"
	"github.com/pbinitiative/zenbpm/pkg/script"
)

type FeelinRunnerFactory struct {
}

func (factory FeelinRunnerFactory) NewRunner() script.Runner {
	return newFeelRunner()
}

type FeelinRuntime struct {
	pool *script.RunnerPool
}

var _ script.DmnFeelRuntime = (*FeelinRuntime)(nil)

func NewFeelinRuntime(maxVmPoolSize int, minVmPoolSize int) script.FeelRuntime {
	return &FeelinRuntime{
		pool: script.NewRunnerPool(FeelinRunnerFactory{}, maxVmPoolSize, minVmPoolSize),
	}
}

func (r *FeelinRuntime) Stop() {
	r.pool.Stop()
}

func (r *FeelinRuntime) UnaryTest(expression string, variableContext map[string]any) (bool, error) {
	var runner = r.pool.GetRunnerFromPool()
	defer r.pool.ReturnRunnerToPool(runner)

	return (*runner.(*FeelinRunner).unaryTest)(expression, variableContext)
}

func (r *FeelinRuntime) UnaryTestStrict(expression string, variableContext map[string]any) (bool, error) {
	var runner = r.pool.GetRunnerFromPool()
	defer r.pool.ReturnRunnerToPool(runner)

	return (*runner.(*FeelinRunner).unaryTestStrict)(expression, variableContext)
}

func (r *FeelinRuntime) ValidateExpression(expression string) error {
	var runner = r.pool.GetRunnerFromPool()
	defer r.pool.ReturnRunnerToPool(runner)

	return (*runner.(*FeelinRunner).validateExpression)(expression)
}

func (r *FeelinRuntime) ValidateUnaryTest(expression string) error {
	var runner = r.pool.GetRunnerFromPool()
	defer r.pool.ReturnRunnerToPool(runner)

	return (*runner.(*FeelinRunner).validateUnaryTest)(expression)
}

func (r *FeelinRuntime) Evaluate(expression string, variableContext map[string]any) (any, error) {
	var runner = r.pool.GetRunnerFromPool()
	defer r.pool.ReturnRunnerToPool(runner)
	res, err := (*runner.(*FeelinRunner).evalFunc)(expression, variableContext)
	if err != nil {
		return nil, err
	}
	if _, ok := res.(func(goja.FunctionCall) goja.Value); ok {
		return nil, nil
	}
	return res, err
}

//go:embed feelin/index.esm.js
var feelinSource string

// intlPolyfill is a minimal Intl polyfill providing the bits of the Intl API that the embedded
// `feelin` (Luxon) runtime touches when evaluating date/time related FEEL expressions such as
// `string(now())`. Goja does not expose Intl; without these stubs Luxon's `SystemZone` and
// `systemLocale()` throw `ReferenceError: Intl is not defined` the first time a date-time value
// is materialised. The stubs intentionally return fixed, locale-agnostic values (en-US / UTC)
// since the FEEL date-time → string conversion only needs them to produce ISO-8601 output via
// `DateTime.toISO()` rather than locale-formatted output.
//
// Luxon symbols touched by this polyfill (revisit on every Luxon / feelin upgrade):
//   - Intl.DateTimeFormat → used by Luxon's SystemZone / DateTime.toISO formatting path
//   - Intl.NumberFormat   → used by Luxon's numbering-system detection
//   - Intl.Locale         → used by Luxon's systemLocale() resolution
//   - Intl.RelativeTimeFormat / Intl.ListFormat → touched indirectly by some Luxon helpers
const intlPolyfill = `
if (typeof Intl === 'undefined') {
  var Intl = {};
}
(function (Intl) {
  function resolved() {
    return { locale: 'en-US', timeZone: 'UTC', numberingSystem: 'latn' };
  }
  function ctor() {
    return {
      resolvedOptions: resolved,
      format: function (v) { return v === undefined ? '' : String(v); },
      formatToParts: function () { return []; },
    };
  }
  if (!Intl.DateTimeFormat) Intl.DateTimeFormat = function () { return ctor(); };
  if (!Intl.NumberFormat) Intl.NumberFormat = function () { return ctor(); };
  if (!Intl.RelativeTimeFormat) Intl.RelativeTimeFormat = function () { return ctor(); };
  if (!Intl.ListFormat) Intl.ListFormat = function () {
    return { format: function (items) { return (items || []).join(', '); } };
  };
  if (!Intl.Locale) Intl.Locale = function (tag) {
    this.locale = tag || 'en-US';
    this.baseName = this.locale;
  };
})(Intl);
`

type FeelinRunner struct {
	vm                 *goja.Runtime
	evalFunc           *func(expression string, variableContext map[string]any) (any, error)
	unaryTest          *func(expression string, variableContext map[string]any) (bool, error)
	unaryTestStrict    *func(expression string, variableContext map[string]any) (bool, error)
	validateExpression *func(expression string) error
	validateUnaryTest  *func(expression string) error
}

func (r *FeelinRunner) Runner() {}

func newFeelRunner() *FeelinRunner {
	r := FeelinRunner{vm: goja.New()}
	if _, err := r.vm.RunString(intlPolyfill); err != nil {
		panic(err)
	}
	//TODO: this can be optimized into using already compiled *Program to skip compilation step
	_, err := r.vm.RunString(feelinSource)
	if err != nil {
		panic(err)
	}
	if _, err := r.vm.RunString(feelRuntimeExtensions); err != nil {
		panic(err)
	}
	r.exportEvaluate()
	r.exportUnaryTest()
	r.exportUnaryTestStrict()
	r.exportValidateExpression()
	r.exportValidateUnaryTest()
	return &r
}

func (r *FeelinRunner) exportEvaluate() {
	var evalFunc func(expression string, variableContext map[string]any) (any, error)
	err := r.vm.ExportTo(r.vm.Get("evaluate"), &evalFunc)
	if err != nil {
		panic(err)
	}
	r.evalFunc = &evalFunc
}

func (r *FeelinRunner) exportUnaryTest() {
	var unaryTest func(expression string, variableContext map[string]any) (bool, error)
	err := r.vm.ExportTo(r.vm.Get("unaryTest"), &unaryTest)
	if err != nil {
		panic(err)
	}
	r.unaryTest = &unaryTest
}

func (r *FeelinRunner) exportUnaryTestStrict() {
	var unaryTestStrict func(expression string, variableContext map[string]any) (bool, error)
	err := r.vm.ExportTo(r.vm.Get("unaryTestStrict"), &unaryTestStrict)
	if err != nil {
		panic(err)
	}
	r.unaryTestStrict = &unaryTestStrict
}

func (r *FeelinRunner) exportValidateExpression() {
	var validateExpression func(expression string) error
	err := r.vm.ExportTo(r.vm.Get("validateExpression"), &validateExpression)
	if err != nil {
		panic(err)
	}
	r.validateExpression = &validateExpression
}

func (r *FeelinRunner) exportValidateUnaryTest() {
	var validateUnaryTest func(expression string) error
	err := r.vm.ExportTo(r.vm.Get("validateUnaryTest"), &validateUnaryTest)
	if err != nil {
		panic(err)
	}
	r.validateUnaryTest = &validateUnaryTest
}

const feelRuntimeExtensions = `
function unaryTestStrict(expression, context, dialect) {
  var missingVariables = [];
  var strictContext = new Proxy(context || {}, {
    has: function (target, name) {
      if (Reflect.has(target, name)) {
        return true;
      }
      if (typeof name === 'string' &&
          !Object.keys(target).some(function (key) {
            return normalizeContextKey(key) === normalizeContextKey(name);
          }) &&
          typeof getBuiltin(name) === 'undefined') {
        missingVariables.push(name);
      }
      return false;
    }
  });

  var result = unaryTest(expression, strictContext, dialect);
  if (missingVariables.length > 0) {
    var names = Array.from(new Set(missingVariables));
    throw new Error('unknown variable(s) in unary test: ' + names.join(', '));
  }
  return result;
}

function validateExpression(expression, dialect) {
  interpreter.evaluate(expression, {}, dialect);
}

function validateUnaryTest(expression, dialect) {
  interpreter.unaryTest(expression, {}, dialect);
}
`
