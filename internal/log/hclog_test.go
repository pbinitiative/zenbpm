package log

import (
	"github.com/stretchr/testify/assert"
	"log/slog"
	"os"
	"testing"
)

var hcLogger *HcLogger

func TestMain(m *testing.M) {
	logOptions := &slog.HandlerOptions{
		AddSource: true,
	}
	logger := slog.New(
		slog.NewTextHandler(os.Stdout, logOptions),
	)
	slog.SetDefault(logger)
	hcLogger = NewHcLog(logger, 1)

	m.Run()
}

func TestHcLogger(t *testing.T) {

	//assert.Equal(t, args, hcLogger.args)
	args := []interface{}{"arg1", "argValue1", "arg2", "argValue2"}
	attrs := []slog.Attr{
		slog.Any("arg1", "argValue1"),
		slog.Any("arg2", "argValue2"),
	}

	t.Run("default logger", func(t *testing.T) {
		assert.Empty(t, hcLogger.args)
		assert.Empty(t, hcLogger.attrs)
		assert.Equal(t, "", hcLogger.name)

		result := hcLogger.argsToAttrs(args)
		assert.ElementsMatch(t, attrs, result)
	})
	t.Run("named logger", func(t *testing.T) {
		namedLogger := hcLogger.Named("testLogger").(*HcLogger)
		assert.Empty(t, namedLogger.args)
		assert.Empty(t, namedLogger.attrs)
		assert.Equal(t, "testLogger", namedLogger.name)

		result := namedLogger.argsToAttrs(args)
		namedAttrs := []slog.Attr{
			slog.Any("logger", "testLogger"),
		}
		namedAttrs = append(namedAttrs, attrs...)
		assert.ElementsMatch(t, namedAttrs, result)

	})
	t.Run("with() logger", func(t *testing.T) {
		newArgs := []interface{}{
			"arg3", "argValue3", "arg4", "argValue4",
		}
		withAttrs := []slog.Attr{
			slog.Any("arg3", "argValue3"),
			slog.Any("arg4", "argValue4"),
		}

		withLogger := hcLogger.With(newArgs...).(*HcLogger)
		assert.ElementsMatch(t, newArgs, withLogger.args)
		assert.ElementsMatch(t, withAttrs, withLogger.attrs)
		assert.Equal(t, "", withLogger.name)

		result := withLogger.argsToAttrs(args)
		withAttrs = append(withAttrs, attrs...)
		assert.ElementsMatch(t, withAttrs, result)
	})
}
